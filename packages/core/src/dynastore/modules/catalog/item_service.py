#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import asyncio
import copy
import logging
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable, FrozenSet, List, Optional, Any, Dict, Union, Sequence, Tuple

if TYPE_CHECKING:
    from dynastore.modules.storage.router import ResolvedDriver

import inspect as _inspect
from dynastore.models.driver_context import DriverContext
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
    ItemsWritePolicy,
)
from dynastore.models.ogc import Feature
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.models.protocols.items import ItemsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.tools.execution_context import in_task_run
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.db_config import shared_queries
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.storage.drivers.pg_sidecars.base import FeaturePipelineContext
# M1b.2: SidecarRegistry is now imported inline, next to each effective-
# sidecars resolution site.  ItemsPostgresqlDriverConfig remains
# imported at module level for type annotations on the bulk-write path
# (see `col_config: ItemsPostgresqlDriverConfig`) — that's a known
# carryover for a future cleanup pass; within M1b, the boundary guard
# targets CollectionService / CatalogService / AssetService only.
from dynastore.modules.catalog.item_query import ItemQueryMixin
from dynastore.modules.catalog.item_distributed import ItemDistributedMixin

logger = logging.getLogger(__name__)


async def _run_query(conn, stmt, params=None):
    """Run a statement on either sync or async connection."""
    result = conn.execute(stmt, params or {})
    if _inspect.isawaitable(result):
        result = await result
    return result


def _build_write_validator(items_schema: Any) -> Any:
    """Build a write-time value-constraint validator from an ``ItemsSchema``.

    Returns a ``jsonschema.Draft202012Validator`` that checks feature
    ``properties`` against the VALUE constraints declared on the items schema
    (type, enum, minimum, maximum, maxLength, pattern, format), or ``None``
    when the schema declares no fields (blob collection — nothing to validate).

    The schema is derived with ``purpose="write"`` so it deliberately omits
    the two structural constraints already enforced earlier on the write path
    — ``additionalProperties`` (owned by the strict-unknown-fields check) and
    ``required`` (owned by the ``NOT NULL`` sidecar columns / ``check_required``
    app-level fallback). Re-asserting either here would raise a second,
    conflicting 422 for the same input.
    """
    from dynastore.modules.storage.driver_config import ItemsSchema
    from dynastore.modules.storage.schema_derive import derive_wire_schema

    if not isinstance(items_schema, ItemsSchema):
        return None
    write_schema = derive_wire_schema(items_schema.fields, purpose="write")
    if not (isinstance(write_schema, dict) and write_schema):
        return None
    from jsonschema import Draft202012Validator

    try:
        return Draft202012Validator(write_schema)
    except Exception as exc:
        raise ValueError(
            f"Derived items-schema write validation is not a valid JSON Schema: {exc}"
        ) from exc


def _validate_feature_properties(validator: Any, raw_item: Dict[str, Any]) -> None:
    """Validate one feature's ``properties`` against a write validator.

    No-op when ``validator`` is ``None``. On a value-constraint violation,
    raises a 422-shaped ``ValueError`` naming the offending field(s); the bulk
    ingestion layer aggregates these into ``IngestionReport`` rejections.
    """
    if validator is None:
        return
    props = raw_item.get("properties") or {}
    violations = sorted(
        validator.iter_errors(props),
        key=lambda e: list(e.absolute_path),
    )
    if violations:
        msg = "; ".join(
            f"{'.'.join(str(p) for p in v.absolute_path) or '<root>'}: {v.message}"
            for v in violations
        )
        raise ValueError(f"Feature properties violate the items schema: {msg}")


def _collect_generated_stats(
    sidecar_payloads: Dict[str, Dict[str, Any]],
    stat_names: set,
) -> Dict[str, Any]:
    """Flatten every statistic a write generated for one item.

    Sidecars stash computed statistics in two shapes: shared JSONB blobs
    (``geom_stats`` / ``place_stats`` / ``attribute_stats`` — pure stat
    containers by construction) and their own typed COLUMNAR columns keyed by
    the :attr:`ComputedField.resolved_name`. The geometries sidecar leaves its
    blob a ``dict``; the attributes sidecar ``json.dumps`` its blob — handle
    both. ``stat_names`` is the COLUMNAR allow-list (resolved-names of stored
    statistic fields from the write policy) so non-stat payload keys — the WKB
    geometry, ``geom_type``, spatial indices, identity columns — never leak in.

    Pure function: the in-memory companion to the read-side expose loop, used
    to surface the *full* computed set to the ingestion audit report without a
    permissive read-policy or any protocol change.
    """
    flat: Dict[str, Any] = {}
    for sc_payload in sidecar_payloads.values():
        if not isinstance(sc_payload, dict):
            continue
        for blob_key in ("geom_stats", "place_stats", "attribute_stats"):
            blob = sc_payload.get(blob_key)
            if isinstance(blob, str):
                try:
                    blob = json.loads(blob)
                except (ValueError, TypeError):
                    blob = None
            if isinstance(blob, dict):
                flat.update(blob)
        for name in stat_names:
            if name in sc_payload:
                flat[name] = sc_payload[name]
    return flat


async def _storage_resolves_columnar_async(
    configs: Any,
    catalog_id: str,
    collection_id: str,
    ft: Any,
) -> bool:
    """Return True when the PG attributes sidecar resolves to COLUMNAR storage.

    Probes the persisted ``CollectionPostgresqlDriverConfig`` for the
    sidecar's ``storage_mode``. Fails open (returns False) on any lookup
    error so an absent driver config — e.g. before the first
    ``ensure_storage`` — does not block writes.

    Decision table (matches the bridge in ``field_constraints.py``):
    * COLUMNAR (explicit)  → always closed (True).
    * JSONB    (explicit)  → open (False); ``strict_unknown_fields`` is the
                             only operator opt-in for JSONB collections.
    * AUTOMATIC + fields   → bridge promotes all fields → COLUMNAR → closed (True).
    * AUTOMATIC + no fields→ JSONB → open (False)  [ft.fields already checked
                             by the caller].
    """
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeStorageMode,
        FeatureAttributeSidecarConfig,
    )
    from dynastore.modules.storage.drivers.collection_postgresql import (
        CollectionPostgresqlDriverConfig,
    )

    try:
        driver_cfg = None
        if configs is not None:
            try:
                driver_cfg = await configs.get_config(
                    CollectionPostgresqlDriverConfig,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
            except Exception:
                driver_cfg = None

        sidecar_mode = AttributeStorageMode.AUTOMATIC
        for sc in getattr(driver_cfg, "sidecars", None) or []:
            if isinstance(sc, FeatureAttributeSidecarConfig):
                sidecar_mode = sc.storage_mode
                break

        if sidecar_mode == AttributeStorageMode.JSONB:
            return False
        # COLUMNAR or AUTOMATIC: AUTOMATIC + non-empty fields → COLUMNAR.
        return bool(ft.fields)
    except Exception:
        return False


# --- Specialized Queries for ItemService ---



soft_delete_item_query = DQLQuery(
    "UPDATE {catalog_id}.{collection_id} SET deleted_at = NOW() WHERE geoid = :geoid AND deleted_at IS NULL;",
    result_handler=ResultHandler.ROWCOUNT,
)


@dataclass(frozen=True)
class _IndexStampContext:
    """Resolved inputs for stamping the canonical identity + access-envelope
    fields onto an index/outbox payload.

    Derived once per write and shared by both index-write paths so they stamp
    identically (#1287): the read-back dispatch
    (:meth:`ItemService._dispatch_index_upsert`) and the atomic OUTBOX bulk
    path (:meth:`ItemService.upsert_bulk`). ``access_envelope`` is ``None``
    unless the collection routes WRITE to an access-aware driver.

    ``external_id`` carries the pre-resolved value from the inbound item (set
    on ``processing_context["external_id"]`` by the sidecar or write-boundary).
    When present it takes precedence over path-based extraction so the stamped
    ``_external_id`` always reflects the original inbound identity, not the
    post-read-back geoid-bearing result.
    """

    external_id_path: Optional[str]
    asset_id: Optional[Any]
    access_envelope: Optional[Dict[str, Any]]
    external_id: Optional[str] = None


class ItemService(ItemQueryMixin, ItemDistributedMixin, ItemsProtocol):
    """Service for item-level operations.

    Explicitly declares conformance to ``ItemsProtocol`` so that static
    analysis tools (mypy) can verify method signatures stay in sync with
    the protocol definition.
    """

    priority: int = 10

    def __init__(self, engine: Optional[DbResource] = None):
        self.engine = engine

    def is_available(self) -> bool:
        return self.engine is not None

    async def _resolve_physical_schema(
        self, catalog_id: str, db_resource: Optional[DbResource] = None
    ) -> Optional[str]:
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            return None
        return await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=db_resource)
        )

    async def _enforce_strict_unknown_fields(
        self,
        catalog_id: str,
        collection_id: str,
        items_list: List[Any],
    ) -> None:
        """Reject batches with properties not declared in ItemsSchema.fields.

        Runs at the unified service-layer entry point (item_service.upsert) so
        every write path — OGC routes via _ingest_items, direct CatalogsProtocol
        callers, sidecar fan-outs — passes through it once. Driver-agnostic:
        no JSON-property-storing backend (PG JSONB, ES) can reject unknown
        keys natively. ``UnknownFieldsError`` is mapped to HTTP 422 by the
        global ``UnknownFieldsExceptionHandler``.

        Closed-schema enforcement (strict binary storage rule): a schema that
        resolves to COLUMNAR storage is implicitly closed — there is no JSONB
        blob on disk to absorb undeclared properties, so every unknown field
        would be silently dropped at ingest. Unknown properties are therefore
        rejected (HTTP 422) whenever the storage resolves COLUMNAR:
        * ``storage_mode=COLUMNAR`` (explicit) → always closed.
        * ``storage_mode=AUTOMATIC`` with a non-empty ``ItemsSchema.fields``
          → resolves COLUMNAR (bridge promotes all fields) → closed.
        * ``storage_mode=JSONB`` (explicit) → open; ``strict_unknown_fields``
          is the only opt-in gate.
        """
        from dynastore.modules.storage.driver_config import ItemsSchema
        from dynastore.modules.storage.field_constraints import (
            check_strict_unknown_fields,
        )

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return
        try:
            ft = await configs.get_config(
                ItemsSchema,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception:
            return
        if not isinstance(ft, ItemsSchema):
            return
        if not ft.fields:
            return

        # Determine whether the schema is closed (unknown props rejected).
        # A COLUMNAR layout (resolved or explicit) is always closed because the
        # table has no JSONB blob to catch undeclared properties.
        # An explicit JSONB pin keeps the schema open unless the operator also
        # set strict_unknown_fields=True.
        schema_is_closed = ft.strict_unknown_fields or await _storage_resolves_columnar_async(
            configs, catalog_id, collection_id, ft
        )
        if not schema_is_closed:
            return

        feature_dicts = [
            it if isinstance(it, dict) else (
                it.model_dump(by_alias=True, exclude_none=False)
                if hasattr(it, "model_dump") else dict(it)
            )
            for it in items_list
        ]
        check_strict_unknown_fields(ft.fields.keys(), feature_dicts)

    async def _resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        from dynastore.modules.storage.router import get_driver, get_write_drivers
        from dynastore.modules.storage.routing_config import Operation

        # Try READ driver first; fall back to the primary WRITE driver.
        # When READ is a non-PG driver (e.g. DuckDB), it has no
        # resolve_physical_table — the PG WRITE driver must supply it.
        for _op in (Operation.READ, None):
            try:
                if _op is None:
                    write_drivers = await get_write_drivers(catalog_id, collection_id)
                    driver = write_drivers[0].driver if write_drivers else None
                else:
                    driver = await get_driver(_op, catalog_id, collection_id)
            except Exception as exc:
                logger.warning(
                    "_get_physical_table: driver resolution failed"
                    " op=%s catalog=%s collection=%s: %s",
                    _op, catalog_id, collection_id, exc,
                )
                continue
            if driver and hasattr(driver, "resolve_physical_table"):
                result = await getattr(driver, "resolve_physical_table")(
                    catalog_id, collection_id, db_resource=db_resource
                )
                if result:
                    return result
        return None

    async def _get_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        config_provider: Optional[ConfigsProtocol] = None,
        db_resource: Optional[DbResource] = None,
    ):
        """Fetch driver config (sidecars, partitioning, collection_type).

        Uses the READ driver first; falls back to the primary WRITE driver
        when READ is a non-PG driver (e.g. DuckDB) so that the PG path
        receives a ItemsPostgresqlDriverConfig with valid sidecars.
        """
        from dynastore.modules.storage.router import get_driver, get_write_drivers
        from dynastore.modules.storage.routing_config import Operation

        # Try READ driver; if it returns a non-PG config, fall back to WRITE.
        config = None
        for _op in (Operation.READ, None):
            try:
                if _op is None:
                    write_drivers = await get_write_drivers(catalog_id, collection_id)
                    driver = write_drivers[0].driver if write_drivers else None
                else:
                    driver = await get_driver(_op, catalog_id, collection_id)
            except Exception as exc:
                logger.warning(
                    "_get_collection_config: driver resolution failed"
                    " op=%s catalog=%s collection=%s: %s",
                    _op, catalog_id, collection_id, exc,
                )
                continue
            if driver is None:
                continue
            config = await driver.get_driver_config(
                catalog_id, collection_id, db_resource=db_resource
            )
            # If the driver config has physical_table support (PG), use it.
            if hasattr(config, "physical_table"):
                return config
            # Non-PG config (e.g. DuckDB) — try next driver.
        return config  # Return whatever we got last (may be None)

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        col_config: Any,
        lang: str = "en",
        context: Optional[FeaturePipelineContext] = None,
        read_policy: Optional[Any] = None,
    ) -> Feature:
        """
        Canonical row-to-Feature mapper. Runs each configured sidecar's
        ``map_row_to_feature`` in declaration order, producing a fully
        populated GeoJSON/STAC Feature.

        Pipeline (ordered by sidecar config):
          1. **Hub** (this initialiser): ``feature.id`` defaults to ``geoid``.
          2. **Geometry sidecar**: sets ``geometry``, ``bbox``, optional stats
             properties.
          3. **Attributes sidecar**: optionally overrides ``id`` with
             ``external_id`` (controlled by
             ``ItemsReadPolicy.feature_type.external_id_as_feature_id``,
             default True), populates schema-driven ``properties``.
          4. **STAC sidecar** (optional): transforms the GeoJSON Feature into
             a STAC Item — adds ``links``, converts timestamps, attaches the
             asset reference from context.

        Each sidecar receives the *same* ``FeaturePipelineContext`` so sidecars
        can share data (e.g. attributes → STAC via ``asset_id``) without
        direct coupling. Internal-field filtering is delegated to sidecars
        via ``get_internal_columns()``.

        ``read_policy`` (optional) is published into context as
        ``_items_read_policy`` so sidecars can consult wire-shape decisions
        (e.g. external_id-as-feature-id) without re-fetching configs per row.
        """

        if not row:
            return Feature(type="Feature", geometry=None, properties={})

        _mapping = getattr(row, "_mapping", None)
        row_dict = dict(_mapping) if _mapping is not None else dict(row)

        # Hub contribution: initialise the feature id from the canonical
        # identity column. The select aliases the identity expression to
        # ``id`` (``<expr> AS id`` — default ``h.geoid``, or the COALESCE'd
        # external_id when the read policy flips), so the result row carries an
        # ``id`` key, not a bare ``geoid``. Read ``id`` first and fall back to
        # a ``geoid`` column for raw/legacy rows. Reading the wrong key here is
        # what produced ``feature.id = None`` and ``/items/None`` self links.
        # Sidecars (e.g. Attributes) may override this later in the pipeline.
        identity = row_dict.get("id")
        if identity is None:
            identity = row_dict.get("geoid")
        feature = Feature(
            type="Feature",
            geometry=None,
            properties={},
            id=str(identity) if identity is not None else None,
        )

        # Shared context propagated through the entire sidecar pipeline.
        if context is None:
            context = FeaturePipelineContext(lang=lang)
        if read_policy is not None and context.get("_items_read_policy") is None:
            context["_items_read_policy"] = read_policy

        # M1b.2: resolve effective sidecars (core defaults + registry
        # injections) so default-body collections — whose `col_config`
        # now has an empty `sidecars` list (plan §Principle — default-
        # fast invariant) — still get their row-to-feature pipeline
        # materialised.  Explicit caller-supplied sidecars are preserved;
        # the registry layers in any missing types for the collection
        # type.  The helper is cheap enough to call per-row, but most
        # callers iterate many rows with a shared col_config so the
        # resolved list is stable across the loop.
        from dynastore.modules.storage.drivers.pg_sidecars import (
            SidecarRegistry,
            _effective_sidecars,
        )
        from dynastore.modules.storage.drivers.pg_sidecars.base import (
            SidecarProtocol,
        )
        sidecar_configs = _effective_sidecars(
            col_config, catalog_id="", collection_id="",
        )
        # Note: ``stac_metadata`` is now a first-class sidecar with its
        # own DDL/JOIN/SELECT (PR-G2). When the stac extension is loaded,
        # ``SidecarRegistry.get_injected_sidecar_configs`` already injects
        # ``StacItemsSidecarConfig`` into the effective list above — no
        # read-time overlay append needed. Consumer-gating happens at
        # SQL-build time in ``QueryOptimizer`` so non-STAC consumers don't
        # JOIN or SELECT the stac_metadata payload columns.

        if sidecar_configs:
            # Gather all internal columns to prevent property leaking across sidecars
            all_internal = set()
            for sc_config in sidecar_configs:
                sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                if sidecar:
                    all_internal.update(sidecar.get_internal_columns())
            context._all_internal_cols = all_internal

            resolved_sidecars: List[SidecarProtocol] = []
            for sc_config in sidecar_configs:
                sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                if sidecar:
                    resolved_sidecars.append(sidecar)
                    sidecar.map_row_to_feature(row_dict, feature, context=context)

            # Read-shape exposure: surface ItemsReadPolicy.feature_type.expose
            # computed values onto properties once, after every sidecar has
            # contributed — so failure_mode sees the full producible set and the
            # loop is not duplicated per sidecar.
            SidecarProtocol.apply_exposed_computed_values(
                resolved_sidecars, row_dict, feature, context
            )

            # Bridge context to feature model_extra for extension generators (e.g. STAC)
            if context:
                # model_extra is already initialized by Pydantic 'extra="allow"'
                sidecar_data = context._sidecar_store
                all_internal = context.all_internal_columns
                # Never overwrite defined model fields (id, geometry, etc.)
                # via __pydantic_extra__ — that would clobber values already
                # set by the sidecar pipeline.
                model_fields = set(Feature.model_fields)
                # Explicit field names from the request (see #1827): a SYSTEM
                # field named explicitly in select may be surfaced into
                # model_extra so join consumers can use it as a join key.
                # Wildcard selects leave requested_fields empty, preserving
                # the existing behaviour.
                from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS as _SYSTEM_FIELD_KEYS
                _requested = getattr(context, "requested_fields", None) or set()
                _system_set = frozenset(_SYSTEM_FIELD_KEYS)
                # Producer contract (#1826): user attributes live ONLY in
                # ``feature.properties``. The attributes sidecar already placed
                # every requested column there, so do NOT also echo it up as a
                # top-level foreign member — that duplication is non-spec
                # GeoJSON on the OGC Features wire (``{"CODE":..,"properties":
                # {"CODE":..}}``) and forces every consumer to reconcile
                # ``model_extra`` against ``properties`` (the band-aid added in
                # #1818). Inter-sidecar internal keys (asset_id, *_langs,
                # place, coordRefSys, …) are never in ``properties`` so they
                # keep flowing; an explicitly requested SYSTEM field (#1827
                # join key) is surfaced regardless.
                _props_keys = set(feature.properties) if feature.properties else set()
                # Derived/computed values (geometry ``stats`` — area/perimeter/
                # length/centroid… — and ``system`` names) are governed solely
                # by the read policy: exposed onto ``properties`` via
                # ``feature_type.expose`` (handled above) or folded into the
                # gated ``stats``/``system`` sections by
                # ``_apply_expose_all_sections``. ``pushdown_read_select`` keeps
                # unexposed computed columns out of the SELECT on the common
                # path, but the attributes sidecar still republishes the *entire*
                # fetched row into context for inter-sidecar use — so when the
                # projection cannot be narrowed (schemaless collection, no read
                # policy, wildcard caller) those columns are present here yet
                # absent from ``properties`` and from any ``get_internal_columns()``
                # set. Without this backstop they would leak onto the Feature
                # root as foreign members regardless of the policy. A producible
                # computed value surfaces here only when the policy selected it
                # (it then rides ``_requested``, set from the narrowed/explicit
                # select) — e.g. a #1827 join key — mirroring the producer
                # contract #1826 applied to user attributes.
                _gated_computed: set = set()
                for _sc in resolved_sidecars:
                    _gated_computed.update(_sc.producible_computed_names())
                for sid, data in sidecar_data.items():
                    if isinstance(data, dict):
                        # Merge dicts if it's a standard sidecar publication,
                        # but skip raw internal columns (e.g. item_title,
                        # external_extensions) that are only meant for
                        # inter-sidecar communication.
                        for k, v in data.items():
                            surface_requested = k in _requested and k in _system_set
                            if k in _props_keys and not surface_requested:
                                continue
                            # Policy-governed computed value, not explicitly
                            # requested -> never flat on the Feature root.
                            if k in _gated_computed and k not in _requested:
                                continue
                            if (k not in all_internal or surface_requested) and k not in model_fields:
                                if feature.__pydantic_extra__ is not None:
                                    feature.__pydantic_extra__[k] = v
                    else:
                        if sid not in model_fields:
                            if feature.__pydantic_extra__ is not None:
                                feature.__pydantic_extra__[sid] = data

            # expose_all (#1402): attach the report-style ``stats`` + ``system``
            # sections as top-level GeoJSON foreign members beside ``properties``.
            self._apply_expose_all_sections(
                feature, row_dict, resolved_sidecars, context
            )
        else:
            logger.warning(
                "No sidecars configured for col_config; returning minimal "
                "feature with geoid-based id."
            )

        return feature

    @staticmethod
    def _apply_expose_all_sections(
        feature: Feature,
        row: Dict[str, Any],
        resolved_sidecars: List[Any],
        context: Optional[FeaturePipelineContext],
    ) -> None:
        """Attach the ``stats`` + ``system`` report sections (#1402, #1827).

        Runs in two modes:

        Full mode (``expose_all=True``): builds complete ``system`` + ``stats``
        top-level GeoJSON foreign members (RFC 7946 §6.1) from the read row,
        mirroring the ingestion report envelope.

        Partial mode (``expose_all=False``, explicit fields requested): when
        ``context.requested_fields`` contains system or stats keys, builds
        only the intersection — surfacing, for example, ``external_id`` into
        ``feature.__pydantic_extra__["system"]`` so a DWH join with
        ``join_source="system"`` can resolve it. Normal wildcard reads
        (requested_fields empty) exit early and are byte-identical to
        pre-#1827 behavior.

        ``properties`` is left untouched in both modes — it stays user-only.
        Any recognised stats/system key already attached flat (via the sidecar
        foreign-member bridge) is folded into its section so it is not emitted
        twice.
        """
        from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

        policy = (
            context.get("_items_read_policy") if context is not None else None
        )
        feature_type = getattr(policy, "feature_type", None)
        expose_all = bool(getattr(feature_type, "expose_all", False))
        requested: set = getattr(context, "requested_fields", None) or set()

        # Fast exit: expose_all is off AND no explicit system/stats field requested.
        # This keeps normal wildcard reads byte-identical to pre-#1827. (#1827)
        system_keys = set(SYSTEM_FIELD_KEYS)
        if not expose_all and not requested:
            return
        if feature_type is None and not requested:
            return

        # Collect all producible stat names from sidecars so we can resolve
        # which requested names land in stats vs system.
        all_stat_names: set = set()
        for sidecar in resolved_sidecars:
            all_stat_names.update(sidecar.producible_computed_names())
        all_stat_names -= system_keys

        # Determine which system fields to include.
        if expose_all:
            system_include = system_keys
        else:
            system_include = requested & system_keys

        # Determine which stat names to include.
        if expose_all:
            stats_include: set | None = None  # all producible
        else:
            stats_include = requested & all_stat_names

        if not expose_all and not system_include and not stats_include:
            # Requested fields contain neither system nor stats names; nothing to do.
            return

        system: Dict[str, Any] = {}
        for key in SYSTEM_FIELD_KEYS:
            if key not in system_include:
                continue
            value = row.get(key)
            if value is not None:
                system[key] = value

        stats: Dict[str, Any] = {}
        for sidecar in resolved_sidecars:
            for name in sidecar.producible_computed_names():
                if name in system_keys or name in stats:
                    continue
                if stats_include is not None and name not in stats_include:
                    continue
                found, value = sidecar.resolve_computed_value(row, name)
                if found and value is not None:
                    stats[name] = value

        # Fold any flat foreign members the bridge already attached into their
        # section so a recognised key is not emitted both flat and grouped.
        extra = feature.__pydantic_extra__
        if extra is not None:
            for key in list(extra.keys()):
                if key in system_include:
                    system.setdefault(key, extra.pop(key))
                elif stats_include is not None and key in stats_include:
                    extra.pop(key, None)
                elif expose_all and key in stats:
                    extra.pop(key, None)
            if expose_all:
                # Full mode: always attach both sections (mirrors pre-#1827 behavior).
                extra["stats"] = stats
                extra["system"] = system
            else:
                # Partial mode: only attach non-empty sections to avoid polluting
                # normal reads with empty foreign members.
                if system:
                    extra["system"] = system
                if stats:
                    extra["stats"] = stats

    @staticmethod
    def _resolved_driver_is_es_items(driver: Any) -> bool:
        """True when a resolved driver is one of the ES items drivers.

        Detected via the structural ``is_es_items_driver`` class marker
        (set on ``_ItemsElasticsearchBase``, inherited by both the public and
        private ES items drivers) so this guard does not import the ES driver
        classes — keeping the catalog module free of a hard ES dependency.
        """
        return bool(getattr(driver, "is_es_items_driver", False))

    async def _es_items_driver_simplify_enabled(
        self,
        driver: Any,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[DbResource] = None,
    ) -> bool:
        """Resolve whether an ES items driver has geometry simplification on.

        The private driver exposes ``_resolve_simplify_geometry``; the public
        driver resolves its ``simplify_geometry`` flag through
        ``get_driver_config``. Defaults to True (simplify-by-default, revised
        2026-06-18) — a config-read failure must fail OPEN (simplify), never
        reject.
        """
        resolver = getattr(driver, "_resolve_simplify_geometry", None)
        if resolver is not None:
            try:
                result = await resolver(
                    catalog_id, collection_id, db_resource=db_resource,
                )
                if result is None:
                    return True  # not configured → fail open (accept + auto-simplify)
                return bool(result)
            except Exception:
                return True
        get_config = getattr(driver, "get_driver_config", None)
        if get_config is not None:
            try:
                cfg = await get_config(
                    catalog_id, collection_id, db_resource=db_resource,
                )
                return bool(getattr(cfg, "simplify_geometry", True))
            except Exception:
                return True
        return True

    async def _enforce_es_geometry_size_limit(
        self,
        catalog_id: str,
        collection_id: str,
        items_list: List[Any],
        resolved_drivers: List[Any],
        *,
        db_resource: Optional[DbResource] = None,
        ctx: Optional[DriverContext] = None,
        is_single: bool = True,
    ) -> List[Any]:
        """Pre-write 10 MB geometry guard for ES-backed collections (#1248).

        ES is an async secondary: the PG primary commits before the ES write
        ever runs, so a true "don't keep the primary row" reject for an
        oversized geometry MUST happen BEFORE any driver write. When the
        resolved WRITE routing includes an ES items secondary whose
        ``simplify_geometry`` is disabled, an item whose geometry serializes
        above ``DEFAULT_MAX_BYTES`` (10 MB) is rejected before anything is
        written to ANY driver (primary included).

        How the rejection is delivered depends on the ingest shape (#1260):

        * **Single-item** ingest (``is_single``), or any call without a
          ``_rejections`` out-list on *ctx*: raise a 422-shaped ``ValueError``
          that fails the request — there is no per-item channel to report into.
        * **Bulk** ingest with a rejection channel: drain each oversized item
          into ``ctx.extensions["_rejections"]`` (the same out-list the PG
          per-row path uses; ``_ingest_items`` turns it into a 207
          ``IngestionReport``) and drop it from the returned write set, so the
          good items in the batch are still accepted.

        Returns the items that may proceed to the write path (the input list
        unchanged when nothing is dropped).

        No reject when no ES items secondary is routed (e.g. a PG-only
        catalog) — PG handles large geometries fine; the 10 MB ceiling is an
        Elasticsearch per-document constraint. No reject when the routed ES
        driver has simplification enabled, since it will shrink the geometry
        to fit on its own.

        The byte measurement uses the GeoJSON serialization of the geometry
        alone (``geometry_geojson_size``): the ES per-document size is
        dominated by the geometry payload and the threshold is an ES
        constraint, so the geometry's own serialized size is the stable,
        driver-shape-independent signal.
        """
        from dynastore.tools.geometry_simplify import (
            DEFAULT_MAX_BYTES,
            geometry_geojson_size,
        )

        # Find ES items secondaries that index exact geometry (simplify off).
        guarded = False
        for resolved in resolved_drivers:
            driver = getattr(resolved, "driver", resolved)
            if not self._resolved_driver_is_es_items(driver):
                continue
            if await self._es_items_driver_simplify_enabled(
                driver, catalog_id, collection_id, db_resource=db_resource,
            ):
                continue
            guarded = True
            break

        if not guarded:
            return items_list

        def _reason(item_id: Any, size: int) -> str:
            return (
                f"Item '{item_id}' geometry is {size} bytes, exceeding the "
                f"{DEFAULT_MAX_BYTES}-byte (10 MB) Elasticsearch "
                f"per-document limit. ES geometry simplification is ON by "
                f"default, but this collection has explicitly set "
                f"'simplify_geometry: false' on its Elasticsearch items driver, "
                f"so the oversized item is rejected before any write. To accept "
                f"and auto-simplify it, remove that override or set "
                f"'simplify_geometry: true' (PostgreSQL keeps full resolution)."
            )

        # The per-item rejection out-list, when the caller seeded one
        # (``_ingest_items`` does for every HTTP ingest). ``None`` for direct
        # service calls with no 207 channel.
        sink = None
        if ctx is not None and isinstance(getattr(ctx, "extensions", None), dict):
            seeded = ctx.extensions.get("_rejections")
            if isinstance(seeded, list):
                sink = seeded

        kept: List[Any] = []
        for item in items_list:
            if isinstance(item, dict):
                geometry = item.get("geometry")
                item_id = item.get("id")
            else:
                geometry = getattr(item, "geometry", None)
                item_id = getattr(item, "id", None)
            size = geometry_geojson_size(geometry)
            if size <= DEFAULT_MAX_BYTES:
                kept.append(item)
                continue

            # Single-item ingest, or no 207 channel: fail the request.
            if is_single or sink is None:
                raise ValueError(_reason(item_id, size))

            # Bulk with a channel: report this item and drop it; keep the rest.
            sink.append(
                {
                    "geoid": None,
                    "external_id": str(item_id) if item_id is not None else None,
                    "sidecar_id": "geometries",
                    "matcher": None,
                    "reason": "geometry_too_large",
                    "message": _reason(item_id, size),
                }
            )

        return kept

    async def _enforce_es_geometry_poison_check(
        self,
        catalog_id: str,
        collection_id: str,
        items_list: List[Any],
        resolved_drivers: List[Any],
        *,
        db_resource: Optional[DbResource] = None,
        ctx: Optional[DriverContext] = None,
        is_single: bool = True,
    ) -> List[Any]:
        """Pre-write poison-geometry guard for ES-backed collections (#2112).

        ES is an async secondary: the PG primary commits before the ES write
        ever runs.  Certain geometry shapes cause ES to permanently reject a
        document (``invalid_shape_exception`` for duplicate/degenerate
        coordinates, ``mapper_parsing_exception`` for wrong-type inputs such as
        a ``GeometryCollection`` where a polygon field is mapped).  These are
        listed in
        :data:`~dynastore.modules.elasticsearch.bulk_classify._POISON_ERROR_TYPES`
        as the authoritative classification.

        This guard runs at the same scope as the 10 MB size guard: it activates
        only when the resolved WRITE routing includes an ES items secondary
        whose ``simplify_geometry`` is disabled.  PG-only catalogs are never
        affected — PG stores geometry as-is with no shape restrictions.

        Repair-first contract (#2044 / #2112):
        For each item the guard attempts to repair the geometry (``make_valid``
        + polygonal coercion) before deciding:

        * **Already valid** → accept unchanged.
        * **Repairable** → mutate the item's geometry in place and accept
          (the ES driver receives a clean shape without an extra round-trip).
        * **Poison-after-repair** (collapses to empty; no polygonal area
          survives) → reject.  Rejection delivery follows the same channel as
          the size guard:

          - Single-item ingest, or no rejection channel: raise
            ``ValueError`` (→ HTTP 400 via ``_ingest_items``).
          - Bulk ingest with a rejection channel: drain to
            ``ctx.extensions["_rejections"]`` (→ HTTP 207 IngestionReport).

        Returns the items that may proceed to the write path (the input list
        unchanged when no item is rejected or repaired in a way that removes
        it).
        """
        from dynastore.tools.geometry_repair import repair_geometry_for_es

        # Activate only when an ES items secondary without simplification is
        # present — mirrors the size-guard activation logic exactly.
        guarded = False
        for resolved in resolved_drivers:
            driver = getattr(resolved, "driver", resolved)
            if not self._resolved_driver_is_es_items(driver):
                continue
            if await self._es_items_driver_simplify_enabled(
                driver, catalog_id, collection_id, db_resource=db_resource,
            ):
                continue
            guarded = True
            break

        if not guarded:
            return items_list

        def _poison_reason(item_id: Any, geom_type: Optional[str]) -> str:
            type_hint = f" (type '{geom_type}')" if geom_type else ""
            return (
                f"Item '{item_id}' has a geometry{type_hint} that collapses to "
                f"empty after repair (make_valid + polygonal coercion) and would "
                f"always be rejected by Elasticsearch with an "
                f"'invalid_shape_exception' or 'mapper_parsing_exception'. "
                f"Fix the geometry before re-submitting."
            )

        # The per-item rejection out-list, when the caller seeded one.
        sink = None
        if ctx is not None and isinstance(getattr(ctx, "extensions", None), dict):
            seeded = ctx.extensions.get("_rejections")
            if isinstance(seeded, list):
                sink = seeded

        kept: List[Any] = []
        for item in items_list:
            if isinstance(item, dict):
                geometry = item.get("geometry")
                item_id = item.get("id")
            else:
                geometry = getattr(item, "geometry", None)
                item_id = getattr(item, "id", None)

            geom_type = (
                geometry.get("type") if isinstance(geometry, dict) else None
            )

            repaired, was_repaired, is_poison = repair_geometry_for_es(geometry)

            if is_poison:
                reason = _poison_reason(item_id, geom_type)
                if is_single or sink is None:
                    raise ValueError(reason)
                sink.append(
                    {
                        "geoid": None,
                        "external_id": str(item_id) if item_id is not None else None,
                        "sidecar_id": "geometries",
                        "matcher": None,
                        "reason": "geometry_poison",
                        "message": reason,
                    }
                )
                continue

            # Repair succeeded — mutate the item in place so the ES driver
            # receives the fixed geometry without a separate repair step.
            if was_repaired and repaired is not None:
                if isinstance(item, dict):
                    item["geometry"] = repaired
                else:
                    try:
                        object.__setattr__(item, "geometry", repaired)
                    except (AttributeError, TypeError):
                        # Frozen model or non-settable attribute: accept as-is.
                        # The ES indexer will encounter the invalid geometry and
                        # the async classify path will surface it.
                        logger.debug(
                            "geometry_repair: could not mutate geometry on item %s "
                            "(model is frozen or non-dict); accepted unchanged",
                            item_id,
                        )

            kept.append(item)

        return kept

    async def upsert(
        self,
        catalog_id: str,
        collection_id: str,
        items: Union[Dict[str, Any], List[Dict[str, Any]], Any],
        ctx: Optional[DriverContext] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], Any]:
        """
        Create or update items (single or bulk).

        Args:
            catalog_id: Catalog identifier
            collection_id: Collection identifier
            items: Feature, FeatureCollection, STACItem, or raw dict/list
            ctx: Optional driver context carrying db_resource and processing hints

        Returns:
            Created/Updated item(s) (single or list)
        """
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        # ── Resolve external → internal ids at the write boundary ─────────────
        # catalog_id and collection_id from URL path params are public external
        # ids.  Everything persisted (ES _source catalog_id/collection_id, ES
        # index name, ES _routing, PG hub schema name, OUTBOX task rows) must
        # use the IMMUTABLE internal id so a rename never stales stored items.
        # Resolution is done once here so all downstream code (Branch A and B,
        # _dispatch_index_upsert, fan-out) receives internal ids.
        from dynastore.tools.discovery import get_protocol as _get_proto
        from dynastore.models.protocols import CatalogsProtocol as _CatProto
        _catalogs_svc = _get_proto(_CatProto)
        if _catalogs_svc is not None:
            _internal_cat = await _catalogs_svc.resolve_catalog_id(
                catalog_id, allow_missing=True
            )
            if _internal_cat is not None:
                catalog_id = _internal_cat
            _internal_col = await _catalogs_svc.collections.resolve_collection_id(
                catalog_id, collection_id, allow_missing=True
            )
            if _internal_col is not None:
                collection_id = _internal_col

        # Determine if single or bulk to return consistent type
        is_single = False
        items_list = []

        # Handle input types
        if isinstance(items, list):
            items_list = items
        elif isinstance(items, dict) and items.get("type") == "FeatureCollection":
            items_list = items.get("features", [])
        elif hasattr(items, "features") and getattr(items, "type", None) == "FeatureCollection":
            # Handle FeatureCollection Pydantic model
            items_list = getattr(items, "features")
        else:
            # Handle single item passed as list or other iterable
            # Single item (Feature, STACItem, dict)
            is_single = True
            items_list = [items]

        if not items_list:
            raise ValueError("No features provided. A FeatureCollection must contain at least one feature.")

        # ItemsSchema.strict_unknown_fields enforcement.
        # Service-layer because no JSON-property-storing driver (PG JSONB,
        # ES) can natively reject unknown keys, and Branch B (PG primary)
        # below bypasses driver.write_entities entirely — so this is the
        # single point all writers cross.
        await self._enforce_strict_unknown_fields(catalog_id, collection_id, items_list)

        # ── Branch A: non-PG primary write driver ─────────────────────────
        # When the primary WRITE driver is not postgresql, delegate the entire
        # write to that driver.  PG-specific logic (sidecars, hub table, sidecar
        # payloads, QueryOptimizer) is skipped — the driver owns its own write path.
        # Post-commit fan-out and event emission still run after this branch.
        #
        # Trust the waterfall: ItemsRoutingConfig.operations[WRITE] has a
        # code-level default of [ItemsPostgresqlDriver], so this list is
        # never empty in a correctly bootstrapped deploy. If it is empty, the
        # resolver surfaces ConfigResolutionError → HTTP 500 ops alert.
        from dynastore.modules.storage.router import get_write_drivers
        resolved_drivers = await get_write_drivers(catalog_id, collection_id)
        primary = resolved_drivers[0]

        # Pre-write 10 MB geometry guard (#1248). Runs before BOTH the
        # non-PG primary branch (A) and the PG-primary branch (B) so an
        # oversized geometry destined for an exact-geometry ES secondary is
        # rejected with HTTP 422 before any driver write — the PG primary row
        # is never created (ES is an async secondary; rejecting post-commit
        # would leave PG and ES inconsistent).
        # Bulk ingests deliver an oversized geometry as a per-item 207
        # rejection (the returned list drops it); single-item ingests still
        # fail with a 4xx (#1260).
        items_list = await self._enforce_es_geometry_size_limit(
            catalog_id, collection_id, items_list, resolved_drivers,
            db_resource=db_resource, ctx=ctx, is_single=is_single,
        )
        if not items_list:
            # Every item in the bulk was drained as an oversized-geometry
            # rejection (single-item oversized raises above, so this is only
            # reachable for bulk). Skip the no-op primary write; the seeded
            # ``_rejections`` out-list already carries the per-item 207 report.
            return []

        # Pre-write poison-shape guard (#2112). Runs after the size guard so
        # we never attempt repair on a geometry that is already too large.
        # Repairs are applied in place (geometry mutated on the item dict)
        # so the write path receives an immediately-indexable shape.
        items_list = await self._enforce_es_geometry_poison_check(
            catalog_id, collection_id, items_list, resolved_drivers,
            db_resource=db_resource, ctx=ctx, is_single=is_single,
        )
        if not items_list:
            return []

        from dynastore.models.protocols.storage_driver import Capability
        if primary is not None and Capability.QUERY_FALLBACK_SOURCE not in primary.driver.capabilities:
            # Phase 2b tile-cache (#1297): capture each item's current extent
            # BEFORE the upsert so the invalidate task can also drop the tiles
            # a moved geometry USED to occupy. Gated + degrade-safe — never
            # blocks the write; returns [] when the cache is inactive or the
            # item doesn't yet exist (CREATE path contributes nothing).
            prior_bboxes_a = await self._capture_prior_bboxes_for_update(
                catalog_id, collection_id, items_list,
            )

            chunk_size = getattr(primary.driver, "preferred_chunk_size", 0)
            if chunk_size > 0 and len(items_list) > chunk_size:
                results = []
                for i in range(0, len(items_list), chunk_size):
                    chunk = items_list[i : i + chunk_size]
                    chunk_results = await primary.driver.write_entities(
                        catalog_id,
                        collection_id,
                        chunk,
                        context=processing_context,
                    )
                    results.extend(chunk_results or [])
            else:
                results = await primary.driver.write_entities(
                    catalog_id,
                    collection_id,
                    items_list,
                    context=processing_context,
                )
            results = results or []

            # Fan-out to secondary drivers (positions 1+)
            if results:
                await self._fan_out_to_secondary_drivers(
                    catalog_id, collection_id, results, _primary_already_written=True,
                )

            # Single dispatcher call site for index propagation —
            # replaces the per-driver ``_on_item_upsert`` event listeners.
            # Phase 2f: pass the engine so the dispatcher can open its own
            # wrapping TX for atomic OUTBOX enqueue.
            if results:
                _index_results = await self._dispatch_index_upsert(
                    catalog_id, collection_id, results,
                    db_resource=db_resource or self.engine,
                    processing_context=processing_context,
                )
                # Propagate per-indexer results via ctx.extensions so callers
                # (e.g. the ingestion task) can inspect secondary-index health
                # without additional round-trips.
                if ctx is not None and _index_results:
                    existing = ctx.extensions.get("_index_results") or {}
                    # Merge per-batch results: accumulate succeeded/failed totals.
                    for indexer_id, bulk_res in _index_results.items():
                        if indexer_id in existing:
                            prev = existing[indexer_id]
                            from dynastore.models.protocols.indexer import BulkResult
                            existing[indexer_id] = BulkResult(
                                total=prev.total + bulk_res.total,
                                succeeded=prev.succeeded + bulk_res.succeeded,
                                failed=prev.failed + bulk_res.failed,
                                failures=prev.failures + bulk_res.failures,
                            )
                        else:
                            existing[indexer_id] = bulk_res
                    ctx.extensions["_index_results"] = existing

            # Write-reactive tile-cache invalidation (#1292 / #1297 Phase 2b):
            # mark stale the tiles the new feature bboxes touch, and also the
            # tiles the old footprint used to occupy (prior_bboxes_a — the
            # geometry-MOVE case). No-op + degrade-safe; never blocks the write.
            if results:
                await self._dispatch_tile_cache_invalidation(
                    catalog_id, collection_id, results,
                    db_resource=db_resource or self.engine,
                    processing_context=processing_context,
                    prior_bboxes=prior_bboxes_a or None,
                )

            # Emit events for non-indexer subscribers (audit, telemetry).
            # Index propagation no longer rides this channel — it goes
            # through the dispatcher above so failures are attributable
            # per-indexer with proper retry semantics.
            if results:
                try:
                    from dynastore.models.protocols.event_bus import EventBusProtocol
                    from dynastore.modules.catalog.event_service import CatalogEventType
                    events_protocol = get_protocol(EventBusProtocol)
                    if events_protocol:
                        if is_single:
                            await events_protocol.emit(
                                event_type=CatalogEventType.ITEM_CREATION,
                                catalog_id=catalog_id,
                                collection_id=collection_id,
                                item_id=str(results[0].id) if results[0].id else None,
                                payload=results[0].model_dump(by_alias=True, exclude_unset=True),
                            )
                        else:
                            await events_protocol.emit(
                                event_type=CatalogEventType.BULK_ITEM_CREATION,
                                catalog_id=catalog_id,
                                collection_id=collection_id,
                                payload={
                                    "count": len(results),
                                    "items_subset": [
                                        r.model_dump(by_alias=True, exclude_none=True)
                                        for r in results[:10]
                                    ],
                                },
                            )
                except Exception as e:
                    logger.warning("Failed to emit item creation events: %s", e)

            return results[0] if is_single else results

        # ── Branch B: PostgreSQL primary ─────────────────────────────────
        # Three-phase ingestion. Each phase opens the narrowest possible
        # connection so no single thread holds a write transaction across
        # the whole batch.
        #
        #   1. Resolve config + sidecars + physical table (read-only, brief)
        #   2. Prepare every item in memory (no DB) and dedupe partition keys
        #   3. DDL: pre-create the unique partitions (separate brief tx)
        #   4. Write: chunked write transactions; each commits its hub +
        #      sidecar inserts before yielding the conn back to the pool.
        #      No per-item read-back inside the write tx — that used to
        #      accumulate AccessShare locks on every iteration.
        #   5. Read-back: one bulk SELECT WHERE geoid = ANY(:geoids) on a
        #      separate read conn, after writes have committed.

        # Phase 2b tile-cache (#1297): capture each item's current extent
        # BEFORE Phase 4 (the write) so the invalidate task can also drop the
        # tiles a moved geometry USED to occupy. Gated + degrade-safe — never
        # blocks the write; returns [] when the cache is inactive or the item
        # doesn't yet exist (CREATE path contributes nothing).
        prior_bboxes_b = await self._capture_prior_bboxes_for_update(
            catalog_id, collection_id, items_list,
        )

        engine = db_resource or self.engine
        from dynastore.tools.identifiers import generate_geoid

        # Phase 1 — config + sidecars + physical table
        async with managed_transaction(engine) as conn:
            _configs = get_protocol(ConfigsProtocol)
            assert _configs is not None, "ConfigsProtocol not registered"
            collection_config = await _configs.get_config(
                CollectionPluginConfig, catalog_id, collection_id,
                ctx=DriverContext(db_resource=conn),
            )
            max_bulk = collection_config.max_bulk_features
            if len(items_list) > max_bulk:
                raise ValueError(
                    f"FeatureCollection contains {len(items_list)} features, "
                    f"exceeding the maximum of {max_bulk}. "
                    f"Split into smaller batches."
                )

            assert primary is not None, "primary driver required for PostgreSQL write path"

            # Lazy activation: a pending collection is activated on its first
            # insert. `activate_collection` is idempotent (no-op if already
            # active) and reuses the current transaction so the routing pin
            # commits atomically with the first item write.
            catalogs_for_activation = get_protocol(CatalogsProtocol)
            assert catalogs_for_activation is not None, "CatalogsProtocol not registered"
            # Lifecycle gate: a deleted (tombstoned or hard-deleted) collection
            # must never be silently re-provisioned by lazy activation. Raises
            # CollectionNotAliveError before any DDL or routing pin happens.
            await catalogs_for_activation.ensure_alive(
                catalog_id, collection_id, db_resource=conn,
            )
            if not await catalogs_for_activation.is_active(
                catalog_id, collection_id, db_resource=conn,
            ):
                await catalogs_for_activation.activate_collection(
                    catalog_id, collection_id,
                    ctx=DriverContext(db_resource=conn),
                )

            col_config = await primary.driver.get_driver_config(
                catalog_id, collection_id, db_resource=conn,
            )

            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn,
            )
            if not phys_table:
                # Fallback: activation did not pin a physical_table (e.g. no
                # storage driver registered in test environments).  Run the
                # legacy PG-specific promotion path so the table gets created.
                await self.ensure_physical_table_exists(
                    catalog_id, collection_id, col_config, db_resource=conn,
                )
                phys_table = await self._resolve_physical_table(
                    catalog_id, collection_id, db_resource=conn,
                )

            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn,
            )

            # M1b.2: resolve effective sidecars (core defaults for the
            # collection_type + registry injections) so default-body
            # collections still activate sidecars at first write time.
            # Phase 1.6: collection_type lives on its own PluginConfig.
            from dynastore.modules.catalog.catalog_config import CollectionInfo
            from dynastore.modules.storage.drivers.pg_sidecars import (
                SidecarRegistry,
                _effective_sidecars,
            )
            from dynastore.modules.storage.drivers.postgresql import (
                _resolve_stac_items_pg,
            )
            configs = get_protocol(ConfigsProtocol)
            ct = await configs.get_config(
                CollectionInfo, catalog_id=catalog_id, collection_id=collection_id,
            ) if configs else CollectionInfo()
            stac_items_pg = await _resolve_stac_items_pg(
                catalog_id, collection_id, configs=configs,
            )
            sidecar_configs = _effective_sidecars(
                col_config,
                catalog_id=catalog_id,
                collection_id=collection_id,
                collection_type=ct.kind.value,
                context={"stac_items_pg": stac_items_pg, "allow_geometry": ct.allow_geometry},
            )

            sidecars: List[Any] = []
            if sidecar_configs:
                from dynastore.modules.db_config.exceptions import ConfigResolutionError
                for sc_config in sidecar_configs:
                    try:
                        sidecars.append(SidecarRegistry.get_sidecar(sc_config))
                    except ValueError as exc:
                        sidecar_type = getattr(sc_config, "sidecar_type", "<unknown>")
                        extra = (
                            "extension_stac" if sidecar_type == "stac_metadata"
                            else f"extension for sidecar '{sidecar_type}'"
                        )
                        raise ConfigResolutionError(
                            (
                                f"Collection '{catalog_id}/{collection_id}' is configured with "
                                f"sidecar '{sidecar_type}', but the required extension is not "
                                f"installed in this service. Reinstall with the "
                                f"'dynastore[{extra}]' extra, or remove '{sidecar_type}' "
                                f"from the collection's sidecars."
                            ),
                            missing_key=f"sidecar:{sidecar_type}",
                            scope_tried=[f"catalog={catalog_id}", f"collection={collection_id}"],
                            hint=(
                                f"Sidecar '{sidecar_type}' is persisted on this collection "
                                f"but no implementation is registered in this service's SCOPE. "
                                f"Install 'dynastore[{extra}]' or drop the sidecar from the "
                                f"collection config."
                            ),
                        ) from exc

            # Run item_metadata before attributes so its in-place prune of
            # feature.properties takes effect before attributes captures them.
            _PRUNE_FIRST = {"item_metadata"}
            sidecars_ordered = sorted(
                sidecars, key=lambda s: (0 if s.sidecar_id in _PRUNE_FIRST else 1),
            )

            # Resolve ItemsWritePolicy once for the whole batch. It's the
            # single source of truth for ``external_id_field`` /
            # ``require_external_id``; sidecars read it off the per-item
            # context. ``None`` here means "no policy configured" — sidecars
            # then skip extraction and conflict resolution falls back to geoid.
            items_write_policy: Optional[ItemsWritePolicy] = None
            if configs is not None:
                wp = await configs.get_config(
                    ItemsWritePolicy,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    ctx=DriverContext(db_resource=conn),
                )
                if isinstance(wp, ItemsWritePolicy):
                    items_write_policy = wp

        # Phase 2 — prepare every item in memory; dedupe partition keys
        # Value-constraint validation: every feature's ``properties`` is
        # validated against a JSON Schema derived from ``ItemsSchema`` (the
        # single source of truth) before sidecar work begins. A violation
        # raises a 422-shaped ValueError naming the offending field; the bulk
        # ingestion layer aggregates these into IngestionReport rejections.
        # See ``_build_write_validator`` for what the derived schema does and
        # does not assert (value constraints only — unknown-key and required
        # checks are owned by other write-path mechanisms). The validation
        # schema is derived directly from ``ItemsSchema`` here, mirroring the
        # read path in ``get_collection_schema``; ``ItemsSchema`` is the single
        # source of truth for the wire shape.
        schema_validator = None
        validation_configs = get_protocol(ConfigsProtocol)
        if validation_configs is not None:
            from dynastore.modules.storage.driver_config import ItemsSchema

            try:
                _items_schema = await validation_configs.get_config(
                    ItemsSchema,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
            except Exception:
                _items_schema = None
            schema_validator = _build_write_validator(_items_schema)

        # G1 (#1457): Pre-resolve the access envelope once for the whole batch.
        # ``_resolve_access_envelope`` returns a non-None dict only when
        # ``_collection_uses_access_aware_driver`` is True (G4 wires in PG-sidecar
        # detection).  Resolving here — outside the per-item loop and outside any
        # DB transaction — avoids repeated async calls and ensures the envelope is
        # identical for every item in the batch (visibility + owner are batch-level
        # properties, not per-item).  The result is injected into ``item_context``
        # below so ``AccessEnvelopeSidecar.prepare_upsert_payload`` receives it
        # via ``context["_access_envelope"]``.
        _batch_access_envelope: Optional[Dict[str, Any]] = (
            await self._resolve_access_envelope(
                catalog_id, collection_id, processing_context,
            )
        )

        prepared: List[Dict[str, Any]] = []
        unique_partition_values: set = set()
        # Per-row preparation rejections. A single feature that fails value
        # validation (items-schema ValueError) or a sidecar pre-write check
        # must NOT collapse the whole batch — it is recorded here and the loop
        # moves to the next item, exactly as Phase 4 does for the distributed
        # upsert's ``SidecarRejectedError``. Merged with the Phase 4 rejections
        # and surfaced via ``ctx.extensions["_rejections"]`` so the HTTP layer
        # builds a 207 IngestionReport and the ingestion job reports the row as
        # a failure instead of aborting (the row carries ``record`` for that).
        prep_rejections: List[Dict[str, Any]] = []
        for item_data in items_list:
            if hasattr(item_data, "model_dump"):
                raw_item = item_data.model_dump(by_alias=True, exclude_unset=True)  # type: ignore[reportAttributeAccessIssue]
            elif isinstance(item_data, dict):
                raw_item = item_data
            else:
                raise ValueError(f"Unsupported item type: {type(item_data)}")

            try:
                _validate_feature_properties(schema_validator, raw_item)

                geoid = generate_geoid()
                item_context: Dict[str, Any] = {
                    "geoid": geoid,
                    "operation": "insert",
                    "_raw_item": raw_item,
                    # Pristine snapshot taken before any sidecar mutates raw_item.
                    # A prune-first sidecar (item_metadata) strips title/description/
                    # keywords and ``:``-namespaced extension keys from the shared
                    # ``properties`` in place so the attributes residue stays clean.
                    # The stac_metadata sidecar, which runs afterwards and owns those
                    # extension keys (extra_fields), reads from this snapshot so its
                    # content isn't lost to the earlier strip.
                    "_pristine_item": copy.deepcopy(raw_item),
                    "_items_write_policy": items_write_policy,
                    **(processing_context or {}),
                    # G1 (#1457): inject the pre-resolved access envelope so
                    # AccessEnvelopeSidecar.prepare_upsert_payload can build the
                    # sub-table row.  None when the collection does not use an
                    # access-aware driver; the sidecar skips the write in that case.
                    **({"_access_envelope": _batch_access_envelope}
                       if _batch_access_envelope is not None else {}),
                }
                # Pre-resolve external_id from the inbound feature before any
                # sidecar runs so PG sidecars and the index-stamp path share
                # one consistent value derived from the INBOUND item (not the
                # post-read-back geoid-bearing result).
                if items_write_policy is not None:
                    _ext = items_write_policy.resolve_external_id(raw_item)
                    if _ext is not None:
                        item_context["external_id"] = _ext
                hub_payload: Dict[str, Any] = {
                    "geoid": geoid,
                    "transaction_time": datetime.now(timezone.utc),
                    "deleted_at": None,
                }
                sidecar_payloads: Dict[str, Dict[str, Any]] = {}
                partition_values: Dict[str, Any] = {}

                for sidecar in sidecars_ordered:
                    val_result = sidecar.validate_insert(raw_item, item_context)
                    if not val_result.valid:
                        raise ValueError(
                            f"Sidecar {sidecar.sidecar_id} rejected item: {val_result.error}"
                        )
                    sc_payload = sidecar.prepare_upsert_payload(raw_item, item_context)
                    if sc_payload:
                        sidecar_payloads[sidecar.sidecar_id] = sc_payload
                        for pk in sidecar.get_partition_keys():
                            if pk in sc_payload:
                                partition_values[pk] = sc_payload[pk]
                                item_context[pk] = sc_payload[pk]

                hub_payload.update(partition_values)
                if col_config.partitioning.enabled and partition_values:
                    # Single-level partitioning today — first value is the partition key.
                    unique_partition_values.add(next(iter(partition_values.values())))

                # geometry_hash is now PG-generated on the geometries sidecar
                # (issue #220) — STORED column ``encode(digest(ST_AsBinary(geom),
                # 'sha256'), 'hex')``.  No longer carried on the hub row; the
                # matcher JOINs the sidecar to read it.

                prepared.append({
                    "geoid": geoid,
                    "hub_payload": hub_payload,
                    "sidecar_payloads": sidecar_payloads,
                    "item_context": item_context,
                })
            except ValueError as exc:
                # One bad row → one rejection, not a dead batch. ``record`` is
                # carried so the ingestion job can report the failed row's
                # attributes; the HTTP drain ignores the extra key.
                props = raw_item.get("properties") if isinstance(raw_item, dict) else None
                ext = None
                if isinstance(props, dict):
                    ext = props.get("external_id") or props.get("id")
                if ext is None and isinstance(raw_item, dict):
                    ext = raw_item.get("id")
                prep_rejections.append({
                    "geoid": None,
                    "external_id": ext,
                    "sidecar_id": None,
                    "matcher": None,
                    "reason": "validation_error",
                    "message": str(exc),
                    "record": raw_item,
                })

        # Phase 3 — pre-create unique partitions on a brief DDL conn so the
        # write tx isn't holding a write lock during DDL coordination.
        # Prefer self.engine (fresh pool connection) over engine so DDL runs in
        # its own independent transaction instead of a SAVEPOINT on the outer
        # connection. A DDL failure inside a SAVEPOINT poisons the outer
        # connection's asyncpg wire state even after rollback, causing
        # InFailedSQLTransactionError on the next SAVEPOINT open (Phase 4).
        if col_config.partitioning.enabled and unique_partition_values:
            async with managed_transaction(self.engine or engine) as ddl_conn:
                for p_val in unique_partition_values:
                    await self.ensure_partition_exists(
                        catalog_id, collection_id, col_config, p_val,
                        ctx=DriverContext(db_resource=ddl_conn),
                    )

        # Phase 4 — chunked writes. Each chunk commits its own tx; row locks
        # are released between chunks instead of accumulating across the whole
        # ingestion. Per-collection knob via `CollectionPluginConfig.ingest_chunk_size`
        # (default 50 — safe for geometry-heavy payloads; lightweight
        # attribute-only collections can raise it).
        chunk_size = collection_config.ingest_chunk_size
        write_results: List[Dict[str, Any]] = []
        # Per-item generated info for the ingestion audit report, built 1:1
        # with ``write_results`` (and therefore with the ``results`` read-back
        # below). Carries the persisted geoid, the source external_id, the
        # asset_id, and every computed statistic — the full set, including
        # statistics the collection's read policy does not ``expose``. Surfaced
        # to the caller via ``ctx.extensions`` (the ``_rejections`` escape-hatch
        # precedent), so no protocol / read-policy / new-method change is
        # needed. ``stat_names`` is the COLUMNAR allow-list: stored statistic
        # fields from the write policy (the SSOT for computed fields).
        from dynastore.modules.storage.computed_fields import _STATISTIC_STORAGE_KINDS
        stat_names = {
            cf.resolved_name
            for cf in (getattr(items_write_policy, "compute", None) or [])
            if getattr(cf, "kind", None) in _STATISTIC_STORAGE_KINDS
            and getattr(cf, "storage_mode", None) is not None
        }
        generated_stats: List[Dict[str, Any]] = []
        # Per-row rejection collector. SidecarRejectedError is raised by the
        # distributed upsert's acceptance check BEFORE any DB writes for the
        # offending item, so catching it inside the chunk loop does not poison
        # the enclosing transaction and sibling items in the same chunk still
        # commit. Rejections are surfaced to the caller via
        # ``ctx.extensions["_rejections"]`` so the HTTP layer can build a 207
        # ``IngestionReport`` instead of reporting a 500 on a single bad row.
        from dynastore.modules.storage.errors import SidecarRejectedError, ConflictError
        rejections: List[Dict[str, Any]] = []
        _use_batch_insert = (
            items_write_policy is not None
            and getattr(items_write_policy, "enable_batch_insert", False)
        )
        for start in range(0, len(prepared), chunk_size):
            chunk = prepared[start:start + chunk_size]

            # ── Batch fast path ────────────────────────────────────────────
            # Attempt a multi-row INSERT when enable_batch_insert is True and
            # the chunk has more than one plan.  The batch method opens its own
            # managed_transaction; if it raises (geometry error, constraint
            # violation, etc.) the transaction has already rolled back and we
            # fall through to the per-row path which opens a fresh one.
            # ConflictError (REFUSE_FAIL / refuse_batch) must propagate
            # immediately — same as the per-row path.
            if _use_batch_insert and len(chunk) > 1:
                _batch_ok = False
                try:
                    async with managed_transaction(engine) as conn:
                        _batch_rows, _batch_rejs = (
                            await self.batch_insert_or_update_distributed(
                                conn,
                                catalog_id,
                                collection_id,
                                chunk,
                                col_config=col_config,
                                sidecars=sidecars,
                                write_policy=items_write_policy,
                            )
                        )
                    rejections.extend(_batch_rejs)
                    for plan, row in zip(chunk, _batch_rows):
                        if row is None:
                            continue  # rejected — already in _batch_rejs
                        write_results.append(row)
                        item_ctx = plan["item_context"]
                        hub_pl = plan["hub_payload"]
                        generated_stats.append({
                            "geoid": row.get("geoid", plan["geoid"]),
                            "external_id": item_ctx.get("external_id"),
                            "asset_id": item_ctx.get("asset_id"),
                            "transaction_time": hub_pl.get("transaction_time"),
                            "deleted_at": hub_pl.get("deleted_at"),
                            "validity": hub_pl.get("validity"),
                            "stats": _collect_generated_stats(
                                plan["sidecar_payloads"], stat_names
                            ),
                        })
                    _batch_ok = True
                except ConflictError:
                    raise
                except Exception as _batch_exc:
                    logger.warning(
                        "Batch insert fell back to per-row for chunk [%d:%d]: %s",
                        start, start + chunk_size, _batch_exc,
                    )
                if _batch_ok:
                    continue

            # ── Per-row path (unchanged) ───────────────────────────────────
            async with managed_transaction(engine) as conn:
                for plan in chunk:
                    try:
                        new_row = await self.insert_or_update_distributed(
                            conn,
                            catalog_id,
                            collection_id,
                            plan["hub_payload"],
                            plan["sidecar_payloads"],
                            col_config=col_config,
                            sidecars=sidecars,
                            processing_context=plan["item_context"],
                        )
                    except SidecarRejectedError as rej:
                        rejections.append({
                            "geoid": str(rej.geoid) if rej.geoid else plan["geoid"],
                            "external_id": rej.external_id,
                            "sidecar_id": rej.sidecar_id,
                            "matcher": rej.matcher,
                            "reason": rej.reason,
                            "message": str(rej),
                            # Original feature for job-side failure reporting;
                            # the HTTP rejection drain ignores the extra key.
                            "record": plan["item_context"].get("_raw_item"),
                        })
                        continue
                    if new_row is None:
                        logger.error(
                            f"FATAL: insert_or_update_distributed returned None for geoid: {plan['geoid']}"
                        )
                        raise RuntimeError(
                            f"Failed to upsert item. Geoid: {plan['geoid']}"
                        )
                    write_results.append(new_row)
                    # Built in lockstep with write_results so it stays aligned
                    # with the order-preserving read-back below. The geoid is
                    # the persisted one (an identity match updates an existing
                    # row, whose geoid differs from the freshly minted plan
                    # geoid); external_id / asset_id were stamped onto the
                    # shared item_context by the sidecars during Phase 2.
                    item_ctx = plan["item_context"]
                    hub_payload = plan["hub_payload"]
                    generated_stats.append({
                        "geoid": new_row.get("geoid", plan["geoid"]),
                        "external_id": item_ctx.get("external_id"),
                        "asset_id": item_ctx.get("asset_id"),
                        # Lifecycle fields from the hub row. ``validity`` is
                        # present only when the collection's temporal sidecar
                        # contributes it as a partition key (see Phase 2 merge
                        # of ``partition_values`` into ``hub_payload``); a
                        # ``None`` here is dropped by ``_build_report_envelope``.
                        "transaction_time": hub_payload.get("transaction_time"),
                        "deleted_at": hub_payload.get("deleted_at"),
                        "validity": hub_payload.get("validity"),
                        "stats": _collect_generated_stats(
                            plan["sidecar_payloads"], stat_names
                        ),
                    })

        # Hand rejections to the caller via the typed DriverContext escape
        # hatch. The OGC mixin seeds an empty list before the call and drains
        # this key after so rejections and accepted rows can be combined into
        # a single 207 IngestionReport. Phase 2 preparation rejections (bad
        # value / sidecar pre-check) precede Phase 4 acceptance rejections.
        all_rejections = prep_rejections + rejections
        if ctx is not None and all_rejections:
            ctx.extensions["_rejections"] = all_rejections

        # Phase 5 — bulk read-back on a fresh conn, post-commit. One SELECT
        # for the whole batch (vs N inside the write tx). Preserves input
        # order so callers can correlate results 1:1 with the request.
        result_geoids = [r["geoid"] for r in write_results]
        if not phys_schema or not phys_table:
            raise RuntimeError("Physical schema/table unavailable for bulk read-back")
        async with managed_transaction(engine) as read_conn:
            results = await self.fetch_features_bulk(
                read_conn, phys_schema, phys_table, result_geoids, col_config,
                catalog_id=catalog_id, collection_id=collection_id,
            )

        # Publish per-item generated info (geoid / external_id / asset_id and
        # every computed statistic) to the caller via the typed DriverContext
        # escape hatch — the same mechanism as ``_rejections`` above. The list
        # is aligned 1:1 with the returned ``results`` (both follow
        # ``write_results`` order). The ingestion task drains this to enrich its
        # audit report with the full computed set; every other caller ignores
        # it. Guarded by length so a read-back that drops a row degrades to no
        # enrichment rather than mis-zipped stats.
        if ctx is not None and generated_stats and len(generated_stats) == len(results):
            ctx.extensions["_generated_stats"] = generated_stats

        # ── Post-commit: fan-out to secondary drivers ──────────────────
        if results:
            await self._fan_out_to_secondary_drivers(
                catalog_id, collection_id, results
            )

        # ── Post-commit: dispatcher fan-out for index propagation ──────
        # Single call site replaces the per-driver ``_on_item_upsert``
        # event listeners.  Phase 2f: pass the engine so the dispatcher
        # can open its own wrapping TX for atomic OUTBOX enqueue.
        if results:
            # Per-item external_id resolved from the INBOUND items (built 1:1
            # with results in ``generated_stats``, keyed by the persisted geoid
            # so an identity-match update maps correctly). Passing it makes the
            # index stamp use the same value PG stored — one external_id for
            # every driver, independent of path-vs-default and the geoid swap.
            _ext_by_geoid = {
                str(gs["geoid"]): str(gs["external_id"])
                for gs in generated_stats
                if gs.get("geoid") is not None and gs.get("external_id") is not None
            }
            # Return value is logged by the dispatcher — callers of upsert_bulk
            # that need per-indexer health must call _dispatch_index_upsert
            # directly and inspect the returned dict.
            await self._dispatch_index_upsert(
                catalog_id, collection_id, results,
                db_resource=engine,
                processing_context=processing_context,
                external_id_by_id=_ext_by_geoid or None,
            )

        # ── Post-commit: write-reactive tile-cache invalidation (#1292 / #1297 Phase 2b) ──
        # Mark stale the tiles the new feature bboxes touch, and also the tiles
        # the old footprint used to occupy (prior_bboxes_b — the geometry-MOVE
        # case). No-op + degrade-safe; never blocks the write.
        if results:
            await self._dispatch_tile_cache_invalidation(
                catalog_id, collection_id, results, db_resource=engine,
                processing_context=processing_context,
                prior_bboxes=prior_bboxes_b or None,
            )

        # ── Post-commit: emit events for non-indexer subscribers ──────
        if results:
            try:
                from dynastore.models.protocols.event_bus import EventBusProtocol
                from dynastore.modules.catalog.event_service import CatalogEventType
                events_protocol = get_protocol(EventBusProtocol)
                if events_protocol:
                    if is_single:
                        await events_protocol.emit(
                            event_type=CatalogEventType.ITEM_CREATION,
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            item_id=str(results[0].id) if results[0].id else None,
                            payload=results[0].model_dump(by_alias=True, exclude_unset=True)
                        )
                    else:
                        await events_protocol.emit(
                            event_type=CatalogEventType.BULK_ITEM_CREATION,
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            payload={"count": len(results), "items_subset": [r.model_dump(by_alias=True, exclude_none=True) for r in results[:10]]}
                        )
            except Exception as e:
                logger.warning(f"Failed to emit item creation events: {e}")

        # Single-item callers expect a bare row or None (when the sole item
        # was rejected by the write policy); bulk callers always get a list,
        # possibly empty if every item was rejected.
        if is_single:
            return results[0] if results else None
        return results

    # ------------------------------------------------------------------
    # Index dispatcher fan-out — replaces ES event-driven listeners
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_by_path(doc: Dict[str, Any], field_path: Optional[str]) -> Any:
        """Read a dotted ``field_path`` (e.g. ``properties.CODE``) out of a doc."""
        if not field_path:
            return None
        node: Any = doc
        for part in field_path.split("."):
            if not isinstance(node, dict):
                return None
            node = node.get(part)
            if node is None:
                return None
        return node

    async def _resolve_external_id_path(
        self, catalog_id: str, collection_id: str,
    ) -> Optional[str]:
        """Resolve the ``ItemsWritePolicy.external_id_path`` for a collection."""
        from dynastore.modules.storage.driver_config import ItemsWritePolicy

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return None
        try:
            policy = await configs.get_config(
                ItemsWritePolicy,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception:
            return None
        getter = getattr(policy, "external_id_path", None)
        try:
            path = getter() if callable(getter) else None
        except Exception:
            return None
        return str(path) if path else None

    async def _resolve_access_envelope(
        self,
        catalog_id: str,
        collection_id: str,
        processing_context: Optional[Dict[str, Any]],
        feature: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Resolve the access-envelope stamping values for an index dispatch.

        Returns ``{_visibility, _owner, _attrs}`` to stamp onto the index
        payload, or ``None`` when the collection does NOT route to an
        access-aware driver (``applies_access_filter=True``) — nothing is
        stamped in that case.

        Sources:

        * ``_owner``      ← creating principal's subject id from
          ``processing_context`` (``owner`` / ``principal_id`` /
          ``subject_id``).
        * ``_visibility`` ← catalog's ``CatalogLookupAudience.is_public``
          (``"public"`` / ``"private"``). Defaults to ``"private"``.
        * ``_attrs``      ← per-collection ``AttributeStampingPolicy``.
          When the policy is absent or ``attribute_paths`` is empty the key is
          omitted entirely (behaviour unchanged vs. pre-#1441).  Only
          ``$.properties.<field>`` paths are resolved in this slice.
        """
        if not await self._collection_uses_access_aware_driver(
            catalog_id, collection_id,
        ):
            return None

        pc = processing_context or {}
        owner = pc.get("owner") or pc.get("principal_id") or pc.get("subject_id")

        visibility = "private"  # closed default for the tenant-isolated index
        try:
            from dynastore.modules.iam.audience_configs import CatalogLookupAudience

            configs = get_protocol(ConfigsProtocol)
            if configs is not None:
                audience = await configs.get_config(
                    CatalogLookupAudience, catalog_id=catalog_id,
                )
                if audience is not None and getattr(audience, "is_public", False):
                    visibility = "public"
        except Exception:
            # Missing/unavailable audience config → keep the closed default.
            pass

        envelope: Dict[str, Any] = {
            "_visibility": visibility,
            "_owner": str(owner) if owner is not None else None,
        }

        # --- Attribute stamping (#1441) -----------------------------------
        attrs = await self._stamp_attrs(
            catalog_id, collection_id, feature or pc,
        )
        if attrs:
            envelope["_attrs"] = attrs
        # ------------------------------------------------------------------

        return envelope

    async def _stamp_attrs(
        self,
        catalog_id: str,
        collection_id: str,
        source: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Extract per-collection attribute values from ``source`` for ``_attrs``.

        Reads :class:`~dynastore.modules.iam.stamping_config.AttributeStampingPolicy`
        for the collection. For each declared path (only ``$.properties.<key>``
        is supported in this slice) extracts the value from ``source`` (a raw
        Feature dict or processing context dict).  Returns an empty dict when
        the policy is absent or ``attribute_paths`` is empty.
        """
        try:
            from dynastore.modules.iam.stamping_config import (
                AttributeStampingPolicy,
                stamp_attrs_from_feature,
            )

            configs = get_protocol(ConfigsProtocol)
            if configs is None:
                return {}
            policy = await configs.get_config(
                AttributeStampingPolicy,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            if policy is None:
                return {}
            paths: Dict[str, str] = getattr(policy, "attribute_paths", {}) or {}
            if not paths:
                return {}
        except Exception:
            return {}

        return stamp_attrs_from_feature(source, paths)

    async def _collection_uses_access_aware_driver(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> bool:
        """True when any WRITE driver for the collection opts in to row-level ABAC.

        Two detection branches:

        1. **ES-envelope** — the driver class carries ``applies_access_filter=True``
           (the standardised attribute set by the private Elasticsearch driver).
        2. **PG-sidecar** — the driver exposes a ``get_driver_config`` method (PG
           drivers) and its per-collection config lists a sidecar with
           ``sidecar_type == "access_envelope"`` (#1457 G4).

        Returns ``False`` on any resolution error so a misconfigured collection
        never gets access fields stamped.
        """
        try:
            from dynastore.modules.storage.router import get_write_drivers

            resolved = await get_write_drivers(catalog_id, collection_id)
        except Exception:
            return False

        for r in resolved:
            # Branch 1: ES-envelope driver (applies_access_filter class attr).
            if getattr(type(r.driver), "applies_access_filter", False):
                return True

            # Branch 2: PG sidecar with sidecar_type == "access_envelope" (G4).
            get_cfg = getattr(r.driver, "get_driver_config", None)
            if callable(get_cfg):
                try:
                    from dynastore.modules.storage.drivers.pg_sidecars import (
                        driver_sidecars,
                    )

                    drv_cfg = await get_cfg(catalog_id, collection_id)  # type: ignore[misc]
                    if any(
                        getattr(sc, "sidecar_type", None) == "access_envelope"
                        for sc in driver_sidecars(drv_cfg)
                    ):
                        return True
                except Exception:
                    pass  # fail-open for this branch; ES check already handled above

        return False

    async def _resolve_index_stamp_context(
        self,
        catalog_id: str,
        collection_id: str,
        processing_context: Optional[Dict[str, Any]],
    ) -> _IndexStampContext:
        """Resolve the identity + access-envelope stamping inputs once.

        Single derivation point shared by the read-back index dispatch
        (:meth:`_dispatch_index_upsert`) and the atomic OUTBOX bulk path
        (:meth:`upsert_bulk`) so a collection populated via either path indexes
        the same canonical ``_external_id`` / ``_asset_id`` — and, when it
        routes WRITE to an access-aware driver, the same ``_visibility`` /
        ``_owner`` / ``_attrs``. See #1287 and #1441.
        """
        return _IndexStampContext(
            external_id_path=await self._resolve_external_id_path(
                catalog_id, collection_id,
            ),
            asset_id=(processing_context or {}).get("asset_id"),
            access_envelope=await self._resolve_access_envelope(
                catalog_id, collection_id, processing_context,
            ),
            external_id=(processing_context or {}).get("external_id"),
        )

    def _apply_index_stamp(
        self,
        payload: Dict[str, Any],
        ctx: _IndexStampContext,
    ) -> Dict[str, Any]:
        """Stamp canonical identity + access-envelope fields onto ``payload``.

        Mutates and returns ``payload`` — which must be the index/outbox record
        body, never the API-returned Feature nor the primary-store row.
        ``_external_id`` is overwritten from the resolved path; every other
        field uses ``setdefault`` so an explicit value already on the payload
        wins. The ES read path strips these internal ``_*`` keys, so they never
        surface to clients.

        ``_attrs`` is stamped when the collection has an
        :class:`~dynastore.modules.iam.stamping_config.AttributeStampingPolicy`
        with a non-empty ``attribute_paths`` map.
        """
        if ctx.external_id is not None:
            payload["_external_id"] = str(ctx.external_id)
        elif ctx.external_id_path:
            ext = self._extract_by_path(payload, ctx.external_id_path)
            if ext is not None:
                payload["_external_id"] = str(ext)
        if ctx.asset_id is not None:
            payload.setdefault("_asset_id", str(ctx.asset_id))
        if ctx.access_envelope is not None:
            # Stamp only the access fields with a resolved value; ``_owner`` may
            # be ``None`` (no principal in context) — omit it then so the doc
            # builder simply does not set ``owner``.
            payload.setdefault("_visibility", ctx.access_envelope["_visibility"])
            if ctx.access_envelope["_owner"] is not None:
                payload.setdefault("_owner", ctx.access_envelope["_owner"])
            # ``_attrs`` from the per-collection stamping policy (#1441).
            attrs = ctx.access_envelope.get("_attrs")
            if attrs:
                payload.setdefault("_attrs", attrs)
        return payload

    async def _dispatch_index_upsert(
        self,
        catalog_id: str,
        collection_id: str,
        results: List["Feature"],
        *,
        db_resource: Optional[DbResource] = None,
        processing_context: Optional[Dict[str, Any]] = None,
        external_id_by_id: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Fan items out to every configured Indexer for the collection.

        Single dispatcher call site for items, replacing the per-driver
        ``_on_item_upsert`` event listeners.  Driver-agnostic: ES public,
        ES private, vector DB, audit log — anything implementing the slim
        :class:`Indexer` Protocol and pinned as a secondary-index
        ``WRITE`` entry (``secondary_index=True``) in ``operations[WRITE]``.

        Failure surfaces are decided by routing-config ``on_failure`` per
        entry — ``OUTBOX`` enqueues a durable retry row, ``WARN`` logs,
        ``FATAL`` raises out of this method.

        Phase 2f atomic OUTBOX: when an engine is supplied, the dispatch
        is wrapped in its own ``managed_transaction`` so ``ctx.pg_conn``
        is non-None.  The ``TaskTableOutboxWriter.enqueue`` INSERT and the
        indexer attempt then live in the same TX — outbox writes are
        durable instead of being skipped with a warning.  Cost: one extra
        TX open/commit per dispatch call (cheap vs the indexer
        round-trip).  Non-OUTBOX policies (FATAL, WARN, IGNORE) are
        unaffected by the wrapping TX since they don't write to the outbox
        table.

        Identity tracking fields (``_external_id`` derived per-item from the
        write policy's ``external_id_path``; ``_asset_id`` from the ingestion
        ``processing_context``) are stamped onto the index *payload only* — not
        the API-returned Feature — so every indexer (the public projection and
        the tenant-private doc builder both read ``_external_id`` / ``_asset_id``)
        carries the canonical envelope identity. The ES read path strips these
        internal ``_*`` fields, so they never surface to clients.

        ``external_id_by_id`` maps a persisted geoid → the external_id resolved
        from the *inbound* item (``ItemsWritePolicy.resolve_external_id``). It is
        the authoritative per-item override because ``results`` are the
        post-write read-back whose ``id`` is the geoid — re-deriving external_id
        from the result here would lose the inbound identity (and the no-path
        default ``id`` would resolve to the geoid, not the tenant's id). The PG
        distributed-upsert path builds this map (it has the inbound contexts);
        when absent, the stamp falls back to ``stamp_ctx`` (path extraction).
        """
        if not results:
            return {}

        from dynastore.models.protocols.indexer import IndexOp
        from dynastore.modules.storage.index_dispatcher import (
            get_index_dispatcher,
        )

        # Identity + access-envelope stamping seam (#1285/#1287): resolve once
        # per dispatch. ``access_envelope`` is ``None`` unless the collection
        # routes WRITE to an access-aware driver, so non-envelope collections
        # stamp nothing beyond the canonical identity and their stored docs are
        # unchanged. Shared with the atomic OUTBOX path (:meth:`upsert_bulk`) so
        # both index-write paths stamp identically.
        stamp_ctx = await self._resolve_index_stamp_context(
            catalog_id, collection_id, processing_context,
        )

        dispatcher = get_index_dispatcher()
        id_less = [r for r in results if r.id is None]
        if id_less:
            logger.error(
                "item_service._dispatch_index_upsert: %d result(s) have no id "
                "in catalog=%s collection=%s — these items WILL NOT be dispatched "
                "to any index driver and their index entries will not be created "
                "or updated.",
                len(id_less), catalog_id, collection_id,
            )
        ops = []
        for r in results:
            if r.id is None:
                continue
            payload = self._apply_index_stamp(
                r.model_dump(by_alias=True, exclude_none=True), stamp_ctx,
            )
            # Per-item external_id override: the resolved inbound value wins over
            # any stamp_ctx-derived one, because ``results`` carry the geoid as
            # ``id`` and cannot recover the inbound identity (esp. the no-path
            # default, which would otherwise resolve to the geoid).
            if external_id_by_id:
                _eid = external_id_by_id.get(str(r.id))
                if _eid is not None:
                    payload["_external_id"] = str(_eid)
            ops.append(IndexOp(
                op_type="upsert",
                entity_type="item",
                entity_id=str(r.id),
                payload=payload,
            ))
        if not ops:
            return {}

        engine = db_resource or self.engine
        if engine is None:
            # No engine available — degrade to non-atomic enqueue (matches
            # pre-Phase-2f behaviour).  Outbox writer logs its own warning
            # when ctx.pg_conn is None.
            return await self._do_dispatch(
                dispatcher, catalog_id, collection_id, ops, pg_conn=None,
            )

        # Inside a task/job run the ASYNC secondary write is absorbed inline
        # (see IndexDispatcher.fan_out_bulk). Do NOT wrap that inline ES
        # fan-out in a single wrapping transaction: it would park one pooled
        # connection with an open transaction for the whole sequential
        # dispatch, re-introducing the DB-pool / idle-in-transaction pressure
        # this inline path exists to relieve. Instead hand the dispatcher a
        # transaction factory so it opens a SHORT transaction per chunk —
        # the connection is held only across one chunk's dispatch (kept open
        # so an on-failure outbox enqueue stays durable) and released between
        # chunks.
        if in_task_run():
            return await self._do_dispatch(
                dispatcher, catalog_id, collection_id, ops,
                pg_conn=None,
                tx_factory=lambda: managed_transaction(engine),
            )

        # Serving path — Phase 2f: open a single wrapping TX so the outbox
        # INSERT (if any) is atomic with the indexer attempt.
        async with managed_transaction(engine) as conn:
            return await self._do_dispatch(
                dispatcher, catalog_id, collection_id, ops, pg_conn=conn,
            )

    async def _do_dispatch(
        self,
        dispatcher: Any,
        catalog_id: str,
        collection_id: str,
        ops: List[Any],
        *,
        pg_conn: Optional[DbResource],
        tx_factory: Optional[Callable[[], Any]] = None,
    ) -> Dict[str, Any]:
        """Inner dispatch helper — split out so the wrapping TX in
        :meth:`_dispatch_index_upsert` is a single ``async with`` block.

        ``tx_factory``, when supplied, is a zero-arg callable returning an
        ``async with``-able transaction (used by the in-task-run inline path
        so the dispatcher opens a fresh short transaction per chunk instead
        of running under one long-lived ``pg_conn``).

        Returns the per-indexer :class:`BulkResult` dict from
        :meth:`~IndexDispatcher.fan_out_bulk` so callers can inspect
        secondary-index health without additional round-trips.
        """
        from dynastore.models.protocols.indexer import IndexContext
        from dynastore.modules.storage.index_dispatcher import IndexerFatal
        from dynastore.tools.correlation import get_correlation_id

        ctx = IndexContext(
            catalog=catalog_id,
            collection=collection_id,
            correlation_id=get_correlation_id() or "",
            pg_conn=pg_conn,
            entity_type="item",
        )
        try:
            return await dispatcher.fan_out_bulk(ctx, ops, tx_factory=tx_factory)
        except IndexerFatal:
            # FATAL contract: a routing entry with on_failure=FATAL
            # MUST propagate so the caller's TX rolls back.  Don't
            # swallow.
            raise
        except Exception as e:  # noqa: BLE001 — non-FATAL paths absorb errors
            logger.warning(
                "Index dispatcher fan-out failed for %s/%s (%d items): %s",
                catalog_id, collection_id, len(ops), e,
            )
            return {}

    async def _dispatch_tile_cache_invalidation(
        self,
        catalog_id: str,
        collection_id: str,
        results: List["Feature"],
        *,
        db_resource: Optional[DbResource] = None,
        prior_bboxes: Optional[Sequence[Tuple[float, float, float, float]]] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write-reactive tile-cache invalidation on create/update (#1292/#1298).

        For each batch of written features, enqueue a ``tiles_invalidate`` task
        with the batch's per-feature bboxes. That task runs IN-PROCESS as a
        background task on the catalog service and deletes the cached tiles the
        new bboxes make stale (across the served zoom range, coalesced + capped);
        the next read repopulates lazily. The heavy seed/renew path stays on the
        ``tiles_preseed`` Cloud Run Job.

        Both call sites run this AFTER the data write has committed (the
        feature rows are already durable), so the task row is independent —
        nothing to be atomic with. The enqueue needs the DB engine and the
        catalog's tenant physical schema to INSERT the task row; both are
        resolved here from the in-scope ``db_resource``/``self.engine`` and
        ``CatalogsProtocol.resolve_physical_schema`` (the same pieces the
        index-dispatch path resolves).

        Capability-gated and degrade-safe inside
        ``enqueue_tile_invalidation_task`` — it is a no-op when no tile reader /
        cache store is present, and never raises out, so a missing or
        misconfigured cache can never block a write. The ``dedup_key`` embeds a
        coverage signature, so it coalesces only an already-queued invalidate
        task whose coverage is identical (safe); an edit at a distinct bbox gets
        its own task and is never dropped. ``results`` carries the NEW bboxes
        (create/update); ``prior_bboxes`` carries pre-write extents a feature
        used to occupy — the DELETE case (``results`` empty) and the
        geometry-MOVE case (#1297) — unioned into the same coverage so the old
        footprint's tiles are dropped too.
        """
        if not results and not prior_bboxes:
            return
        if (processing_context or {}).get("defer_tile_invalidation"):
            # The caller (bulk ingestion) suppresses per-batch invalidation and
            # enqueues ONE coalesced tiles_invalidate for the whole ingested
            # extent when it finishes — otherwise a large ingestion spawns one
            # task per write batch (hundreds of redundant invalidations).
            return
        from dynastore.modules.tiles.tile_cache_sync import (
            enqueue_tile_invalidation_task,
        )

        try:
            engine = db_resource or self.engine
            if engine is None:
                logger.debug(
                    "tile_cache: no engine available; skipping invalidation "
                    "enqueue for %s/%s", catalog_id, collection_id,
                )
                return
            schema = await self._resolve_physical_schema(
                catalog_id, db_resource=engine,
            )
            if schema is None:
                logger.debug(
                    "tile_cache: cannot resolve physical schema for %s; "
                    "skipping invalidation enqueue", catalog_id,
                )
                return
            # Attribute the enqueued task to the originating principal when
            # the write path supplied one (matches the same sources used by
            # ``_resolve_access_envelope`` — owner / principal_id / subject_id).
            # Falls back to ``"system:tile_cache_invalidation"`` inside
            # ``enqueue_tile_invalidation_task`` so audit queries on
            # ``caller_id IS NULL`` stay meaningful.
            pc = processing_context or {}
            caller_id = (
                pc.get("caller_id")
                or pc.get("principal_id")
                or pc.get("subject_id")
                or pc.get("owner")
            )
            await enqueue_tile_invalidation_task(
                catalog_id, collection_id, results,
                engine=engine, schema=schema,
                prior_bboxes=prior_bboxes,
                caller_id=caller_id,
            )
        except Exception as exc:  # noqa: BLE001 — invalidation never breaks a write
            logger.warning(
                "tile_cache: invalidation dispatch failed for %s/%s: %s",
                catalog_id, collection_id, exc,
            )

    # ------------------------------------------------------------------
    # Atomic bulk upsert — Phase 9 outbox-aware path
    # ------------------------------------------------------------------

    async def upsert_bulk(
        self,
        catalog_id: str,
        collection_id: str,
        items: List[Dict[str, Any]],
        *,
        db_resource: Optional[DbResource] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Atomic bulk upsert with same-item coalescing + outbox enqueue.

        Opens ONE wrapping TX and performs three actions inside it:

        1. **Coalesce** input items by ``id`` (latest wins). Three ops on
           the same id collapse to a single PG row + a single outbox row,
           halving outbox volume on hot-update collections without losing
           fidelity (a consumer applying the original ops in order would
           converge on the same final state).
        2. **Run FATAL routing entries inline** — every entry under
           ``ItemsRoutingConfig.operations[WRITE]`` with
           ``on_failure=FATAL`` and ``write_mode=SYNC`` writes through
           ``CollectionItemsStore.write_entities`` on the wrapping conn.
           Any driver failure raises out of the TX, rolling everything
           back.
        3. **Enqueue outbox rows for ASYNC OUTBOX entries** — every
           secondary-index ``WRITE`` entry (``secondary_index=True``) in
           ``operations[WRITE]`` with ``write_mode=ASYNC`` and
           ``on_failure=OUTBOX`` gets one outbox row per coalesced item,
           via ``OutboxStore.enqueue_bulk(conn, ...)`` on the SAME conn.
           A failed enqueue rolls back the PG write — the central
           guarantee that closes the PG/secondary-index drift hole.

        Bypasses :class:`IndexDispatcher` for the OUTBOX-enqueue case
        (we already have the routing in hand and want to share the conn);
        the dispatcher's missing-indexer path remains the entry point for
        per-call ops without explicit routing knowledge. ``WARN`` /
        ``IGNORE`` secondary-index entries are not enqueued here — they're
        tolerant by design and are still handled by the post-commit
        dispatcher path on the legacy :meth:`upsert` flow.

        Each OUTBOX record's payload carries the same canonical identity +
        access-envelope stamping as the read-back dispatch path (#1287):
        ``_external_id`` from the write policy's ``external_id_path``,
        ``_asset_id`` from ``processing_context``, and — when the collection
        routes WRITE to an access-aware driver — ``_visibility`` / ``_owner``.
        Stamping is applied to a per-record copy, so the
        inline FATAL ``write_entities`` (primary store) write still receives the
        unstamped items.

        Test injection seams (set on the instance, not the constructor):
        ``_test_routing_resolver``, ``_test_driver_registry``,
        ``_test_managed_transaction``. When None, the method resolves through
        the production helpers. The async-OUTBOX enqueue always goes through
        the module-level ``enqueue_storage_op`` (#1807 P4), which tests patch
        directly — there is no separate outbox-store seam.
        """
        from dynastore.models.protocols.indexing import OutboxRecord
        from dynastore.modules.storage.driver_instance_id import (
            compute_driver_instance_id,
        )
        from dynastore.modules.storage.routing_config import (
            FailurePolicy, Operation, WriteMode,
        )
        from dynastore.tools.identifiers import generate_uuidv7

        if not items:
            return []

        # ── Coalesce same-item ops (latest wins) ──────────────────────
        coalesced: Dict[str, Dict[str, Any]] = {}
        no_id: List[Dict[str, Any]] = []
        for it in items:
            iid = it.get("id") if isinstance(it, dict) else None
            if iid is None:
                no_id.append(it)
            else:
                coalesced[str(iid)] = it
        deduped: List[Dict[str, Any]] = list(coalesced.values()) + no_id

        # ── Resolve routing (test seam first, prod helper fallback) ───
        routing_resolver = getattr(self, "_test_routing_resolver", None)
        if routing_resolver is not None:
            routing = await routing_resolver(catalog_id, collection_id)
        else:
            from dynastore.models.protocols import ConfigsProtocol
            from dynastore.modules.storage.routing_config import (
                ItemsRoutingConfig,
            )
            configs = get_protocol(ConfigsProtocol)
            assert configs is not None, "ConfigsProtocol not registered"
            routing = await configs.get_config(
                ItemsRoutingConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )

        ops_map = getattr(routing, "operations", {}) or {}
        from dynastore.modules.storage.routing_config import (
            secondary_index_entries,
        )
        write_entries = list(ops_map.get(Operation.WRITE, []))

        fatal_entries = [
            e for e in write_entries
            if e.on_failure == FailurePolicy.FATAL
            and e.write_mode == WriteMode.SYNC
        ]
        async_outbox_entries = secondary_index_entries(
            ops_map, async_outbox_only=True,
        )

        # ── Resolve driver registry (test seam first) ─────────────────
        registry = getattr(self, "_test_driver_registry", None)

        async def _resolve_driver(driver_id: str):
            if registry is not None:
                return await registry(driver_id)
            # Production: look up CollectionItemsStore impls by snake_case
            # driver_id from the central DriverRegistry — the same store
            # the routing resolver pins WRITE entries against.
            from dynastore.modules.storage.driver_registry import (
                DriverRegistry,
            )
            return DriverRegistry.get_collection(driver_id)

        # ── Resolve TX manager (test seam first) ──────────────────────
        tx_manager = getattr(
            self, "_test_managed_transaction", managed_transaction,
        )
        engine = db_resource or self.engine
        if engine is None:
            raise RuntimeError(
                "upsert_bulk requires a DbResource (constructor engine or "
                "explicit db_resource kwarg)."
            )

        # ── Wrapping TX: PG writes + outbox enqueue commit together ──
        async with tx_manager(engine) as conn:
            # FATAL entries: bulk-write inline through each driver.
            for entry in fatal_entries:
                driver = await _resolve_driver(entry.driver_ref)
                if driver is None:
                    from dynastore.modules.db_config.exceptions import (
                        ConfigResolutionError,
                    )
                    raise ConfigResolutionError(
                        (
                            f"FATAL routing entry '{entry.driver_ref}' has no "
                            f"registered driver — upsert_bulk cannot proceed."
                        ),
                        missing_key=entry.driver_ref,
                        required_fields=[],
                        scope_tried=["driver_registry"],
                        hint=(
                            "Either register the driver in the SCOPE or "
                            "downgrade the routing entry to WARN/OUTBOX."
                        ),
                    )
                await driver.write_entities(
                    catalog_id, collection_id, deduped,
                    db_resource=conn,
                )

            # ASYNC OUTBOX entries: build OutboxRecords + enqueue on the
            # SAME conn. Failure here rolls back the PG write above — the
            # atomicity guarantee that closes the drift hole.
            if async_outbox_entries:
                # Resolve the identity + access-envelope stamping inputs once
                # for this collection (#1287). Applied to a per-record copy
                # below so the inline FATAL primary write above keeps the
                # unstamped items.
                stamp_ctx = await self._resolve_index_stamp_context(
                    catalog_id, collection_id, processing_context,
                )
                # Resolve the write policy once for per-item external_id
                # resolution. When no policy is configured, this is None and
                # per-item resolution below is skipped. Failure is intentionally
                # swallowed — missing write-policy means no external_id stamp,
                # not a crash.
                _write_policy = None
                try:
                    _bulk_configs = get_protocol(ConfigsProtocol)
                    if _bulk_configs is not None:
                        _write_policy = await _bulk_configs.get_config(
                            ItemsWritePolicy,
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                        )
                except Exception:
                    _write_policy = None
                records: List[OutboxRecord] = []
                for entry in async_outbox_entries:
                    inst = compute_driver_instance_id(
                        entry.driver_ref, catalog_id, collection_id,
                    )
                    for it in deduped:
                        item_id = it.get("id") if isinstance(it, dict) else None
                        item_id_str = str(item_id) if item_id is not None else None
                        payload_copy = dict(it)
                        # Per-item external_id resolution: inject _external_id
                        # from the inbound item so every OUTBOX record carries
                        # the correct identity even when stamp_ctx.external_id
                        # is None (batch-level context has no per-item value)
                        # and stamp_ctx.external_id_path is None (no path
                        # configured). When stamp_ctx already resolved a value
                        # via ctx.external_id or path extraction, _apply_index_stamp
                        # will overwrite this — the per-item pre-set only fills
                        # the gap for the no-path case.
                        if _write_policy is not None:
                            _item_ext = _write_policy.resolve_external_id(payload_copy)
                            if _item_ext is not None and "_external_id" not in payload_copy:
                                payload_copy["_external_id"] = _item_ext
                        records.append(OutboxRecord(
                            op_id=generate_uuidv7(),
                            driver_id=entry.driver_ref,
                            driver_instance_id=inst,
                            collection_id=collection_id,
                            op="upsert",
                            payload=self._apply_index_stamp(payload_copy, stamp_ctx),
                            item_id=item_id_str,
                            idempotency_key=item_id_str or str(generate_uuidv7()),
                        ))
                if records:
                    # Enqueue storage rows and drain trigger co-transactionally
                    # on the caller's conn so a primary-write rollback leaves no
                    # rows in tasks.storage either.
                    from dynastore.modules.storage.storage_emit import (
                        enqueue_storage_op,
                    )
                    await enqueue_storage_op(
                        conn,
                        catalog_id=catalog_id,
                        rows=records,
                    )

        return deduped

    # ------------------------------------------------------------------
    # Multi-driver write fan-out
    # ------------------------------------------------------------------

    async def _fan_out_to_secondary_drivers(
        self,
        catalog_id: str,
        collection_id: str,
        features: List[Feature],
        *,
        _primary_already_written: bool = True,
    ) -> None:
        """Fan-out writes to secondary drivers after the primary commit.

        ``_primary_already_written=True`` (default) skips position 0 (primary)
        since it was already written by the caller (Branch A or B in ``upsert``).

        Sync drivers run in parallel (``asyncio.gather``).  If any sync
        driver fails, drivers that succeeded and declare
        ``DriverCapability.TRANSACTIONAL`` are compensated (delete).

        Async drivers fire after the sync phase succeeds (fire-and-forget).
        """
        from dynastore.modules.concurrency import run_in_background
        from dynastore.modules.storage.router import get_write_drivers, ResolvedDriver
        from dynastore.modules.storage.routing_config import FailurePolicy, WriteMode

        try:
            resolved = await get_write_drivers(catalog_id, collection_id)
        except Exception:
            return  # no routing configured — nothing to fan out

        # Position 0 is the primary driver — already written by the caller.
        secondaries = resolved[1:] if _primary_already_written else resolved
        # Item-indexer drivers (the ES public/private drivers —
        # ``is_item_indexer=True``, pinned as ``secondary_index=True`` WRITE
        # entries) are owned by the index dispatcher
        # (``_dispatch_index_upsert`` → ``index_bulk``), which stamps the
        # canonical identity (``_external_id`` / ``_asset_id``) onto the index
        # payload. Fanning them out here as storage drivers too would
        # double-write the same index via ``write_entities`` — which rebuilds
        # the tenant doc from the read-back Feature (no identity, no context)
        # and races the dispatcher's stamped write (last-writer-wins drops the
        # identity fields, #1289). The dispatcher is their single write path
        # (role separation, #990); skip them here.
        secondaries = [
            r for r in secondaries
            if not getattr(type(r.driver), "is_item_indexer", False)
        ]
        if not secondaries:
            return

        sync_drivers = [r for r in secondaries if r.write_mode == WriteMode.SYNC]
        async_drivers = [r for r in secondaries if r.write_mode == WriteMode.ASYNC]

        # ── Sync phase: parallel writes ───────────────────────────────
        if sync_drivers:
            tasks = [
                r.driver.write_entities(catalog_id, collection_id, features)
                for r in sync_drivers
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            succeeded: List[ResolvedDriver] = []
            has_fatal = False
            first_fatal_exc: Optional[Exception] = None

            for r, result in zip(sync_drivers, results):
                if isinstance(result, BaseException):
                    if r.on_failure == FailurePolicy.FATAL:
                        has_fatal = True
                        if first_fatal_exc is None:
                            first_fatal_exc = result if isinstance(result, Exception) else RuntimeError(str(result))
                    elif r.on_failure == FailurePolicy.WARN:
                        logger.warning(
                            "Secondary sync driver '%s' write failed for %s/%s: %s",
                            r.driver_ref, catalog_id, collection_id, result,
                        )
                    # IGNORE: silent
                else:
                    succeeded.append(r)

            if has_fatal:
                # Compensate succeeded sync drivers that support rollback
                await self._compensate_drivers(
                    succeeded, catalog_id, collection_id, features
                )
                raise first_fatal_exc  # type: ignore[misc]

        # ── Async phase: fire-and-forget ──────────────────────────────
        # ``run_in_background`` holds a strong reference so the loop cannot
        # GC the task mid-execution and silently drop the secondary write.
        for r in async_drivers:
            run_in_background(
                self._async_secondary_write(r, catalog_id, collection_id, features),
                name=f"item_secondary_write:{r.driver_ref}:{catalog_id}/{collection_id}",
            )

    async def _async_secondary_write(
        self,
        resolved: "ResolvedDriver",
        catalog_id: str,
        collection_id: str,
        features: List[Feature],
    ) -> None:
        """Fire-and-forget wrapper with logging on failure."""
        from dynastore.modules.storage.routing_config import FailurePolicy

        try:
            await resolved.driver.write_entities(catalog_id, collection_id, features)
        except Exception as err:
            if resolved.on_failure == FailurePolicy.FATAL:
                logger.error(
                    "Async secondary driver '%s' FATAL write failed for %s/%s: %s",
                    resolved.driver_ref, catalog_id, collection_id, err,
                )
            elif resolved.on_failure == FailurePolicy.WARN:
                logger.warning(
                    "Async secondary driver '%s' write failed for %s/%s: %s",
                    resolved.driver_ref, catalog_id, collection_id, err,
                )

    async def _compensate_drivers(
        self,
        drivers: List["ResolvedDriver"],
        catalog_id: str,
        collection_id: str,
        features: List[Feature],
    ) -> None:
        """Compensating rollback: delete written entities from drivers that
        support TRANSACTIONAL capability."""
        from dynastore.modules.storage.driver_config import DriverCapability

        entity_ids = [str(f.id) for f in features if f.id]
        if not entity_ids:
            return

        for r in drivers:
            driver_caps = getattr(r.driver, "capabilities", frozenset())
            if DriverCapability.TRANSACTIONAL not in driver_caps:
                continue
            try:
                await r.driver.delete_entities(
                    catalog_id, collection_id, entity_ids
                )
                logger.info(
                    "Compensated driver '%s' for %s/%s (%d entities)",
                    r.driver_ref, catalog_id, collection_id, len(entity_ids),
                )
            except Exception as comp_err:
                logger.error(
                    "Compensating rollback failed for driver '%s' on %s/%s: %s",
                    r.driver_ref, catalog_id, collection_id, comp_err,
                )

    # Query methods (get_features, get_item, search_items, stream_items, etc.)
    # are provided by ItemQueryMixin.

    # Distributed methods (insert_or_update_distributed, etc.)
    # are provided by ItemDistributedMixin.

    async def delete_item_language(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        lang: str,
        ctx: Optional[DriverContext] = None,
    ) -> int:
        """
        Deletes a specific language variant from an item's attributes.
        Actually, it looks for localized fields in 'attributes' and removes the key.
        """
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn
            )
            if not phys_schema or not phys_table:
                return 0

            # Privileged system read: delete_item_language is a write-path
            # operation with no end-user principal; allow all rows.
            from dynastore.models.protocols.access_filter import AccessFilter
            # Fetch the item to identify localized fields in attributes
            item = await self.get_item(
                catalog_id, collection_id, item_id,
                ctx=DriverContext(db_resource=conn),
                access_filter=AccessFilter.allow_everything(),
            )
            if not item:
                return 0

            attributes = getattr(item, "attributes", None) or {}
            if isinstance(attributes, str):
                attributes = json.loads(attributes)

            # Identifies fields that are potentially localized (e.g. title, description, or custom)
            # and removes the requested language.
            modified = False
            for key, value in attributes.items():
                if isinstance(value, dict):
                    # Check if it looks like a localized dict
                    from dynastore.models.localization import _LANGUAGE_METADATA

                    if any(k in _LANGUAGE_METADATA for k in value.keys()):
                        if lang in value:
                            if len(value) <= 1:
                                # We might skip deletion if it's the only language,
                                # but usually for items, we can either keep or remove.
                                # Let's follow the 'error if last language' rule if it makes sense.
                                # For STAC items, maybe it's less strict, but let's be consistent.
                                continue

                            del value[lang]
                            modified = True

            if not modified:
                return 0

            update_sql = f'UPDATE "{phys_schema}"."{phys_table}" SET attributes = :attr WHERE geoid = :geoid;'
            rows = await DQLQuery(
                update_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(
                conn, attr=json.dumps(attributes, cls=CustomJSONEncoder), geoid=item_id
            )
            return rows

    async def ensure_physical_table_exists(
        self,
        catalog_id: str,
        collection_id: str,
        col_config: ItemsPostgresqlDriverConfig,
        db_resource: Optional[DbResource] = None,
    ):
        async with managed_transaction(db_resource or self.engine) as conn:
            from dynastore.modules.db_config.locking_tools import acquire_startup_lock

            async with acquire_startup_lock(
                conn, f"promote_{catalog_id}_{collection_id}"
            ):
                phys_schema = await self._resolve_physical_schema(
                    catalog_id, db_resource=conn
                )
                phys_table = await self._resolve_physical_table(
                    catalog_id, collection_id, db_resource=conn
                )
                force_create = False
                catalogs = get_protocol(CatalogsProtocol)
                if not phys_table:
                    logger.info(
                        f"Promoting collection {catalog_id}:{collection_id} to physical storage."
                    )
                    phys_table = collection_id
                    force_create = True
                    from dynastore.modules.storage.router import get_driver as _get_driver
                    from dynastore.modules.storage.routing_config import Operation as _Op
                    _drv = await _get_driver(_Op.WRITE, catalog_id, collection_id)
                    if hasattr(_drv, "set_physical_table"):
                        await getattr(_drv, "set_physical_table")(
                            catalog_id, collection_id, phys_table, db_resource=conn
                        )

                if catalogs is not None and (force_create or not await shared_queries.table_exists_query.execute(
                    conn, schema=phys_schema, table=phys_table
                )):
                    await getattr(catalogs, "create_physical_collection")(
                        conn,
                        phys_schema,
                        catalog_id,
                        collection_id,
                        physical_table=phys_table,
                        layer_config=col_config.model_dump(),
                    )

    async def ensure_partition_exists(
        self,
        catalog_id: str,
        collection_id: str,
        col_config: ItemsPostgresqlDriverConfig,
        partition_value: Any,
        ctx: Optional[DriverContext] = None,
    ):
        db_resource = ctx.db_resource if ctx else None
        partitioning = col_config.partitioning
        if not partitioning.enabled or not partitioning.partition_keys:
            return
        # Current simplify: we use the first partition key for the physical partition routing
        # In a fully composite world, partition_value should be a list/tuple.
        if partition_value is None:
            return
        if partition_value is None:
            return
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            phys_table = await self._resolve_physical_table(
                catalog_id, collection_id, db_resource=conn
            )
            if not phys_schema or not phys_table:
                return
            # Determine strategy for the tool (simplified)
            # If the value is a date/datetime, use RANGE, else LIST
            from datetime import date, datetime

            tool_strategy = (
                "RANGE" if isinstance(partition_value, (date, datetime)) else "LIST"
            )
            interval = (
                None  # We might need to derive this if we had TimePartitionStrategy
            )

            from dynastore.modules.db_config.partition_tools import (
                ensure_partition_exists as ensure_partition_tool,
            )

            await ensure_partition_tool(
                conn=conn,
                table_name=phys_table,
                strategy=tool_strategy,
                partition_value=partition_value,
                schema=phys_schema,
                interval=interval,
                parent_table_name=phys_table,
                parent_table_schema=phys_schema,
            )

    async def get_collection_fields(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Returns a dictionary of field definitions for the collection.
        Aggregates fields from the physical table introspection and configured sidecars.
        """
        # Resolve physical details internally
        schema = await self._resolve_physical_schema(
            catalog_id, db_resource=db_resource
        )
        table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )

        fields = {}
        from dynastore.modules.storage.drivers.pg_sidecars.base import (
            FieldDefinition,
            FieldCapability,
        )

        # 1. Try to get config-based definitions if logical IDs provided
        if catalog_id and collection_id:
            try:
                col_config = await self._get_collection_config(
                    catalog_id, collection_id, db_resource=db_resource
                )

                if col_config:
                    optimizer = QueryOptimizer(col_config)
                    # A. Aggregate from all sidecars via QueryOptimizer
                    fields.update(optimizer.get_all_queryable_fields())

                    # B. Hub Fields (geoid, transaction_time, etc.)
                    hub_cols = col_config.get_column_definitions()
                    for name in hub_cols.keys():
                        if name not in fields:
                            # Infer canonical data_type from the SQL def
                            # (see ``dynastore.models.field_types``).
                            sql_type = hub_cols[name].upper()
                            d_type = "string"
                            if "TIMESTAMP" in sql_type:
                                d_type = "timestamp"
                            elif "UUID" in sql_type:
                                d_type = "uuid"

                            fields[name] = FieldDefinition(
                                name=name,
                                sql_expression=f"h.{name}",
                                data_type=d_type,
                                capabilities=[
                                    FieldCapability.FILTERABLE,
                                    FieldCapability.SORTABLE,
                                    FieldCapability.GROUPABLE,
                                ],
                            )

            except (ValueError, KeyError, AttributeError, LookupError, OSError) as e:
                logger.warning(
                    f"Failed to resolve collection config for fields for {catalog_id}/{collection_id}: {e}"
                )

        # 2. Introspect Physical Table (Fallback & Validation)
        # Use a transaction for introspection
        async with managed_transaction(db_resource or self.engine) as conn:
            from dynastore.modules.db_config.tools import _get_table_columns_query
            # Single source of truth for PG-native -> canonical data_type (#1216).
            from dynastore.modules.storage.field_constraints import pg_native_to_canonical

            rows = await _get_table_columns_query.execute(
                conn, schema=schema, table=table
            )

            for row in rows:
                col_name = row.column_name
                # If already defined by config, skip (config is more authoritative for aliases/sql_expression)
                if col_name in fields:
                    continue

                # ``udt_name`` carries the concrete type for USER-DEFINED columns
                # (e.g. ``geometry``); prefer it so PostGIS columns canonicalize.
                udt = str(row.udt_name).lower()
                raw = str(row.data_type).lower()
                dtype = pg_native_to_canonical(udt if raw == "user-defined" else raw)

                # Simple object mocking FieldDefinition for introspection-only columns
                # We don't know the alias here, so we assume column name
                fields[col_name] = FieldDefinition(
                    name=col_name,
                    sql_expression=col_name,
                    data_type=dtype,
                    capabilities=[FieldCapability.FILTERABLE],
                )

        return fields

    async def get_categorized_fields(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Tuple[FrozenSet[str], FrozenSet[str], FrozenSet[str]]:
        """Return ``(system, stats, properties)`` field-name sets for the collection.

        Derives category membership from the collection's PG sidecar contracts.
        Falls back to empty sets when no PG driver config is available.
        """
        from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS
        from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars
        from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

        system_keys: FrozenSet[str] = frozenset(SYSTEM_FIELD_KEYS)

        col_config = await self._get_collection_config(
            catalog_id, collection_id, db_resource=db_resource
        )
        if not col_config:
            return (system_keys, frozenset(), frozenset())

        stats_names: set[str] = set()
        prop_names: set[str] = set()

        for sc_config in driver_sidecars(col_config):
            sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
            if not sidecar:
                continue
            for name in sidecar.producible_computed_names():
                if name not in system_keys:
                    stats_names.add(name)
            for name in sidecar.get_property_field_names():
                if name not in system_keys and name not in stats_names:
                    prop_names.add(name)

        return (system_keys, frozenset(stats_names), frozenset(prop_names))

    async def get_collection_schema(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Returns the composed JSON Schema for the collection's Feature output.

        The user-data wire shape (``properties``) is derived from
        ``ItemsSchema`` — the single source of truth — via ``derive_wire_schema``.
        Sidecars contribute cross-cutting fragments (``geometry``, STAC
        ``stac_extensions``/``assets``, item-metadata ``title``/``description``/
        ``keywords``) which are overlaid on the derived ``properties``. When the
        collection declares no fields (blob) the sidecar aggregation is the sole
        source.
        """
        col_config = await self._get_collection_config(
            catalog_id, collection_id, db_resource=db_resource
        )
        if not col_config:
            raise ValueError(f"Collection {catalog_id}/{collection_id} not found.")

        from dynastore.modules.catalog.query_optimizer import QueryOptimizer
        optimizer = QueryOptimizer(col_config)
        sidecar_schema = optimizer.get_feature_type_schema()

        configs = get_protocol(ConfigsProtocol)
        policy_schema: Optional[Dict[str, Any]] = None
        if configs is not None:
            # Derive the wire schema from items_schema (the SSOT). When the
            # collection declares no fields (blob), the sidecar aggregation
            # remains the sole source.
            from dynastore.modules.storage.driver_config import ItemsSchema
            from dynastore.modules.storage.schema_derive import derive_wire_schema

            try:
                its = await configs.get_config(
                    ItemsSchema,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
            except Exception:
                its = None
            if isinstance(its, ItemsSchema):
                policy_schema = derive_wire_schema(
                    its.fields, strict=its.strict_unknown_fields
                )

        if policy_schema is None:
            return sidecar_schema

        # Overlay: the derived schema's properties are the SSOT for user data;
        # sidecar contributions (stac_extensions, title, etc.) fill in
        # any cross-cutting fields the schema doesn't declare.
        policy_props = policy_schema.get("properties", {}) if isinstance(policy_schema.get("properties"), dict) else {}
        sidecar_props = sidecar_schema.get("properties", {}) if isinstance(sidecar_schema.get("properties"), dict) else {}
        merged_props = {**sidecar_props, **policy_props}

        merged_required = list(
            dict.fromkeys(
                [*sidecar_schema.get("required", []), *policy_schema.get("required", [])]
            )
        )

        return {
            "type": "object",
            "properties": merged_props,
            "geometry": sidecar_schema.get(
                "geometry", {"type": "object", "description": "GeoJSON geometry"}
            ),
            "required": merged_required or ["geometry", "properties"],
        }

    # NOTE: map_row_to_feature is defined once at the top of this class.
    # The canonical implementation (using the sidecar pipeline) is the single
    # source of truth — do not add a second definition here.

    @property
    def count_items_by_asset_id_query(self) -> DQLQuery:
        """Query builder for counting items by asset ID."""

        def _builder(conn, params):
            phys_schema = params["catalog_id"]
            phys_table = params["collection_id"]
            asset_id = params["asset_id"]

            sql = f'SELECT count(*) FROM "{phys_schema}"."{phys_table}" WHERE extra_metadata->\'assets\' ? :asset_id AND deleted_at IS NULL'
            return sql, {"asset_id": asset_id}

        return DQLQuery.from_builder(_builder, result_handler=ResultHandler.SCALAR_ONE)

    @property
    def list_items_by_asset_id_query(self) -> DQLQuery:
        """Query builder for listing item IDs that link to a given asset.

        Used by the reverse-cascade path to enumerate dependent items
        before deletion. Returns ``external_id`` rows since that's what
        ``CatalogService.delete_item`` accepts.
        """

        def _builder(conn, params):
            phys_schema = params["catalog_id"]
            phys_table = params["collection_id"]
            asset_id = params["asset_id"]

            sql = (
                f'SELECT external_id FROM "{phys_schema}"."{phys_table}" '
                f"WHERE extra_metadata->'assets' ? :asset_id "
                f"AND deleted_at IS NULL"
            )
            return sql, {"asset_id": asset_id}

        return DQLQuery.from_builder(_builder, result_handler=ResultHandler.ALL_DICTS)

