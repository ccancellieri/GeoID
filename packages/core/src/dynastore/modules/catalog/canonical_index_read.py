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

"""Policy-free raw-row reader for the canonical ES index builder (#1800).

Reads raw PG rows + resolves sidecars for a set of geoids in a single
batched SELECT, WITHOUT applying any ``ItemsReadPolicy`` (no
``external_id_as_feature_id`` id-flip, no ``expose`` filtering).

The result feeds :func:`~dynastore.modules.elasticsearch.canonical_doc.build_canonical_index_doc`
at the ES write boundary so the indexed document has the correct canonical
envelope shape.

Internal seams (_fetch_raw_rows, _resolve_sidecars_for, _get_col_config,
_resolve_collection_type) are kept module-private but importable for
test-injection via ``patch``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# shapely is an optional dependency elsewhere in this codebase (SCOPE-trimmed
# deployments may omit it) — guard the same way
# ``dynastore.tools.geometry_normalize`` does so a missing bbox on a
# database-free feature (:func:`canonical_input_from_feature`) degrades to
# ``None`` instead of an ImportError.
try:
    from shapely.geometry import shape as _shapely_shape

    _SHAPELY_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without shapely
    _SHAPELY_AVAILABLE = False


def _bbox_from_geometry(geometry: Optional[Dict[str, Any]]) -> Optional[List[float]]:
    """Best-effort ``[minx, miny, maxx, maxy]`` bbox from a GeoJSON geometry.

    Returns ``None`` when *geometry* is falsy, unparseable, or shapely is
    unavailable — callers treat ``None`` as "no bbox available", the same
    degrade-safe contract the rest of this module uses.
    """
    if not geometry or not _SHAPELY_AVAILABLE:
        return None
    try:
        return list(_shapely_shape(geometry).bounds)
    except Exception:
        return None


def _get_db_engine() -> Optional[Any]:
    """Resolve a DB engine via the registered DatabaseProtocol.

    Used as the last-resort fallback in :func:`_fetch_raw_rows` when
    ``db_resource`` is None — covers the Cloud Run JOB/worker context
    where a bare ``ItemService()`` carries no engine but the process-wide
    ``DatabaseProtocol`` does.  Returns ``None`` when no protocol is
    registered (e.g. test or import-only context).
    """
    try:
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.tools.discovery import get_protocol

        db = get_protocol(DatabaseProtocol)
        if db is None:
            return None
        return db.engine
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Public data type
# ---------------------------------------------------------------------------


@dataclass
class CanonicalIndexInput:
    """All inputs required by :func:`build_canonical_index_doc` for one item.

    Produced once per geoid by :func:`read_canonical_index_inputs`.  Callers
    must not apply any read policy after this point — the data is already
    policy-free and intended for the ES write boundary only.

    Attributes:
        row:                   Raw PG row dict including all sidecar columnar
                               columns (area, centroid, hashes, validity, …).
        resolved_sidecars:     Sidecar instances that can answer
                               ``producible_computed_names`` /
                               ``resolve_computed_value`` for this collection.
        geometry:              GeoJSON geometry as a plain ``dict``, or ``None``
                               when the collection has no geometry column.
        bbox:                  Bounding-box list ``[minx, miny, maxx, maxy]``,
                               or ``None``.
        user_properties:       User-facing attribute dict — only schema-declared
                               / JSONB user fields.  No SYSTEM_FIELD_KEYS, no
                               stats.  GeoJSON/STAC reserved members (``assets``,
                               ``stac_extensions``) are excluded from here and
                               surfaced via ``stac_reserved_members`` instead so
                               the canonical doc builder can place them at the ES
                               document top level where
                               ``unproject_item_from_es`` can restore them on
                               read.
        access:                Access-envelope dict (``{_visibility, _owner,
                               _attrs}``) for the access-aware ES driver
                               variant, recomputed from stored state (#2687):
                               ``_owner`` from the hub row's persisted
                               ``access_owner`` column, ``_visibility`` /
                               ``_attrs`` from live config. ``None`` when the
                               collection does not route to an access-aware
                               WRITE driver, or when the recompute itself
                               failed — a caller writing into a resolved
                               access-aware driver must treat ``None`` as a
                               failure to retry, never as "no envelope
                               needed" (see ``_resolve_access_context``).
        stac_reserved_members: Per-item STAC members that must live at the ES
                               doc top level (``assets``, ``stac_extensions``).
                               Populated when these keys are found in the
                               attributes JSONB blob — the case for default
                               (no-schema/JSONB) catalogs whose ``stac_metadata``
                               sidecar is not active.  ``None`` when the
                               ``stac_metadata`` sidecar owns them instead.
    """

    row: Dict[str, Any]
    resolved_sidecars: List[Any] = field(default_factory=list)
    geometry: Optional[Dict[str, Any]] = None
    bbox: Optional[List[float]] = None
    user_properties: Optional[Dict[str, Any]] = None
    access: Optional[Dict[str, Any]] = None
    stac_reserved_members: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Internal seams (replaced by test patches)
# ---------------------------------------------------------------------------


async def _get_col_config(
    catalog_id: str,
    collection_id: str,
    db_resource: Optional[Any] = None,
) -> Optional[Any]:
    """Resolve ``ItemsPostgresqlDriverConfig`` for a collection.

    Delegates to the same config-waterfall the PG driver uses, preferring
    the WRITE driver config (which always returns a PG config) so sidecars
    are consistently resolved even when the READ driver is ES.
    """
    try:
        from dynastore.modules.storage.router import get_write_drivers
        write_drivers = await get_write_drivers(catalog_id, collection_id)
        if not write_drivers:
            return None
        driver = write_drivers[0].driver
        return await driver.get_driver_config(
            catalog_id, collection_id, db_resource=db_resource,
        )
    except Exception as exc:
        logger.warning(
            "canonical_index_read._get_col_config: failed for %s/%s: %s",
            catalog_id, collection_id, exc,
        )
        return None


async def _resolve_sidecars_for(col_config: Any, catalog_id: str, collection_id: str) -> List[Any]:
    """Return the ordered list of resolved sidecar instances for a collection.

    Uses the same ``_effective_sidecars`` + ``SidecarRegistry.get_sidecar``
    path as :meth:`ItemService.map_row_to_feature` — without running the
    pipeline so we can share the resolved list independently of a Feature.

    #2655: resolves the real ``CollectionInfo.kind`` / ``allow_geometry``
    (same lookup ``ItemsPostgresqlDriver._get_effective_driver_config`` and
    ``collection_has_geometry()`` use) so a RECORDS collection's resolved
    sidecar list correctly omits the geometry sidecar here too, instead of
    relying on ``_effective_sidecars``'s "VECTOR" fallback default.
    """
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.catalog.catalog_config import CollectionInfo
        from dynastore.modules.storage.drivers.pg_sidecars import (
            SidecarRegistry,
            _effective_sidecars,
        )
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        ct = await configs.get_config(
            CollectionInfo, catalog_id=catalog_id, collection_id=collection_id,
        ) if configs else CollectionInfo()
        sidecar_configs = _effective_sidecars(
            col_config,
            catalog_id=catalog_id,
            collection_id=collection_id,
            collection_type=ct.kind.value,
            context={"allow_geometry": ct.allow_geometry},
        )
        resolved: List[Any] = []
        for sc_config in sidecar_configs:
            sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
            if sidecar is not None:
                resolved.append(sidecar)
        return resolved
    except Exception as exc:
        logger.warning(
            "canonical_index_read._resolve_sidecars_for: %s/%s: %s",
            catalog_id, collection_id, exc,
        )
        return []


async def _resolve_access_context(
    catalog_id: str, collection_id: str,
) -> "tuple[bool, Optional[str], Dict[str, str]]":
    """Resolve ``(is_access_aware, visibility, attribute_stamping_paths)`` once
    per :func:`read_canonical_index_inputs` call (#2687).

    ``_owner`` is read per-row from the hub row's persisted ``access_owner``
    column (see ``_apply_access_envelope`` below) — nothing to resolve here.
    ``_visibility`` and the attribute-stamping paths are batch-level (catalog
    / collection scoped), so resolving them once and applying per row avoids
    a config round trip per geoid.

    Degrade-safe like the sibling resolvers in this module
    (:func:`_get_col_config`, :func:`_resolve_sidecars_for`): any failure is
    logged and folded into "not access-aware" / "no visibility" / "no attrs"
    rather than raised — a collection that does not actually need an
    envelope must never be blocked by this. The drain's fail-closed
    guarantee ("never index an access-aware doc without its envelope") is
    enforced by the caller that already knows it is about to write into a
    resolved access-aware driver — see
    ``StorageDrainTask._build_canonical_doc`` — not by this best-effort
    resolver.
    """
    try:
        from dynastore.modules.storage.access_envelope import (
            collection_uses_access_aware_driver,
        )

        is_access_aware = await collection_uses_access_aware_driver(
            catalog_id, collection_id,
        )
    except Exception as exc:
        logger.warning(
            "canonical_index_read._resolve_access_context: access-aware "
            "detection failed for %s/%s: %s",
            catalog_id, collection_id, exc,
        )
        return False, None, {}

    if not is_access_aware:
        return False, None, {}

    visibility: Optional[str] = None
    attrs_paths: Dict[str, str] = {}
    try:
        from dynastore.modules.storage.access_envelope import (
            resolve_attribute_stamping_paths,
            resolve_catalog_visibility,
        )

        visibility = await resolve_catalog_visibility(catalog_id)
        attrs_paths = await resolve_attribute_stamping_paths(catalog_id, collection_id)
    except Exception as exc:
        logger.warning(
            "canonical_index_read._resolve_access_context: envelope "
            "recompute failed for %s/%s: %s — rows will carry no access "
            "envelope this read.",
            catalog_id, collection_id, exc,
        )
        # ``is_access_aware`` stays True: the caller (the drain) must treat a
        # missing envelope on an access-aware collection as a failure to
        # retry, never as "not access-aware" — see module docstring above.
        visibility = None
        attrs_paths = {}

    return is_access_aware, visibility, attrs_paths


def _apply_access_envelope(
    row: Dict[str, Any],
    user_properties: Optional[Dict[str, Any]],
    *,
    is_access_aware: bool,
    visibility: Optional[str],
    attrs_paths: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    """Build one row's ``{_visibility, _owner, _attrs}`` envelope (#2687).

    ``None`` when the collection is not access-aware (zero behaviour change)
    OR when the batch-level visibility recompute failed (fail-closed — an
    absent envelope on an access-aware collection must never be indexed;
    see :func:`_resolve_access_context`). ``_owner`` comes from the hub
    row's persisted ``access_owner`` column — ``None`` when no principal was
    present at write time.
    """
    if not is_access_aware or visibility is None:
        return None

    from dynastore.modules.iam.stamping_config import stamp_attrs_from_feature

    envelope: Dict[str, Any] = {
        "_visibility": visibility,
        "_owner": row.get("access_owner"),
    }
    if attrs_paths:
        attrs = stamp_attrs_from_feature(
            {"properties": user_properties or {}}, attrs_paths,
        )
        if attrs:
            envelope["_attrs"] = attrs
    return envelope


async def _resolve_collection_type(
    catalog_id: str, collection_id: str,
) -> tuple[Optional[str], Optional[bool]]:
    """Resolve ``(CollectionInfo.kind, allow_geometry)`` for a collection.

    #2655: same lookup ``_resolve_sidecars_for`` and
    ``ItemsPostgresqlDriver._get_effective_driver_config`` already use.
    Kept separate from ``_resolve_sidecars_for`` (rather than folding the
    tuple into its return value) so that value's existing ``List[Any]``
    contract — relied on by test mocks — stays unchanged. ``ConfigsProtocol``
    caches ``CollectionInfo`` lookups, so this second fetch is cheap.
    """
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.catalog.catalog_config import CollectionInfo
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        ct = await configs.get_config(
            CollectionInfo, catalog_id=catalog_id, collection_id=collection_id,
        ) if configs else CollectionInfo()
        return ct.kind.value, ct.allow_geometry
    except Exception as exc:
        logger.warning(
            "canonical_index_read._resolve_collection_type: %s/%s: %s",
            catalog_id, collection_id, exc,
        )
        return None, None


async def _fetch_raw_rows(
    catalog_id: str,
    collection_id: str,
    geoids: List[str],
    col_config: Any,
    db_resource: Optional[Any] = None,
) -> Dict[str, Dict[str, Any]]:
    """Batch-fetch raw PG rows for *geoids*, keyed by geoid.

    Reuses the same SQL-build infrastructure as ``ItemService.get_item``
    (``_apply_query_transformations`` → raw ``text()`` execute) but issues a
    single ``WHERE geoid = ANY(:ids)`` query for the whole batch, avoiding
    N+1 round-trips on ``index_bulk`` calls.

    A geoid absent from the returned dict because the query ran and simply
    found no matching row (deleted or never written) is the ONLY legitimate
    "missing" outcome — callers skip those geoids. Anything that prevents the
    query from running at all (no DB engine, unresolved physical table,
    connection/mapping errors) is a failure, not an absence, and propagates
    as an exception (#2731): swallowing it here made every row in the batch
    look identical to a deleted item, which silently dropped ~5200 items from
    the ES index on 2026-07-02 when the re-read hit a transient error.
    """
    if not geoids:
        return {}

    from sqlalchemy import text as _sa_text

    from dynastore.modules.catalog.item_service import ItemService
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
    from dynastore.models.query_builder import FieldSelection, QueryRequest
    from dynastore.tools.db import validate_sql_identifier

    validate_sql_identifier(catalog_id)
    validate_sql_identifier(collection_id)

    item_svc = ItemService()
    # Resolve the engine to use: prefer the explicitly-passed db_resource,
    # then the engine on the locally-constructed ItemService (available in
    # the API process where ItemService is registered with a live pool),
    # then fall back to the process-wide DatabaseProtocol engine (available
    # in Cloud Run JOB/worker processes where no ItemService engine is wired).
    effective_resource = db_resource or item_svc.engine or _get_db_engine()
    if effective_resource is None:
        raise RuntimeError(
            f"canonical_index_read._fetch_raw_rows: no DB engine available for "
            f"{catalog_id}/{collection_id} — DatabaseProtocol is not registered "
            f"in this process."
        )
    async with managed_transaction(effective_resource) as conn:
        phys_schema = await item_svc._resolve_physical_schema(
            catalog_id, db_resource=conn,
        )
        phys_table = await item_svc._resolve_physical_table(
            catalog_id, collection_id, db_resource=conn,
        )
        if not phys_schema or not phys_table:
            raise RuntimeError(
                f"canonical_index_read._fetch_raw_rows: cannot resolve physical "
                f"table for {catalog_id}/{collection_id}"
            )

        if col_config is None:
            col_config = await item_svc._get_collection_config(
                catalog_id, collection_id, db_resource=conn,
            )

        request = QueryRequest(
            item_ids=[str(g) for g in geoids],
            limit=len(geoids),
            select=[FieldSelection(field="*")],
        )
        query_ctx: Dict[str, Any] = {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "col_config": col_config,
        }
        sql, params = await item_svc._apply_query_transformations(
            request, query_ctx, catalog_id, collection_id, col_config,
            db_resource=conn, consumer=ConsumerType.GENERIC,
        )
        import inspect as _inspect

        result = conn.execute(_sa_text(sql), params or {})
        if _inspect.isawaitable(result):
            result = await result

        rows: Dict[str, Dict[str, Any]] = {}
        for raw in result.mappings():
            row_dict = dict(raw)
            geoid = row_dict.get("geoid")
            if geoid:
                rows[str(geoid)] = row_dict
        return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def read_canonical_index_inputs(
    catalog_id: str,
    collection_id: str,
    geoids: List[str],
    *,
    db_resource: Optional[Any] = None,
) -> Dict[str, CanonicalIndexInput]:
    """Read raw PG rows + resolve sidecars for *geoids*, policy-free.

    Returns a dict mapping geoid → :class:`CanonicalIndexInput`.  Geoids
    that are absent in PG (deleted or race) are silently omitted — the
    caller (ES write boundary) should skip those ops. That omission is only
    ever a legitimate "queried and found nothing" outcome (#2731): any
    failure to run the underlying query (missing DB engine, unresolved
    physical table, connection error) raises instead of being folded into
    the same empty result — see :func:`_fetch_raw_rows`.

    No ``ItemsReadPolicy`` is applied: ``id`` in the returned row is always
    the geoid; ``external_id_as_feature_id`` is never flipped; ``expose``
    filtering is never applied.  This is intentional — the ES canonical
    doc must reflect the stored state, not any read-policy reshaping.

    Args:
        catalog_id:    Catalog identifier.
        collection_id: Collection identifier.
        geoids:        List of geoid strings to fetch (may be empty).
        db_resource:   Optional existing DB connection/engine to reuse.

    Returns:
        Dict mapping each found geoid to its :class:`CanonicalIndexInput`.

    Raises:
        Exception: propagated unchanged when the underlying re-read fails
            (see :func:`_fetch_raw_rows`) — callers must treat this as
            "unknown state, retry", never as "rows deleted".
    """
    if not geoids:
        return {}

    col_config = await _get_col_config(catalog_id, collection_id, db_resource=db_resource)
    resolved_sidecars = await _resolve_sidecars_for(col_config, catalog_id, collection_id)
    collection_type, allow_geometry = await _resolve_collection_type(catalog_id, collection_id)
    is_access_aware, visibility, attrs_paths = await _resolve_access_context(
        catalog_id, collection_id,
    )
    raw_rows = await _fetch_raw_rows(
        catalog_id, collection_id, geoids, col_config, db_resource=db_resource,
    )

    result: Dict[str, CanonicalIndexInput] = {}
    for geoid, row in raw_rows.items():
        geometry, bbox, user_properties, stac_reserved_members = _extract_feature_parts(
            row, col_config, resolved_sidecars, catalog_id, collection_id,
            collection_type=collection_type, allow_geometry=allow_geometry,
        )
        access = _apply_access_envelope(
            row, user_properties,
            is_access_aware=is_access_aware, visibility=visibility,
            attrs_paths=attrs_paths,
        )
        result[geoid] = CanonicalIndexInput(
            row=row,
            resolved_sidecars=resolved_sidecars,
            geometry=geometry,
            bbox=bbox,
            user_properties=user_properties,
            access=access,
            stac_reserved_members=stac_reserved_members,
        )

    return result


async def has_canonical_source(catalog_id: str, collection_id: str) -> bool:
    """Does this collection's WRITE fan-out include a driver capable of
    supplying canonical inputs (i.e. one :func:`read_canonical_index_inputs`
    can actually read from)?

    Routing-based signal only: capability here means "a driver in the fan-out
    implements ``resolve_physical_table``" — it says nothing about whether
    that driver's storage has been *provisioned* yet (see
    :func:`ensure_canonical_source_ready`). Callers building an ES/search
    document use this to decide between hydrating from the canonical read
    or falling back to a feature-derived doc (:func:`canonical_input_from_feature`).

    Degrade-safe: any routing-resolution failure is treated as "no canonical
    source" — a resolution error here must not block the caller's write, it
    just means the write falls back to the feature-derived doc.
    """
    try:
        from dynastore.modules.storage.router import get_write_drivers
        write_drivers = await get_write_drivers(catalog_id, collection_id)
    except Exception:
        return False
    return any(
        hasattr(resolved.driver, "resolve_physical_table")
        for resolved in write_drivers
    )


async def ensure_canonical_source_ready(
    catalog_id: str, collection_id: str, *, db_resource: Optional[Any] = None,
) -> None:
    """Lazily activate a pending collection via ``CatalogsProtocol``.

    ``has_canonical_source`` only checks routing config — it says nothing
    about whether the resolved driver's storage has actually been
    provisioned yet. A collection whose first-ever write reaches
    :func:`read_canonical_index_inputs` without going through
    ``ItemService.upsert``'s own lazy-activation gate (e.g. a bulk harvester
    writing through a non-PG-primary path) would otherwise stay pending
    forever: every batch would hit :func:`_fetch_raw_rows`'s "cannot resolve
    physical table" RuntimeError, since nothing ever calls
    ``activate_collection`` (#3046).

    Mirrors ``ItemService.upsert``'s own gate (``ensure_alive`` →
    ``is_active`` → ``activate_collection``) and is equally driver-agnostic:
    ``activate_collection`` provisions whichever driver this collection's
    WRITE routing resolves to.

    Degrade-safe like ``has_canonical_source``: no ``CatalogsProtocol``
    registered is a no-op, and any failure of the activation sequence itself
    (e.g. ``ensure_alive`` raising ``CollectionNotAliveError`` for a
    collection still in its ``PROVISIONING`` window, or a transient lookup
    error) is swallowed rather than propagated. Unlike ``ItemService.upsert``
    — the primary REST write path, where that error is meant to surface as an
    HTTP 409 for the client to retry — this helper backs the ES driver's
    secondary-index write path, which has no client to hand a 409 to; the
    caller's fallback is to skip canonical hydration and use the
    feature-derived doc instead.
    """
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.tools.discovery import get_protocol

    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return
    try:
        await catalogs.ensure_alive(catalog_id, collection_id, db_resource=db_resource)
        if not await catalogs.is_active(catalog_id, collection_id, db_resource=db_resource):
            from dynastore.models.driver_context import DriverContext

            await catalogs.activate_collection(
                catalog_id, collection_id,
                ctx=DriverContext(db_resource=db_resource),
            )
    except Exception:
        return


# ---------------------------------------------------------------------------
# Feature-part extraction (no read policy)
# ---------------------------------------------------------------------------


def _extract_feature_parts(
    row: Dict[str, Any],
    col_config: Any,
    resolved_sidecars: List[Any],
    catalog_id: str,
    collection_id: str,
    *,
    collection_type: Optional[str] = None,
    allow_geometry: Optional[bool] = None,
) -> tuple[Optional[Dict[str, Any]], Optional[List[float]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Extract geometry, bbox, user-only properties, and STAC reserved members from a raw PG row.

    Does NOT apply any read policy:
    - ``id`` stays as the geoid.
    - ``expose`` filtering is not applied.
    - Stats / system keys are excluded from ``user_properties``.

    Strategy:
    1. Run ``map_row_to_feature(row, col_config, read_policy=None)`` to
       materialise geometry (geometry sidecar) and base attributes.
    2. Scrub ``user_properties`` by removing any key that is either a
       SYSTEM_FIELD_KEY or producible by the resolved sidecars as a
       computed / stats value — those live in ``system`` / ``stats``
       in the canonical doc, not in ``properties``.
    3. Also exclude known sidecar internal columns that may have leaked
       into properties via the JSONB fallback loop in the attributes sidecar.

    ``collection_type`` / ``allow_geometry`` (#2655, both optional) thread
    the real ``CollectionInfo.kind`` resolved by the caller into
    ``ItemService.map_row_to_feature`` — same resolution
    ``_resolve_sidecars_for`` above already uses — so a RECORDS row is
    mapped without resolving a geometry sidecar. Harmless either way: the
    geometry sidecar only acts when ``"geom"`` is present in ``row``, and
    the SELECT that produced ``row`` never projects it for RECORDS.

    Geometry is converted to a plain dict (no Pydantic type info).
    """
    from dynastore.modules.catalog.item_service import ItemService
    from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

    _system_keys = frozenset(SYSTEM_FIELD_KEYS)

    # Collect all internal columns from the resolved sidecars so they can
    # be excluded from user_properties.
    all_internal: set = set(_system_keys)
    for sc in resolved_sidecars:
        try:
            all_internal.update(sc.get_internal_columns())
        except Exception:
            pass

    # Collect all sidecar-producible computed names (stats + system) so they
    # can be excluded from user_properties even when the JSONB fallback loop
    # accidentally put them in.
    all_computed: set = set(_system_keys)
    for sc in resolved_sidecars:
        try:
            all_computed.update(sc.producible_computed_names())
        except Exception:
            pass

    item_svc = ItemService()
    # read_policy=None → no id-flip, no expose filtering.
    feature = item_svc.map_row_to_feature(
        row, col_config, read_policy=None,
        collection_type=collection_type, allow_geometry=allow_geometry,
    )

    # Geometry: convert from Pydantic geometry to plain dict.
    geometry: Optional[Dict[str, Any]] = None
    if feature.geometry is not None:
        geom = feature.geometry
        if isinstance(geom, dict):
            geometry = geom
        elif hasattr(geom, "model_dump"):
            geometry = geom.model_dump(exclude_none=True)
        elif hasattr(geom, "__geo_interface__"):
            geometry = dict(geom.__geo_interface__)
        else:
            try:
                geometry = dict(geom)
            except Exception:
                geometry = None

    # BBox: geojson_pydantic Feature has a .bbox attribute.
    bbox: Optional[List[float]] = None
    raw_bbox = getattr(feature, "bbox", None)
    if raw_bbox is not None:
        try:
            bbox = list(raw_bbox)
        except Exception:
            bbox = None

    # GeoJSON/STAC reserved members that must sit at the ES document top
    # level (not inside ``properties``) so ``unproject_item_from_es`` can
    # restore them verbatim on read.  For default (no-schema/JSONB) catalogs
    # without a ``stac_metadata`` sidecar, ``assets`` and ``stac_extensions``
    # are stored inside the attributes JSONB blob and therefore appear in
    # ``feature.properties`` after the JSONB is unpacked.
    # ``project_item_for_es`` would silently drop them (they are in
    # ``_RESERVED_MEMBER_KEYS``), so we extract them here and route them
    # through the ``stac_reserved_members`` path instead.
    _STAC_TOP_LEVEL_KEYS: frozenset = frozenset({"assets", "stac_extensions"})

    # User properties: scrub stats, system, internal-column keys, and
    # GeoJSON/STAC reserved members that the JSONB fallback loop may have
    # mixed in.
    user_properties: Optional[Dict[str, Any]] = None
    stac_reserved_members: Optional[Dict[str, Any]] = None
    if feature.properties is not None:
        exclude = all_computed | all_internal
        raw_props = feature.properties
        stac_rsv: Dict[str, Any] = {}
        for _k in _STAC_TOP_LEVEL_KEYS:
            if _k in raw_props:
                stac_rsv[_k] = raw_props[_k]
        if stac_rsv:
            stac_reserved_members = stac_rsv
        user_properties = {
            k: v for k, v in raw_props.items()
            if k not in exclude and k not in _STAC_TOP_LEVEL_KEYS
        }

    return geometry, bbox, user_properties, stac_reserved_members


# ---------------------------------------------------------------------------
# No-PG canonical input (file-backed collections, #375)
# ---------------------------------------------------------------------------

# GeoJSON/STAC reserved members that must sit at the ES document top level
# (not inside ``properties``) so ``unproject_item_from_es`` restores them
# verbatim on read.  Mirrors the set handled by ``_extract_feature_parts``.
_STAC_TOP_LEVEL_KEYS: frozenset = frozenset({"assets", "stac_extensions"})


def canonical_input_from_feature(
    feature: Dict[str, Any],
    catalog_id: str,
    collection_id: str,
    *,
    geoid: str,
    external_id: Optional[Any] = None,
    asset_id: Optional[Any] = None,
    sidecars: Optional[List[Any]] = None,
) -> CanonicalIndexInput:
    """Build a :class:`CanonicalIndexInput` from a serialized feature, no PG read.

    The canonical ES document is normally assembled from a raw PostgreSQL row via
    :func:`read_canonical_index_inputs`.  A file-backed collection has no PG rows,
    so this elevates the feature-derived fallback (previously inline in
    ``ItemsElasticsearchDriver.write_entities``) into a first-class, database-free
    producer.  The result has the same canonical shape as the PG path; only the
    ``stats``/``system`` sections that require a PG row + sidecars are absent.

    Args:
        feature:       Serialized GeoJSON/STAC feature dict (``IndexOp.payload``
                       shape): ``geometry``, ``bbox``, ``properties``, optional
                       ``assets`` / ``stac_extensions``.
        catalog_id:    Catalog identifier (kept for symmetry / future per-catalog
                       handling; not used to touch the database).
        collection_id: Collection identifier.
        geoid:         The geoid to stamp as ``row["geoid"]`` and, downstream, the
                       canonical document ``id`` (``_id`` in ES).
        external_id:   Optional external id to thread into the row.
        asset_id:      Optional source asset id to thread into the row.
        sidecars:      Optional resolved sidecars; defaults to ``[]`` (file path).

    Returns:
        A :class:`CanonicalIndexInput` ready for ``build_canonical_index_doc``.
    """
    from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

    _sys_keys = frozenset(SYSTEM_FIELD_KEYS)

    raw_props = feature.get("properties") or {}
    stac_reserved: Dict[str, Any] = {}
    for _k in _STAC_TOP_LEVEL_KEYS:
        if _k in feature and feature[_k] is not None:
            stac_reserved[_k] = feature[_k]
        elif _k in raw_props and raw_props[_k] is not None:
            stac_reserved[_k] = raw_props[_k]

    user_properties = {
        k: v
        for k, v in raw_props.items()
        if k not in _sys_keys and k not in _STAC_TOP_LEVEL_KEYS
    }

    geom = feature.get("geometry")
    geometry = geom if isinstance(geom, dict) else None
    bbox_val = feature.get("bbox")
    # Fall back to a shapely-computed bbox when the feature doesn't carry one
    # explicitly (#2864) — ES spatial filtering/sort relies on ``bbox`` being
    # present, and a PG-hydrated canonical doc always has one (PG sidecar
    # computes it), so the feature-only path must not silently omit it.
    bbox = (
        list(bbox_val) if bbox_val is not None else _bbox_from_geometry(geometry)
    )

    row: Dict[str, Any] = {"geoid": geoid}
    if external_id is not None:
        row["external_id"] = str(external_id)
    if asset_id is not None:
        row["asset_id"] = str(asset_id)

    return CanonicalIndexInput(
        row=row,
        resolved_sidecars=list(sidecars) if sidecars else [],
        geometry=geometry,
        bbox=bbox,
        user_properties=user_properties or None,
        access=None,
        stac_reserved_members=stac_reserved or None,
    )


__all__ = [
    "CanonicalIndexInput",
    "read_canonical_index_inputs",
    "canonical_input_from_feature",
]
