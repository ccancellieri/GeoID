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

"""
Elasticsearch Storage Drivers.

Two drivers in this module:

* ``ItemsElasticsearchDriver``  (driver_ref ``"elasticsearch"``)
  Items are written directly to a per-tenant index
  ``{prefix}-items-{catalog_id}`` (helper :func:`get_tenant_items_index`)
  with ``_routing=collection_id`` so a single index hosts every collection
  of one catalog while keeping shard locality per collection.  The index
  is enrolled in the platform alias ``{prefix}-items-public`` so OGC
  discovery search routes can target one alias regardless of tenant.
  Catalog and collection documents are owned by the dedicated
  ``catalog_es_driver`` / ``collection_es_driver`` modules (see
  :mod:`dynastore.modules.elasticsearch`).

* ``AssetElasticsearchDriver``  (driver_ref ``"elasticsearch_assets"``)
  Indexes asset metadata into per-catalog ``{prefix}-assets-{catalog_id}``
  indices.  Driven by the secondary-index ``WRITE`` entries
  (``secondary_index=True``) in ``AssetRoutingConfig.operations[WRITE]``
  (auto-augmented with discoverable ``AssetIndexer`` impls) and dispatched via
  ``AssetEntitySyncSubscriber`` from the events outbox — single-writer fan-out,
  no per-driver listener block.  Direct programmatic indexing via
  ``index_asset()`` / ``delete_asset()`` remains available.

The private driver (``ItemsElasticsearchPrivateDriver``) lives in
its own self-contained subpackage at
:mod:`dynastore.modules.storage.drivers.elasticsearch_private` so the
``[private]`` extras group can be opted in or out without touching this
file.

All drivers register as async event listeners, dispatched by the post-PR-#261
operation-based router via the secondary-index ``WRITE`` entries
(``secondary_index=True``) in ``ItemsRoutingConfig.operations[WRITE]`` /
``AssetRoutingConfig.operations[WRITE]``.
"""

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator, ClassVar, Dict, FrozenSet, List, Optional, Union

if TYPE_CHECKING:
    from dynastore.modules.storage.driver_config import ItemsWritePolicy
    from dynastore.modules.storage.read_policy import ItemsReadPolicy
    from dynastore.modules.storage.storage_location import StorageLocation

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.teardown_lane import TeardownLane
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.models.query_builder import AssetFilter, QueryRequest
from dynastore.modules.elasticsearch.items_query import (
    PUBLIC_ENVELOPE_FIELDS,
    EnvelopeFields,
)
from dynastore.modules.protocols import ModuleProtocol
from dynastore.modules.tools.asset_filters import build_es_query
from dynastore.modules.storage.driver_config import (
    AssetElasticsearchDriverConfig,
    ItemsElasticsearchDriverConfig,
)
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.routing_config import Operation

# Canonical ES write-boundary imports (#1800 Task 5).
# Imported at module level so tests can patch them as module attributes.
from dynastore.modules.catalog.canonical_index_read import (
    canonical_input_from_feature,
    read_canonical_index_inputs,
)
from dynastore.modules.elasticsearch.items_projection import resolve_catalog_known_fields

# Geometry simplification — shapely is an optional dependency; guard so the
# module can be imported in environments without it (tests, lean deployments).
# Tests can patch ``dynastore.modules.storage.drivers.elasticsearch.maybe_simplify_for_es``.
try:
    from dynastore.tools.geometry_simplify import maybe_simplify_for_es
except ImportError:  # shapely not installed

    def maybe_simplify_for_es(  # type: ignore[misc]
        doc: dict,
        *,
        simplify: bool,
        max_bytes: int = 10_000_000,
        max_iterations: int = 3,
        geometry_key: str = "geometry",
    ) -> "tuple[dict, float, str]":
        """No-op fallback when shapely is not available."""
        return doc, 1.0, "none"

logger = logging.getLogger(__name__)


def _clamp_geometry_budget(target: Optional[int]) -> int:
    """Return a valid ES geometry byte budget from *target*.

    Rules:
    - None → ``DEFAULT_MAX_BYTES`` (10 MB ES ceiling)
    - value > ``DEFAULT_MAX_BYTES`` → ``DEFAULT_MAX_BYTES`` (never exceed the ES hard limit)
    - value ≤ 0 → ``DEFAULT_MAX_BYTES`` (treat non-positive as unset)
    - 1 ≤ value ≤ ``DEFAULT_MAX_BYTES`` → value (passes through unchanged)

    This is a pure function with no I/O and is easily unit-tested in isolation.
    ``_resolve_simplify_max_bytes`` delegates to it so the clamping logic lives
    in exactly one place.
    """
    from dynastore.tools.geometry_simplify import DEFAULT_MAX_BYTES

    if target is None or target <= 0:
        return DEFAULT_MAX_BYTES
    return min(int(target), DEFAULT_MAX_BYTES)


def _es_client_required() -> Any:
    """Return the ES async client; raise if the platform module hasn't started."""
    from dynastore.modules.elasticsearch.client import get_client

    es = get_client()
    if es is None:
        raise RuntimeError(
            "ES client not initialised — ElasticsearchModule.lifespan must "
            "have started before items operations are invoked."
        )
    return es


def _tenant_items_index(catalog_id: str) -> str:
    """Resolve the per-tenant items index name for the active deployment."""
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_tenant_items_index

    return get_tenant_items_index(get_index_prefix(), catalog_id)


# Per-process cache: catalogs whose tenant items index has already been
# added to the platform public alias on this worker. Live indexer paths
# (index() / index_bulk()) consult this before issuing the alias call so
# the cost is bounded to one ES round-trip per (catalog, process) lifetime
# rather than per-write. ``ensure_storage`` and the OUTBOX drainer also
# fire ``add_index_to_public_alias`` (idempotent ES-side) but neither
# runs on the live ASYNC indexer path that ES auto-creation hits, leaving
# new tenant indices unaliased and invisible to alias-scoped search.
_ALIAS_REGISTERED_CATALOGS: set = set()

# Per-process cache: catalogs whose tenant items index has already been
# ensured (created with the correct mapping + alias) on this worker from the
# synchronous ES-primary write path. ES-primary (ES-only) collections have no
# collection-creation hook that runs ``ensure_storage``; without it the first
# ``write_entities`` lets ES auto-create the index with DYNAMIC mappings —
# ``collection`` becomes an analyzed ``text`` field, so the ``term`` filter in
# list/search never matches and items are retrievable by id but invisible to
# ``_search``. Bounded to one ensure per (catalog, process) lifetime.
_ITEMS_INDEX_ENSURED_CATALOGS: set = set()


async def _ensure_in_public_alias_once(catalog_id: str, index_name: str) -> None:
    """Add ``index_name`` to the platform public alias, at most once per
    process. Failures are swallowed-with-log; the OUTBOX drainer and
    ``ensure_storage`` paths cover the same ground so a transient miss
    here is recovered by the next write through one of those routes.
    """
    if catalog_id in _ALIAS_REGISTERED_CATALOGS:
        return
    try:
        from dynastore.modules.elasticsearch.aliases import (
            add_index_to_public_alias,
        )
        await add_index_to_public_alias(index_name)
    except Exception as exc:
        logger.warning(
            "ItemsElasticsearchDriver: alias-add failed for '%s': %s",
            index_name, exc,
        )
        return
    _ALIAS_REGISTERED_CATALOGS.add(catalog_id)


def _apply_geometry_simplification(
    doc: Dict[str, Any], factor: float, mode: str,
) -> None:
    """Stamp geometry-simplification metadata into ``doc["system"]``.

    Writes ``doc.system.geometry_simplification = {factor, mode}`` only
    when *mode* is not ``"none"`` (i.e. simplification actually ran).
    The flat ``_simplification_factor`` / ``_simplification_mode`` keys
    are intentionally NOT written — the canonical nested object under
    ``system`` is the authoritative location (#1828 Phase 2).

    Alias for :func:`_stamp_simplification`; both names are kept so
    internal callers in this module need no churn.
    """
    if mode == "none":
        return
    doc.setdefault("system", {})["geometry_simplification"] = {
        "factor": factor,
        "mode": mode,
    }


# Canonical name used by private and envelope drivers; same implementation.
_stamp_simplification = _apply_geometry_simplification


# ---------------------------------------------------------------------------
# Shared base
# ---------------------------------------------------------------------------

class _ElasticsearchBase:
    """Shared helpers for ES storage drivers."""

    def is_available(self) -> bool:
        """Available whenever the shared ES client is wired up.

        The platform's :class:`ElasticsearchModule.lifespan` initialises the
        singleton; this returns ``True`` once that has run. Shared by every ES
        driver (items public/private and assets).
        """
        try:
            from dynastore.modules.elasticsearch.client import get_client
        except (ImportError, ModuleNotFoundError):
            return False
        return get_client() is not None

    def _get_client(self) -> Any:
        """Return the shared async ES client, raising if not initialised.

        Single accessor for every ES driver — wraps the module-level
        :func:`_es_client_required` so subclasses never import
        :func:`dynastore.modules.elasticsearch.client.get_client` directly.
        """
        return _es_client_required()

    async def get_driver_config(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol
        from dynastore.modules.storage.driver_config import ItemsElasticsearchDriverConfig

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return ItemsElasticsearchDriverConfig()
        config = await configs.get_config(
            ItemsElasticsearchDriverConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=DriverContext(db_resource=db_resource),
        )
        if config is None:
            return ItemsElasticsearchDriverConfig()
        return config

    @staticmethod
    def _feature_to_stac_item(
        feature: Any, catalog_id: str, collection_id: str,
    ) -> dict:
        """Serialize a Feature to a STAC item dict."""
        if hasattr(feature, "model_dump"):
            doc = feature.model_dump(by_alias=True, exclude_none=True)
        elif isinstance(feature, dict):
            doc = dict(feature)
        else:
            doc = dict(feature)
        doc.setdefault("id", doc.get("id"))
        doc["collection"] = collection_id
        return doc

    @staticmethod
    def _normalize_entities(
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
    ) -> list:
        if isinstance(entities, FeatureCollection):
            return list(entities.features) if entities.features else []
        if isinstance(entities, list):
            return entities
        return [entities]

    @staticmethod
    def _extract_item_id(entity: Any) -> Optional[str]:
        if hasattr(entity, "id"):
            return entity.id
        if isinstance(entity, dict):
            return entity.get("id")
        return None


# ---------------------------------------------------------------------------
# Shared items base — public + private ES item drivers
# ---------------------------------------------------------------------------

class _ItemsElasticsearchBase(_ElasticsearchBase):
    """Shared surface for the two ES *items* drivers.

    Holds the structural-search capability, the single index-name seam every
    index-naming method routes through, and the collection-routing seam plus
    the data-side ops (``count``/``extents``/``aggregate``/``introspect``)
    that differ between the public and private drivers ONLY in those two
    seams. The genuinely divergent CRUD surface (``write_entities`` /
    ``read_entities`` / ``index`` / ``index_bulk`` / ``ensure_storage`` /
    ``drop_storage`` / ``delete_entities`` / ``location`` /
    ``get_entity_fields``) stays on each concrete driver because the doc model
    (public STAC projection vs private ``build_tenant_feature_doc``), index
    lifecycle (public alias enrolment vs tenant-only), conflict policy, and
    read reconstruction genuinely differ.

    The :data:`is_es_items_driver` marker lets non-ES code
    (``item_service._resolved_driver_is_es_items``) detect an ES items driver
    structurally without importing either concrete class.
    """

    # Structural marker for the two ES items drivers — consumed by
    # ``modules/catalog/item_service.py`` (which must stay free of a hard ES
    # import) via ``getattr(driver, "is_es_items_driver", False)``.
    is_es_items_driver: ClassVar[bool] = True

    teardown_lane: ClassVar[TeardownLane] = TeardownLane.ASYNC_CASCADE

    # Both ES items drivers can serve a pre-translated CQL2 filter
    # (``QueryRequest.es_filter``) through ``_query_request_to_es`` — so the
    # STAC ``/search`` dispatch may translate a CQL2 ``filter`` to ES DSL and
    # route it here instead of falling back to PostgreSQL. Gated by the STAC
    # dispatch via ``getattr(driver, "supports_cql_es", False)``.
    supports_cql_es: ClassVar[bool] = True

    # Row-level ABAC opt-in marker. ``False`` here means the search dispatch
    # never compiles a ``compile_read_filter`` for this driver and never sets
    # ``QueryRequest.access_filter`` — so the public and private ES item
    # drivers (and every non-envelope driver) behave exactly as before. The
    # standardized envelope driver overrides this to ``True`` to opt in to
    # document-level read scoping (#1285).
    applies_access_filter: ClassVar[bool] = False

    # System-envelope field names this driver's index carries. The structural
    # query SSOT (``build_items_query``) addresses whichever shape the resolved
    # index uses; the public per-catalog index uses the STAC-flavoured default,
    # the private driver overrides with the canonical names (see
    # ``items_query.EnvelopeFields``).
    _envelope_fields: ClassVar[EnvelopeFields] = PUBLIC_ENVELOPE_FIELDS

    # Driver-specific config class resolved by :meth:`_resolve_simplify_geometry`.
    # Each concrete driver overrides this to its own
    # ``ItemsElasticsearch*DriverConfig`` subclass so the shared base can
    # look up the per-driver config row without knowing the subclass name.
    # The public driver uses ``ItemsElasticsearchDriverConfig`` (the default
    # set here); private and envelope drivers override with their own types.
    _driver_config_class: ClassVar[Any] = None  # set per-driver below

    # ------------------------------------------------------------------
    # Override seams
    # ------------------------------------------------------------------

    def _items_index_name(self, catalog_id: str) -> str:
        """Resolve the per-tenant items index this driver reads/writes.

        The SINGLE seam every index-naming method routes through. Public →
        ``{prefix}-items-{catalog_id}``; private →
        ``{prefix}-{catalog_id}-private-items``. Concrete drivers override.
        """
        raise NotImplementedError

    def _collection_routing(self, collection_id: Optional[str]) -> Optional[str]:
        """Resolve the ES ``_routing`` key for collection-scoped data ops.

        The public per-tenant index is sharded by ``_routing=collection_id``
        so single-collection queries hit one shard; the private index is not
        routed by collection (override returns ``None``).
        """
        return collection_id

    async def index_available(self, catalog_id: str) -> bool:
        """Whether this driver's backing items index exists for ``catalog_id``.

        Navigation honours the routing config's ordered driver list. When this
        ES driver is the resolved read/search primary but its per-tenant index
        has not been created (a PG-only catalog, or indexing has not yet run),
        it cannot serve. Callers treat ``False`` as "skip to the next
        configured driver" — the PG read fallback — instead of returning an
        empty ES result that hides PG-resident rows (the #914 silent-empty
        consequence). Falling back to PG is always safe for items because PG is
        the WRITE primary / system of record. Fails closed to ``False`` so a
        missing client or a lookup error also degrades to the next driver.
        """
        from dynastore.modules.elasticsearch.client import get_client

        es = get_client()
        if es is None:
            return False
        try:
            return bool(
                await es.indices.exists(index=self._items_index_name(catalog_id))
            )
        except Exception:  # noqa: BLE001 — degrade to the next configured driver
            return False

    # ------------------------------------------------------------------
    # Shared index bootstrap and bulk tally helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _ensure_index(
        es: Any,
        index_name: str,
        mapping: Dict[str, Any],
        settings_fn: Any,
    ) -> None:
        """Idempotently create ``index_name`` if absent (race-tolerant).

        Used by the private and envelope drivers' write/index/ensure_storage
        paths — a single code path for the create-if-absent + swallow
        ``resource_already_exists`` pattern.

        ``settings_fn`` must be an async callable with no required arguments
        that returns the index settings dict (e.g.
        ``get_private_items_index_settings``).

        The public driver's ``ensure_storage`` is NOT routed here because it
        carries additional alias-enrolment logic (``_ensure_in_public_alias_once``)
        that is public-only behaviour.
        """
        if await es.indices.exists(index=index_name):
            return
        try:
            await es.indices.create(
                index=index_name,
                body={
                    "settings": await settings_fn(),
                    "mappings": mapping,
                },
            )
        except Exception as exc:
            if "resource_already_exists" not in str(exc):
                raise

    @staticmethod
    def _tally_bulk_response(
        resp: Any,
        ops_count: int,
        *,
        driver_name: str = "",
        catalog: str = "",
        collection: str = "",
        index_name: str = "",
    ) -> "tuple[int, List[Dict[str, Any]]]":
        """Parse an ES ``_bulk`` response into ``(succeeded, failures)``.

        Shared by the private and envelope ``index_bulk`` paths.  The public
        driver reuses this too (contributing the #914 zero/zero warning so
        that all three drivers log diagnostic context when ES returns a
        response shape that yields no per-item results).

        Returns
        -------
        succeeded : int
        failures  : list of ``{"id": ..., "reason": ...}`` dicts
        """
        items = (resp or {}).get("items", []) if isinstance(resp, dict) else []
        succeeded = 0
        failures: List[Dict[str, Any]] = []
        for it in items:
            entry = next(iter(it.values())) if isinstance(it, dict) and it else {}
            err = entry.get("error") if isinstance(entry, dict) else None
            if err:
                failures.append({
                    "id": entry.get("_id"),
                    "reason": str(
                        err.get("reason", err) if isinstance(err, dict) else err
                    ),
                })
            else:
                succeeded += 1
        # #914 — when the parsed result is a silent no-op (succeeded=0 with
        # no per-item failures), log the raw response shape so operators
        # can tell ``items=[]`` (request never hit ES) from a shape we
        # don't parse.
        if succeeded == 0 and not failures and ops_count > 0:
            logger.warning(
                "%s.index_bulk: ES bulk returned a shape that yielded "
                "0 succeeded / 0 failed for %d ops "
                "(catalog=%s collection=%s index=%s). resp_type=%s "
                "resp_keys=%s items_len=%d errors=%s",
                driver_name or "ItemsElasticsearchBase",
                ops_count,
                catalog,
                collection,
                index_name,
                type(resp).__name__,
                list(resp.keys()) if isinstance(resp, dict) else None,
                len(items),
                resp.get("errors") if isinstance(resp, dict) else None,
            )
        return succeeded, failures

    # ------------------------------------------------------------------
    # Shared simplification helpers
    # ------------------------------------------------------------------

    async def _resolve_simplify_geometry(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> bool:
        """Return the ``simplify_geometry`` flag from this driver's config.

        Resolves the per-driver ``ItemsElasticsearch*DriverConfig`` via the
        ConfigsProtocol waterfall, using :attr:`_driver_config_class` to
        address the right config row.  Each concrete driver sets
        ``_driver_config_class`` to its own config type; the public driver
        falls back to ``get_driver_config`` (which already resolves
        ``ItemsElasticsearchDriverConfig``).

        Degrade-safe: returns ``True`` (fail open to simplify) when the configs
        protocol is unavailable or the config row is missing.  Only an explicit
        ``simplify_geometry = False`` in the resolved config disables
        simplification.
        """
        config_cls = self.__class__._driver_config_class
        try:
            if config_cls is None:
                # Public driver path: delegate to the generic get_driver_config.
                driver_config = await self.get_driver_config(
                    catalog_id, collection_id, db_resource=db_resource,
                )
                return bool(getattr(driver_config, "simplify_geometry", True))

            from dynastore.models.protocols.configs import ConfigsProtocol
            from dynastore.models.driver_context import DriverContext
            from dynastore.tools.discovery import get_protocol

            configs = get_protocol(ConfigsProtocol)
            if configs is None:
                return True
            config = await configs.get_config(
                config_cls,
                catalog_id=catalog_id,
                collection_id=collection_id,
                ctx=DriverContext(db_resource=db_resource),
            )
            return bool(getattr(config, "simplify_geometry", True))
        except Exception:
            return True

    async def _resolve_simplify_max_bytes(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> int:
        """Effective ES geometry byte budget for simplification.

        Reads ``simplify_target_bytes`` from this driver's config and clamps it
        to ``(1, DEFAULT_MAX_BYTES)``.  Returns ``DEFAULT_MAX_BYTES`` (the 10 MB
        ES ceiling) when unset or when the config is unavailable.
        """
        from dynastore.tools.geometry_simplify import DEFAULT_MAX_BYTES

        config_cls = self.__class__._driver_config_class
        try:
            if config_cls is None:
                cfg = await self.get_driver_config(
                    catalog_id, collection_id, db_resource=db_resource,
                )
            else:
                from dynastore.models.protocols.configs import ConfigsProtocol
                from dynastore.models.driver_context import DriverContext
                from dynastore.tools.discovery import get_protocol

                configs = get_protocol(ConfigsProtocol)
                if configs is None:
                    return DEFAULT_MAX_BYTES
                cfg = await configs.get_config(
                    config_cls,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    ctx=DriverContext(db_resource=db_resource),
                )
        except Exception:
            return DEFAULT_MAX_BYTES
        return _clamp_geometry_budget(getattr(cfg, "simplify_target_bytes", None))

    # ------------------------------------------------------------------
    # Shared concrete methods — identical across private and envelope drivers
    # (and the public driver where noted)
    # ------------------------------------------------------------------

    # Backend label for StorageLocation.  The public driver overrides this by
    # providing its own ``location()`` impl; private and envelope set their own
    # values and inherit the base implementation below.
    _location_backend: ClassVar[str] = ""

    async def get_entity_fields(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        entity_level: str = "item",
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Return the introspected field set as a dict keyed by field name.

        Delegates to the inherited ``introspect_schema`` so the ES-type →
        canonical/capability mapping lives in exactly one place
        (``es_introspect_mapping``), shared across all three ES items drivers.
        """
        if entity_level != "item" or not collection_id:
            return {}
        fields = await self.introspect_schema(catalog_id, collection_id)
        return {getattr(f, "name", str(f)): f for f in fields}

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        raise NotImplementedError(
            f"{type(self).__name__}.export_entities: not supported. "
            "Export from the primary driver instead."
        )

    async def rename_storage(
        self,
        catalog_id: str,
        old_collection_id: str,
        new_collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        raise NotImplementedError(
            f"{type(self).__name__}: rename_storage is not supported. "
            "Renaming on this backend would require a full reindex."
        )

    async def restore_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        db_resource: Optional[Any] = None,
    ) -> int:
        from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
        raise SoftDeleteNotSupportedError(
            f"{type(self).__name__}: restore_entities is not implemented; "
            "deletes are physical removals (no soft-delete tombstone)."
        )

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        """Delete items by geoid from this driver's index.

        Shared by the private and envelope drivers: raises on ``soft=True``
        (neither index tracks a soft-delete tombstone), then hard-deletes each
        geoid, swallowing per-item exceptions.  The public driver overrides
        this because it (a) accepts ``soft=True`` with a log instead of raising
        and (b) passes ``routing=collection_id`` on each delete call.
        """
        from dynastore.modules.storage.errors import SoftDeleteNotSupportedError

        if soft:
            raise SoftDeleteNotSupportedError(
                f"{type(self).__name__} does not support soft delete."
            )

        index_name = self._items_index_name(catalog_id)
        es = self._get_client()
        deleted = 0

        for geoid in entity_ids:
            try:
                await es.delete(index=index_name, id=geoid)
                deleted += 1
            except Exception:
                pass

        return deleted

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Idempotent index creation; each concrete driver implements this."""
        raise NotImplementedError

    async def ensure_indexer(self, ctx) -> None:
        """Idempotent bootstrap — delegates to :meth:`ensure_storage`."""
        await self.ensure_storage(ctx.catalog, ctx.collection)

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this index.

        Shared by the private and envelope drivers; both use the same
        ``{prefix}/{catalog_id}`` identifier shape and no collection
        routing key.  The public driver overrides this method because
        its identifiers include a ``routing`` key.
        """
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.storage.storage_location import StorageLocation

        prefix = _get_index_prefix()
        index_name = self._items_index_name(catalog_id)
        backend = self.__class__._location_backend or type(self).__name__
        return StorageLocation(
            backend=backend,
            canonical_uri=f"es://{index_name}",
            identifiers={"index": index_name, "prefix": prefix, "catalog_id": catalog_id},
            display_label=index_name,
        )

    # ------------------------------------------------------------------
    # CollectionItemsStore Protocol — data-side ops
    # ------------------------------------------------------------------
    # Identical between public and private modulo the two seams: the index
    # name (:meth:`_items_index_name`) and the collection routing key
    # (:meth:`_collection_routing` — ``collection_id`` for the routed public
    # index, ``None`` for the unrouted private index).

    async def count_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> int:
        from dynastore.modules.elasticsearch.client import get_client
        from dynastore.modules.elasticsearch.items_es_ops import es_count_items

        es = get_client()
        if es is None:
            return 0
        # ``_query_request_to_es`` returns an enveloped ``{"query": ...}``;
        # ``es_count_items`` adds its own collection scope, so it wants the
        # inner query only (a double envelope is a malformed count body).
        inner = (
            self._query_request_to_es(request, self._envelope_fields).get("query")
            if request is not None
            else None
        )
        # Multi-collection: scoping is carried by the query's terms filter, so
        # drop the single-collection scope + routing (mirrors read_entities).
        multi = bool(request is not None and request.collections)
        return await es_count_items(
            es,
            self._items_index_name(catalog_id),
            query=inner,
            collection=None if multi else collection_id,
            routing=None if multi else self._collection_routing(collection_id),
        )

    async def compute_extents(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        from dynastore.modules.elasticsearch.client import get_client
        from dynastore.modules.elasticsearch.items_es_ops import es_extents

        es = get_client()
        if es is None:
            return None
        return await es_extents(
            es,
            self._items_index_name(catalog_id),
            collection=collection_id,
            routing=self._collection_routing(collection_id),
        )

    async def aggregate(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        aggregation_type: str,
        field: Optional[str] = None,
        request: Optional[Any] = None,
        db_resource: Optional[Any] = None,
    ) -> Any:
        from dynastore.modules.elasticsearch.client import get_client
        from dynastore.modules.elasticsearch.items_es_ops import es_aggregate

        es = get_client()
        if es is None:
            return None
        query = (
            self._query_request_to_es(request, self._envelope_fields)
            if request is not None
            else None
        )
        return await es_aggregate(
            es,
            self._items_index_name(catalog_id),
            aggregation_type=aggregation_type,
            field=field,
            query=query,
            collection=collection_id,
            routing=self._collection_routing(collection_id),
        )

    async def introspect_schema(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Any]:
        from dynastore.modules.elasticsearch.client import get_client
        from dynastore.modules.elasticsearch.items_es_ops import es_introspect_mapping

        es = get_client()
        if es is None:
            return []
        return await es_introspect_mapping(es, self._items_index_name(catalog_id))

    @staticmethod
    def _query_request_to_es(
        request: QueryRequest,
        fields: EnvelopeFields = PUBLIC_ENVELOPE_FIELDS,
    ) -> dict:
        """Convert a QueryRequest to an ES query body.

        Structural dimensions (``item_ids``, ``collections``, ``bbox``,
        ``intersects``, ``datetime``) are translated by the shared
        :func:`~dynastore.modules.elasticsearch.items_query.build_items_query`
        SSOT so the streaming read/count path produces exactly the same DSL the
        search path used to build. Any remaining attribute predicates carried on
        ``filters`` (``eq`` / ``like`` / a legacy ``bbox`` condition) are merged
        as additional ``must`` clauses.

        ``fields`` selects the system-envelope field names of the resolved
        index (callers pass ``self._envelope_fields`` so the private index's
        ``collection_id`` / ``geoid`` / ``external_id`` are addressed instead
        of the public ``collection`` / ``id`` / ``_external_id``).
        """
        from dynastore.modules.elasticsearch.items_query import build_items_query

        inner = build_items_query(
            ids=request.item_ids,
            collections=request.collections,
            bbox=request.bbox,
            intersects=request.intersects,
            datetime=request.datetime,
            fields=fields,
        )

        # Build an alias map from logical field names to the resolved envelope
        # field names for the active index.  Only the known identity-envelope
        # fields are remapped; every other field (e.g. ``properties.*``) falls
        # through unchanged.
        _identity_remap = {
            "external_id": fields.external_id,
            "geoid": fields.geoid,
            "id": fields.item_id,
            "collection_id": fields.collection,
            "collection": fields.collection,
        }

        extra_must: list = []
        for f in request.filters:
            op = f.operator if isinstance(f.operator, str) else f.operator.value
            resolved_field = _identity_remap.get(f.field, f.field)
            if op in ("eq", "="):
                extra_must.append({"term": {resolved_field: f.value}})
            elif op in ("like", "ilike"):
                extra_must.append({"wildcard": {resolved_field: f.value}})
            elif op in ("bbox", "&&") and f.field == "geometry":
                coords = f.value
                if isinstance(coords, (list, tuple)) and len(coords) >= 4:
                    extra_must.append({
                        "geo_shape": {
                            "geometry": {
                                "shape": {
                                    "type": "envelope",
                                    "coordinates": [
                                        [coords[0], coords[3]],
                                        [coords[2], coords[1]],
                                    ],
                                },
                                "relation": "intersects",
                            }
                        }
                    })

        if extra_must:
            # Fold attribute predicates into the structural bool. A ``match_all``
            # (no structural dims) collapses to just the attribute musts.
            if "bool" in inner:
                bool_body = dict(inner["bool"])
                bool_body["must"] = list(bool_body.get("must", [])) + extra_must
                inner = {"bool": bool_body}
            else:
                inner = {"bool": {"must": extra_must}}

        # Fold a pre-translated CQL2→ES filter clause (set by the STAC
        # ``/search`` dispatch) into the query body via the shared merge helper.
        # AND-ed into ``bool.filter`` so it composes with the structural dims and
        # attribute predicates above; a ``match_all`` base collapses to it.
        es_filter = getattr(request, "es_filter", None)
        if es_filter:
            from dynastore.modules.storage.drivers.es_common import merge_es_filter
            inner = merge_es_filter(inner, es_filter)

        return {"query": inner}

    @staticmethod
    def _build_source_filter(request: Optional[QueryRequest]) -> Optional[Dict[str, Any]]:
        """Build the ES ``_source`` clause for an items search, honouring
        ``QueryRequest.skip_geometry`` and ``QueryRequest.select``.

        Two orthogonal push-downs:

        * ``skip_geometry=True`` → ``_source.excludes`` includes
          ``"geometry"`` (the STAC/GeoJSON structural geometry member on
          every items index — see
          :func:`~dynastore.modules.elasticsearch.items_projection.project_item_for_es`)
          so ES does not return the geometry bytes at all. The service-layer
          normaliser still forces ``Feature.geometry = null`` as a safety
          net for hits that arrived from elsewhere.
        * Explicit ``select`` (any selection other than the default ``*``)
          → ``_source.includes`` lists the requested property names
          (under ``properties.*``) plus the GeoJSON/STAC structural
          members so the doc still round-trips through
          :func:`unproject_item_from_es`. Geometry is added unless
          ``skip_geometry`` also excludes it.

        Returns ``None`` (no ``_source`` filtering) for the default browse
        (``select=[*]``, ``skip_geometry=False``).
        """
        if request is None:
            return None

        skip_geom = bool(getattr(request, "skip_geometry", False))
        sel = list(request.select or [])
        explicit_select = bool(sel) and not any(s.field == "*" for s in sel)

        source: Dict[str, Any] = {}

        if explicit_select:
            # Round-trip-safe includes: the structural members that
            # ``unproject_item_from_es`` rehydrates at the top level, plus the
            # requested property names addressed under ``properties.*`` and
            # ``properties.extras.*`` (the write path tucks unknowns into
            # ``extras``).
            structural = ["id", "type", "bbox", "collection", "links",
                          "assets", "stac_version", "stac_extensions",
                          "geoid", "external_id",
                          "collection_id"]
            if not skip_geom:
                structural.append("geometry")
            prop_paths: list = []
            for s in sel:
                name = s.field
                if not name or name == "*":
                    continue
                if name.startswith("properties."):
                    prop_paths.append(name)
                else:
                    prop_paths.append(f"properties.{name}")
                    prop_paths.append(f"properties.extras.{name}")
            source["includes"] = structural + prop_paths

        if skip_geom:
            source.setdefault("excludes", [])
            if "geometry" not in source["excludes"]:
                source["excludes"].append("geometry")

        return source or None

    def _build_read_search_body(
        self,
        collection_id: str,
        request: Optional[QueryRequest],
        limit: int,
        offset: int,
        fields: EnvelopeFields = PUBLIC_ENVELOPE_FIELDS,
    ) -> tuple:
        """Build the ``(body, params)`` for a streaming items search.

        Single-collection (the common ``/items`` browse): force a
        ``{"term": {<collection-field>: …}}`` filter and route to that
        collection's shard. Multi-collection (``request.collections`` set, e.g.
        STAC ``/search`` across collections): the ``build_items_query`` SSOT
        already scopes via a ``terms`` filter, so query all shards with no
        single-collection routing. ``fields`` selects the index's envelope
        field names (see :meth:`_query_request_to_es`).

        Instance method (not ``@staticmethod``) so the query translation goes
        through ``self._query_request_to_es`` and honours subclass overrides.
        The envelope driver overrides ``_query_request_to_es`` to AND the
        row-level ``request.access_filter`` into the body and fail closed when
        none was established (#2048); a static dispatch to the base would skip
        that override and query the access-controlled envelope index with no
        row-level filter. The plain driver inherits the base translation, so
        this path is unchanged for it.

        Projection push-down (#1385): if the request carries
        ``skip_geometry=True`` or an explicit ``select``, the ES body adds a
        ``_source`` clause so ES omits the heavy ``geometry`` bytes / narrows
        the returned source to the requested properties. The service-layer
        post-fetch projection remains the universal safety net.
        """
        base = (
            self._query_request_to_es(request, fields)
            if request
            else {"query": {"match_all": {}}}
        )
        base_query = base.get("query", {"match_all": {}})
        size = limit if request is None or request.limit is None else request.limit
        from_ = offset if request is None or request.offset is None else request.offset
        params: Dict[str, Any] = {"size": str(size), "from": str(from_)}

        source_filter = _ItemsElasticsearchBase._build_source_filter(request)

        # Build ES sort clause from STAC sortby when the request carries it.
        # ``parse_sort`` handles field-path resolution (properties.* → extras.*)
        # and direction; its return value is ``[{<field>: {order}}, {"_score": …}]``.
        # Naively extending from each entry appends a ``_score`` tiebreaker after
        # every field, making ES treat ``_score`` as a higher-priority sort than
        # every field after the first. Instead, collect only the leading
        # non-``_score`` clause from each call, then append a single tiebreaker.
        sort_clause: Optional[List[Dict[str, Any]]] = None
        if request is not None and getattr(request, "sortby", None):
            from dynastore.modules.elasticsearch.items_query import parse_sort
            field_clauses: List[Dict[str, Any]] = []
            for entry in request.sortby:  # type: ignore[union-attr]
                for clause in parse_sort(entry):
                    if "_score" not in clause:
                        field_clauses.append(clause)
            if field_clauses:
                sort_clause = field_clauses + [{"_score": {"order": "desc"}}]

        if request is not None and request.collections:
            # Multi-collection: scoping is already in base_query's terms filter.
            body: Dict[str, Any] = {"query": base_query}
            if source_filter is not None:
                body["_source"] = source_filter
            if sort_clause is not None:
                body["sort"] = sort_clause
            return body, params

        collection_filter = {"term": {fields.collection: collection_id}}
        body = {
            "query": {
                "bool": {
                    "must": [base_query],
                    "filter": [collection_filter],
                }
            }
        }
        if source_filter is not None:
            body["_source"] = source_filter
        if sort_clause is not None:
            body["sort"] = sort_clause
        params["routing"] = collection_id
        return body, params


# ---------------------------------------------------------------------------
# ItemsElasticsearchDriver — public STAC items index
# ---------------------------------------------------------------------------

class ItemsElasticsearchDriver(
    TypedDriver[ItemsElasticsearchDriverConfig], _ItemsElasticsearchBase, ModuleProtocol,
):
    """Elasticsearch storage driver for STAC items.

    Items are written directly via the async ES client to a single
    per-tenant index ``{prefix}-items-{catalog_id}`` keyed by
    ``_routing=collection_id`` for shard locality. The driver enrolls
    each per-tenant index in the platform alias
    ``{prefix}-items-public`` on first ``ensure_storage`` so OGC
    discovery search routes can target that alias regardless of tenant.

    Catalog and collection documents are owned by the dedicated
    :class:`CatalogElasticsearchDriver` and
    :class:`CollectionElasticsearchDriver` (see
    :mod:`dynastore.modules.elasticsearch`).

    Registered as ``storage_elasticsearch`` via entry points.

    Indexer marker — opts in to :class:`ItemIndexer` so the items routing
    config auto-registers it in ``operations[WRITE]`` as a secondary-index
    entry (``secondary_index=True``).
    """

    is_item_indexer: ClassVar[bool] = True

    # ES (public) is the canonical async secondary index + primary SEARCH
    # backend for items routing.  It auto-defaults into WRITE (as a secondary
    # index, identified by ``is_item_indexer``) and SEARCH.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset({Operation.SEARCH, Operation.WRITE})

    priority: int = 50
    preferred_chunk_size: int = 500
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.SOFT_DELETE,
        Capability.EXTERNAL_ID_TRACKING,
        Capability.TEMPORAL_VALIDITY,
        Capability.PHYSICAL_ADDRESSING,
        Capability.INTROSPECTION,
    })
    preferred_for: FrozenSet[Hint] = frozenset({Hint.SEARCH, Hint.GEOMETRY_SIMPLIFIED})
    supported_hints: FrozenSet[Hint] = frozenset({
        Hint.SEARCH, Hint.FULLTEXT,
        Hint.GEOMETRY_SIMPLIFIED,  # PR #185 default routing: ES serves the fast simplified-geometry read path
        Hint.SPATIAL_FILTER, Hint.ATTRIBUTE_FILTER, Hint.SORT,
        Hint.AGGREGATION, Hint.COUNT, Hint.STATISTICS,
    })

    def _items_index_name(self, catalog_id: str) -> str:
        """Public per-tenant items index ``{prefix}-items-{catalog_id}``."""
        return _tenant_items_index(catalog_id)

    @property
    def es_client(self) -> Any:
        """Shared async ES client.

        The platform's ``ElasticsearchModule.lifespan`` initialises the
        singleton; this property returns the same instance every driver
        and the search extension use. Consumers outside this module
        (notably ``SearchService``) reach the ES engine through this
        property so that nothing imports
        :func:`dynastore.modules.elasticsearch.client.get_client` directly
        — the search extension reuses the platform engine via the
        routing-resolved driver instance.

        Raises ``RuntimeError`` when ``ElasticsearchModule.lifespan``
        has not started yet.
        """
        return _es_client_required()

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """No-op lifecycle.

        Item-tier propagation runs through the :class:`IndexDispatcher`
        (Phase 2d) — invoked directly from ``item_service.upsert`` and
        ``item_query.delete``. Catalog and collection documents are
        owned by :class:`CatalogElasticsearchDriver` and
        :class:`CollectionElasticsearchDriver` (in
        :mod:`dynastore.modules.elasticsearch`), so this driver does
        not subscribe to any catalog/collection events.
        """
        yield

    # ------------------------------------------------------------------
    # StorageDriverProtocol — Items
    # ------------------------------------------------------------------

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        """Write/upsert entities to Elasticsearch using the canonical envelope.

        Builds the ES ``_source`` from a batched raw-PG read via
        :func:`read_canonical_index_inputs` + :func:`build_canonical_index_doc`
        so every write path (direct ingest, bulk-reindex, outbox FATAL sync)
        lands in the same canonical shape (``stats.*`` / ``system.*`` /
        ``properties`` user-only, ``id``=geoid, ``_external_id`` tracker).

        When no raw PG row is found for a given geoid (non-PG-primary config
        or race with a concurrent delete), a feature-derived fallback is emitted
        so the write never silently drops items.

        Write-conflict policies (``ItemsWritePolicy``):
        - UPDATE (default): stable doc_id=geoid; upsert in place.
        - REFUSE: skip if a doc with the same external_id already exists.
        - NEW_VERSION: timestamped doc_id suffix; stores ``_valid_from``/
          ``_valid_to`` from context.

        Batch-level via ``on_batch_conflict``:
        - REFUSE (``refuse_batch``): raise ``ConflictError`` if any external_id
          already exists.
        """
        from datetime import datetime, timezone

        from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc
        from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput

        items = self._normalize_entities(entities)
        if not items:
            return []
        es = _es_client_required()
        index_name = self._items_index_name(catalog_id)
        # ES-primary (ES-only) write: ensure the tenant items index exists with
        # the correct mapping (``collection`` as keyword) and alias enrolment
        # BEFORE writing. Otherwise ES auto-creates it with dynamic mappings on
        # first write and the list/search ``term`` filter never matches. Bounded
        # to once per (catalog, process); ``ensure_storage`` is itself idempotent.
        if catalog_id not in _ITEMS_INDEX_ENSURED_CATALOGS:
            await self.ensure_storage(catalog_id, collection_id)
            _ITEMS_INDEX_ENSURED_CATALOGS.add(catalog_id)
        known_fields = await resolve_catalog_known_fields(catalog_id)

        # Issue #1248: auto-simplify by default. Simplification can be disabled
        # by setting ``simplify_geometry = false`` in the driver config; the
        # byte budget is clamped to the ES 10 MB ceiling.
        simplify_geometry = await self._resolve_simplify_geometry(
            catalog_id, collection_id, db_resource=db_resource,
        )
        simplify_max_bytes = await self._resolve_simplify_max_bytes(
            catalog_id, collection_id, db_resource=db_resource,
        )

        # Service-layer enforcement of FieldDefinition.required / .unique for
        # drivers (like ES) that don't advertise native REQUIRED_ENFORCEMENT /
        # UNIQUE_ENFORCEMENT. Only runs when the collection's
        # ItemsSchema has allow_app_level_enforcement=True; otherwise
        # config admission would have already rejected the constraints.
        await self._enforce_field_constraints(catalog_id, collection_id, items)

        # Resolve write policy from the config waterfall.
        policy = await self._resolve_write_policy(catalog_id, collection_id)

        # Extract context fields (ingestion metadata — not part of feature payload).
        ctx = context or {}
        asset_id: Optional[str] = ctx.get("asset_id")
        valid_from = ctx.get("valid_from")
        valid_to = ctx.get("valid_to")

        # --- Pre-pass: collect geoids for the batch canonical read (#1800) ---
        # Canonical _source is built from the raw PG row + resolved sidecars for
        # each item.  A single batched SELECT per collection avoids N+1 reads.
        # Geoid sources (in priority order):
        #   1. top-level "geoid" key (expose_geoid=True on the read policy)
        #   2. system.geoid (expose_all=True path)
        #   3. properties.geoid (some callers surface it there)
        #   4. context["geoid"] (explicitly supplied by the call-site, e.g. outbox)
        #   5. top-level "id" — last resort. On the post-write secondary fan-out
        #      (PG-primary, e.g. pg+es collections) the canonical ``id`` already
        #      IS the geoid. Without this source the batched PG read is skipped
        #      and the feature-derived fallback emits a minimal doc, silently
        #      dropping the ``stats`` lane and every ``system`` field except
        #      geoid (external_id, *_hash, validity). A non-geoid id (ES-only
        #      producer id) simply misses the PG SELECT and lands on the same
        #      fallback as before, so adding it here is safe.
        item_stac_docs: List[dict] = []
        item_geoids: List[Optional[str]] = []
        for item in items:
            stac_doc = self._feature_to_stac_item(item, catalog_id, collection_id)
            item_stac_docs.append(stac_doc)
            geoid_for_item = (
                stac_doc.get("geoid")
                or (stac_doc.get("system") or {}).get("geoid")
                or (stac_doc.get("properties") or {}).get("geoid")
                or ctx.get("geoid")
                or stac_doc.get("id")
            )
            item_geoids.append(geoid_for_item)

        # Batch-fetch canonical inputs for all non-None geoids.
        batch_geoids = [g for g in item_geoids if g is not None]
        canonical_inputs: Dict[str, Any] = {}
        if batch_geoids:
            canonical_inputs = await read_canonical_index_inputs(
                catalog_id, collection_id, batch_geoids, db_resource=db_resource,
            )

        written: List = []
        prepped_bulk: list = []

        for item, stac_doc, geoid_for_id in zip(items, item_stac_docs, item_geoids):
            # Resolve external_id from the configured ComputedField path.
            external_id = self._extract_external_id_from_doc(stac_doc, policy.external_id_path())

            # Build the ES doc_id based on conflict policy.
            from dynastore.modules.storage.driver_config import WriteConflictPolicy

            base_id = external_id or stac_doc.get("id")

            # Batch-level check — runs before item-level; raises on first match.
            if policy.on_batch_conflict is not None and external_id:
                from dynastore.modules.storage.driver_config import BatchConflictPolicy
                if (
                    policy.on_batch_conflict == BatchConflictPolicy.REFUSE
                    and await self._es_doc_exists_by_external_id(
                        es, index_name, collection_id, external_id,
                    )
                ):
                    from dynastore.modules.storage.errors import ConflictError
                    raise ConflictError(
                        f"ES driver: external_id '{external_id}' already exists "
                        f"in {catalog_id}/{collection_id} (policy=refuse_batch)"
                    )

            # Item-level check — skip this entity, continue batch.
            if policy.on_conflict == WriteConflictPolicy.REFUSE:
                if external_id and await self._es_doc_exists_by_external_id(
                    es, index_name, collection_id, external_id,
                ):
                    logger.debug(
                        "ES write_entities(REFUSE): external_id '%s' exists — skipped",
                        external_id,
                    )
                    continue

            # NEW_VERSION: each version gets a unique doc_id. Store validity window.
            versioned_suffix: Optional[str] = None
            if policy.on_conflict == WriteConflictPolicy.NEW_VERSION:
                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
                versioned_suffix = ts
                # An open lower bound (ValiditySpec.start_from is None) keeps
                # ``_valid_from`` unset; otherwise a missing start defaults to
                # the ingestion instant for this new version. (#1172)
                start_is_open = (
                    policy.validity is not None and policy.validity.start_from is None
                )
                if valid_from is None and not start_is_open:
                    valid_from = datetime.now(timezone.utc).isoformat()

            # Resolve the canonical doc — prefer the raw PG row; fall back to a
            # feature-derived minimal doc when the row is absent (non-PG-primary
            # config, concurrent delete, or race).
            ci: Optional[CanonicalIndexInput] = (
                canonical_inputs.get(geoid_for_id) if geoid_for_id else None
            )
            if ci is not None:
                es_doc = build_canonical_index_doc(
                    ci.row,
                    resolved_sidecars=ci.resolved_sidecars,
                    known_fields=known_fields,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    geometry=ci.geometry,
                    bbox=ci.bbox,
                    user_properties=ci.user_properties,
                    access=ci.access,
                    stac_reserved_members=ci.stac_reserved_members,
                )
                doc_id = geoid_for_id
            else:
                # Fallback: feature-derived canonical doc (no stats/system).
                # Preserves identity + user properties + geometry; the canonical
                # shape is maintained (stats/system sections are empty/absent).
                #
                # For ES-only STAC collections (stac_storage=ES, no PG sidecar),
                # this is the primary write path.  Per-item STAC content
                # (assets, stac_extensions) lives only in the inbound feature doc
                # and must be threaded through to the ES _source so
                # unproject_item_from_es can surface them on read.
                raw_props = stac_doc.get("properties") or {}
                from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS as _SFK
                _sys_keys = frozenset(_SFK)
                user_props = {k: v for k, v in raw_props.items() if k not in _sys_keys}
                geom = stac_doc.get("geometry")
                bbox_val = stac_doc.get("bbox")
                fallback_row: Dict[str, Any] = {"geoid": geoid_for_id or base_id}
                if external_id is not None:
                    fallback_row["external_id"] = str(external_id)
                if asset_id is not None:
                    fallback_row["asset_id"] = str(asset_id)

                # Collect per-item STAC reserved members present in the
                # serialized feature.  ``assets`` and ``stac_extensions`` are
                # already in ``_RESERVED_MEMBER_KEYS`` so unproject_item_from_es
                # passes them through verbatim — they just need to be stored.
                _stac_reserved: Dict[str, Any] = {}
                _raw_assets = stac_doc.get("assets")
                if _raw_assets is not None:
                    _stac_reserved["assets"] = _raw_assets
                _raw_exts = stac_doc.get("stac_extensions")
                if _raw_exts is not None:
                    _stac_reserved["stac_extensions"] = _raw_exts

                es_doc = build_canonical_index_doc(
                    fallback_row,
                    resolved_sidecars=[],
                    known_fields=known_fields,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    geometry=geom if isinstance(geom, dict) else None,
                    bbox=list(bbox_val) if bbox_val is not None else None,
                    user_properties=user_props or None,
                    access=None,
                    stac_reserved_members=_stac_reserved or None,
                )
                doc_id = geoid_for_id or base_id
                logger.debug(
                    "write_entities: no raw PG row for geoid=%s in %s/%s — "
                    "using feature-derived fallback doc",
                    geoid_for_id, catalog_id, collection_id,
                )

            if doc_id is None:
                logger.error(
                    "ES write_entities: skipping item with no id in %s/%s "
                    "— this item will NOT be indexed in Elasticsearch.",
                    catalog_id, collection_id,
                )
                continue

            # NEW_VERSION: append timestamp suffix to the doc_id (not to the
            # canonical id in _source — that stays as geoid).
            if versioned_suffix is not None:
                doc_id = f"{doc_id}_{versioned_suffix}"
            # Propagate ingestion-context tracking fields onto the _source so
            # historical versions retain their validity window.
            if valid_from is not None:
                es_doc["_valid_from"] = valid_from
            if valid_to is not None:
                es_doc["_valid_to"] = valid_to
            # asset_id is carried only by the canonical root ``asset_id`` identity
            # field (build_canonical_index_doc writes it from the row). The legacy
            # ``_asset_id`` ``_source`` mirror is no longer stamped — it was an
            # unindexed duplicate of the root field (#1285 identity convergence).

            # Geometry simplification (#1248/#1828) — operates on the assembled
            # _source dict so the canonical envelope is intact; metadata is
            # recorded in system.geometry_simplification (nested, typed).
            es_doc, factor, mode = maybe_simplify_for_es(
                es_doc, simplify=simplify_geometry, max_bytes=simplify_max_bytes,
            )
            _apply_geometry_simplification(es_doc, factor, mode)

            prepped_bulk.append({
                "action": {"index": {
                    "_index": index_name,
                    "_id": str(doc_id),
                    "routing": collection_id,
                }},
                "doc": es_doc,
            })
            written.append(item)

        if prepped_bulk:
            body: list = []
            submitted_ids: list = []
            for entry in prepped_bulk:
                body.append(entry["action"])
                body.append(entry["doc"])
                submitted_ids.append(entry["action"]["index"]["_id"])
            from dynastore.modules.elasticsearch._mapping_errors import (
                maybe_raise_bulk_mapping_mismatch,
                raise_on_bulk_errors,
            )
            # ``write_entities`` is the synchronous ES-*primary* write path
            # (ES-only / ES-primary collections): there is no PG row and no
            # async indexer behind it, so this write IS the source of truth and
            # callers expect read-after-write. ``refresh="wait_for"`` blocks
            # until the doc is visible to ``_search`` — without it, ``refresh=
            # false`` plus ES *search-idle* (auto-refresh pauses after 30s with
            # no searches) leaves freshly-written items retrievable by id but
            # invisible to ``_search``/list. The async secondary indexer paths
            # (``index``/``index_bulk``) stay ``refresh="false"`` — they are
            # eventual by design and drained from the outbox.
            resp = await es.bulk(body=body, params={"refresh": "wait_for"})
            maybe_raise_bulk_mapping_mismatch(resp, index_name)
            raise_on_bulk_errors(resp, index_name, submitted_ids)

        return written

    async def _enforce_field_constraints(
        self,
        catalog_id: str,
        collection_id: str,
        items: List[Any],
    ) -> None:
        """App-level fallback enforcement of FieldDefinition.required / .unique.

        Only runs when the collection's ItemsSchema has
        ``allow_app_level_enforcement=True`` (otherwise admission would have
        rejected any constrained fields). Raises
        ``RequiredFieldMissingError`` (HTTP 400) or
        ``UniqueConstraintViolationError`` (HTTP 409).
        """
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.driver_config import ItemsSchema
        from dynastore.modules.storage.field_constraints import (
            check_required, check_unique,
        )
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
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

        if not ft.allow_app_level_enforcement:
            return

        feature_dicts = [
            it if isinstance(it, dict) else (
                it.model_dump(by_alias=True, exclude_none=False)
                if hasattr(it, "model_dump") else dict(it)
            )
            for it in items
        ]
        check_required(ft.fields, feature_dicts)

        async def _exists(field_name: str, value: Any) -> bool:
            # In-batch dedup only for now; a proper ES term query per unique
            # field is a follow-up (driver-specific CQL2/term construction).
            return False

        await check_unique(ft.fields, feature_dicts, exists=_exists)

    @staticmethod
    async def _resolve_write_policy(
        catalog_id: str, collection_id: str,
    ) -> "ItemsWritePolicy":
        """Resolve ItemsWritePolicy from the config waterfall."""
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy,
        )
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        try:
            configs = get_protocol(ConfigsProtocol)
            if configs:
                result = await configs.get_config(
                    ItemsWritePolicy,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
                if isinstance(result, ItemsWritePolicy):
                    return result
        except Exception:
            pass
        return ItemsWritePolicy()

    @staticmethod
    async def _resolve_read_policy(
        catalog_id: str, collection_id: str,
    ) -> "ItemsReadPolicy":
        """Resolve ItemsReadPolicy from the config waterfall.

        Degrade-safe: any miss (no configs protocol, no stored row, fetch
        error) returns the default policy so a read never 500s on a missing
        wire-shape config.
        """
        from dynastore.modules.storage.read_policy import ItemsReadPolicy
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        try:
            configs = get_protocol(ConfigsProtocol)
            if configs:
                result = await configs.get_config(
                    ItemsReadPolicy,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
                if isinstance(result, ItemsReadPolicy):
                    return result
        except Exception:
            pass
        return ItemsReadPolicy()

    @staticmethod
    def _es_source_to_feature(
        source: Dict[str, Any],
        read_policy: Optional["ItemsReadPolicy"] = None,
    ) -> Feature:
        """Reconstruct a read-contract Feature from an indexed ES ``_source``.

        The indexed ``_source`` is shaped for indexing, not for the wire:
        unknown attributes are nested under ``properties.extras``, internal
        ``_*`` tracking fields and leaked echo keys sit at the top level, and
        ``geometry`` may be empty ``{}``. Returning it verbatim produced
        malformed GeoJSON (extras nesting, top-level attribute echo via the
        ``extra="allow"`` Feature model, ``"geometry": {}``).

        Structural normalisation (un-project ``extras`` → flat ``properties``,
        drop internal/echo top-level keys, null an empty geometry) is
        delegated to :func:`unproject_item_from_es`. Read-policy exposure is
        layered on here so an ES-served read matches the PostgreSQL
        ``map_row_to_feature`` contract:

        * ``external_id_as_feature_id`` → the indexed root ``external_id``
          becomes the feature ``id``;
        * ``expose_geoid`` / ``expose_created`` gate the ``geoid`` / ``created``
          properties (suppressed unless the policy opts in).
        """
        from dynastore.modules.elasticsearch.items_projection import (
            unproject_item_from_es,
        )

        clean = unproject_item_from_es(source)
        props = clean.get("properties")
        if read_policy is not None and isinstance(props, dict):
            ft = read_policy.feature_type
            if getattr(ft, "external_id_as_feature_id", False) and isinstance(source, dict):
                ext = source.get("external_id")
                if ext is not None:
                    clean["id"] = str(ext)
            if not getattr(ft, "expose_geoid", False):
                props.pop("geoid", None)
            if not getattr(ft, "expose_created", False):
                props.pop("created", None)
        return Feature.model_validate(clean)

    @staticmethod
    def _extract_external_id_from_doc(doc: dict, field_path: Optional[str]) -> Optional[str]:
        """Extract external_id from a feature/ES doc using the configured path.

        Mirrors the PG-side resolution (``_walk_external_id_path`` /
        ``ItemsWritePolicy.resolve_external_id`` in ``driver_config``) so the ES
        write boundary resolves the SAME value the PG primary does:

          1. dotted path ``a.b.c`` → traverse nested dicts;
          2. bare name → top-level lookup, then ``properties[name]`` fallback.

        The properties fallback is the load-bearing part: a bare configured path
        like ``"CODE"`` carries its value under ``properties`` on the wire
        feature, so without the fallback it resolves to ``None`` on the ES side —
        dropping ``system.external_id`` in the feature-derived path, degrading the
        ES ``_id`` to the geoid, and defeating the REFUSE existence check.
        """
        if field_path is None:
            return None
        if "." in field_path:
            val = doc
            for part in field_path.split("."):
                if not isinstance(val, dict):
                    return None
                val = val.get(part)
                if val is None:
                    return None
            return str(val)
        val = doc.get(field_path)
        if val is None:
            props = doc.get("properties")
            if isinstance(props, dict):
                val = props.get(field_path)
        return str(val) if val is not None else None

    @staticmethod
    async def _es_doc_exists_by_external_id(
        es: Any, index_name: str, collection_id: str, external_id: str,
    ) -> bool:
        """Check whether any document in the tenant index for this
        collection carries ``external_id == external_id``.

        Uses ``_routing=collection_id`` so the count hits a single shard.
        """
        try:
            resp = await es.count(
                index=index_name,
                body={
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {"collection": collection_id}},
                                {"term": {"external_id": external_id}},
                            ]
                        }
                    }
                },
                params={"routing": collection_id},
            )
            return resp.get("count", 0) > 0
        except Exception:
            return False

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        context: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        from dynastore.modules.storage.routing_config import (
            get_output_transformers_for_search,
        )
        from dynastore.models.protocols.entity_transform import (
            TransformChainContext,
        )
        from dynastore.modules.storage.transform_runtime import (
            restore_transform_chain,
        )
        from dynastore.tools.typed_store.base import _to_snake

        es = _es_client_required()
        index_name = self._items_index_name(catalog_id)
        # Resolve the wire-shape policy once per query so every hit is
        # reconstructed against the same read contract (id source, exposure).
        read_policy = await self._resolve_read_policy(catalog_id, collection_id)
        # Resolve the output-transformer chain once per query (empty → no-op).
        restore_chain = await get_output_transformers_for_search(
            catalog_id,
            entity="item",
            collection_id=collection_id,
            driver_ref=_to_snake(type(self).__name__),
        )
        # One restore context per query (read path → no pg_conn); the shared
        # cache lets the restore chain batch any lookups across the page (#1568).
        restore_ctx = TransformChainContext()

        if entity_ids:
            for eid in entity_ids:
                try:
                    resp = await es.get(
                        index=index_name, id=eid,
                        params={"routing": collection_id},
                    )
                    src = resp.get("_source")
                    if src is not None:
                        feature = self._es_source_to_feature(src, read_policy)
                        if restore_chain:
                            feature = await restore_transform_chain(
                                feature,
                                restore_chain,
                                catalog_id=catalog_id,
                                collection_id=collection_id,
                                entity_kind="item",
                                ctx=restore_ctx,
                            )
                        yield feature
                except Exception as exc:
                    logger.warning(
                        "ItemsElasticsearchDriver: failed to fetch/transform item"
                        " id=%s catalog=%s collection=%s: %s",
                        eid, catalog_id, collection_id, exc,
                    )
        else:
            # Single-collection scopes+routes to one collection's shard;
            # multi-collection (request.collections) queries all shards.
            body, params = self._build_read_search_body(
                collection_id, request, limit, offset, self._envelope_fields,
            )
            try:
                resp = await es.search(
                    index=index_name,
                    body=body,
                    params=params,
                )
                for hit in resp.get("hits", {}).get("hits", []):
                    hit_id = hit.get("_id", "<unknown>")
                    try:
                        feature = self._es_source_to_feature(
                            hit["_source"], read_policy,
                        )
                        if restore_chain:
                            feature = await restore_transform_chain(
                                feature,
                                restore_chain,
                                catalog_id=catalog_id,
                                collection_id=collection_id,
                                entity_kind="item",
                                ctx=restore_ctx,
                            )
                        yield feature
                    except Exception as exc:
                        logger.warning(
                            "ItemsElasticsearchDriver: failed to transform search hit"
                            " id=%s catalog=%s collection=%s: %s",
                            hit_id, catalog_id, collection_id, exc,
                        )
            except Exception as e:
                logger.warning(
                    "ItemsElasticsearchDriver: search failed for %s/%s: %s",
                    catalog_id, collection_id, e,
                )

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        if soft:
            logger.info(
                "ES delete_entities(soft=True): marking %d entities as deleted",
                len(entity_ids),
            )
        es = _es_client_required()
        index_name = self._items_index_name(catalog_id)
        deleted = 0
        for eid in entity_ids:
            try:
                await es.delete(
                    index=index_name, id=eid,
                    params={"routing": collection_id, "ignore": "404"},
                )
                deleted += 1
            except Exception as exc:
                logger.warning(
                    "ItemsElasticsearchDriver: failed to delete item"
                    " id=%s catalog=%s collection=%s: %s",
                    eid, catalog_id, collection_id, exc,
                )
        return deleted

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Idempotently create the per-tenant items index and enrol it
        in the platform public alias.

        The index ``{prefix}-{catalog_id}-items`` hosts every collection's
        items for this catalog; collection scoping is enforced via
        ``_routing=collection_id`` on every write/read. Membership in the
        platform public alias ``{prefix}-items`` makes the data
        discoverable through OGC search routes regardless of tenant.

        ``collection_id`` is accepted for protocol parity but ignored —
        the same tenant index serves all collections of the catalog.
        """
        from dynastore.modules.elasticsearch.aliases import (
            add_index_to_public_alias,
        )
        from dynastore.modules.elasticsearch.mappings import (
            ITEMS_INDEX_CAP_SAFE_MAPPING_PATCH,
            build_item_mapping,
        )
        from dynastore.modules.elasticsearch.items_projection import (
            resolve_catalog_known_fields,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_items_index_settings,
        )

        es = _es_client_required()
        index_name = self._items_index_name(catalog_id)

        try:
            exists = await es.indices.exists(index=index_name)
        except Exception as exc:
            logger.warning(
                "ItemsElasticsearchDriver.ensure_storage: exists() failed for "
                "'%s': %s", index_name, exc,
            )
            exists = False

        if not exists:
            # Snapshot the per-catalog Tier-1 ∪ Tier-2 known-fields at
            # index-create time. Live edits to ``mapping`` do not
            # retro-patch ES (ES disallows tightening a live mapping);
            # they take effect on the next index rebuild.
            known_fields = await resolve_catalog_known_fields(catalog_id)
            try:
                await es.indices.create(
                    index=index_name,
                    body={
                        "settings": await get_items_index_settings(),
                        "mappings": build_item_mapping(known_fields),
                    },
                )
                logger.info(
                    "ItemsElasticsearchDriver: created tenant items index '%s' "
                    "with %d known fields (Tier 1 ∪ Tier 2).",
                    index_name, len(known_fields),
                )
            except Exception as exc:
                if "resource_already_exists" not in str(exc):
                    raise
                logger.warning(
                    "ItemsElasticsearchDriver.ensure_storage: create('%s') "
                    "lost race to a concurrent create — proceeding",
                    index_name,
                )
        else:
            # Best-effort: ensure an older index (pre-#1295, `extras:
            # object,dynamic:true`) at least has the new `_search_text`
            # root field so writes carrying it aren't rejected by the
            # strict root mapping. The `extras` field itself can't be
            # retyped from `object` to `flattened` in place — that
            # needs a reindex, tracked as the migration follow-up.
            try:
                await es.indices.put_mapping(
                    index=index_name,
                    body=ITEMS_INDEX_CAP_SAFE_MAPPING_PATCH,
                )
            except Exception as exc:
                logger.warning(
                    "ItemsElasticsearchDriver.ensure_storage: put_mapping "
                    "cap-safe patch on existing index '%s' failed: %s",
                    index_name, exc,
                )

        # Idempotent — safe even if the index was already a member.
        await add_index_to_public_alias(index_name)

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
        **kwargs: Any,
    ) -> None:
        from dynastore.modules.elasticsearch.aliases import (
            remove_index_from_public_alias,
        )

        es = _es_client_required()
        index_name = self._items_index_name(catalog_id)
        if collection_id:
            try:
                await es.delete_by_query(
                    index=index_name,
                    body={"query": {"term": {"collection": collection_id}}},
                    params={"routing": collection_id, "refresh": "false"},
                )
            except Exception as e:
                logger.warning(
                    "drop_storage collection delete_by_query failed for %s/%s: %s",
                    catalog_id, collection_id, e,
                )
        else:
            await remove_index_from_public_alias(index_name)
            try:
                await es.indices.delete(
                    index=index_name, params={"ignore_unavailable": "true"},
                )
            except Exception as e:
                logger.warning("drop_storage catalog delete failed: %s", e)

    # ------------------------------------------------------------------
    # Generic Indexer Protocol — slim, dispatcher-facing surface
    # ------------------------------------------------------------------
    # export_entities and ensure_indexer are inherited from
    # _ItemsElasticsearchBase.

    async def index(self, ctx, op) -> None:
        """Apply a single :class:`IndexOp` to the per-tenant items index.

        Called by :class:`IndexDispatcher` from inside the caller's PG
        transaction.  Failure raises; the dispatcher applies the
        configured ``FailurePolicy`` (FATAL → caller rollback, OUTBOX
        → enqueue retry row in same TX, WARN → log).

        No routing-membership guard here: the dispatcher only invokes
        drivers pinned as secondary-index ``WRITE`` entries
        (``secondary_index=True``) in ``operations[WRITE]`` for this
        ``(catalog, collection)`` — the guard lives in the routing layer,
        not the driver.
        """
        if op.entity_type != "item":
            # Items driver only handles item-tier ops.  Non-item routes
            # land on a different Indexer registered for that tier.
            return
        if not ctx.collection:
            raise ValueError(
                "ItemsElasticsearchDriver.index: collection is required for item ops",
            )

        es = _es_client_required()
        index_name = self._items_index_name(ctx.catalog)
        await _ensure_in_public_alias_once(ctx.catalog, index_name)

        if op.op_type == "delete":
            await es.delete(
                index=index_name, id=op.entity_id,
                params={"routing": ctx.collection, "ignore": "404"},
            )
            return

        # op_type == "upsert": build the canonical doc from a raw PG read.
        # Bypasses op.payload entirely so the indexed document
        # uses the canonical envelope (stats/system/properties/access) built
        # directly from the raw row + resolved sidecars — no read-policy
        # filtering, no external_id_as_feature_id id-flip (#1800).
        known_fields = await resolve_catalog_known_fields(ctx.catalog)
        inputs = await read_canonical_index_inputs(
            ctx.catalog, ctx.collection, [op.entity_id],
            db_resource=ctx.pg_conn,
        )
        ci = inputs.get(op.entity_id)
        if ci is None:
            # No raw PG row.  For a PG-primary collection this means the row
            # vanished between write and index (race / soft-delete) → skip.
            # For a file-backed collection there is never a PG row, so fall
            # back to a feature-derived canonical doc built from op.payload.
            if op.payload:
                ci = canonical_input_from_feature(
                    op.payload, ctx.catalog, ctx.collection,
                    geoid=op.entity_id,
                    external_id=op.payload.get("external_id"),
                    asset_id=op.payload.get("asset_id"),
                )
            else:
                logger.debug(
                    "ItemsElasticsearchDriver.index: %s/%s/%s — no raw row and "
                    "no payload; skipping",
                    ctx.catalog, ctx.collection, op.entity_id,
                )
                return
        from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc
        doc = build_canonical_index_doc(
            ci.row,
            resolved_sidecars=ci.resolved_sidecars,
            known_fields=known_fields,
            catalog_id=ctx.catalog,
            collection_id=ctx.collection,
            geometry=ci.geometry,
            bbox=ci.bbox,
            user_properties=ci.user_properties,
            access=ci.access,
            stac_reserved_members=ci.stac_reserved_members,
        )
        simplify_geometry = await self._resolve_simplify_geometry(ctx.catalog, ctx.collection)
        simplify_max_bytes = await self._resolve_simplify_max_bytes(ctx.catalog, ctx.collection)
        doc, factor, mode = maybe_simplify_for_es(
            doc, simplify=simplify_geometry, max_bytes=simplify_max_bytes,
        )
        _apply_geometry_simplification(doc, factor, mode)
        await es.index(
            index=index_name, id=op.entity_id, body=doc,
            params={"routing": ctx.collection},
        )

    async def index_bulk(self, ctx, ops):
        """Apply a batch of :class:`IndexOp` via the ES ``_bulk`` API.

        Returns a :class:`BulkResult` summarising success / failure
        per-op.  An unhandled exception (auth, connection) raises; the
        dispatcher applies the configured ``FailurePolicy`` to the whole
        batch.

        Upsert ops are built from the canonical doc builder (#1800):
        geoids are batched into a single raw-PG read, then each doc is
        assembled via ``build_canonical_index_doc``.  ``op.payload`` is
        ignored for the doc body (the canonical raw-row path supersedes it).
        Delete ops are passed through unchanged.
        ``_id`` is always the geoid (``op.entity_id``).
        """
        from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc
        from dynastore.models.protocols.indexer import BulkResult

        if not ops:
            return BulkResult()
        if not ctx.collection:
            raise ValueError(
                "ItemsElasticsearchDriver.index_bulk: collection is required for item ops",
            )

        es = _es_client_required()
        index_name = self._items_index_name(ctx.catalog)
        await _ensure_in_public_alias_once(ctx.catalog, index_name)
        known_fields = await resolve_catalog_known_fields(ctx.catalog)
        simplify_geometry = await self._resolve_simplify_geometry(ctx.catalog, ctx.collection)
        simplify_max_bytes = await self._resolve_simplify_max_bytes(ctx.catalog, ctx.collection)

        # Batch-fetch canonical inputs for all upsert ops in one PG round-trip.
        upsert_geoids = [
            op.entity_id
            for op in ops
            if op.entity_type == "item" and op.op_type == "upsert"
        ]
        # Pass ctx.pg_conn as db_resource so the PG read uses the live
        # connection from the caller's transaction when available (covers the
        # Cloud Run JOB/worker context where the dispatcher's IndexContext
        # carries the wrapping TX opened by _dispatch_index_upsert Phase 2f).
        canonical_inputs = await read_canonical_index_inputs(
            ctx.catalog, ctx.collection, upsert_geoids,
            db_resource=ctx.pg_conn,
        ) if upsert_geoids else {}

        body: List[dict] = []
        for op in ops:
            if op.entity_type != "item":
                continue
            if op.op_type == "delete":
                body.append({"delete": {
                    "_index": index_name, "_id": op.entity_id,
                    "routing": ctx.collection,
                }})
                continue
            # op_type == "upsert": build canonical doc from raw-row inputs.
            ci = canonical_inputs.get(op.entity_id)
            if ci is None:
                # No raw PG row.  PG-primary collection → row vanished between
                # write and index (race / soft-delete), skip.  File-backed
                # collection → never has a PG row, so fall back to a
                # feature-derived canonical doc built from op.payload.
                if op.payload:
                    # Outbox payloads carry the canonical identity under the
                    # stamped ``_external_id`` / ``_asset_id`` keys (see
                    # ItemService._apply_index_stamp); a bare ``external_id`` /
                    # ``asset_id`` is never present. Read the stamped keys first
                    # (falling back to the bare names for any non-outbox caller)
                    # so the feature-derived doc still carries
                    # ``system.external_id`` / ``asset_id`` instead of silently
                    # dropping them.
                    ci = canonical_input_from_feature(
                        op.payload, ctx.catalog, ctx.collection,
                        geoid=op.entity_id,
                        external_id=(
                            op.payload.get("_external_id")
                            or op.payload.get("external_id")
                        ),
                        asset_id=(
                            op.payload.get("_asset_id")
                            or op.payload.get("asset_id")
                        ),
                    )
                else:
                    logger.debug(
                        "ItemsElasticsearchDriver.index_bulk: %s/%s/%s — no raw "
                        "row and no payload; skipping upsert op",
                        ctx.catalog, ctx.collection, op.entity_id,
                    )
                    continue
            doc = build_canonical_index_doc(
                ci.row,
                resolved_sidecars=ci.resolved_sidecars,
                known_fields=known_fields,
                catalog_id=ctx.catalog,
                collection_id=ctx.collection,
                geometry=ci.geometry,
                bbox=ci.bbox,
                user_properties=ci.user_properties,
                access=ci.access,
                stac_reserved_members=ci.stac_reserved_members,
            )
            doc, factor, mode = maybe_simplify_for_es(
                doc, simplify=simplify_geometry, max_bytes=simplify_max_bytes,
            )
            _apply_geometry_simplification(doc, factor, mode)
            body.append({"index": {
                "_index": index_name, "_id": op.entity_id,
                "routing": ctx.collection,
            }})
            body.append(doc)

        if not body:
            return BulkResult(total=len(ops))

        resp = await es.bulk(body=body, params={"refresh": "false"})
        succeeded, failures = self._tally_bulk_response(
            resp, len(ops),
            driver_name="ItemsElasticsearchDriver",
            catalog=ctx.catalog,
            collection=ctx.collection,
            index_name=index_name,
        )
        return BulkResult(
            total=len(ops),
            succeeded=succeeded,
            failed=len(failures),
            failures=failures,
        )

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this collection.

        The tenant index hosts every collection of the catalog; the
        ``routing`` identifier records the per-collection shard key so
        consumers can reproduce reads.
        """
        from dynastore.modules.storage.storage_location import StorageLocation

        index_name = self._items_index_name(catalog_id)
        return StorageLocation(
            backend="elasticsearch",
            canonical_uri=f"es://{index_name}?routing={collection_id}",
            identifiers={
                "index": index_name,
                "routing": collection_id,
            },
            display_label=f"{index_name} (routing={collection_id})",
        )

    # Data-side ops (count/extents/aggregate/introspect), get_entity_fields,
    # export_entities, rename_storage, restore_entities, and ensure_indexer are
    # inherited from :class:`_ItemsElasticsearchBase`; the public per-tenant
    # index is sharded by ``_routing=collection_id`` so the default
    # ``_collection_routing`` (returns ``collection_id``) and
    # ``_items_index_name`` above suffice.



# ---------------------------------------------------------------------------
# AssetElasticsearchDriver — per-catalog asset index
# ---------------------------------------------------------------------------

class AssetElasticsearchDriver(
    TypedDriver[AssetElasticsearchDriverConfig], _ElasticsearchBase, ModuleProtocol,
):
    """Elasticsearch storage driver for asset metadata.

    Multi-tier scope today
    ----------------------
    The per-catalog index ``{prefix}-assets-{catalog_id}`` (mapping at
    ``modules/elasticsearch/mappings.py:ASSET_MAPPING``) carries BOTH
    catalog-tier and collection-tier assets — the mapping has both
    ``catalog_id`` (always set) and a nullable ``collection_id``
    (NULL for catalog-tier assets, set for collection-tier).  This is
    NOT a per-tier driver; it serves the catalog/collection asset
    spectrum from one index.

    Indexer marker — opts in to :class:`AssetIndexer` only.  The
    ``AssetIndexer`` marker is documented as tier-spanning at the
    catalog/collection level (see ``models/protocols/indexer.py``); per-tier
    asset routing is unnecessary today because both tiers land in the
    same index.

    Extension axis — future tier expansions
    ---------------------------------------
    Two future tiers are pre-declared as marker Protocols in
    ``models/protocols/indexer.py`` but have no implementer yet:

    * :class:`ItemAssetIndexer` (``is_item_asset_indexer``) — for
      promoting item-embedded assets to first-class index entries.
      Today STAC item docs store ``assets`` as opaque blob
      (``COMMON_PROPERTIES`` declares ``"assets": {"enabled": False}``);
      promotion is a deferred STAC read/write refactor.  When it lands,
      this class will add ``is_item_asset_indexer: ClassVar[bool] = True``
      and start emitting per-item-asset documents to the same per-catalog
      index (with ``item_id`` populated; mapping field already added for
      forward-compat).
    * :class:`PlatformAssetIndexer` (``is_platform_asset_indexer``) —
      for assets above the catalog scope (no design yet; ``AssetBase``
      requires ``catalog_id`` today).

    Both marker opt-ins are deferred until the consumer tier ships;
    the markers themselves exist so future drivers (or this driver's
    future extension) can self-register without a rename.

    Lifecycle wiring
    ----------------
    No lifespan-time wiring is required.  Asset writes flow through the
    ``AssetIndexer`` secondary-index ``WRITE`` entry (``secondary_index=True``)
    in ``AssetRoutingConfig.operations[WRITE]`` —
    invoked by ``AssetService``'s secondary-driver fan-out — and through
    direct programmatic calls to ``index_asset()`` / ``delete_asset()``.

    Registered as ``storage_elasticsearch_assets`` via entry points.
    """

    is_asset_indexer: ClassVar[bool] = True

    teardown_lane: ClassVar[TeardownLane] = TeardownLane.ASYNC_CASCADE

    # Asset ES is the canonical async secondary index + primary SEARCH
    # backend for asset metadata routing.  It auto-defaults into WRITE (as a
    # secondary index, identified by ``is_asset_indexer``) and SEARCH.
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset({Operation.SEARCH, Operation.WRITE})

    priority: int = 52
    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.PHYSICAL_ADDRESSING,
    })
    preferred_for: FrozenSet[Hint] = frozenset({Hint.SEARCH, Hint.ASSETS})
    supported_hints: FrozenSet[Hint] = frozenset({Hint.SEARCH, Hint.ASSETS, Hint.FULLTEXT})

    # ``is_available`` / ``_get_client`` inherited from ``_ElasticsearchBase``.

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        # Asset writes flow exclusively through the AssetIndexer secondary-index
        # WRITE entry (secondary_index=True) in AssetRoutingConfig.operations[WRITE]
        # — invoked by AssetService's
        # secondary-driver fan-out. The previous CatalogEventType.ASSET_*
        # listener path was retired to eliminate a dual-write race against
        # the same index.
        yield

    # ------------------------------------------------------------------
    # Public API — direct programmatic indexing
    # ------------------------------------------------------------------

    async def index_asset(
        self, catalog_id: str, asset_doc: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Index a single asset document."""
        from dynastore.modules.elasticsearch.mappings import (
            get_assets_index_name, ASSET_MAPPING,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_assets_index_settings,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            try:
                await es.indices.create(
                    index=index_name,
                    body={
                        "settings": await get_assets_index_settings(),
                        "mappings": ASSET_MAPPING,
                    },
                )
            except Exception as exc:
                if "resource_already_exists" not in str(exc):
                    raise

        asset_id = asset_doc.get("asset_id", asset_doc.get("id"))
        await es.index(index=index_name, id=asset_id, body=asset_doc)

    async def delete_asset(
        self, catalog_id: str, asset_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Delete a single asset document from the index."""
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        try:
            await es.delete(index=index_name, id=asset_id)
        except Exception as exc:
            logger.warning(
                "AssetElasticsearchDriver: failed to delete asset"
                " id=%s catalog=%s: %s",
                asset_id, catalog_id, exc,
            )

    # ------------------------------------------------------------------
    # Generic Indexer Protocol — slim, dispatcher-facing surface
    # ------------------------------------------------------------------

    async def ensure_indexer(self, ctx) -> None:
        """Idempotent bootstrap — creates ``{prefix}-assets-{catalog_id}``
        with ``ASSET_MAPPING`` if missing.  No alias today (assets are
        per-catalog only; no platform-wide assets alias).
        """
        await self.ensure_storage(ctx.catalog, ctx.collection)

    async def index(self, ctx, op) -> None:
        """Apply a single asset :class:`IndexOp` via the existing
        :meth:`index_asset` / :meth:`delete_asset` helpers.

        Skips ops whose ``entity_type`` is not ``"asset"`` — different
        tier; a different Indexer fields it.
        """
        if op.entity_type != "asset":
            return
        if op.op_type == "delete":
            await self.delete_asset(ctx.catalog, op.entity_id)
            return
        # upsert
        doc = dict(op.payload or {})
        doc.setdefault("asset_id", op.entity_id)
        doc.setdefault("catalog_id", ctx.catalog)
        if ctx.collection is not None:
            doc.setdefault("collection_id", ctx.collection)
        await self.index_asset(ctx.catalog, doc)

    async def index_bulk(self, ctx, ops):
        """Bulk-apply a batch of asset ops.

        Delegates per-op to :meth:`index` for now — asset writes are
        rare enough vs item writes that a single ES round-trip per op
        isn't a hot-path concern.  A native ``_bulk`` implementation
        can land later if profiling motivates it.
        """
        from dynastore.models.protocols.indexer import BulkResult

        succeeded = 0
        failures: List[Dict[str, Any]] = []
        for op in ops:
            if op.entity_type != "asset":
                continue
            try:
                await self.index(ctx, op)
                succeeded += 1
            except Exception as exc:  # noqa: BLE001 — surface per-op failures
                failures.append({"id": op.entity_id, "reason": str(exc)})
        return BulkResult(
            total=len(ops),
            succeeded=succeeded,
            failed=len(failures),
            failures=failures,
        )

    # ------------------------------------------------------------------
    # StorageDriverProtocol
    # ------------------------------------------------------------------

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        from dynastore.modules.elasticsearch.mappings import (
            get_assets_index_name, ASSET_MAPPING,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_assets_index_settings,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        items = self._normalize_entities(entities)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            try:
                await es.indices.create(
                    index=index_name,
                    body={
                        "settings": await get_assets_index_settings(),
                        "mappings": ASSET_MAPPING,
                    },
                )
            except Exception as exc:
                if "resource_already_exists" not in str(exc):
                    raise

        bulk_body: list = []
        asset_ids: list = []
        for item in items:
            doc = item if isinstance(item, dict) else (
                item.model_dump(by_alias=True, exclude_none=True)
                if hasattr(item, "model_dump") else dict(item)
            )
            doc.setdefault("catalog_id", catalog_id)
            doc.setdefault("collection_id", collection_id)
            asset_id = doc.get("asset_id", doc.get("id", ""))
            bulk_body.append({"index": {"_index": index_name, "_id": asset_id}})
            bulk_body.append(doc)
            asset_ids.append(str(asset_id))

        if bulk_body:
            from dynastore.modules.elasticsearch._mapping_errors import (
                maybe_raise_bulk_mapping_mismatch,
                raise_on_bulk_errors,
            )
            resp = await es.bulk(body=bulk_body)
            maybe_raise_bulk_mapping_mismatch(resp, index_name)
            raise_on_bulk_errors(resp, index_name, asset_ids)

        return items if isinstance(items, list) else list(items)

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        context: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        # By-id lookup path. Read-side (output) transform chains are not
        # applied here by design (geoid#1643); only search_assets (the SEARCH
        # path on this driver) invokes restore_transform_chain. Declaring
        # output_transformers on a by-id READ entry will not fire.
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        if not entity_ids:
            return

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        if not await es.indices.exists(index=index_name):
            return

        for asset_id in entity_ids:
            try:
                resp = await es.get(index=index_name, id=asset_id)
                source = resp["_source"]
                yield Feature(
                    type="Feature",
                    id=source.get("asset_id", asset_id),
                    geometry=None,
                    properties=source,
                )
            except Exception as exc:
                logger.warning(
                    "AssetElasticsearchDriver: failed to fetch asset"
                    " id=%s catalog=%s: %s",
                    asset_id, catalog_id, exc,
                )

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        if soft:
            raise SoftDeleteNotSupportedError(
                "AssetElasticsearchDriver does not support soft delete."
            )
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        deleted = 0
        for asset_id in entity_ids:
            try:
                await es.delete(index=index_name, id=asset_id)
                deleted += 1
            except Exception as exc:
                logger.warning(
                    "AssetElasticsearchDriver: failed to delete asset"
                    " id=%s catalog=%s: %s",
                    asset_id, catalog_id, exc,
                )
        return deleted

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        from dynastore.modules.elasticsearch.mappings import (
            get_assets_index_name, ASSET_MAPPING,
        )
        from dynastore.modules.elasticsearch.index_config import (
            get_assets_index_settings,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        if not await es.indices.exists(index=index_name):
            try:
                await es.indices.create(
                    index=index_name,
                    body={
                        "settings": await get_assets_index_settings(),
                        "mappings": ASSET_MAPPING,
                    },
                )
            except Exception as exc:
                if "resource_already_exists" not in str(exc):
                    raise

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
        **kwargs: Any,
    ) -> None:
        if soft:
            raise SoftDeleteNotSupportedError(
                "AssetElasticsearchDriver does not support soft drop."
            )
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        if collection_id:
            # Collection-scoped teardown: delete only THIS collection's asset
            # docs. The assets index is per-catalog (a single index hosts every
            # collection, discriminated by the ``collection_id`` keyword), so a
            # whole-index delete here would wipe sibling collections' assets.
            # Scope by ``collection_id`` instead — mirrors the items driver so
            # the async cascade can safely own collection-granular asset
            # teardown.
            try:
                await es.delete_by_query(
                    index=index_name,
                    body={"query": {"term": {"collection_id": collection_id}}},
                    params={"ignore_unavailable": "true", "refresh": "false"},
                )
            except Exception as e:
                logger.warning(
                    "AssetElasticsearchDriver.drop_storage: collection "
                    "delete_by_query failed for %s/%s: %s",
                    catalog_id, collection_id, e,
                )
        else:
            # Catalog-scoped teardown: drop the whole per-catalog assets index.
            await es.indices.delete(
                index=index_name, params={"ignore_unavailable": "true"},
            )

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        raise NotImplementedError(
            "AssetElasticsearchDriver.export_entities: not supported."
        )

    # ------------------------------------------------------------------
    # AssetStore read methods
    # ------------------------------------------------------------------

    async def get_asset(
        self,
        catalog_id: str,
        asset_id: str,
        *,
        collection_id: Optional[str] = None,
        db_resource=None,
    ) -> Optional[Dict[str, Any]]:
        """Return a single asset document from ES by its ID, or None.

        Direct-get visibility contract: an asset the caller has no visibility
        grant for is indistinguishable from a missing one — returns None so
        the HTTP layer renders 404, never 403 or 200-with-data.
        """
        from dynastore.models.protocols.visibility import resolve_asset_listing_ids
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        visible_ids = await resolve_asset_listing_ids(catalog_id, collection_id)
        if visible_ids is not None and asset_id not in visible_ids:
            return None

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()
        try:
            resp = await es.get(index=index_name, id=asset_id)
            return resp["_source"]
        except Exception:
            return None

    async def search_assets(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        filters: Optional[List[AssetFilter]] = None,
        limit: int = 100,
        offset: int = 0,
        all_collections: bool = False,
        db_resource=None,
    ) -> List[Dict[str, Any]]:
        """Search asset documents in ES.

        ``filters`` is an optional list of :class:`AssetFilter`. The operator
        set and ES-clause translation live in
        :func:`dynastore.modules.tools.asset_filters.build_es_query`
        (shared with the PG driver so both backends honour the same operators).
        Dot-notation fields (``metadata.license_id``) resolve natively in ES.
        ``None``/empty → ``match_all``.

        Collection scope mirrors the PG asset driver (tri-state):
        - ``collection_id="<id>"`` → ``term`` filter on that collection.
        - ``collection_id=None`` and ``all_collections=False`` → catalog-tier
          assets only, via a ``must_not exists collection_id`` clause (a doc
          bound to no collection has the field absent/null). This keeps ES and
          PG scoping identical for the default catalog-tier search.
        - ``all_collections=True`` → no collection clause; spans every
          collection plus the catalog tier under the catalog.

        Asset listing visibility: when the request published a caller snapshot,
        the listing is transparently narrowed to the asset ids that caller may
        see — applied as an ES ``terms`` clause, fail-closed when the visible
        set is empty.
        """
        from dynastore.models.protocols.visibility import resolve_asset_listing_ids
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix

        vis_collection_id = None if all_collections else collection_id
        visible_ids = await resolve_asset_listing_ids(catalog_id, vis_collection_id)
        if visible_ids is not None and not visible_ids:
            return []

        index_name = get_assets_index_name(_get_index_prefix(), catalog_id)
        es = self._get_client()

        base_query = build_es_query(filters or [])
        if all_collections:
            pass  # no collection clause — span the whole catalog
        elif collection_id:
            base_query = {
                "bool": {
                    "must": [base_query],
                    "filter": [{"term": {"collection_id": collection_id}}],
                }
            }
        else:
            base_query = {
                "bool": {
                    "must": [base_query],
                    "must_not": [{"exists": {"field": "collection_id"}}],
                }
            }

        if visible_ids is not None:
            # Inject the asset_id allowlist as an ES ``terms`` filter.
            # Mirroring the collection_es_driver visible_ids pattern.
            existing = base_query if "bool" in base_query else {"bool": {"must": [base_query]}}
            existing.setdefault("bool", {}).setdefault("filter", []).append(
                {"terms": {"asset_id": sorted(visible_ids)}}
            )
            base_query = existing

        try:
            resp = await es.search(
                index=index_name,
                query=base_query,
                size=limit,
                from_=offset,
            )
            hits = [hit["_source"] for hit in resp["hits"]["hits"]]
            from dynastore.models.protocols.entity_transform import (
                TransformChainContext,
            )
            from dynastore.modules.storage.routing_config import (
                get_output_transformers_for_search,
            )
            from dynastore.modules.storage.transform_runtime import (
                restore_transform_chain,
            )
            from dynastore.tools.typed_store.base import _to_snake

            chain = await get_output_transformers_for_search(
                catalog_id,
                entity="asset",
                collection_id=collection_id,
                driver_ref=_to_snake(type(self).__name__),
            )
            if not chain:
                return hits
            # One restore context per query — shared cache across the page (#1568).
            restore_ctx = TransformChainContext()
            restored: List[Dict[str, Any]] = []
            for hit in hits:
                restored.append(
                    await restore_transform_chain(
                        hit,
                        chain,
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                        entity_kind="asset",
                        ctx=restore_ctx,
                    )
                )
            return restored
        except Exception as e:
            logger.error("AssetElasticsearchDriver.search_assets failed: %s", e)
            return []

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        """Return typed physical storage coordinates for this asset index."""
        from dynastore.modules.storage.storage_location import StorageLocation
        from dynastore.modules.elasticsearch.client import get_index_prefix as _get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_assets_index_name

        prefix = _get_index_prefix()
        index_name = get_assets_index_name(prefix, catalog_id)
        return StorageLocation(
            backend="elasticsearch_assets",
            canonical_uri=f"es://{index_name}",
            identifiers={"index": index_name, "prefix": prefix, "catalog_id": catalog_id},
            display_label=index_name,
        )


