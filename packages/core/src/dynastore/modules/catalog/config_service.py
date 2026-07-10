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

import logging
import json
from collections import OrderedDict
from typing import Optional, Dict, Any, List, Tuple, Type, Union
from dynastore.tools.cache import cached, DEFAULT_CONFIG_CACHE_TTL, DEFAULT_CONFIG_CACHE_L1_TTL
from dynastore.modules.storage.router import invalidate_router_cache


def _materialise_ref_row(row: Optional[Dict[str, Any]], ref_key: str) -> Optional["PluginConfig"]:
    """F.4c.2 — turn a ``(class_key, config_data)`` row into a validated ``PluginConfig``.

    Returns ``None`` for an empty row or when the row's ``class_key`` is not in
    the live registry (warning logged so dropped rows surface in operator
    triage).  Inline import resolves the late-binding via
    ``platform_config_service.resolve_config_class``.
    """
    if not row:
        return None
    cls = resolve_config_class(row["class_key"])
    if cls is None:
        logger.warning(
            "get_config_by_ref: ref %r stored class_key %r not in registry",
            ref_key,
            row["class_key"],
        )
        return None
    return _validate_stored_config(cls, row["config_data"])


def _maybe_bust_router(cls: Type["PluginConfig"], catalog_id: Optional[str], collection_id: Optional[str]) -> None:
    """Bust the distributed router cache only for routing-config writes.

    Non-routing writes (TilesConfig, FeaturesPluginConfig, ...) don't affect
    driver resolution, so nuking the whole Valkey cache for them is pure waste.
    """
    try:
        from dynastore.modules.storage.routing_config import (
            ItemsRoutingConfig,
            AssetRoutingConfig,
        )
        if not issubclass(cls, (ItemsRoutingConfig, AssetRoutingConfig)):
            return
    except Exception:
        return
    invalidate_router_cache(catalog_id, collection_id)
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DbResource,
)
from dynastore.modules.db_config.shared_queries import list_page_with_count
from dynastore.models.driver_context import DriverContext
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.json import CustomJSONEncoder

# Class-as-identity config API.
from dynastore.models.plugin_config import PluginConfig, require_config_class, resolve_config_class
from dynastore.modules.db_config.platform_config_service import (
    enforce_config_immutability,
    restore_system_assigned_fields,
    run_apply_handlers,
    run_validate_handlers,
)
from dynastore.modules.db_config.stored_config_read import _validate_stored_config
from dynastore.modules.db_config.locking_tools import check_table_exists
from dynastore.modules.db_config.config_version import (
    decode_config_version,
    encode_config_version,
)
from dynastore.modules.db_config.exceptions import ConfigVersionConflictError
from dynastore.modules.db_config.typed_store.ddl import (
    CATALOG_CONFIGS_TABLE,
    COLLECTION_CONFIGS_TABLE,
)
from dynastore.modules.db_config.typed_store import config_queries as _cq
from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol

logger = logging.getLogger(__name__)


def _serialize_config_for_db(config: "PluginConfig") -> str:
    """Serialize a config to the JSON string persisted in the configs table.

    ``secret_mode="db"`` → every Secret field serializes to its encrypted
    envelope before jsonb persistence (see tools/secrets.py). ``exclude_unset``
    → store only the fields the caller explicitly sent; class defaults and
    parent-tier values are resolved at read time by the ``get_config`` waterfall,
    not baked in here. A non-model ``config`` (already a plain mapping) is passed
    through to ``json.dumps`` unchanged.

    Single SSOT for the write-side serialization contract shared by every
    ``set_config`` / ``set_config_by_ref`` path (catalog and collection tiers).
    """
    config_data = (
        config.model_dump(mode="json", context={"secret_mode": "db"}, exclude_unset=True)
        if hasattr(config, "model_dump")
        else config
    )
    return json.dumps(config_data, cls=CustomJSONEncoder)

# ==============================================================================
#  STORAGE & SCHEMAS
# ==============================================================================

# Table name constants live in typed_store/ddl.py — imported above.


# Identity normaliser: accept either a class or a class_key string and return
# (class, class_key).  Endpoints that receive dynamic string path params go
# through this at the service boundary.
def _resolve(config_cls: "Union[str, Type[PluginConfig]]") -> "tuple[Type[PluginConfig], str]":
    cls = require_config_class(config_cls)
    return cls, cls.class_key()


# ==============================================================================
#  CACHES
# ==============================================================================


@cached(
    maxsize=8192,
    ttl=DEFAULT_CONFIG_CACHE_TTL,
    namespace="catalog_config",
    ignore=["engine", "catalog_manager"],
    l1_ttl=DEFAULT_CONFIG_CACHE_L1_TTL,
    condition=lambda v: v is not None,
)
async def _catalog_config_cache(
    engine: DbResource,
    catalog_manager: CatalogsProtocol,
    catalog_id: str,
    class_key: str,
) -> Optional[dict]:
    """Database fetcher (returned as dict for immutability)."""
    validate_sql_identifier(catalog_id)
    async with managed_transaction(engine) as conn:
        phys_schema = await catalog_manager.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
        )
        if not phys_schema:
            return None

        if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
            return None

        return await _cq.select_catalog_config(phys_schema).execute(conn, ref_key=class_key)


@cached(
    maxsize=16384,
    ttl=DEFAULT_CONFIG_CACHE_TTL,
    namespace="collection_config",
    ignore=["engine", "catalog_manager"],
    l1_ttl=DEFAULT_CONFIG_CACHE_L1_TTL,
    condition=lambda v: v is not None,
)
async def _collection_config_cache(
    engine: DbResource,
    catalog_manager: CatalogsProtocol,
    catalog_id: str,
    collection_id: str,
    class_key: str,
) -> Optional[dict]:
    """Database fetcher (returned as dict for immutability)."""
    validate_sql_identifier(catalog_id)
    validate_sql_identifier(collection_id)
    async with managed_transaction(engine) as conn:
        phys_schema = await catalog_manager.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
        )
        if not phys_schema:
            return None

        if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
            return None

        return await _cq.select_collection_config(phys_schema).execute(
            conn,
            collection_id=collection_id,
            ref_key=class_key,
        )


# ==============================================================================
#  COLLECTION-SCOPED CONFIG CACHE INVALIDATION
# ==============================================================================


def invalidate_collection_config_cache(catalog_id: str, collection_id: str) -> None:
    """Drop every ``_collection_config_cache`` entry for ``(catalog_id, collection_id)``.

    ``_collection_config_cache`` is keyed per ``(catalog_id, collection_id,
    class_key)``.  There is no single-key invalidation that covers all
    class_key variants at once via the standard ``cache_invalidate`` helper,
    because that requires knowing the exact class_key up front.

    Instead, we use ``cache_clear_prefix`` — added to every ``@cached``
    async wrapper — which scans for keys whose prefix matches the
    ``(catalog_id, collection_id)`` sub-namespace.  The sub-namespace string
    is constructed to match the format that ``_make_cache_key`` produces::

        collection_config|repr(catalog_id)|repr(collection_id)|repr(class_key)

    so the prefix ``collection_config|repr(catalog_id)|repr(collection_id)``
    covers every class_key entry for that collection.

    Called from ``_invalidate_collection_lifecycle_caches`` on every lifecycle
    transition (create / PROVISIONING→ACTIVE / soft-delete / hard-delete /
    reclaim) so stale configs cannot be served after a reclaim where the same
    id is reused.
    """
    sub_ns = f"collection_config|{repr(catalog_id)}|{repr(collection_id)}"
    clear_prefix = getattr(_collection_config_cache, "cache_clear_prefix", None)
    if clear_prefix is not None:
        clear_prefix(sub_ns)


def invalidate_catalog_config_caches(internal_catalog_id: str) -> None:
    """Drop every catalog- and collection-config cache entry for a catalog.

    Catalog delete/reclaim and re-creation have no lifecycle-invalidation hook
    into ``_catalog_config_cache`` / ``_collection_config_cache`` today (#2895):
    a config resolved just before a delete stays cached for up to the L2 TTL
    (300s) and would be served to a reclaimed catalog id that reuses the same
    internal id. Same ``cache_clear_prefix`` mechanism as
    ``invalidate_collection_config_cache`` — the catalog-config key format is
    ``catalog_config|repr(catalog_id)|repr(class_key)`` and the
    collection-config key format is
    ``collection_config|repr(catalog_id)|repr(collection_id)|repr(class_key)``,
    so a prefix of just ``repr(catalog_id)`` on each covers every class_key
    (and, for collections, every collection) under this catalog.

    ``internal_catalog_id`` must already be the immutable internal id — every
    read (``get_catalog_config_internal_cached`` /
    ``get_collection_config_internal_cached``) and write invalidation in this
    module key on that form (see ``ConfigService._internal_catalog_id``).
    """
    cat_sub_ns = f"catalog_config|{repr(internal_catalog_id)}"
    cat_clear_prefix = getattr(_catalog_config_cache, "cache_clear_prefix", None)
    if cat_clear_prefix is not None:
        cat_clear_prefix(cat_sub_ns)

    col_sub_ns = f"collection_config|{repr(internal_catalog_id)}"
    col_clear_prefix = getattr(_collection_config_cache, "cache_clear_prefix", None)
    if col_clear_prefix is not None:
        col_clear_prefix(col_sub_ns)


# ==============================================================================
#  MANAGER
# ==============================================================================


# Hot-working-set bound for ``_validated_config_cache`` — matches the row
# caches' maxsize (1024). Entries beyond the bound are the least recently
# resolved (class, catalog, collection) triples and simply re-validate on
# next use.
_VALIDATED_CONFIG_CACHE_MAXSIZE = 1024


class ConfigService(ConfigsProtocol):
    """Hierarchical Configuration Manager with framework-level immutability enforcement."""

    priority: int = 10

    def __init__(
        self,
        engine: Optional[DbResource] = None,
        catalog_manager: Optional[CatalogsProtocol] = None,
        platform_config_manager: Optional[PlatformConfigsProtocol] = None,
    ):
        self.engine = engine
        self._catalog_manager = catalog_manager
        self._catalogs_service = catalog_manager
        self._platform_config_service = platform_config_manager
        # Per-process memo of fully-validated waterfall results, keyed by
        # (class_key, catalog_id, collection_id) and identity-guarded against
        # the exact base/delta objects it was computed from — correctness
        # never depends on an entry surviving, so LRU eviction is safe. The
        # bound exists because an unbounded dict grows with every
        # catalog x collection ever resolved in the process (#3160: the
        # cold-boot deny-policy scan pushed workers into OOM through it).
        self._validated_config_cache: OrderedDict[
            Tuple[str, Optional[str], Optional[str]],
            Tuple[PluginConfig, Optional[dict], Optional[dict], PluginConfig],
        ] = OrderedDict()

    @property
    def catalog_manager(self) -> CatalogsProtocol:
        return self._get_catalog_manager()

    @catalog_manager.setter
    def catalog_manager(self, value: CatalogsProtocol):
        self._catalogs_service = value

    @property
    def platform_config_service(self) -> PlatformConfigsProtocol:
        return self._get_platform_config_service()

    @platform_config_service.setter
    def platform_config_service(self, value: PlatformConfigsProtocol):
        self._platform_config_service = value

    def is_available(self) -> bool:
        return self.engine is not None

    def _clear_validated_config_cache(self) -> None:
        self._validated_config_cache.clear()

    def _get_catalog_manager(self) -> CatalogsProtocol:
        if self._catalogs_service is None:
            from dynastore.tools.discovery import get_protocol
            self._catalogs_service = get_protocol(CatalogsProtocol)
        assert self._catalogs_service is not None, "CatalogsProtocol not registered"
        return self._catalogs_service

    def _get_platform_config_service(self) -> PlatformConfigsProtocol:
        if self._platform_config_service is None:
            from dynastore.tools.discovery import get_protocol
            self._platform_config_service = get_protocol(PlatformConfigsProtocol)
        if self._platform_config_service is None:
            from dynastore.modules.db_config.platform_config_service import PlatformConfigService
            self._platform_config_service = PlatformConfigService()
        return self._platform_config_service

    async def get_config(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
        config_snapshot: Optional[Dict[str, Any]] = None,
    ) -> PluginConfig:
        """Retrieves configuration with a waterfall MERGE:
        1. Snapshot (if provided, overrides everything)
        2. Start from platform tier (platform row overlaid on code defaults)
        3. Overlay catalog delta (if catalog_id)
        4. Overlay collection delta (if collection_id)

        Each DB row holds only the keys explicitly set at that tier (a delta).
        Fields absent from a delta inherit from the parent tier, so bumping a
        class default or platform value propagates through without rewriting
        per-catalog / per-collection rows.
        """
        cls, class_key = _resolve(config_cls)
        db_resource = ctx.db_resource if ctx else None
        use_validated_cache = db_resource is None and config_snapshot is None

        # Tier 0: Snapshot (authoritative override)
        if config_snapshot is not None:
            if class_key in config_snapshot:
                data = config_snapshot[class_key]
                if data:
                    # Snapshots built from ``list_catalog_configs`` carry
                    # already-validated instances; only raw dicts (e.g. a
                    # stored-row shape written before a field rename) need the
                    # tolerant-read treatment.
                    if isinstance(data, cls):
                        return data
                    return _validate_stored_config(cls, data)
            # Authoritative snapshot: skip DB tiers, fall through to platform.
            catalog_id = None
            collection_id = None

        # Base: platform + code defaults (fully-populated model)
        base = await self._get_platform_config_service().get_config(
            cls, ctx=DriverContext(db_resource=db_resource) if db_resource else None
        )

        if not catalog_id:
            return base

        # #1079 (c): if this catalog froze a still-valid defaults snapshot for
        # ``cls`` at creation, use it as the inheritance base instead of the
        # live platform/code default — so a later default change does not
        # silently re-resolve into collections that already inherited it. The
        # snapshot is schema-id-gated: on a class-schema change the entry is
        # ignored and we keep the live ``base`` above. Shared with the
        # composed-config view via ``resolve_catalog_snapshot_base`` so the
        # two paths agree by construction (#2830).
        from dynastore.modules.catalog.config_snapshot import (
            resolve_catalog_snapshot_base,
        )
        snap_base = await resolve_catalog_snapshot_base(
            cls, catalog_id, self.get_catalog_defaults_snapshot,
        )
        if snap_base is not None:
            base = snap_base

        # Collect per-tier deltas top-down.
        deltas: list[dict] = []
        catalog_delta: Optional[dict] = None
        collection_delta: Optional[dict] = None

        # Tier 2: Catalog delta
        if not db_resource:
            catalog_delta = await self.get_catalog_config_internal_cached(
                catalog_id, class_key
            )
        else:
            async with managed_transaction(db_resource) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if phys_schema and await check_table_exists(
                    conn, CATALOG_CONFIGS_TABLE, phys_schema
                ):
                    catalog_delta = await _cq.select_catalog_config(phys_schema).execute(
                        conn, ref_key=class_key
                    )
                else:
                    catalog_delta = None
        if catalog_delta:
            deltas.append(catalog_delta)

        # Tier 1: Collection delta
        if collection_id:
            if not db_resource:
                collection_delta = await self.get_collection_config_internal_cached(
                    catalog_id, collection_id, class_key
                )
            else:
                async with managed_transaction(db_resource) as conn:
                    phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                        catalog_id, ctx=DriverContext(db_resource=conn)
                    )
                    if phys_schema and await check_table_exists(
                        conn, COLLECTION_CONFIGS_TABLE, phys_schema
                    ):
                        # Issue #2430: Resolve to internal ID for lookup
                        try:
                            resolved = await self._get_catalog_manager().collections.resolve_collection_ids(
                                catalog_id, collection_id, allow_missing=True
                            )
                            internal_collection_id = resolved.id
                        except Exception:
                            internal_collection_id = collection_id
                        collection_delta = await _cq.select_collection_config(phys_schema).execute(
                            conn, collection_id=internal_collection_id, ref_key=class_key
                        )
                    else:
                        collection_delta = None
            if collection_delta:
                deltas.append(collection_delta)

        if not deltas:
            return base

        cache_key: Optional[Tuple[str, Optional[str], Optional[str]]] = None
        if use_validated_cache:
            cache_key = (class_key, catalog_id, collection_id)
            cached = self._validated_config_cache.get(cache_key)
            if (
                cached is not None
                and cached[0] is base
                and cached[1] is catalog_delta
                and cached[2] is collection_delta
            ):
                self._validated_config_cache.move_to_end(cache_key)
                return cached[3]

        # Merge base.model_dump() with deltas in order (catalog → collection).
        # mode="python" preserves native types for round-trip re-validation.
        # ``_validate_stored_config`` strips keys the live class schema no
        # longer declares (warning logged) so a stored delta written by an
        # older deploy (field later removed) never injects an unknown key
        # that would cause extra_forbidden on validation.  The class default
        # for the missing field is used instead.
        merged: Dict[str, Any] = base.model_dump(mode="python")
        for delta in deltas:
            merged.update(delta)
        resolved = _validate_stored_config(cls, merged)
        if cache_key is not None:
            self._validated_config_cache[cache_key] = (
                base,
                catalog_delta,
                collection_delta,
                resolved,
            )
            self._validated_config_cache.move_to_end(cache_key)
            while len(self._validated_config_cache) > _VALIDATED_CONFIG_CACHE_MAXSIZE:
                self._validated_config_cache.popitem(last=False)
        return resolved

    async def get_configs_batch(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        catalog_id: str,
        collection_ids: "List[str]",
        ctx: Optional[DriverContext] = None,
    ) -> "Dict[str, PluginConfig]":
        """Resolve the same waterfall as :meth:`get_config` for many collections
        of one catalog in a bounded number of round trips (#2865 PG-fallback
        fix).

        ``get_config`` called once per collection with ``ctx.db_resource`` set
        bypasses the L1/L2 caches by design (a live connection means "read
        through", e.g. inside an open transaction) and re-does the catalog-tier
        work — physical-schema resolution, table-existence checks, catalog
        delta fetch — on every call even though none of it varies by
        collection. For a large catalog (thousands of collections) that is
        thousands of redundant round trips on a single connection.

        This fetches the catalog-tier delta ONCE and the collection-tier
        deltas for every id in ``collection_ids`` in a SINGLE
        ``collection_id = ANY(...)`` query, then applies the identical
        base → catalog → collection merge :meth:`get_config` uses. Only
        meaningful when ``ctx.db_resource`` is set; without it, per-collection
        resolution already goes through the (cheap, already-cached)
        ``get_collection_config_internal_cached`` path.

        Returns a config for every id in ``collection_ids`` (never partial).
        """
        cls, class_key = _resolve(config_cls)
        db_resource = ctx.db_resource if ctx else None

        base = await self._get_platform_config_service().get_config(
            cls, ctx=DriverContext(db_resource=db_resource) if db_resource else None
        )

        from dynastore.modules.catalog.config_snapshot import (
            resolve_catalog_snapshot_base,
        )
        snap_base = await resolve_catalog_snapshot_base(
            cls, catalog_id, self.get_catalog_defaults_snapshot,
        )
        if snap_base is not None:
            base = snap_base

        catalog_delta: Optional[dict] = None
        collection_delta_by_id: Dict[str, dict] = {}

        if not db_resource:
            catalog_delta = await self.get_catalog_config_internal_cached(catalog_id, class_key)
            for cid in collection_ids:
                delta = await self.get_collection_config_internal_cached(
                    catalog_id, cid, class_key
                )
                if delta:
                    collection_delta_by_id[cid] = delta
        else:
            async with managed_transaction(db_resource) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True,
                )
                if phys_schema:
                    if await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                        catalog_delta = await _cq.select_catalog_config(phys_schema).execute(
                            conn, ref_key=class_key,
                        )
                    if collection_ids and await check_table_exists(
                        conn, COLLECTION_CONFIGS_TABLE, phys_schema
                    ):
                        internal_to_external: Dict[str, str] = {}
                        internal_ids: List[str] = []
                        for cid in collection_ids:
                            try:
                                resolved = await (
                                    self._get_catalog_manager().collections.resolve_collection_ids(
                                        catalog_id, cid, allow_missing=True,
                                    )
                                )
                                internal_id = resolved.id
                            except Exception:
                                internal_id = cid
                            internal_ids.append(internal_id)
                            internal_to_external[internal_id] = cid
                        rows = await _cq.select_collection_configs_batch(phys_schema).execute(
                            conn, collection_ids=internal_ids, ref_key=class_key,
                        )
                        for row in rows:
                            raw_id: str = row["collection_id"]
                            ext_id: str = internal_to_external.get(raw_id, raw_id)
                            collection_delta_by_id[ext_id] = row["config_data"]

        result: Dict[str, PluginConfig] = {}
        base_dump: Optional[Dict[str, Any]] = None
        for cid in collection_ids:
            delta = collection_delta_by_id.get(cid)
            if not catalog_delta and not delta:
                result[cid] = base
                continue
            if base_dump is None:
                base_dump = base.model_dump(mode="python")
            merged = dict(base_dump)
            if catalog_delta:
                merged.update(catalog_delta)
            if delta:
                merged.update(delta)
            result[cid] = _validate_stored_config(cls, merged)
        return result

    async def _internal_catalog_id(self, catalog_id: str) -> str:
        """Normalize ``catalog_id`` to its immutable internal id for cache keys.

        ``resolve_catalog_id`` is external-only (returns ``None`` for an
        already-internal id, by design — see its docstring), so the
        None-guard leaves an already-internal id unchanged. Every
        ``_catalog_config_cache`` / ``_collection_config_cache`` read and
        invalidation call site goes through this so the two agree on the same
        key regardless of which id form the caller holds (#2895).
        """
        resolved = await self._get_catalog_manager().resolve_catalog_id(
            catalog_id, allow_missing=True
        )
        return resolved if resolved is not None else catalog_id

    async def get_catalog_config_internal_cached(
        self, catalog_id: str, class_key: str
    ) -> Optional[dict]:
        assert self.engine is not None, "ConfigService.engine not initialised"
        catalogs = self._get_catalog_manager()
        catalog_id = await self._internal_catalog_id(catalog_id)
        return await _catalog_config_cache(
            self.engine, catalogs, catalog_id, class_key
        )

    async def get_catalog_defaults_snapshot(
        self, catalog_id: str
    ) -> Optional[Dict[str, Any]]:
        """Return the catalog's frozen defaults-snapshot blob (#1079 c), or ``None``.

        Raw ``{class_key: {"schema_id": ..., "data": ...}}`` blob written by
        :meth:`snapshot_catalog_defaults` at catalog creation. Shared accessor
        both this method's own ``get_config`` and the composed-config view
        (``extensions/configs``) call so they resolve the snapshot base
        identically (#2830).
        """
        from dynastore.modules.catalog.config_snapshot import SNAPSHOT_REF_KEY
        return await self.get_catalog_config_internal_cached(catalog_id, SNAPSHOT_REF_KEY)

    async def get_persisted_config(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
    ) -> Optional[dict]:
        """Return the raw row at the exact tier implied by the ids.

        No waterfall, no class-default filling. ``None`` when the row is
        absent. Used by PATCH handlers to merge partial updates over the
        tier-local delta rather than the resolved view.
        """
        _, class_key = _resolve(config_cls)
        if catalog_id and collection_id:
            return await self.get_collection_config_internal_cached(
                catalog_id, collection_id, class_key
            )
        if catalog_id:
            return await self.get_catalog_config_internal_cached(catalog_id, class_key)
        platform_svc = self._get_platform_config_service()
        getter = getattr(platform_svc, "get_platform_config_internal_cached", None)
        if getter is None:
            return None
        return await getter(class_key)

    async def list_collection_config_deltas(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        catalog_id: str,
        ctx: Optional[DriverContext] = None,
    ) -> List[Dict[str, Any]]:
        """Every collection-level delta row stored for ``config_cls`` in
        ``catalog_id``, in one query — see ConfigsProtocol (#3160).

        Tier-local: raw partial dicts exactly as persisted, no waterfall,
        no class-default filling. Collections without a stored row inherit
        the catalog-scope resolution and do not appear here.
        """
        _, class_key = _resolve(config_cls)
        validate_sql_identifier(catalog_id)
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
            )
            if not phys_schema:
                return []
            if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                return []
            rows = await _cq.select_collection_configs_for_ref(phys_schema).execute(
                conn, ref_key=class_key
            )
        return [row for row in rows if row]

    async def get_config_versioned(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
    ) -> "Tuple[PluginConfig, Optional[str]]":
        """Versioned read for CAS writes (#2707) — see ConfigsProtocol.

        Platform scope delegates to ``PlatformConfigService.get_config_versioned``.
        Catalog/collection scope resolves ``config`` through the normal
        ``get_config`` waterfall, then pairs it with the CAS token read
        directly from that tier's own row (bypassing
        ``_catalog_config_cache`` / ``_collection_config_cache`` — the
        token must reflect the true current row).
        """
        cls, class_key = _resolve(config_cls)
        db_resource = ctx.db_resource if ctx else None

        if catalog_id is None and collection_id is None:
            platform_svc = self._get_platform_config_service()
            getter = getattr(platform_svc, "get_config_versioned", None)
            if getter is None:
                return await self.get_config(cls, ctx=ctx), None
            return await getter(
                cls, ctx=DriverContext(db_resource=db_resource) if db_resource else None
            )

        resolved = await self.get_config(cls, catalog_id, collection_id, ctx)

        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            validate_sql_identifier(catalog_id)
            validate_sql_identifier(collection_id)
            try:
                resolved_ids = await self._get_catalog_manager().collections.resolve_collection_ids(
                    catalog_id, collection_id, allow_missing=True
                )
                internal_collection_id = resolved_ids.id
            except Exception:
                internal_collection_id = collection_id
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema or not await check_table_exists(
                    conn, COLLECTION_CONFIGS_TABLE, phys_schema
                ):
                    return resolved, None
                row = await _cq.select_collection_config_versioned(phys_schema).execute(
                    conn, collection_id=internal_collection_id, ref_key=class_key
                )
            return resolved, (encode_config_version(row["updated_at"]) if row else None)

        # collection_id is None here, and the (catalog_id is None and
        # collection_id is None) case already returned above — so catalog_id
        # must be set.
        assert catalog_id is not None
        validate_sql_identifier(catalog_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
            )
            if not phys_schema or not await check_table_exists(
                conn, CATALOG_CONFIGS_TABLE, phys_schema
            ):
                return resolved, None
            row = await _cq.select_catalog_config_versioned(phys_schema).execute(
                conn, ref_key=class_key
            )
        return resolved, (encode_config_version(row["updated_at"]) if row else None)

    async def get_collection_config_internal_cached(
        self, catalog_id: str, collection_id: str, class_key: str
    ) -> Optional[dict]:
        assert self.engine is not None, "ConfigService.engine not initialised"

        # Issue #2430: Resolve to internal ID for consistent cache lookup.
        # After migration, all configs are keyed on internal ID. During migration
        # window, this may return None for old external-ID-keyed rows; the caller
        # should handle that gracefully.
        catalogs = self._get_catalog_manager()
        try:
            resolved = await catalogs.collections.resolve_collection_ids(
                catalog_id, collection_id, allow_missing=True
            )
            internal_collection_id = resolved.id
        except Exception:
            # Fall back to original ID if resolution fails
            internal_collection_id = collection_id

        # #2895: also normalize catalog_id (see _internal_catalog_id).
        catalog_id = await self._internal_catalog_id(catalog_id)

        return await _collection_config_cache(
            self.engine, catalogs, catalog_id, internal_collection_id, class_key
        )

    async def set_config(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        config: PluginConfig,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        check_immutability: bool = True,
        ctx: Optional[DriverContext] = None,
        expected_version: Optional[str] = None,
    ) -> "PluginConfig":
        """Persist a config and return it.

        The returned object is the post-apply config — validate handlers
        and the ``_self_register_*`` augmentation mutate ``config`` in
        place before the upsert, so callers (and the configs API route)
        get the *effective* stored shape rather than ``None``.  #738/#747
        — returning the config is what replaces the silent ``200 + null``.

        ``expected_version`` (#2707): see ConfigsProtocol — ``None``
        writes unconditionally; a token from ``get_config_versioned``
        makes this an atomic compare-and-set at the tier implied by
        ``(catalog_id, collection_id)``.
        """
        cls, class_key = _resolve(config_cls)
        db_resource = ctx.db_resource if ctx else None
        self._clear_validated_config_cache()
        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            await self._set_collection_config(
                catalog_id, collection_id, cls, config,
                check_immutability=check_immutability, db_resource=db_resource,
                expected_version=expected_version,
            )
        elif catalog_id is not None:
            await self._set_catalog_config(
                catalog_id, cls, config,
                check_immutability=check_immutability, db_resource=db_resource,
                expected_version=expected_version,
            )
        else:
            await self._get_platform_config_service().set_config(
                cls, config,
                check_immutability=check_immutability,
                ctx=DriverContext(db_resource=db_resource) if db_resource else None,
                expected_version=expected_version,
            )
        return config

    async def snapshot_catalog_defaults(
        self, catalog_id: str, ctx: Optional[DriverContext] = None
    ) -> None:
        """#1079 (c): freeze the catalog's inherited defaults at creation.

        Resolves the platform/code default for every stable behaviour/shape
        value-config and stores them as a single schema-id-tagged blob (the
        reserved ``__catalog_defaults_snapshot__`` row). ``get_config`` then
        uses that blob as the inheritance base for this catalog and its
        collections, so a later platform/code default change can no longer
        silently re-resolve into already-materialised collections.

        Best-effort per class: a config that cannot be resolved is skipped (it
        simply keeps live inheritance) rather than aborting catalog creation.
        Driver/routing configs are excluded by ``snapshottable_config_classes``.
        """
        from dynastore.modules.catalog.config_snapshot import (
            SNAPSHOT_REF_KEY,
            serialize_for_snapshot,
            snapshottable_config_classes,
        )

        db_resource = ctx.db_resource if ctx else None
        snapshot: Dict[str, Any] = {}
        for cls in snapshottable_config_classes():
            try:
                resolved = await self.get_config(cls, ctx=ctx)
                snapshot[cls.class_key()] = serialize_for_snapshot(resolved)
            except Exception:  # noqa: BLE001 — one bad class must not block creation
                logger.warning(
                    "snapshot_catalog_defaults: skipping %s for catalog %s",
                    cls.class_key(),
                    catalog_id,
                    exc_info=True,
                )
        if not snapshot:
            return

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                return
            # ``catalog_configs.schema_id`` is NOT NULL; the reserved snapshot
            # blob has no PluginConfig class, so it carries the sentinel ref as
            # its schema_id (schemas are no longer persisted in a registry).
            await _cq.upsert_catalog_config(phys_schema).execute(
                conn,
                ref_key=SNAPSHOT_REF_KEY,
                class_key=SNAPSHOT_REF_KEY,
                schema_id=SNAPSHOT_REF_KEY,
                config_data=json.dumps(snapshot, cls=CustomJSONEncoder),
            )

        _catalog_config_cache.cache_invalidate(
            self.engine,
            self._get_catalog_manager(),
            await self._internal_catalog_id(catalog_id),
            SNAPSHOT_REF_KEY,
        )
        self._clear_validated_config_cache()

    async def _enforce_first_write_against_inherited(
        self,
        cls: Type[PluginConfig],
        config: PluginConfig,
        catalog_id: str,
        collection_id: Optional[str],
        conn: DbResource,
    ) -> None:
        """#1198: enforce immutability on a first write at a tier.

        When a tier (catalog or collection) has no stored row yet for ``cls``,
        the resource's effective value is *inherited* from a parent tier and may
        already be materialized (e.g. a catalog default baked into a collection's
        physical columns on its first item write). The plain materialization gate
        is skipped in that case (``current_config is None``), which let a
        first-time override introduce a divergent Immutable shape — re-opening
        #1079 via the collection tier.

        Resolve the inherited baseline via the waterfall (``get_config`` with the
        current tier's row still absent → parent-tier-effective) and enforce
        immutability against it, restricted to the fields the caller explicitly
        sent so a partial override does not raise false positives on Immutable
        fields a parent tier customized but the caller never touched.
        """
        set_fields = set(getattr(config, "__pydantic_fields_set__", set()) or set())
        if not set_fields:
            return
        inherited = await self.get_config(
            cls,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=DriverContext(db_resource=conn),
        )
        if inherited is None:
            return
        await enforce_config_immutability(
            inherited, config,
            catalog_id=catalog_id, collection_id=collection_id, conn=conn,
            restrict_to_fields=set_fields,
        )

    async def _resolve_or_create_phys_schema(
        self, catalog_id: str, conn: Any, *, reensure_on_missing: bool
    ) -> str:
        """Ensure the catalog exists and return its physical schema.

        Resolves the catalog's physical schema (creating the catalog on the
        first pass). If still unresolved, retries once — the set-config path
        re-runs ``ensure_catalog_exists`` first (``reensure_on_missing=True``);
        the ref-keyed path does not. Raises ValueError if it cannot be resolved.
        """
        await self._get_catalog_manager().ensure_catalog_exists(
            catalog_id, ctx=DriverContext(db_resource=conn)
        )
        phys_schema = await self._get_catalog_manager().resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
        )
        if not phys_schema:
            if reensure_on_missing:
                await self._get_catalog_manager().ensure_catalog_exists(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
        if not phys_schema:
            raise ValueError(
                f"Could not resolve physical schema for catalog '{catalog_id}'."
            )
        return phys_schema

    async def _enforce_write_immutability(
        self,
        cls: Type[PluginConfig],
        config: PluginConfig,
        current_data: Optional[Dict[str, Any]],
        catalog_id: str,
        collection_id: Optional[str],
        conn: DbResource,
    ) -> None:
        """Enforce the config-immutability freeze for a ``set_config`` write.

        ``current_data`` is the stored row from the scope-specific
        select-for-update, or ``None`` on first write at this tier. Strips
        caller values for machine-assigned (Computed) fields before
        enforce/persist (#1135 — works on first-write too), then either:

        * enforces immutability against the stored value, or
        * on first write at this tier, enforces against the inherited
          (parent-tier) baseline so a divergent override cannot slip past the
          freeze on an already-materialized resource (#1198).

        Shared by the catalog and collection ``_set_*_config`` paths; the
        scope-specific select stays at the call site (explicit per tier).
        """
        current_config = _validate_stored_config(cls, current_data) if current_data else None
        restore_system_assigned_fields(cls, config, current_config)
        if current_config is not None:
            await enforce_config_immutability(
                current_config, config,
                catalog_id=catalog_id, collection_id=collection_id, conn=conn,
            )
        else:
            await self._enforce_first_write_against_inherited(
                cls, config, catalog_id, collection_id, conn,
            )

    async def _set_catalog_config(
        self,
        catalog_id: str,
        cls: Type[PluginConfig],
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
        expected_version: Optional[str] = None,
    ) -> None:
        validate_sql_identifier(catalog_id)
        class_key = cls.class_key()

        # #2435: a config with no explicitly-set fields serialises to {} under
        # exclude_unset=True.  {} is falsy at read time — the waterfall skips it —
        # so the row provides no value.  Return early to avoid an unnecessary write
        # transaction and spurious config-cache / router-cache invalidation.
        _serialized = _serialize_config_for_db(config)
        if _serialized == "{}":
            logger.debug(
                "%s: skipping empty config write at catalog scope (%s); "
                "waterfall defaults apply without a stored row",
                class_key, catalog_id,
            )
            return

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_or_create_phys_schema(
                catalog_id, conn, reensure_on_missing=True
            )

            if check_immutability:
                current_data = await _cq.select_catalog_config_for_update(phys_schema).execute(
                    conn, ref_key=class_key
                )
                await self._enforce_write_immutability(
                    cls, config, current_data, catalog_id, None, conn
                )

            # Phase 2 — validate (pre-persist).
            await run_validate_handlers(cls, config, catalog_id, None, conn)

            if expected_version is not None:
                rowcount = await _cq.cas_update_catalog_config(phys_schema).execute(
                    conn,
                    ref_key=class_key,
                    class_key=class_key,
                    schema_id=type(config).schema_id(),
                    config_data=_serialized,
                    expected_version=decode_config_version(expected_version),
                )
                if rowcount == 0:
                    raise ConfigVersionConflictError(
                        f"set_config({class_key!r}) at catalog {catalog_id!r}: "
                        f"expected_version {expected_version!r} no longer matches "
                        f"the stored row (or the row is absent) — a concurrent "
                        f"writer changed it. Re-read via get_config_versioned "
                        f"and retry."
                    )
            else:
                await _cq.upsert_catalog_config(phys_schema).execute(
                    conn,
                    ref_key=class_key,
                    class_key=class_key,
                    schema_id=type(config).schema_id(),
                    config_data=_serialized,
                )

            # Phase 3 — apply (post-persist, best-effort).
            await run_apply_handlers(cls, config, catalog_id, None, conn)

        _catalog_config_cache.cache_invalidate(
            self.engine,
            self._get_catalog_manager(),
            await self._internal_catalog_id(catalog_id),
            class_key,
        )
        _maybe_bust_router(cls, catalog_id, None)

    async def _set_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        cls: Type[PluginConfig],
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
        expected_version: Optional[str] = None,
    ) -> None:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        class_key = cls.class_key()

        # #2435: a config with no explicitly-set fields serialises to {} under
        # exclude_unset=True.  {} is falsy at read time — the waterfall skips it —
        # so the row provides no value.  Return early to avoid an unnecessary write
        # transaction and spurious config-cache / router-cache invalidation.
        _serialized = _serialize_config_for_db(config)
        if _serialized == "{}":
            logger.debug(
                "%s: skipping empty config write at collection scope (%s/%s); "
                "waterfall defaults apply without a stored row",
                class_key, catalog_id, collection_id,
            )
            return

        # Issue #2430: Resolve collection_id to immutable internal ID for persistence.
        # This ensures configs are keyed on the stable internal ID, not the mutable
        # external_id. Both forms (external or internal) are accepted at the API
        # boundary, but persistence always uses internal ID.
        catalogs = self._get_catalog_manager()
        resolved = await catalogs.collections.resolve_collection_ids(
            catalog_id, collection_id, allow_missing=True
        )
        # Use internal ID for all persistence operations
        internal_collection_id = resolved.id
        # Keep original for cache invalidation (handles both forms)
        original_collection_id = collection_id

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_or_create_phys_schema(
                catalog_id, conn, reensure_on_missing=True
            )

            # JIT-create the thin collection registry row if it doesn't yet
            # exist. Upfront config workflow: `PUT .../collections/{col}/configs/{key}`
            # always succeeds — every sub-config resolves from the waterfall,
            # so a minimal Collection can be materialised without the caller
            # first hitting `POST /collections`.
            await catalogs.ensure_collection_exists(
                catalog_id, internal_collection_id, ctx=DriverContext(db_resource=conn)
            )

            if check_immutability:
                current_data = await _cq.select_collection_config_for_update(phys_schema).execute(
                    conn,
                    collection_id=internal_collection_id,
                    ref_key=class_key,
                )
                await self._enforce_write_immutability(
                    cls, config, current_data, catalog_id, internal_collection_id, conn
                )

            # Phase 2 — validate (pre-persist).
            await run_validate_handlers(cls, config, catalog_id, internal_collection_id, conn)

            if expected_version is not None:
                rowcount = await _cq.cas_update_collection_config(phys_schema).execute(
                    conn,
                    collection_id=internal_collection_id,
                    ref_key=class_key,
                    class_key=class_key,
                    schema_id=type(config).schema_id(),
                    config_data=_serialized,
                    expected_version=decode_config_version(expected_version),
                )
                if rowcount == 0:
                    raise ConfigVersionConflictError(
                        f"set_config({class_key!r}) at {catalog_id!r}/"
                        f"{internal_collection_id!r}: expected_version "
                        f"{expected_version!r} no longer matches the stored row "
                        f"(or the row is absent) — a concurrent writer changed "
                        f"it. Re-read via get_config_versioned and retry."
                    )
            else:
                await _cq.upsert_collection_config(phys_schema).execute(
                    conn,
                    collection_id=internal_collection_id,
                    ref_key=class_key,
                    class_key=class_key,
                    schema_id=type(config).schema_id(),
                    config_data=_serialized,
                )

            # Phase 3 — apply (post-persist, best-effort).
            await run_apply_handlers(cls, config, catalog_id, internal_collection_id, conn)

        _collection_config_cache.cache_invalidate(
            self.engine, catalogs, await self._internal_catalog_id(catalog_id),
            internal_collection_id, class_key
        )
        _maybe_bust_router(cls, catalog_id, original_collection_id)

    async def list_configs(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        ctx: Optional[DriverContext] = None,
    ) -> Dict[str, Any]:
        db_resource = ctx.db_resource if ctx else None
        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")

            validate_sql_identifier(catalog_id)
            validate_sql_identifier(collection_id)

            # Issue #2430: Resolve to internal ID for lookup
            try:
                resolved = await self._get_catalog_manager().collections.resolve_collection_ids(
                    catalog_id, collection_id, allow_missing=True
                )
                internal_collection_id = resolved.id
            except Exception:
                internal_collection_id = collection_id

            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                    return {"total": 0, "results": []}

                rows, total = await _cq.list_collection_configs_paginated(
                    conn, phys_schema, internal_collection_id, limit, offset
                )

            results = []
            for r in rows:
                ck: str = r["class_key"]
                cls = resolve_config_class(ck)
                if cls is None:
                    logger.warning("Skipping collection_configs row for unknown class_key %r", ck)
                    continue
                results.append({
                    "plugin_id": ck,
                    "config": _validate_stored_config(cls, r["config_data"]).model_dump(),
                })
            return {"total": total, "results": results}

        elif catalog_id is not None:
            validate_sql_identifier(catalog_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                    return {"total": 0, "results": []}

                rows, total = await _cq.list_catalog_configs_paginated(
                    conn, phys_schema, limit, offset
                )

            results = []
            for r in rows:
                ck: str = r["class_key"]
                if ck.startswith("__"):
                    continue  # reserved internal row (e.g. #1079 defaults snapshot)
                cls = resolve_config_class(ck)
                if cls is None:
                    logger.warning("Skipping catalog_configs row for unknown class_key %r", ck)
                    continue
                results.append({
                    "plugin_id": ck,
                    "config": _validate_stored_config(cls, r["config_data"]).model_dump(),
                })
            return {"total": total, "results": results}
        else:
            configs = await self._get_platform_config_service().list_configs()
            all_results = [
                {"plugin_id": klass.class_key(), "config": cfg.model_dump()}
                for klass, cfg in configs.items()
            ]
            total = len(all_results)
            return {"total": total, "results": all_results[offset : offset + limit]}

    async def list_catalog_configs(
        self, catalog_id: str, ctx: Optional[DriverContext] = None
    ) -> Dict[str, PluginConfig]:
        validate_sql_identifier(catalog_id)
        db_resource = ctx.db_resource if ctx else None

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                return {}

            if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                return {}

            rows = await _cq.list_catalog_configs(phys_schema).execute(conn)

        configs: Dict[str, PluginConfig] = {}
        for row in rows:
            class_key: str = row["class_key"]
            if class_key.startswith("__"):
                continue  # reserved internal row (e.g. #1079 defaults snapshot)
            cls = resolve_config_class(class_key)
            if cls is None:
                logger.warning("Skipping catalog_configs row for unknown class_key %r", class_key)
                continue
            configs[class_key] = _validate_stored_config(cls, row["config_data"])
        return configs

    # -----------------------------------------------------------------
    # F.4c.2 — ref-keyed read API
    # -----------------------------------------------------------------

    async def list_refs_at_scope(
        self,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
    ) -> Dict[str, str]:
        """Tier-local enumeration of ``{ref_key: class_key}`` at the implied scope."""
        db_resource = ctx.db_resource if ctx else None

        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            validate_sql_identifier(catalog_id)
            validate_sql_identifier(collection_id)

            # Issue #2430: Resolve to internal ID for lookup
            try:
                resolved = await self._get_catalog_manager().collections.resolve_collection_ids(
                    catalog_id, collection_id, allow_missing=True
                )
                internal_collection_id = resolved.id
            except Exception:
                internal_collection_id = collection_id

            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return {}
                if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                    return {}
                rows = await _cq.list_collection_refs(phys_schema).execute(
                    conn, collection_id=internal_collection_id
                )
            return {r["ref_key"]: r["class_key"] for r in rows}

        if catalog_id is not None:
            validate_sql_identifier(catalog_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return {}
                if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                    return {}
                rows = await _cq.list_catalog_refs(phys_schema).execute(conn)
            return {r["ref_key"]: r["class_key"] for r in rows}

        # Platform scope — delegate to PlatformConfigService.list_refs.
        platform_svc = self._get_platform_config_service()
        getter = getattr(platform_svc, "list_refs", None)
        if getter is None:
            return {}
        return await getter()

    async def get_config_by_ref(
        self,
        ref_key: str,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
    ) -> Optional[PluginConfig]:
        """Tier-local read by ``ref_key`` at the implied scope.

        Resolves the dispatch class from the row's ``class_key`` discriminator
        and validates the JSON payload through that class.  Returns ``None``
        when the row is absent or its ``class_key`` is no longer registered
        (warning logged).  Does NOT walk the waterfall — refs are explicit.
        """
        db_resource = ctx.db_resource if ctx else None

        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            validate_sql_identifier(catalog_id)
            validate_sql_identifier(collection_id)

            # Issue #2430: Resolve to internal ID for lookup
            try:
                resolved = await self._get_catalog_manager().collections.resolve_collection_ids(
                    catalog_id, collection_id, allow_missing=True
                )
                internal_collection_id = resolved.id
            except Exception:
                internal_collection_id = collection_id

            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return None
                if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                    return None
                row = await _cq.select_collection_config_by_ref(phys_schema).execute(
                    conn, collection_id=internal_collection_id, ref_key=ref_key
                )
            return _materialise_ref_row(row, ref_key)

        if catalog_id is not None:
            validate_sql_identifier(catalog_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return None
                if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                    return None
                row = await _cq.select_catalog_config_by_ref(phys_schema).execute(
                    conn, ref_key=ref_key
                )
            return _materialise_ref_row(row, ref_key)

        # Platform scope — delegate to PlatformConfigService.get_config_by_ref.
        platform_svc = self._get_platform_config_service()
        getter = getattr(platform_svc, "get_config_by_ref", None)
        if getter is None:
            return None
        return await getter(
            ref_key,
            ctx=DriverContext(db_resource=db_resource) if db_resource else None,
        )

    # -----------------------------------------------------------------
    # F.4c.4 — ref-keyed write API
    # -----------------------------------------------------------------

    async def set_config_by_ref(
        self,
        ref_key: str,
        config: PluginConfig,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        check_immutability: bool = True,
        ctx: Optional[DriverContext] = None,
    ) -> "PluginConfig":
        """Tier-local write at ``(ref_key, scope)`` — see ConfigsProtocol.

        Returns the post-apply config (mutated in place by the validate
        phase) — #738/#747, replaces the silent ``200 + null``.
        """
        if collection_id is not None and catalog_id is None:
            raise ValueError("catalog_id is required when collection_id is provided")

        db_resource = ctx.db_resource if ctx else None
        cls = type(config)
        self._clear_validated_config_cache()

        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            await self._set_collection_config_by_ref(
                ref_key, catalog_id, collection_id, cls, config,
                check_immutability=check_immutability, db_resource=db_resource,
            )
            return config

        if catalog_id is not None:
            await self._set_catalog_config_by_ref(
                ref_key, catalog_id, cls, config,
                check_immutability=check_immutability, db_resource=db_resource,
            )
            return config

        # Platform scope — delegate.
        platform_svc = self._get_platform_config_service()
        setter = getattr(platform_svc, "set_config_by_ref", None)
        if setter is None:
            raise RuntimeError(
                "set_config_by_ref: platform service does not implement the "
                "ref-keyed write API"
            )
        await setter(
            ref_key, config,
            check_immutability=check_immutability,
            ctx=DriverContext(db_resource=db_resource) if db_resource else None,
        )
        # Class-keyed cache lives on the platform service; nothing to bust here.
        _maybe_bust_router(cls, None, None)
        return config

    async def _set_catalog_config_by_ref(
        self,
        ref_key: str,
        catalog_id: str,
        cls: Type[PluginConfig],
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        validate_sql_identifier(catalog_id)
        class_key = cls.class_key()

        # #2435: skip write when no fields were explicitly set (see _set_catalog_config).
        _serialized = _serialize_config_for_db(config)
        if _serialized == "{}":
            logger.debug(
                "%s: skipping empty config write (by-ref=%r) at catalog scope (%s); "
                "waterfall defaults apply without a stored row",
                class_key, ref_key, catalog_id,
            )
            return

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_or_create_phys_schema(
                catalog_id, conn, reensure_on_missing=False
            )

            existing = await _cq.select_catalog_config_by_ref(phys_schema).execute(
                conn, ref_key=ref_key
            )
            if existing:
                stored_class_key = existing["class_key"]
                if stored_class_key != class_key:
                    raise ValueError(
                        f"set_config_by_ref({ref_key!r}) at catalog "
                        f"{catalog_id!r}: row stored as class_key="
                        f"{stored_class_key!r}, refusing to overwrite with "
                        f"class_key={class_key!r}.  Delete the ref first or "
                        f"pick a different name."
                    )
                if check_immutability:
                    await enforce_config_immutability(
                        _validate_stored_config(cls, existing["config_data"]), config,
                        catalog_id=catalog_id, collection_id=None, conn=conn,
                    )

            # Phase 2 — validate (pre-persist).
            await run_validate_handlers(cls, config, catalog_id, None, conn)

            await _cq.upsert_catalog_config(phys_schema).execute(
                conn,
                ref_key=ref_key,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=_serialized,
            )

            # Phase 3 — apply (post-persist, best-effort).
            await run_apply_handlers(cls, config, catalog_id, None, conn)

        _catalog_config_cache.cache_invalidate(
            self.engine,
            self._get_catalog_manager(),
            await self._internal_catalog_id(catalog_id),
            class_key,
        )
        _maybe_bust_router(cls, catalog_id, None)

    async def _set_collection_config_by_ref(
        self,
        ref_key: str,
        catalog_id: str,
        collection_id: str,
        cls: Type[PluginConfig],
        config: PluginConfig,
        check_immutability: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        class_key = cls.class_key()

        # #2435: skip write when no fields were explicitly set (see _set_collection_config).
        _serialized = _serialize_config_for_db(config)
        if _serialized == "{}":
            logger.debug(
                "%s: skipping empty config write (by-ref=%r) at collection scope (%s/%s); "
                "waterfall defaults apply without a stored row",
                class_key, ref_key, catalog_id, collection_id,
            )
            return

        # Issue #2430: Resolve collection_id to immutable internal ID for persistence.
        catalogs = self._get_catalog_manager()
        resolved = await catalogs.collections.resolve_collection_ids(
            catalog_id, collection_id, allow_missing=True
        )
        internal_collection_id = resolved.id
        original_collection_id = collection_id

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_or_create_phys_schema(
                catalog_id, conn, reensure_on_missing=False
            )
            await catalogs.ensure_collection_exists(
                catalog_id, internal_collection_id, ctx=DriverContext(db_resource=conn)
            )

            existing = await _cq.select_collection_config_by_ref(phys_schema).execute(
                conn, collection_id=internal_collection_id, ref_key=ref_key
            )
            if existing:
                stored_class_key = existing["class_key"]
                if stored_class_key != class_key:
                    raise ValueError(
                        f"set_config_by_ref({ref_key!r}) at "
                        f"{catalog_id}/{internal_collection_id}: row stored as "
                        f"class_key={stored_class_key!r}, refusing to "
                        f"overwrite with class_key={class_key!r}."
                    )
                if check_immutability:
                    await enforce_config_immutability(
                        _validate_stored_config(cls, existing["config_data"]), config,
                        catalog_id=catalog_id, collection_id=internal_collection_id, conn=conn,
                    )

            # Phase 2 — validate (pre-persist).
            await run_validate_handlers(cls, config, catalog_id, internal_collection_id, conn)

            await _cq.upsert_collection_config(phys_schema).execute(
                conn,
                collection_id=internal_collection_id,
                ref_key=ref_key,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=_serialized,
            )

            # Phase 3 — apply (post-persist, best-effort).
            await run_apply_handlers(cls, config, catalog_id, internal_collection_id, conn)

        _collection_config_cache.cache_invalidate(
            self.engine,
            catalogs,
            await self._internal_catalog_id(catalog_id),
            internal_collection_id,
            class_key,
        )
        _maybe_bust_router(cls, catalog_id, original_collection_id)

    async def delete_config_by_ref(
        self,
        ref_key: str,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
    ) -> bool:
        """Tier-local delete at ``(ref_key, scope)`` — see ConfigsProtocol."""
        db_resource = ctx.db_resource if ctx else None
        self._clear_validated_config_cache()

        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            validate_sql_identifier(catalog_id)
            validate_sql_identifier(collection_id)

            # Issue #2430: Resolve to internal ID for lookup
            try:
                resolved = await self._get_catalog_manager().collections.resolve_collection_ids(
                    catalog_id, collection_id, allow_missing=True
                )
                internal_collection_id = resolved.id
            except Exception:
                internal_collection_id = collection_id

            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return False
                if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                    return False
                existing = await _cq.select_collection_config_by_ref(phys_schema).execute(
                    conn, collection_id=internal_collection_id, ref_key=ref_key
                )
                if not existing:
                    return False
                stored_class_key = existing["class_key"]
                await _cq.delete_collection_config(phys_schema).execute(
                    conn, collection_id=internal_collection_id, ref_key=ref_key,
                )
            _collection_config_cache.cache_invalidate(
                self.engine, self._get_catalog_manager(),
                await self._internal_catalog_id(catalog_id),
                internal_collection_id, stored_class_key,
            )
            cls = resolve_config_class(stored_class_key)
            if cls is not None:
                _maybe_bust_router(cls, catalog_id, collection_id)
            return True

        if catalog_id is not None:
            validate_sql_identifier(catalog_id)
            async with managed_transaction(db_resource or self.engine) as conn:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )
                if not phys_schema:
                    return False
                if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                    return False
                existing = await _cq.select_catalog_config_by_ref(phys_schema).execute(
                    conn, ref_key=ref_key
                )
                if not existing:
                    return False
                stored_class_key = existing["class_key"]
                await _cq.delete_catalog_config(phys_schema).execute(
                    conn, ref_key=ref_key,
                )
            _catalog_config_cache.cache_invalidate(
                self.engine, self._get_catalog_manager(),
                await self._internal_catalog_id(catalog_id), stored_class_key,
            )
            cls = resolve_config_class(stored_class_key)
            if cls is not None:
                _maybe_bust_router(cls, catalog_id, None)
            return True

        platform_svc = self._get_platform_config_service()
        deleter = getattr(platform_svc, "delete_config_by_ref", None)
        if deleter is None:
            return False
        return await deleter(
            ref_key,
            ctx=DriverContext(db_resource=db_resource) if db_resource else None,
        )

    async def search(
        self,
        query: Optional[str] = None,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        ctx: Optional["DriverContext"] = None,
    ) -> Dict[str, Any]:
        db_resource = ctx.db_resource if ctx else None
        if catalog_id:
            validate_sql_identifier(catalog_id)
        if collection_id:
            validate_sql_identifier(collection_id)

        # Issue #2430: Resolve collection_id to internal ID
        internal_collection_id = collection_id
        if collection_id and catalog_id:
            try:
                resolved = await self._get_catalog_manager().collections.resolve_collection_ids(
                    catalog_id, collection_id, allow_missing=True
                )
                internal_collection_id = resolved.id
            except Exception:
                pass

        async with managed_transaction(db_resource or self.engine) as conn:
            if catalog_id:
                phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn)
                )
                if not phys_schema:
                    return {"total": 0, "results": []}

                catalog_table_exists = await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema)
                collection_table_exists = await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema)
                if not catalog_table_exists and not collection_table_exists:
                    return {"total": 0, "results": []}

                if collection_id:
                    if not collection_table_exists:
                        return {"total": 0, "results": []}
                    sql = f"""
                    SELECT COUNT(*) OVER() as total_count, 'collection' as level, collection_id, class_key, config_data
                    FROM \"{phys_schema}\".{COLLECTION_CONFIGS_TABLE}
                    WHERE collection_id = :collection_id
                    """
                else:
                    parts = []
                    if catalog_table_exists:
                        parts.append(f"""
                        SELECT 'catalog' as level, NULL as collection_id, class_key, config_data
                        FROM \"{phys_schema}\".{CATALOG_CONFIGS_TABLE}""")
                    if collection_table_exists:
                        parts.append(f"""
                        SELECT 'collection' as level, collection_id, class_key, config_data
                        FROM \"{phys_schema}\".{COLLECTION_CONFIGS_TABLE}""")
                    sql = f"""
                    SELECT COUNT(*) OVER() as total_count, level, collection_id, class_key, config_data FROM (
                        {' UNION ALL '.join(parts)}
                    ) sub
                    """

                if query:
                    sql += " WHERE class_key ILIKE :query"

                sql += " ORDER BY level, collection_id, class_key LIMIT :limit OFFSET :offset;"

                rows, total = await list_page_with_count(
                    conn,
                    sql,
                    {
                        "collection_id": internal_collection_id,
                        "query": f"%{query}%" if query else None,
                    },
                    limit=limit,
                    offset=offset,
                )

                results = []
                for r in rows:
                    class_key: str = r["class_key"]
                    cls = resolve_config_class(class_key)
                    if cls is None:
                        continue
                    results.append(
                        {
                            "level": r["level"],
                            "catalog_id": catalog_id,
                            "collection_id": r.get("collection_id"),
                            "plugin_id": class_key,
                            "config": _validate_stored_config(cls, r["config_data"]).model_dump(),
                        }
                    )
                return {"total": total, "results": results}
            else:
                configs = await self._get_platform_config_service().list_configs()
                all_results = []
                for klass, cfg in configs.items():
                    ck = klass.class_key()
                    if not query or query.lower() in ck.lower():
                        all_results.append(
                            {
                                "level": "platform",
                                "plugin_id": ck,
                                "config": cfg.model_dump(),
                            }
                        )
                total = len(all_results)
                return {"total": total, "results": all_results[offset : offset + limit]}

    async def delete_config(
        self,
        config_cls: "Union[str, Type[PluginConfig]]",
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
    ) -> None:
        cls, class_key = _resolve(config_cls)
        db_resource = ctx.db_resource if ctx else None
        self._clear_validated_config_cache()
        if collection_id is not None:
            if catalog_id is None:
                raise ValueError("catalog_id is required when collection_id is provided")
            await self._delete_collection_config(
                catalog_id, collection_id, cls, db_resource=db_resource
            )
        elif catalog_id is not None:
            await self._delete_catalog_config(
                catalog_id, cls, db_resource=db_resource
            )
        else:
            await self._get_platform_config_service().delete_config(
                cls, ctx=DriverContext(db_resource=db_resource) if db_resource else None
            )

    async def _delete_catalog_config(
        self, catalog_id: str, cls: Type[PluginConfig], db_resource: Optional[DbResource] = None
    ) -> bool:
        class_key = cls.class_key()
        validate_sql_identifier(catalog_id)
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                return False

            if not await check_table_exists(conn, CATALOG_CONFIGS_TABLE, phys_schema):
                return False

            rows_affected = await _cq.delete_catalog_config(phys_schema).execute(
                conn, ref_key=class_key
            )

            if rows_affected > 0:
                _catalog_config_cache.cache_invalidate(
                    self.engine, self.catalog_manager,
                    await self._internal_catalog_id(catalog_id), class_key,
                )
                _maybe_bust_router(cls, catalog_id, None)
                return True
        return False

    async def _delete_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        cls: Type[PluginConfig],
        db_resource: Optional[DbResource] = None,
    ) -> bool:
        class_key = cls.class_key()
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        # Issue #2430: Resolve to internal ID for lookup
        try:
            resolved = await self._get_catalog_manager().collections.resolve_collection_ids(
                catalog_id, collection_id, allow_missing=True
            )
            internal_collection_id = resolved.id
        except Exception:
            internal_collection_id = collection_id

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._get_catalog_manager().resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                return False

            if not await check_table_exists(conn, COLLECTION_CONFIGS_TABLE, phys_schema):
                return False

            rows_affected = await _cq.delete_collection_config(phys_schema).execute(
                conn,
                collection_id=internal_collection_id,
                ref_key=class_key,
            )

            if rows_affected > 0:
                _collection_config_cache.cache_invalidate(
                    self.engine,
                    self.catalog_manager,
                    await self._internal_catalog_id(catalog_id),
                    internal_collection_id,
                    class_key,
                )
                _maybe_bust_router(cls, catalog_id, collection_id)
                return True
        return False
