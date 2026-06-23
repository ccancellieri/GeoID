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
from typing import FrozenSet, List, Optional, Any, Dict, Union, Set
from dynastore.tools.cache import cached
from dynastore.models.driver_context import DriverContext

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.catalog.models import Collection
from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig,
)
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.models.protocols.entity_store import CollectionLifecycle
from dynastore.tools.discovery import get_protocol
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.async_utils import signal_bus
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry, LifecycleContext
from dynastore.modules.catalog.event_service import CatalogEventType, emit_event
from dynastore.modules.db_config import shared_queries

logger = logging.getLogger(__name__)

# Process-local set of (catalog_id, collection_id) pairs whose physical table
# has been confirmed to exist in the DB.  Positive results are cached
# indefinitely — physical_table pins are WriteOnce so a table name never
# changes once set.  Negative results are never stored here, so a diverged
# collection (table dropped out-of-band) always goes through re-provisioning
# via ensure_storage until it succeeds, at which point it is added back.
_confirmed_active: "set[tuple[str, str]]" = set()


def _mark_confirmed_active(catalog_id: str, collection_id: str) -> None:
    """Record that (catalog_id, collection_id) has a confirmed physical table."""
    _confirmed_active.add((catalog_id, collection_id))


def _unmark_confirmed_active(catalog_id: str, collection_id: str) -> None:
    """Remove the confirmation — used when re-provisioning is triggered."""
    _confirmed_active.discard((catalog_id, collection_id))


@cached(maxsize=1024, namespace="collection_model", ignore=["service"])
async def _collection_model_cache(
    service: "CollectionService", catalog_id: str, collection_id: str
) -> Optional[Collection]:
    """Process-shared cache for collection metadata models.

    Keyed on ``(catalog_id, collection_id)`` only — ``service`` is ignored so
    every ``CollectionService`` instance shares one cache entry per collection.
    A module-level cache (single decorator closure → single backend) is what
    makes ``cache_invalidate`` from any instance visible to reads issued
    through any other instance; an instance-bound cache would give each
    service its own backend, so a write+invalidate on one instance would leave
    stale entries readable through another (e.g. the facade-internal service
    vs. the standalone one).
    """
    return await service._get_collection_model_db(catalog_id, collection_id)


def _invalidate_collection_model_cache(catalog_id: str, collection_id: str) -> None:
    """Drop the shared collection-model cache entry for a collection.

    ``service`` is part of the cache signature but ignored for keying, so any
    sentinel is fine here.
    """
    _collection_model_cache.cache_invalidate(None, catalog_id, collection_id)


def _invalidate_collection_lifecycle_caches(catalog_id: str, collection_id: str) -> None:
    """Drop every in-process cache that can answer *liveness*, *routing*, or
    *config* for a collection, called post-commit on each lifecycle transition.

    A lifecycle change (create / provisioning→active / soft-delete /
    hard-delete / reclaim) must not leave a cache reporting a stale answer:

    * ``_collection_model_cache`` — a model hit reads as "the collection
      exists"; a stale entry answers ACTIVE for a deleted id.
    * router resolution cache — maps a collection to its physical table /
      driver; a stale entry routes a write at a dropped table after a hard
      delete.
    * ``_collection_config_cache`` — keyed per ``(catalog, collection,
      class_key)``; a stale entry would serve the old collection's config
      to a reclaimed id (same collection_id, new collection).
    * ``_collection_physical_id_cache`` — maps a logical id to the immutable
      physical id used to key tile rows and per-collection log partitions; a
      stale entry after a rename would resolve the old logical id to a live
      physical id (the catalog/collection 404s first, but the entry must still
      be dropped for both the old and the new id).

    All caches are tiered (in-process L1 + optional shared L2); invalidation
    reaches the shared tier, so sibling pods converge within their L1 TTL cap
    rather than relying on full TTL expiry.  The config cache uses a 2 s L1
    cap (``l1_ttl=2``), so the cross-pod staleness window after a lifecycle
    transition is at most 2 s for config and ≤60 s for the model/router caches.

    Truly synchronous cross-pod invalidation (an explicit distributed pub/sub
    signal to collapse the model/router L1 window) remains a tracked follow-up.
    """
    from dynastore.modules.storage.router import invalidate_router_cache
    from dynastore.modules.catalog.config_service import invalidate_collection_config_cache
    from dynastore.modules.catalog.catalog_service import (
        _invalidate_collection_physical_id_cache,
    )

    _invalidate_collection_model_cache(catalog_id, collection_id)
    invalidate_router_cache(catalog_id, collection_id)
    invalidate_collection_config_cache(catalog_id, collection_id)
    _invalidate_collection_physical_id_cache(catalog_id, collection_id)


def _make_collection_exists_query(phys_schema: str) -> DQLQuery:
    """SELECT id FROM ``phys_schema``.collections by id (non-deleted only).

    Used as a pre-check on read/update paths. Factoring keeps the SELECT
    projection and deleted_at filter in one place.
    """
    return DQLQuery(
        f'SELECT id FROM "{phys_schema}".collections '
        "WHERE id = :id AND deleted_at IS NULL;",
        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    )


def _make_collection_list_ids_query(phys_schema: str) -> DQLQuery:
    """SELECT ids of ACTIVE collections in ``phys_schema``, paginated.

    The thin PG ``collections`` registry is the authoritative existence
    ledger for every collection regardless of where its metadata lives (ES,
    DuckDB, or PG).  Listing enumerates ids from here and hydrates each via
    the configured READ router, so a pure-ES (or DuckDB-only) catalog lists
    its collections even when the ES SEARCH index is empty or lagged. Stable
    ``ORDER BY id`` makes ``limit``/``offset`` pagination deterministic.

    ``lifecycle_status IS NULL`` hides mid-provisioning and mid-hard-delete
    rows (#2194 / #2066): the overlay is set to ``'provisioning'`` or
    ``'deleting'`` while async init or teardown is in flight, and cleared
    back to NULL once ACTIVE.  A direct GET-by-id still resolves it so a
    client polling a known id can watch progress.
    """
    return DQLQuery(
        f'SELECT id FROM "{phys_schema}".collections '
        "WHERE deleted_at IS NULL AND lifecycle_status IS NULL "
        "ORDER BY id LIMIT :limit OFFSET :offset;",
        result_handler=ResultHandler.ALL_SCALARS,
    )


class CollectionRenameConflictError(Exception):
    """Raised when the target ``new_id`` already exists in the collections registry."""

    def __init__(self, new_id: str) -> None:
        super().__init__(
            f"Collection id '{new_id}' already exists (including tombstoned records)."
        )
        self.new_id = new_id


class CollectionNotAliveError(Exception):
    """Raised by :meth:`CollectionService.ensure_alive` when the collection
    cannot accept writes.

    Attributes:
        catalog_id:    The catalog that owns the collection.
        collection_id: The collection that failed the liveness check.
        reason:        ``"missing"``      — no registry row (hard-deleted or
                                            never created).
                       ``"tombstoned"``   — registry row present with
                                            ``deleted_at`` set (soft-deleted).
                       ``"provisioning"`` — async init in flight; retry shortly.
                       ``"deleting"``     — hard-delete purge in flight; retry.
                       ``"lookup-error"`` — gate failed closed (state unknown).
    """

    def __init__(self, catalog_id: str, collection_id: str, reason: str) -> None:
        super().__init__(
            f"Collection '{catalog_id}:{collection_id}' is not alive: {reason}"
        )
        self.catalog_id = catalog_id
        self.collection_id = collection_id
        self.reason = reason


class CollectionService:
    """Service for collection-level operations."""

    priority: int = 10

    def __init__(self, engine: Optional[DbResource] = None):
        self.engine = engine
        # The collection-model read cache is a process-shared module-level
        # cache (``_collection_model_cache``) rather than an instance-bound
        # one, so invalidations land in the same backend every instance reads
        # from. See that function's docstring for why this matters.

    def is_available(self) -> bool:
        return self.engine is not None

    async def _resolve_physical_schema(
        self, catalog_id: str, db_resource: Optional[DbResource] = None
    ) -> Optional[str]:
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            return None
        return await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=db_resource) if db_resource else None
        )

    async def _get_pg_driver(self):
        """Get the query-fallback (PostgreSQL) storage driver instance."""
        from dynastore.tools.discovery import get_protocols
        from dynastore.models.protocols.storage_driver import (
            Capability,
            CollectionItemsStore,
        )

        for driver in get_protocols(CollectionItemsStore):
            if Capability.QUERY_FALLBACK_SOURCE in driver.capabilities:
                return driver
        return None

    async def is_alive(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> bool:
        """Return ``True`` if the collection registry row exists with no
        ``deleted_at``.  Returns ``False`` for MISSING and TOMBSTONED states.
        Does NOT raise — use :meth:`ensure_alive` when a hard failure is
        appropriate.
        """
        try:
            lc = await self._get_lifecycle(catalog_id, collection_id, db_resource)
        except Exception:
            return False
        return lc == CollectionLifecycle.ACTIVE

    async def ensure_alive(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Assert the collection is ACTIVE; raise :exc:`CollectionNotAliveError`
        otherwise.  Callers should use this at write-path boundaries to enforce
        the lifecycle gate.  Fail-closed: any unexpected lookup error also
        raises ``CollectionNotAliveError``.
        """
        try:
            lc = await self._get_lifecycle(catalog_id, collection_id, db_resource)
        except Exception as exc:
            raise CollectionNotAliveError(
                catalog_id, collection_id, "lookup-error"
            ) from exc
        if lc == CollectionLifecycle.ACTIVE:
            return
        reason = lc.value if lc != CollectionLifecycle.MISSING else "missing"
        raise CollectionNotAliveError(catalog_id, collection_id, reason)

    async def _get_lifecycle(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Any:
        """Resolve lifecycle via a registered LIFECYCLE-capable CollectionStore
        driver, or fall back to a direct registry SELECT when no such driver
        is available.  Fail-closed — any error propagates to the caller.
        """
        from dynastore.tools.discovery import get_protocols
        from dynastore.models.protocols.entity_store import (
            CollectionStore,
            EntityStoreCapability,
        )

        for driver in get_protocols(CollectionStore):
            caps = getattr(driver, "capabilities", frozenset())
            if EntityStoreCapability.LIFECYCLE in caps:
                return await driver.get_lifecycle(
                    catalog_id, collection_id, db_resource=db_resource
                )

        # Degrade-safe fallback: no capable driver registered (e.g. storage
        # module absent).  Query the registry table directly via the service's
        # own engine.
        _engine = db_resource or self.engine
        if not _engine:
            return CollectionLifecycle.MISSING
        async with managed_transaction(_engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys_schema:
                return CollectionLifecycle.MISSING
            row = await DQLQuery(
                f'SELECT deleted_at, lifecycle_status FROM "{phys_schema}".collections '
                "WHERE id = :id;",
                result_handler=ResultHandler.ONE_DICT,
            ).execute(conn, id=collection_id)
        if row is None:
            return CollectionLifecycle.MISSING
        # Transitional overlay outranks deleted_at — mirrors the authoritative
        # driver resolver (core_postgresql.get_lifecycle). #2066.
        status = row.get("lifecycle_status")
        if status == CollectionLifecycle.DELETING.value:
            return CollectionLifecycle.DELETING
        if status == CollectionLifecycle.PROVISIONING.value:
            return CollectionLifecycle.PROVISIONING
        if row["deleted_at"] is not None:
            return CollectionLifecycle.TOMBSTONED
        return CollectionLifecycle.ACTIVE

    async def _set_lifecycle_status(
        self,
        catalog_id: str,
        collection_id: str,
        status: Optional[str],
        db_resource: Optional[DbResource] = None,
    ) -> bool:
        """Write the transitional overlay ``collections.lifecycle_status`` (#2066).

        ``status=None`` clears the overlay (back to ACTIVE / TOMBSTONED per
        ``deleted_at``); a value moves the row to PROVISIONING / DELETING.  The
        UPDATE is scoped by id only — a tombstoned row can still be pre-marked
        DELETING for a hard-delete purge.  Returns ``True`` when a row was
        touched.  Runs in its own committed transaction when ``db_resource`` is
        None so the new state is visible cross-pod before the caller proceeds.
        """
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys_schema:
                return False
            rows = await DQLQuery(
                f'UPDATE "{phys_schema}".collections SET lifecycle_status = :status '
                "WHERE id = :id;",
                result_handler=ResultHandler.ROWCOUNT,
            ).execute(conn, id=collection_id, status=status)
        return bool(rows)

    async def resolve_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        """Resolve the physical table name for a collection.

        Delegates to ``CatalogsProtocol.resolve_physical_id`` — the registry
        column ``{schema}.collections.physical_id`` is the single authoritative
        source.  The old JSONB ``physical_table`` pin is no longer consulted.
        """
        from dynastore.models.protocols.catalogs import CatalogsProtocol as _CatalogsProtocol
        from dynastore.models.driver_context import DriverContext as _DC
        from dynastore.tools.discovery import get_protocol as _gp

        catalogs = _gp(_CatalogsProtocol)
        if catalogs is None:
            return None
        ctx = _DC(db_resource=db_resource) if db_resource is not None else None
        return await catalogs.resolve_physical_id(
            catalog_id, collection_id, ctx=ctx, allow_missing=True
        )

    async def is_active(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> bool:
        """True once storage has been provisioned for this collection.

        Activation state is derived from the PG driver config's
        ``physical_table`` pin **and** a lightweight catalog existence
        probe to guard against out-of-band divergence (table dropped
        without clearing the pin, or partial-infra failure on an older
        build).

        Logic:
        - No pin (physical_table is None) → False; no probe needed.
        - Pin present + process-local confirmed set hit → True; steady-state
          writes pay only an O(1) dict lookup after the first confirmation.
        - Pin present + confirmed set miss → ``SELECT to_regclass(...)`` DB
          read (one extra round-trip, ~0.1–0.3 ms).  If the table exists,
          add to the confirmed set and return True.  If the table is absent
          (diverged state), remove any stale confirmation and return False so
          the caller's lazy-activation path re-provisions via ensure_storage.
        """
        phys_table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )
        if phys_table is None:
            return False

        # Fast path: table already confirmed in this process.
        if (catalog_id, collection_id) in _confirmed_active:
            return True

        # Slow path: verify the physical table actually exists in PG.
        phys_schema = await self._resolve_physical_schema(
            catalog_id, db_resource=db_resource
        )
        if phys_schema is None:
            # Cannot resolve schema — fall back to pin-only (original behaviour).
            logger.warning(
                "is_active: cannot resolve physical schema for %s/%s; "
                "falling back to pin-only check",
                catalog_id, collection_id,
            )
            return True

        from dynastore.modules.db_config.locking_tools import check_table_exists

        async def _probe(conn: DbResource) -> bool:
            return await check_table_exists(conn, phys_table, schema=phys_schema)

        try:
            if db_resource is not None:
                exists = await _probe(db_resource)
            else:
                async with managed_transaction(self.engine) as conn:
                    exists = await _probe(conn)
        except Exception as exc:
            # Probe errors (connection issues, permission denied) must not
            # silently swallow legitimate writes. Log and fall back to the
            # pin-only check so a transient DB hiccup does not trigger
            # spurious re-provisioning.
            logger.warning(
                "is_active: table existence probe failed for %s/%s.%s: %s; "
                "falling back to pin-only",
                catalog_id, collection_id, phys_table, exc,
            )
            return True

        if exists:
            _mark_confirmed_active(catalog_id, collection_id)
            return True

        # Table is missing despite pin — diverged state.  Clear any prior
        # confirmation so a concurrent call also goes through re-provisioning.
        _unmark_confirmed_active(catalog_id, collection_id)
        logger.warning(
            "is_active: physical table %r not found in schema %r for %s/%s "
            "(pin present but table absent — diverged state); "
            "re-provisioning will be triggered",
            phys_table, phys_schema, catalog_id, collection_id,
        )
        return False

    async def _activate_collection(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        conn: DbResource,
    ) -> None:
        """Provision storage + pin routing for a pending collection.

        Idempotent. Runs `ensure_storage` against the write driver (creates
        hub + sidecar tables / ES index / GCS prefix), then pins
        `ItemsRoutingConfig` at collection scope so future platform
        default changes cannot silently re-route existing data.

        Skipped gracefully when no storage drivers are registered (test
        environments without `StorageModule`).
        """
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import ItemsRoutingConfig

        # Defense in depth: never provision storage for a collection that is
        # not alive, regardless of which caller reached this point.
        # Catalog-scoped activation (collection_id is None, e.g. catalog-level
        # assets) has no collection registry row to check — bypass, matching
        # the upsert funnel gate.
        if collection_id is not None:
            await self.ensure_alive(catalog_id, collection_id, db_resource=conn)

        # Provision storage. `ensure_storage` is idempotent; concurrent
        # first-inserts will both call it safely.  Each driver self-fetches
        # its own config — no cross-driver type confusion possible.
        try:
            write_driver = await get_driver("WRITE", catalog_id, collection_id)
            await write_driver.ensure_storage(
                catalog_id,
                collection_id,
                db_resource=conn,
            )
        except ValueError:
            # No storage drivers registered — PG-native tables are handled
            # separately; nothing to provision.
            return

        # Pin the resolved routing at collection scope. FOR UPDATE row lock
        # serialises concurrent activators; loser sees equal value and the
        # immutability guard short-circuits (equal values accepted).
        try:
            configs = get_protocol(ConfigsProtocol)
            if configs is None:
                raise ValueError("ConfigsProtocol not registered")
            resolved_routing = await configs.get_config(
                ItemsRoutingConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
                ctx=DriverContext(db_resource=conn),
            )
            if resolved_routing:
                await configs.set_config(
                    ItemsRoutingConfig,
                    resolved_routing,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    ctx=DriverContext(db_resource=conn),
                )
        except Exception as _routing_e:
            logger.warning(
                "_activate_collection: failed to pin routing for %s/%s: %s",
                catalog_id, collection_id, _routing_e,
            )

    async def activate_collection(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> None:
        """Ensure the collection is active.

        Idempotent — safe to call on already-active collections. Called
        from the items write path (lazy activation); no REST endpoint
        backs this method (activation happens transparently on the
        first ``POST /items``). Kept on ``CollectionsProtocol`` so
        ``item_service`` can invoke it via the protocol layer.

        On success, the (catalog_id, collection_id) pair is added to the
        process-local confirmed-active set so subsequent ``is_active`` calls
        skip the DB existence probe.
        """
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            await self._activate_collection(
                catalog_id, collection_id, conn=conn,
            )
        # Provisioning committed — the physical table exists.  Mark confirmed
        # so the next is_active call on the write path takes the fast path.
        _mark_confirmed_active(catalog_id, collection_id)

    async def _get_collection_model_db(
        self, catalog_id: str, collection_id: str
    ) -> Optional[Collection]:
        async with managed_transaction(self.engine) as conn:
            return await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )

    async def _get_collection_model_logic(
        self,
        catalog_id: str,
        collection_id: str,
        conn: DbResource,
        *,
        hints: FrozenSet = frozenset(),
    ) -> Optional[Collection]:
        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
        if not phys_schema:
            return None

        # 1. Verify existence in thin PG registry (always PG — thin registry is authoritative).
        exists = await _make_collection_exists_query(phys_schema).execute(
            conn, id=collection_id
        )
        if not exists:
            return None

        # 2. Read metadata via the router — fan-out across every registered
        # CollectionStore driver (PG Core + PG Stac by default).  The
        # router merges the per-domain slices into one dict.
        #
        # Hints are threaded straight through.  An empty hint set keeps the
        # existing merge-all behaviour (byte-identical default read).  A
        # non-empty hint set lets a deployment whose routing config declares
        # hinted READ drivers prefer one driver's view (first-non-None);
        # see ``get_collection_metadata``.
        from dynastore.modules.catalog.collection_router import (
            get_collection_metadata as _route_get_metadata,
        )

        meta_dict = await _route_get_metadata(
            catalog_id, collection_id,
            hints=hints,
            db_resource=conn,
        ) or {}

        # Deserialize JSONB columns
        for key in ["title", "description", "keywords", "license", "links", "assets",
                    "extent", "providers", "summaries", "item_assets", "extra_metadata",
                    "stac_extensions"]:
            val = meta_dict.get(key)
            if isinstance(val, str):
                try:
                    meta_dict[key] = json.loads(val)
                except Exception:
                    meta_dict[key] = None

        data = {
            "id": collection_id,
            "title": meta_dict.get("title"),
            "description": meta_dict.get("description"),
            "keywords": meta_dict.get("keywords"),
            "license": meta_dict.get("license"),
            "links": meta_dict.get("links"),
            "assets": meta_dict.get("assets"),
            "extent": meta_dict.get("extent"),
            "providers": meta_dict.get("providers"),
            "summaries": meta_dict.get("summaries"),
            "item_assets": meta_dict.get("item_assets"),
            "extra_metadata": meta_dict.get("extra_metadata"),
        }

        # 3. CollectionMetadataEnricherProtocol pipeline removed in the
        #    role-based driver refactor (plan §Protocols — deleted).  Any
        #    in-process hook that used to enrich the metadata dict here is
        #    now a TRANSFORM driver routed through CollectionRoutingConfig —
        #    invoked lazily when an endpoint opts in or when the async
        #    reindex pipeline is preparing a transformed INDEX/BACKUP
        #    envelope.  Default read path is deliberately transform-free.

        return Collection.model_validate(data)

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        *,
        hints: FrozenSet = frozenset(),
    ) -> Optional[Collection]:
        """Retrieves a collection by ID, localized."""
        db_resource = ctx.db_resource if ctx else None
        collection_model = await self.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource, hints=hints,
        )
        if not collection_model:
            return None

        # Localize
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.localization import LocalizationProtocol
        loc = get_protocol(LocalizationProtocol)
        if loc:
            collection_model = loc.localize_model(collection_model, lang)
        return collection_model

    async def get_collection_config(
        self, catalog_id: str, collection_id: str, ctx: Optional["DriverContext"] = None
    ) -> CollectionPluginConfig:
        """Retrieves the active driver config for a collection (via routing)."""
        db_resource = ctx.db_resource if ctx else None
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation

        driver = await get_driver(Operation.READ, catalog_id, collection_id)
        return await driver.get_driver_config(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def get_collection_model(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[DbResource] = None,
        *,
        hints: FrozenSet = frozenset(),
    ) -> Optional[Collection]:
        """Return the collection metadata model, optionally hint-routed.

        Cache behaviour (requirement B):
        - When ``hints`` is empty the result is served from the shared
          ``_collection_model_cache`` (keyed by catalog_id + collection_id).
          The cache entry is populated via ``_get_collection_model_db``
          on the no-hint merge-all path, so the cached model is the full
          default envelope (byte-identical to the pre-hints baseline).
        - When ``hints`` is non-empty the cache is bypassed entirely so
          a geometry_simplified read cannot be served a cached
          default-shaped model and vice-versa.
        """
        if db_resource:
            async with managed_transaction(db_resource) as conn:
                return await self._get_collection_model_logic(
                    catalog_id, collection_id, conn, hints=hints,
                )
        if hints:
            # Bypass cache: hinted read must never serve a differently-hinted
            # cached model (e.g. a geometry_simplified ES copy being returned
            # when a geometry_exact PG model is in cache, or vice-versa).
            async with managed_transaction(self.engine) as conn:
                return await self._get_collection_model_logic(
                    catalog_id, collection_id, conn, hints=hints,
                )
        return await _collection_model_cache(self, catalog_id, collection_id)

    async def get_collection_column_names(
        self,
        catalog_id: str,
        collection_id: str,
        ctx: Optional["DriverContext"] = None,
    ) -> Set[str]:
        """Retrieves the physical column names for a collection."""
        db_resource = ctx.db_resource if ctx else None
        phys_schema = await self._resolve_physical_schema(
            catalog_id, db_resource=db_resource
        )
        phys_table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )
        if not phys_schema or not phys_table:
            return set()

        from dynastore.modules.db_config.shared_queries import get_table_column_names

        async def _execute(conn):
            return await get_table_column_names(conn, phys_schema, phys_table)

        if db_resource:
            return await _execute(db_resource)
        assert self.engine is not None, "engine required"
        from sqlalchemy.ext.asyncio import AsyncEngine as _AsyncEngine
        assert isinstance(self.engine, _AsyncEngine), "engine must be AsyncEngine for get_collection_column_names"
        async with self.engine.connect() as conn:
            return await _execute(conn)

    async def ensure_collection_exists(
        self,
        db_resource: DbResource,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
    ) -> None:
        if not await self.get_collection_model(
            catalog_id, collection_id, db_resource=db_resource
        ):
            # If lang is not '*', we provide a simple string which create_collection will localize
            # If lang is '*', we provide the default 'en' dictionary
            title = {"en": collection_id} if lang == "*" else collection_id
            await self.create_collection(
                catalog_id,
                {"id": collection_id, "title": title},
                lang=lang,
                ctx=DriverContext(db_resource=db_resource) if db_resource else None,
            )

    async def create_collection(
        self,
        catalog_id: str,
        collection_definition: Union[Dict[str, Any], Collection],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        **kwargs,
    ) -> Collection:
        db_resource = ctx.db_resource if ctx else None
        if isinstance(collection_definition, dict):
            from dynastore.models.localization import validate_language_consistency

            validate_language_consistency(collection_definition, lang)

        collection_model = (
            Collection.create_from_localized_input(collection_definition, lang)
            if isinstance(collection_definition, dict)
            else collection_definition
        )
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_model.id)

        async with managed_transaction(db_resource or self.engine) as conn:
            # Check catalog exists
            catalogs = get_protocol(CatalogsProtocol)
            assert catalogs is not None, "CatalogsProtocol not registered"
            if not await catalogs.get_catalog_model(catalog_id, ctx=DriverContext(db_resource=conn)):
                raise ValueError(f"Catalog '{catalog_id}' does not exist.")

            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                raise ValueError(f"No physical schema found for catalog '{catalog_id}'")

            logger.info(
                f"[LIFECYCLE] Creating collection '{catalog_id}:{collection_model.id}' in schema '{phys_schema}'"
            )

            # #317: reclaim a soft-deleted (tombstoned) id. A prior default
            # (soft) DELETE leaves the collections row with deleted_at set plus
            # its physical table, metadata sidecars and configs intact. Purge
            # that residue here so the id is reused as a clean, fresh
            # collection. A still-live row (deleted_at IS NULL) is left
            # untouched, so the INSERT below raises the usual conflict.
            tombstoned = await DQLQuery(
                f'SELECT 1 FROM "{phys_schema}".collections WHERE id = :id AND deleted_at IS NOT NULL;',
                result_handler=ResultHandler.ONE_OR_NONE,
            ).execute(conn, id=collection_model.id)
            if tombstoned is not None:
                logger.info(
                    f"[LIFECYCLE] Reclaiming soft-deleted collection "
                    f"'{catalog_id}:{collection_model.id}' for reuse (#317)"
                )
                await self._purge_collection_storage(
                    conn, phys_schema, catalog_id, collection_model.id
                )

            # Get driver config (default/platform config only - collection doesn't exist yet,
            # so we must NOT pass db_resource here; querying collection_configs in a nested
            # transaction before the table may be ready would poison the outer transaction).
            from dynastore.modules.storage.router import get_driver as _get_driver
            from dynastore.modules.storage.routing_config import Operation
            _meta_driver = await _get_driver(Operation.READ, catalog_id, collection_model.id)
            collection_config = await _meta_driver.get_driver_config(
                catalog_id, collection_model.id,
            )

            # Layer config override from input
            layer_config_override = None
            if isinstance(collection_definition, dict):
                layer_config_override = collection_definition.get("layer_config")
                if not layer_config_override and "sidecars" in collection_definition:
                    layer_config_override = {
                        "sidecars": collection_definition["sidecars"]
                    }
            else:
                if hasattr(collection_definition, "layer_config"):
                    layer_config_override = getattr(collection_definition, "layer_config")

                # Check for sidecars in Pydantic model (extra fields)
                if not layer_config_override and hasattr(
                    collection_definition, "sidecars"
                ):
                    # We wrap it in a dict to be compatible with CollectionPluginConfig input
                    layer_config_override = {"sidecars": getattr(collection_definition, "sidecars")}

            # M1b.3: blocks that used to (a) coerce layer_config_override from
            # dict → ItemsPostgresqlDriverConfig and (b) iterate
            # SidecarRegistry.get_injected_sidecar_configs to mutate the
            # override are both deleted.  Driver-specific typing + defaults
            # now live inside the PG driver's init_collection hook (see
            # `_pg_driver_init_collection` in
            # modules/storage/drivers/postgresql.py) and inside
            # `_effective_sidecars` at DDL/read/write time.  The generic
            # service hands `layer_config` down to lifecycle_registry as an
            # opaque payload.

            # Resolved config for this collection creation — caller-supplied
            # override if any, else the plugin default (unused by the PG
            # driver post-refactor; still passed through for non-PG drivers
            # that may consume it via their own init_collection hook).
            init_config = layer_config_override or collection_config

            # Clean kwargs to avoid multiple values for arguments already passed positionally or explicitly
            init_kwargs = kwargs.copy()
            init_kwargs.pop("physical_table", None)
            init_kwargs.pop("layer_config", None)

            # 3. Insert thin registry row (id + catalog_id + lifecycle overlay +
            #    physical_id).
            #    #2066: when external async initializers are registered the
            #    collection is born PROVISIONING and write-gated (409) until the
            #    async init window closes; the finalizer below flips it to
            #    ACTIVE. With no async initializers the row is ACTIVE on commit
            #    (lifecycle_status NULL) — no pointless provisioning window.
            #
            #    physical_id (P2a): a short, stable physical token for the
            #    collection minted once at registry create time. Format matches
            #    the catalog-level generate_physical_name convention (the helper
            #    appends the "_" separator itself, so the prefix is bare "c" —
            #    yielding "c_<suffix>", not a double-underscore "c__<suffix>").
            from dynastore.modules.catalog.catalog_service import generate_physical_name as _gen_phys_name
            collection_physical_id = _gen_phys_name("c")
            provisioning = lifecycle_registry.has_async_collection_initializers()
            insert_sql = f"""
                INSERT INTO "{phys_schema}".collections (id, catalog_id, lifecycle_status, physical_id)
                VALUES (:id, :catalog_id, :lifecycle_status, :physical_id)
                RETURNING id;
            """
            await DQLQuery(insert_sql, result_handler=ResultHandler.SCALAR_ONE).execute(
                conn,
                id=collection_model.id,
                catalog_id=catalog_id,
                lifecycle_status=(
                    CollectionLifecycle.PROVISIONING.value if provisioning else None
                ),
                physical_id=collection_physical_id,
            )

            # 4. Run infrastructure hooks (events partition, logs, proxy —
            #    hub/sidecars are handled by write_driver.ensure_storage() below).
            await lifecycle_registry.init_collection(
                conn,
                phys_schema,
                catalog_id,
                collection_model.id,
                layer_config=init_config,
                **init_kwargs,
            )

            # M1b.3: the unconditional `configs.set_config` that used to
            # persist layer_config_override on EVERY collection create is
            # deleted.  Persistence of PG-driver-specific config now runs
            # inside the PG init_collection hook registered in
            # postgresql.py — and only when the caller actually supplied
            # PG-specific fields (default-fast invariant).  Other driver
            # modules register their own hooks for their own config
            # shapes.

            # 5b. Persist write_policy + schema (ItemsSchema, if provided) BEFORE
            #     ensure_storage so the driver can materialise required/unique
            #     constraints at DDL time (ItemsSchema.fields →
            #     NOT NULL / UNIQUE in the attributes sidecar).
            write_policy_input = None
            schema_input = None
            if isinstance(collection_definition, dict):
                write_policy_input = collection_definition.get("write_policy")
                schema_input = collection_definition.get("schema")
            else:
                _fields = type(collection_definition).model_fields if hasattr(type(collection_definition), "model_fields") else {}
                write_policy_input = getattr(collection_definition, "write_policy", None) if "write_policy" in _fields else None
                schema_input = getattr(collection_definition, "schema", None) if "schema" in _fields else None

            if write_policy_input:
                from dynastore.modules.storage.driver_config import (
                    ItemsWritePolicy,
                )
                policy = (
                    ItemsWritePolicy.model_validate(write_policy_input)
                    if isinstance(write_policy_input, dict)
                    else write_policy_input
                )
                configs = get_protocol(ConfigsProtocol)
                assert configs is not None, "ConfigsProtocol not registered"
                await configs.set_config(
                    ItemsWritePolicy,
                    policy,
                    catalog_id=catalog_id,
                    collection_id=collection_model.id,
                    ctx=DriverContext(db_resource=conn),
                )
            if schema_input:
                from dynastore.modules.storage.driver_config import (
                    ItemsSchema,
                )
                schema_def = (
                    ItemsSchema.model_validate(schema_input)
                    if isinstance(schema_input, dict)
                    else schema_input
                )
                configs = get_protocol(ConfigsProtocol)
                assert configs is not None, "ConfigsProtocol not registered"
                await configs.set_config(
                    ItemsSchema,
                    schema_def,
                    catalog_id=catalog_id,
                    collection_id=collection_model.id,
                    ctx=DriverContext(db_resource=conn),
                )

            # 6. (Lazy activation) Steps formerly responsible for
            #    `ensure_storage` + routing pin have moved to
            #    `_activate_collection`. A newly-created collection is
            #    **pending** until either:
            #       - the first `POST /items` triggers lazy activation, or
            #       - an operator calls `POST /collections/{col}/activate`.
            #    During the pending window, `PUT /configs/.../ItemsRoutingConfig`
            #    (and any other collection-scope config) can be freely set —
            #    the Immutable guard short-circuits while `current=None`.

            # 7. Store collection metadata via the router — fan-out across
            # every registered CollectionStore driver.  Each driver
            # filters the unified payload to its own domain's columns and
            # no-ops on an empty filtered slice.
            from dynastore.modules.catalog.collection_router import (
                upsert_collection_metadata as _route_upsert_metadata,
            )

            metadata_payload = collection_model.model_dump(
                by_alias=True, exclude_none=True
            )
            await _route_upsert_metadata(
                catalog_id, collection_model.id, metadata_payload,
                db_resource=conn,
            )

            # Steps 8 & 9 (write_policy + schema persistence) moved to
            # step 5b above so ensure_storage can honour constraints.

            # Emit domain event so _on_collection_creation listeners (e.g.
            # tasks/event_driver.py) can fan out a collection_creation outbox
            # row, giving /events + /logs parity with catalog_creation.
            await emit_event(
                CatalogEventType.COLLECTION_CREATION,
                catalog_id=catalog_id,
                collection_id=collection_model.id,
                db_resource=conn,
            )

        # Resolve physical_table for async lifecycle context (PG driver only; None for others).
        # Use db_resource when available so uncommitted catalog/collection rows are visible.
        # Fall back to None gracefully if the table hasn't been registered yet.
        try:
            physical_table = await self.resolve_physical_table(
                catalog_id, collection_model.id, db_resource=db_resource
            )
        except (ValueError, Exception):
            physical_table = None

        # Invalidate liveness/routing caches post-commit (#2066).
        _invalidate_collection_lifecycle_caches(catalog_id, collection_model.id)

        # Trigger async lifecycle
        config_snapshot = {}
        try:
            _cfg = get_protocol(ConfigsProtocol)
            if _cfg is not None:
                config_snapshot.update(await _cfg.list_catalog_configs(catalog_id))
        except Exception as exc:
            logger.warning(
                "collection %s/%s: failed to load config snapshot for lifecycle init: %s",
                catalog_id, collection_model.id, exc,
            )

        # #2066: when the row was born PROVISIONING, flip it back to ACTIVE once
        # async init finishes.  The finalizer runs in the background task's
        # `finally`, in its own committed transaction (db_resource=None), so the
        # ACTIVE state is visible cross-pod; it then re-invalidates the liveness
        # caches so the write-gate stops returning 409.
        _on_complete = None
        if provisioning:
            _cat_id, _col_id = catalog_id, collection_model.id

            async def _finalize_provisioning() -> None:
                await self._set_lifecycle_status(_cat_id, _col_id, None)
                _invalidate_collection_lifecycle_caches(_cat_id, _col_id)

            _on_complete = _finalize_provisioning

        lifecycle_registry.init_async_collection(
            catalog_id,
            collection_model.id,
            LifecycleContext(
                physical_schema=phys_schema,
                physical_table=physical_table,
                config=config_snapshot,
            ),
            on_complete=_on_complete,
        )

        # Emit signal to wake up background tasks (Visibility Gap fix)
        await signal_bus.emit(
            "AFTER_COLLECTION_CREATION", identifier=collection_model.id
        )

        result = await self.get_collection_model(
            catalog_id, collection_model.id, db_resource=db_resource
        )
        assert result is not None, f"Collection '{collection_model.id}' not found after creation"
        return result

    async def list_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
        q: Optional[str] = None,
        *,
        hints: FrozenSet = frozenset(),
    ) -> List[Collection]:
        db_resource = ctx.db_resource if ctx else None

        # A filtered listing (free-text ``q``) must go through the SEARCH-
        # capable driver, so keep the routing-driven path for that case: the
        # collection-metadata router picks the configured SEARCH driver for the
        # scope and returns COMPLETE collections.  The READ fallback covers a
        # deploy whose SEARCH slice has no results yet.
        if q:
            from dynastore.modules.catalog.collection_router import (
                search_collection_metadata as _route_search,
            )
            from dynastore.modules.storage.routing_config import Operation

            for op in (Operation.SEARCH, Operation.READ):
                try:
                    rows, _ = await _route_search(
                        catalog_id, q=q, limit=limit, offset=offset,
                        db_resource=db_resource, operation=op,
                    )
                except Exception as exc:
                    logger.warning(
                        "Collection-metadata router %s failed for %s: %s",
                        op, catalog_id, exc,
                    )
                    continue
                if rows:
                    return [Collection.model_validate(row) for row in rows]
            return []

        # Unfiltered listing: enumerate ids from the thin PG registry — the
        # authoritative existence ledger for every backend — and hydrate each
        # via the configured READ router.  This makes listing backend-agnostic:
        # a pure-ES (or DuckDB-only) catalog lists its collections from the
        # registry and reads their metadata from ES/DuckDB, instead of
        # depending on the ES SEARCH index being populated.  Existence lives in
        # PG; metadata lives wherever the preset routes it.
        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return []
            ids = await _make_collection_list_ids_query(phys_schema).execute(
                conn, limit=limit, offset=offset
            )
            collections: List[Collection] = []
            for collection_id in ids:
                model = await self._get_collection_model_logic(
                    catalog_id, collection_id, conn, hints=hints,
                )
                if model is not None:
                    collections.append(model)
            return collections

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates: Dict[str, Any],
        lang: str = "en",
        ctx: Optional["DriverContext"] = None,
    ) -> Optional[Collection]:
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        from dynastore.models.localization import validate_language_consistency

        validate_language_consistency(updates, lang)

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return None

            existing_model = await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )
            if not existing_model:
                return None

            merged_model = existing_model.merge_localized_updates(updates, lang)

            # First verify the collection exists in thin registry
            exists = await _make_collection_exists_query(phys_schema).execute(
                conn, id=collection_id
            )
            if not exists:
                return None

            # Fan-out metadata writes via the collection-metadata router.
            from dynastore.modules.catalog.collection_router import (
                upsert_collection_metadata as _route_upsert_metadata,
            )

            metadata_payload = merged_model.model_dump(
                by_alias=True, exclude_none=True
            )
            await _route_upsert_metadata(
                catalog_id, collection_id, metadata_payload,
                db_resource=conn,
            )

            # Fetch the post-write state inside the TX so the caller's
            # response includes the freshly-merged data (the conn is the
            # only resource where the uncommitted upsert is visible).
            fresh = await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )

        # CRITICAL: invalidate the cache AFTER the transaction commits
        # (closes #199). Pre-fix this happened inside the async-with block
        # while the upsert was still uncommitted. Concurrent readers
        # (background lifecycle hooks, sibling requests) could populate
        # the cache with the OLD pre-write data between cache_invalidate
        # firing and the TX actually committing — leaving the cache with
        # stale data that subsequent GETs would happily serve. Mirror
        # what create_collection (line 624) and delete_collection (923)
        # already do: invalidate after the `async with` exits.
        _invalidate_collection_model_cache(catalog_id, collection_id)

        return fresh

    async def _purge_collection_storage(
        self,
        conn: DbResource,
        phys_schema: str,
        catalog_id: str,
        collection_id: str,
    ) -> Optional[str]:
        """Tear down a collection's physical + metadata footprint within ``conn``.

        Shared by hard delete (``force=True``) and ``create_collection``'s
        tombstone reset (#317): resolves the physical items table from the
        driver config, runs the lifecycle destroy hooks, drops the items
        table, removes the registry row, fans out metadata-table deletion
        (collection_core / collection_stac), and clears ``collection_configs``.

        The caller owns any async external-resource destroy. Returns the
        dropped physical table name (``None`` if the collection was never
        activated, i.e. no storage had been provisioned).
        """
        phys_table = await self.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        await lifecycle_registry.destroy_collection(
            conn, phys_schema, catalog_id, collection_id
        )
        await lifecycle_registry.hard_destroy_collection(
            conn, phys_schema, catalog_id, collection_id
        )
        if phys_table:
            pg_driver = await self._get_pg_driver()
            if pg_driver is not None:
                # Driver-owned teardown (hub + every sidecar), inside this
                # transaction so a failed drop rolls back with the registry row.
                await pg_driver.drop_storage(
                    catalog_id,
                    collection_id,
                    db_resource=conn,
                    physical_table=phys_table,
                    physical_schema=phys_schema,
                )
            else:
                # Degrade-safe for environments without StorageModule. The
                # literal core suffixes are deliberate: this branch only runs
                # when no PG driver is registered, and without the storage
                # module no extension sidecar can have provisioned tables
                # either — so the core set is exhaustive here. (Importing the
                # sidecar registry from a driver package is also forbidden in
                # service code; see test_services_have_no_driver_imports.)
                for suffix in ("attributes", "geometries", "item_metadata", "stac_metadata"):
                    await shared_queries.delete_table_query.execute(
                        conn, schema=phys_schema, table=f"{phys_table}_{suffix}"
                    )
                await shared_queries.delete_table_query.execute(
                    conn, schema=phys_schema, table=phys_table
                )
        await DDLQuery(
            f'DELETE FROM "{phys_schema}".collections WHERE id = :id;'
        ).execute(conn, id=collection_id)
        from dynastore.modules.catalog.collection_router import (
            delete_collection_metadata as _route_delete_metadata,
        )

        await _route_delete_metadata(catalog_id, collection_id, db_resource=conn)
        # ``phys_table`` is the collection's physical_id (from collections.physical_id,
        # resolved above at the start of this method before the collections row was
        # deleted).  Use it as the key into collection_configs so this DELETE succeeds
        # even though the collections row is already gone at this point.
        if phys_table:
            await DQLQuery(
                f'DELETE FROM "{phys_schema}".collection_configs WHERE physical_id = :phys_id;',
                result_handler=ResultHandler.NONE,
            ).execute(conn, phys_id=phys_table)
        return phys_table

    async def _capture_collection_config_snapshot(
        self, catalog_id: str, collection_id: str
    ) -> Dict[str, Any]:
        """Best-effort snapshot of a collection's resolved config for the
        post-commit async external-resource destroyer.

        Resolves via the cache path (no shared transaction connection) so the
        caller can run it BEFORE opening the delete transaction — keeping that
        transaction free of distributed-cache I/O and nested pool acquisition
        that would otherwise hold its connection idle (see the call site in
        ``delete_collection`` for the idle-in-transaction failure this avoids).

        Never raises: a snapshot failure must not block the delete. Returns at
        least ``{catalog_id, collection_id}`` so the destroyer can still locate
        the scope.
        """
        snapshot: Dict[str, Any] = {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
        }
        try:
            configs = get_protocol(ConfigsProtocol)
            if configs is not None:
                coll_config = await configs.get_config(
                    CollectionPluginConfig,
                    catalog_id,
                    collection_id,
                )
                if coll_config:
                    snapshot["collection_config"] = coll_config.model_dump()
        except Exception as e:  # noqa: BLE001 — best-effort; never block delete
            logger.warning(
                f"Failed to capture config snapshot for "
                f"'{catalog_id}:{collection_id}': {e}"
            )
        return snapshot

    async def delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = False,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        # Unit A (#2066): lifecycle gate BEFORE any teardown. Resolve the
        # authoritative state up front so the hard-delete purge — which emits
        # BEFORE/AFTER_HARD_DELETION, runs the cascade owners, and schedules the
        # external-resource destroy — never fires against a MISSING collection
        # (a double-teardown that destroyed resources the id never owned).
        lifecycle = await self._get_lifecycle(
            catalog_id, collection_id, db_resource=db_resource
        )
        if lifecycle == CollectionLifecycle.MISSING:
            logger.info(
                f"[LIFECYCLE] delete_collection no-op: "
                f"'{catalog_id}:{collection_id}' is missing"
            )
            return False
        if not force and lifecycle == CollectionLifecycle.TOMBSTONED:
            # Already soft-deleted: idempotent no-op. Re-tombstoning would be a
            # 0-row UPDATE anyway, but returning here also skips re-emitting
            # COLLECTION_DELETION to the cascade subscribers.
            logger.info(
                f"[LIFECYCLE] delete_collection no-op: "
                f"'{catalog_id}:{collection_id}' already tombstoned"
            )
            return True

        # Unit C (#2066): for a hard delete, pre-mark DELETING in its own
        # committed transaction (db_resource=None ⇒ autonomous commit) so the
        # purge window is observable cross-pod and write-gated (409) instead of
        # flickering ACTIVE↔MISSING. When the caller supplies a db_resource the
        # mark joins their transaction and commits at their boundary.
        #
        # Recovery: if the purge below fails, the autonomous mark leaves the
        # (still-present) row DELETING — write-gated but intact. Re-issuing
        # delete_collection re-resolves DELETING, re-marks idempotently, and
        # retries the purge; a lifecycle-state reaper is the systematic backstop
        # for a creator/deleter pod that crashes mid-window (tracked follow-up).
        if force:
            await self._set_lifecycle_status(
                catalog_id,
                collection_id,
                CollectionLifecycle.DELETING.value,
                db_resource=db_resource,
            )
            _invalidate_collection_lifecycle_caches(catalog_id, collection_id)

        config_snapshot: Dict[str, Any] = {}
        if force:
            # Capture the config snapshot BEFORE opening the delete transaction.
            #
            # This read MUST stay outside the transaction. Resolving a
            # collection config performs distributed-cache I/O and, on a cache
            # miss, acquires a *second* pooled connection for a fallback DB read.
            # Done inside the open transaction (the old behaviour, reading via
            # the shared `conn`), that I/O held the delete connection idle while
            # it waited — and under any latency or pool contention that idle
            # window exceeded idle_in_transaction_session_timeout. PostgreSQL
            # then terminated the backend, so the very next statement on `conn`
            # (the cascade describe_scope below) failed with "the underlying
            # connection is closed", fail-closing the cascade and rolling back
            # the whole hard-delete.
            #
            # The snapshot is best-effort and only consumed post-commit by the
            # async external-resource destroyer (destroy_async_collection), so
            # it does not need transactional consistency with the purge. The
            # DELETING pre-mark above keeps the registry row present
            # (deleted_at IS NULL), so the read still resolves the live config.
            config_snapshot = await self._capture_collection_config_snapshot(
                catalog_id, collection_id
            )

        # Capture the immutable physical id BEFORE the delete (the row still
        # resolves) so we can bust the reverse (physical→logical) cache
        # post-commit on both soft and hard delete — once the row is tombstoned
        # or gone the resolver returns None and can no longer find the key.
        deleted_physical_id: Optional[str] = None
        try:
            # resolve_physical_table returns collections.physical_id (the PG
            # table name == the physical id post-convergence) — the exact key
            # the reverse cache is keyed on.
            deleted_physical_id = await self.resolve_physical_table(
                catalog_id, collection_id
            )
        except Exception:
            deleted_physical_id = None

        phys_table: Optional[str] = None
        async with managed_transaction(db_resource or self.engine) as conn:
            if db_resource is None:
                # Relax idle_in_transaction_session_timeout for THIS delete
                # transaction only (SET LOCAL auto-reverts on commit/rollback).
                #
                # The hard-delete path legitimately interleaves second-connection
                # reads while this connection is open and idle: the cascade
                # snapshot below calls describe_scope, whose
                # RoutingDrivenCascadeOwner resolves routing config via
                # ConfigsProtocol on its *own* pooled connection (by design — it
                # must observe live, pre-drop config), and _route_delete_metadata
                # does the same during the purge. While those reads run on the
                # second connection, this transaction's connection sits idle.
                #
                # The #2250 fix moved the top-level config snapshot out of the
                # transaction, but these in-txn second-connection reads remain by
                # design and a regression test pins describe_scope to run inside
                # the transaction. Under a cold config cache or pool contention
                # the idle gap exceeds the 30s default, PostgreSQL terminates the
                # backend, and the next statement fails with "the underlying
                # connection is closed" (e.g. SAVEPOINT or PreparedStatement
                # .fetch), leaving the row stuck in DELETING after the GCS folder
                # was already removed by the async destroyer.
                #
                # Disabling the idle reaper here is safe: the transaction is
                # still bounded by lock_timeout and per-statement command_timeout,
                # and it is driven by the background task runner — never an
                # abandoned client session that the reaper exists to collect.
                #
                # Issue the SET LOCAL directly on the outer connection — NOT via
                # DDLQuery. DDLQuery wraps the statement in managed_transaction,
                # which opens a SAVEPOINT when the connection is already in a
                # transaction (query_executor.sync/async managed_transaction);
                # a SET LOCAL scoped to that savepoint would not survive its
                # release and would never apply to the outer delete transaction.
                # A direct conn.execute runs in the outer transaction, exactly as
                # the executor itself issues SET LOCAL statement_timeout/lock_timeout.
                from typing import cast

                from sqlalchemy import text
                from sqlalchemy.ext.asyncio import AsyncConnection

                # db_resource is None here, so `conn` is the AsyncConnection we
                # acquired from managed_transaction(self.engine).
                await cast(AsyncConnection, conn).execute(
                    text("SET LOCAL idle_in_transaction_session_timeout = '0'")
                )
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return False

            if force:
                # Snapshot CleanupRefs BEFORE any state is torn down so that
                # describe_scope can read live rows.  Fail-closed: any exception
                # here propagates, rolling back the managed_transaction and
                # aborting the delete.
                from dynastore.modules.catalog.cascade_runtime import CascadeOrchestrator
                from dynastore.modules.catalog.resource_owner import (
                    CleanupMode,
                    ResourceScope,
                    ScopeRef,
                )
                _cascade_orchestrator = CascadeOrchestrator()
                _scope_ref = ScopeRef(
                    scope=ResourceScope.COLLECTION,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )
                await _cascade_orchestrator.snapshot_and_enqueue(
                    conn, _scope_ref, CleanupMode.HARD
                )

                # Lifecycle: BEFORE -> _purge_collection_storage -> HARD_DELETION
                # -> AFTER. Mirrors CatalogService.delete_catalog so the
                # subscribers wired to these events actually fire — most
                # importantly catalog_module._on_collection_hard_deletion
                # (cascade to AssetsProtocol.delete_assets), plus the tile cache
                # invalidator, and the webhook fan-out.
                await emit_event(
                    CatalogEventType.BEFORE_COLLECTION_HARD_DELETION,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    db_resource=conn,
                    physical_schema=phys_schema,
                )

                logger.info(
                    f"[LIFECYCLE] Hard deleting collection '{catalog_id}:{collection_id}'"
                )
                phys_table = await self._purge_collection_storage(
                    conn, phys_schema, catalog_id, collection_id
                )
                logger.info(
                    f"[LIFECYCLE] Hard deleted collection '{catalog_id}:{collection_id}' successfully"
                )

                await emit_event(
                    CatalogEventType.COLLECTION_HARD_DELETION,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    db_resource=conn,
                    physical_schema=phys_schema,
                    physical_table=phys_table,
                )
                await emit_event(
                    CatalogEventType.AFTER_COLLECTION_HARD_DELETION,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    db_resource=conn,
                    physical_schema=phys_schema,
                    physical_table=phys_table,
                )
            else:
                # Soft delete: tombstone the registry row only. The physical
                # table, metadata sidecars and collection_configs are
                # intentionally retained so the id can later be either
                # hard-deleted or reclaimed by create_collection — both of
                # which purge the residue via _purge_collection_storage for a
                # clean reset (#317). Retained configs are inert while the row
                # is tombstoned (every read filters deleted_at IS NULL).
                #
                # Also clear any transitional overlay (#2066): a collection
                # soft-deleted mid-provisioning must resolve cleanly as
                # TOMBSTONED, not keep reading PROVISIONING (the overlay outranks
                # deleted_at).
                soft_delete_sql = (
                    f'UPDATE "{phys_schema}".collections '
                    "SET deleted_at = NOW(), lifecycle_status = NULL "
                    "WHERE id = :id AND deleted_at IS NULL;"
                )
                rows = await DQLQuery(
                    soft_delete_sql, result_handler=ResultHandler.ROWCOUNT
                ).execute(conn, id=collection_id)
                # Only emit on a real state transition; an idempotent re-call
                # against an already-tombstoned row is a no-op and would
                # otherwise re-trigger cascade subscribers spuriously.
                if rows:
                    await emit_event(
                        CatalogEventType.COLLECTION_DELETION,
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                        db_resource=conn,
                        physical_schema=phys_schema,
                    )
                logger.info(
                    f"[LIFECYCLE] Soft deleted collection '{catalog_id}:{collection_id}'"
                )

        # Post-commit (#2066): drop every liveness/routing cache so neither the
        # model cache nor the router answers ACTIVE / a dropped physical table
        # for the transitioned id. The hard-delete row is now gone (MISSING),
        # which also clears the DELETING overlay set in the pre-mark above.
        _invalidate_collection_lifecycle_caches(catalog_id, collection_id)
        if deleted_physical_id:
            from dynastore.modules.catalog.catalog_service import (
                _invalidate_collection_logical_id_cache,
            )

            _invalidate_collection_logical_id_cache(catalog_id, deleted_physical_id)
        if force:
            _unmark_confirmed_active(catalog_id, collection_id)

        if force and phys_schema:
            lifecycle_registry.destroy_async_collection(
                catalog_id,
                collection_id,
                LifecycleContext(
                    physical_schema=phys_schema,
                    physical_table=phys_table,
                    config=config_snapshot,
                ),
            )

        return True

    async def delete_collection_language(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str,
        ctx: Optional["DriverContext"] = None,
    ) -> bool:
        """Deletes a specific language variant from a collection."""
        db_resource = ctx.db_resource if ctx else None
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)

        async with managed_transaction(db_resource or self.engine) as conn:
            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                return False

            model = await self._get_collection_model_logic(
                catalog_id, collection_id, conn
            )
            if not model:
                raise ValueError(
                    f"Collection '{catalog_id}:{collection_id}' not found."
                )

            can_delete = False
            fields_to_update: Dict[str, Any] = {}

            # Localizable fields — all belong to the CORE domain.
            for field in [
                "title",
                "description",
                "keywords",
                "license",
                "extra_metadata",
            ]:
                val = getattr(model, field, None)
                if val:
                    langs = val.get_available_languages()
                    if lang in langs:
                        if len(langs) <= 1:
                            raise ValueError(
                                f"Cannot delete language '{lang}' from field '{field}': it is the only language available."
                            )

                        data = val.model_dump(exclude_none=True)
                        if lang in data:
                            del data[lang]
                            # Router-direct upsert takes raw dicts; each
                            # driver encodes JSONB itself via _to_json().
                            fields_to_update[field] = data
                            can_delete = True

            if not can_delete:
                return False

            from dynastore.modules.catalog.collection_router import (
                upsert_collection_metadata as _route_upsert_metadata,
            )

            await _route_upsert_metadata(
                catalog_id, collection_id, fields_to_update,
                db_resource=conn,
            )

            _invalidate_collection_model_cache(catalog_id, collection_id)
            return True

    async def rename_collection(
        self,
        catalog_id: str,
        collection_id: str,
        new_id: str,
        ctx: Optional[DriverContext] = None,
    ) -> "RenameResponse":
        """Rename a collection's logical id (presentation label change only).

        The physical table (identified by ``physical_id`` / ``physical_table``),
        the partition key ``collection_physical_id`` on ``assets``, metadata
        tables (``collection_core``, ``collection_stac``), and all storage
        backends remain unchanged — they key on the immutable
        ``collection_physical_id``, not the logical label.

        Inside a single serialized transaction this updates only:

        - ``{schema}.collections.id`` — the single logical→physical mapping row.

        The ``assets.collection_id`` presentation column is intentionally left
        stale after a rename; it is never used as a lookup key.
        """
        from dynastore.extensions.ogc_models_shared import RenameResponse

        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        new_id = validate_sql_identifier(new_id)

        # Fast path: no-op when the normalized ids are identical.
        if new_id == collection_id:
            return RenameResponse(
                old_id=collection_id,
                new_id=new_id,
                level="collection",
                warnings=[],
                reindex_required=False,
                iam_manual_update_required=False,
            )

        db_resource = ctx.db_resource if ctx else None

        async with managed_transaction(db_resource or self.engine) as conn:
            if db_resource is None:
                from typing import cast as _cast
                from sqlalchemy import text as _text
                from sqlalchemy.ext.asyncio import AsyncConnection as _AsyncConn
                await _cast(_AsyncConn, conn).execute(
                    _text("SET LOCAL idle_in_transaction_session_timeout = '0'")
                )

            phys_schema = await self._resolve_physical_schema(
                catalog_id, db_resource=conn
            )
            if not phys_schema:
                raise ValueError(f"Catalog '{catalog_id}' not found.")

            # Serialize concurrent renames on the same collection within this schema.
            await DQLQuery(
                "SELECT pg_advisory_xact_lock(hashtext(:lock_key))",
                result_handler=ResultHandler.SCALAR,
            ).execute(conn, lock_key=f"{phys_schema}:col_rename:{collection_id}")

            # Existence check (404) — non-deleted rows only.  Fetch the
            # immutable physical_id while here: it is stable across the rename
            # and is the key for busting the reverse (physical→logical) cache,
            # whose mapping is exactly what this rename changes.
            existing_physical_id = await DQLQuery(
                f'SELECT physical_id FROM "{phys_schema}".collections '
                "WHERE id = :id AND deleted_at IS NULL;",
                result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
            ).execute(conn, id=collection_id)
            if existing_physical_id is None:
                raise ValueError(
                    f"Collection '{collection_id}' not found in catalog '{catalog_id}'."
                )

            # Collision check — include tombstoned rows (409).
            collision = await DQLQuery(
                f'SELECT id FROM "{phys_schema}".collections WHERE id = :new_id;',
                result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
            ).execute(conn, new_id=new_id)
            if collision is not None:
                raise CollectionRenameConflictError(new_id)

            # Update the thin registry PK — the single logical→physical mapping
            # row.  Metadata tables (collection_core, collection_stac) key on
            # collection_physical_id and are unaffected.
            await DDLQuery(
                f'UPDATE "{phys_schema}".collections '
                "SET id = :new_id, updated_at = NOW() "
                "WHERE id = :old_id;",
            ).execute(conn, new_id=new_id, old_id=collection_id)

        from dynastore.modules.catalog.catalog_service import (
            _invalidate_collection_logical_id_cache,
        )

        # Bust caches for both old and new id post-commit.
        _invalidate_collection_lifecycle_caches(catalog_id, collection_id)
        _invalidate_collection_lifecycle_caches(catalog_id, new_id)
        # The reverse (physical→logical) cache is keyed on the immutable
        # physical_id, so the logical-id-keyed lifecycle bust above cannot reach
        # it.  Drop it explicitly: this rename is precisely what remaps the
        # physical_id to a new logical id.
        _invalidate_collection_logical_id_cache(catalog_id, existing_physical_id)

        warnings = [
            "es_reindex_required",
            "iam_manual_update_required",
        ]
        return RenameResponse(
            old_id=collection_id,
            new_id=new_id,
            level="collection",
            warnings=warnings,
            reindex_required=True,
            iam_manual_update_required=True,
        )
