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
CORE PostgreSQL ``CollectionStore`` / ``CatalogStore`` drivers + shared CRUD base classes.

- :class:`CollectionCorePostgresqlDriver`  → ``{schema}.collection_core``
- :class:`CatalogCorePostgresqlDriver`     → ``catalog.catalog_core``

The matching STAC drivers live in
:mod:`dynastore.modules.stac.drivers.postgresql` and inherit
the shared CRUD bases (``_PgCollectionCoreBase`` /
``_PgCatalogCoreBase``) defined in this file — same column-filtered
upsert / delete / search shape, different column tuples and table names.

Each driver owns only its declared columns.  Callers that need the full
envelope run all participating drivers through the metadata router
(:mod:`~dynastore.modules.catalog.catalog_router` for catalogs,
:mod:`~dynastore.modules.catalog.collection_router` for
collections) which fans out the payload on WRITE and merges the slices
on READ.

Capabilities
------------

All four drivers declare ``READ``, ``WRITE``, ``SOFT_DELETE``, and
``PHYSICAL_ADDRESSING``.  Additional capabilities per driver:

- CORE drivers        — ``SEARCH`` (keyword match on title/description).
- STAC collection     — ``SPATIAL_FILTER`` (bbox match on extent).
- STAC catalog        — no SPATIAL_FILTER (catalog STAC has no extent).

Configs
-------

Each driver has an empty-default ``PluginConfig`` subclass so the waterfall
can still dispatch to them without any explicit per-collection config.
No fields are declared — they exist purely as Pydantic identity markers
for the ``ConfigsProtocol`` plumbing.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, ClassVar, Dict, FrozenSet, List, Optional, Tuple

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols.entity_store import (
    CollectionLifecycle,
    EntityStoreCapability,
)
from dynastore.models.protocols.teardown_lane import TeardownLane
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.storage.driver_config import DriverPluginConfig
from dynastore.modules.storage.routing_config import Operation

if TYPE_CHECKING:
    from dynastore.modules.storage.storage_location import StorageLocation


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain column sets (single source of truth — referenced by both the SQL
# builders and the domain-filter helpers below)
# ---------------------------------------------------------------------------

_COLLECTION_CORE_COLUMNS: Tuple[str, ...] = (
    "title", "description", "keywords", "license", "extra_metadata",
)
_CATALOG_CORE_COLUMNS: Tuple[str, ...] = (
    "title", "description", "keywords", "license", "extra_metadata",
)


# ---------------------------------------------------------------------------
# Helpers — shared between the four drivers
# ---------------------------------------------------------------------------


def _get_engine() -> Any:
    """Fetch the async DatabaseProtocol engine, or return None if unavailable."""
    from dynastore.models.protocols import DatabaseProtocol
    from dynastore.tools.discovery import get_protocol

    db = get_protocol(DatabaseProtocol)
    return db.engine if db else None


async def _resolve_physical_schema(
    catalog_id: str, *, db_resource: Optional[DbResource] = None,
) -> Optional[str]:
    """Resolve the physical schema for ``catalog_id`` via CatalogsProtocol."""
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.tools.discovery import get_protocol

    catalogs = get_protocol(CatalogsProtocol)
    if not catalogs:
        return None
    return await catalogs.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=db_resource)
    )


async def _resolve_collection_physical_id(
    catalog_id: str,
    collection_id: str,
    *,
    db_resource: Optional[DbResource] = None,
) -> Optional[str]:
    """Resolve the immutable physical id (``c_…`` token) for a collection.

    Reads ``{schema}.collections.physical_id`` via CatalogsProtocol, passing
    the in-flight connection so the lookup joins the caller's transaction.
    Returns ``None`` when the collection is absent or when the protocol is
    not registered.
    """
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.tools.discovery import get_protocol

    catalogs = get_protocol(CatalogsProtocol)
    if not catalogs:
        return None
    return await catalogs.resolve_physical_id(
        catalog_id, collection_id, ctx=DriverContext(db_resource=db_resource)
    )


def _to_json(value: Any) -> Optional[str]:
    """Serialise ``value`` as a JSON string for PG JSONB storage."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str)
    except Exception:
        return None


def _deserialise_jsonb(row: Dict[str, Any], columns: Tuple[str, ...]) -> Dict[str, Any]:
    """Parse JSONB string columns back into Python objects in-place."""
    for key in columns:
        val = row.get(key)
        if isinstance(val, str):
            try:
                row[key] = json.loads(val)
            except Exception:
                row[key] = None
    return row


def _filter_payload(
    metadata: Dict[str, Any], columns: Tuple[str, ...],
) -> Dict[str, Any]:
    """Return the subset of ``metadata`` whose keys match ``columns`` AND are non-None.

    Callers pass the full envelope; each driver only persists the keys it
    owns.  Unknown keys are silently dropped — the other domain's driver
    will pick them up in its own upsert.

    Critically, keys whose value is ``None`` are **also** dropped — the
    upsert path must not issue ``SET col = NULL`` for absent fields on
    partial updates, or it would clobber previously-populated columns
    on the next write.  A partial update that only carries ``{"title":
    "T2"}`` must leave existing ``description`` / ``keywords`` / … rows
    untouched.  This is the "PATCH, not PUT" semantic the
    :class:`CatalogStore.upsert_catalog_metadata` contract
    assumes (the protocol doc calls it a "write or update").
    """
    return {k: metadata[k] for k in columns if metadata.get(k) is not None}


# ---------------------------------------------------------------------------
# PluginConfig subclasses (identity-only — no fields)
# ---------------------------------------------------------------------------


class CollectionCorePostgresqlDriverConfig(DriverPluginConfig):
    """Identity marker for CollectionCorePostgresqlDriver.

    No fields today.  Future additions (e.g. batching, cache TTLs) layer
    here so the rewriter / waterfall plumbing needs no changes.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "drivers")
    _freeze_at: ClassVar[Optional[str]] = "collection"

    required_engine_class: ClassVar[str] = "postgresql_engine"


class CatalogCorePostgresqlDriverConfig(DriverPluginConfig):
    """Identity marker for CatalogCorePostgresqlDriver."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "drivers")
    _freeze_at: ClassVar[Optional[str]] = "catalog"

    required_engine_class: ClassVar[str] = "postgresql_engine"


# ---------------------------------------------------------------------------
# Collection-tier drivers (per-tenant {schema}.collection_metadata_*)
# ---------------------------------------------------------------------------


class _PgCollectionCoreBase:
    """Shared implementation for the two collection-tier ``CollectionStore`` drivers.

    Domain-specific knobs (table name, column tuple, capability set,
    ``domain`` ClassVar) live on the concrete subclasses; this base
    handles the CRUD bodies that don't vary by domain.

    The class explicitly does NOT inherit ``CollectionStore`` —
    that protocol is a structural ``typing.Protocol`` (`runtime_checkable`)
    and instances of the concrete subclasses satisfy it via duck-typing.
    """

    # Declared on subclasses.
    _table: ClassVar[str]              # e.g. "collection_core"
    _columns: ClassVar[Tuple[str, ...]]  # columns this driver owns
    capabilities: FrozenSet[str]

    async def is_available(self) -> bool:
        return _get_engine() is not None

    async def get_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        engine = db_resource or _get_engine()
        if not engine:
            return None
        async with managed_transaction(engine) as conn:
            phys = await _resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys:
                return None
            coll_phys = await _resolve_collection_physical_id(
                catalog_id, collection_id, db_resource=conn
            )
            if not coll_phys:
                return None
            sql = (
                f'SELECT * FROM "{phys}".{self._table} '
                f'WHERE collection_physical_id = :id;'
            )
            row = await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, id=coll_phys)
            if not row:
                return None
            return _deserialise_jsonb(dict(row), self._columns)

    async def upsert_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Partial-update UPSERT: only columns the caller explicitly set are touched.

        The column list baked into the SQL is derived from the filtered
        payload (``_filter_payload`` drops ``None`` keys).  Columns the
        caller did not supply stay untouched in the DO UPDATE SET —
        which means a caller that only carries ``{"title": "T2"}``
        will not NULL-out ``description`` / ``keywords`` / … on a row
        that previously had them.

        Entirely-empty payloads become a ``{updated_at}``-only bump
        rather than an INSERT-with-only-pk (which would run a NULL
        UPSERT and defeat the purpose of the None filter).  The
        absence of payload columns is still an explicit signal that
        the row existed at least once; treating it as a no-op would
        silently swallow "touch for freshness" semantics that INDEX
        / BACKUP downstream consumers rely on via the ``updated_at``
        token (see role-based driver plan §Freshness contract).
        """
        engine = db_resource or _get_engine()
        if not engine:
            raise RuntimeError("DatabaseProtocol not available")
        payload = _filter_payload(metadata, self._columns)
        async with managed_transaction(engine) as conn:
            phys = await _resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys:
                raise RuntimeError(f"No physical schema for catalog '{catalog_id}'")
            coll_phys = await _resolve_collection_physical_id(
                catalog_id, collection_id, db_resource=conn
            )
            if not coll_phys:
                raise RuntimeError(
                    f"No physical id for collection '{catalog_id}/{collection_id}'"
                )
            await self._upsert_collection_row(
                conn, phys, coll_phys, payload,
            )

    async def _upsert_collection_row(
        self,
        conn: Any,
        phys_schema: str,
        coll_phys_id: str,
        payload: Dict[str, Any],
    ) -> None:
        """Issue the ``{phys_schema}.{_table}`` partial UPSERT.

        Split out from :meth:`upsert_metadata` so subclasses that add
        domain-specific behaviour on top (e.g. a STAC driver that
        also writes a derived extent index) can override it without
        duplicating the engine / schema resolution boilerplate.

        ``coll_phys_id`` is the immutable collection physical id (the
        ``c_…`` token from ``collections.physical_id``), already resolved
        by :meth:`upsert_metadata` before this helper is called.
        """
        supplied_cols: Tuple[str, ...] = tuple(payload.keys())
        params: Dict[str, Any] = {"id": coll_phys_id}
        for c in supplied_cols:
            params[c] = _to_json(payload[c])

        if not supplied_cols:
            # Caller supplied no column values — only bump updated_at.
            # INSERT … ON CONFLICT DO UPDATE SET updated_at = NOW()
            # writes the minimal row (collection_physical_id + default NULLs)
            # on fresh insert, then subsequent upserts just tick the
            # freshness token.
            sql = (
                f'INSERT INTO "{phys_schema}".{self._table} '
                f'(collection_physical_id, updated_at) VALUES (:id, NOW()) '
                f'ON CONFLICT (collection_physical_id) DO UPDATE SET '
                f'updated_at = NOW();'
            )
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn, **params,
            )
            return

        col_list = ", ".join(supplied_cols)
        val_placeholders = ", ".join(f":{c}" for c in supplied_cols)
        update_list = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in supplied_cols
        )
        sql = (
            f'INSERT INTO "{phys_schema}".{self._table} '
            f'(collection_physical_id, {col_list}, updated_at) '
            f'VALUES (:id, {val_placeholders}, NOW()) '
            f'ON CONFLICT (collection_physical_id) DO UPDATE SET '
            f'{update_list}, updated_at = NOW();'
        )
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, **params,
        )

    async def delete_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        engine = db_resource or _get_engine()
        if not engine:
            raise RuntimeError("DatabaseProtocol not available")
        async with managed_transaction(engine) as conn:
            phys = await _resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys:
                return
            coll_phys = await _resolve_collection_physical_id(
                catalog_id, collection_id, db_resource=conn
            )
            if not coll_phys:
                return
            if soft and "extra_metadata" in self._columns:
                # Tombstone via extra_metadata JSON — preserves row for recovery.
                sql = (
                    f'UPDATE "{phys}".{self._table} SET '
                    f"extra_metadata = jsonb_set("
                    f"  COALESCE(extra_metadata, '{{}}'::jsonb), "
                    f"  '{{_deleted_at}}', to_jsonb(now()::text)), "
                    f'updated_at = NOW() '
                    f'WHERE collection_physical_id = :id;'
                )
            else:
                sql = (
                    f'DELETE FROM "{phys}".{self._table} '
                    f'WHERE collection_physical_id = :id;'
                )
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn, id=coll_phys
            )

    async def search_metadata(
        self,
        catalog_id: str,
        *,
        q: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[str] = None,
        filter_cql: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Default implementation: subclasses override when they support search."""
        return [], 0

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """Empty default — these drivers have no per-collection config today."""
        return None

    async def location(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> "StorageLocation":
        from dynastore.modules.storage.storage_location import StorageLocation

        phys = await _resolve_physical_schema(catalog_id) or catalog_id
        return StorageLocation(
            backend="postgresql",
            canonical_uri=f"postgresql://{phys}.{self._table}",
            identifiers={
                "schema": phys, "table": self._table,
            },
            display_label=f"{phys}.{self._table}",
        )

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """No-op: PG metadata tables are created by the per-tenant DDL in
        ``modules.catalog.db_init.metadata_domain_split`` at catalog provisioning
        time, not on a per-driver basis.
        """
        return None

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        """No-op counterpart to :meth:`ensure_storage`.

        The per-tenant schema backing the CORE/STAC collection metadata
        tables is dropped out-of-band when the catalog is deprovisioned;
        individual collection rows are removed via :meth:`delete_metadata`
        and by the routing-driven hard-delete cascade through the
        collection-store index driver — never by a per-driver storage drop
        here.  Declaring it keeps the inner driver structurally conformant
        with the ``CollectionStore`` protocol (which gained ``drop_storage``
        for that cascade).
        """
        return None

    async def get_lifecycle(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
        **kwargs: Any,
    ) -> "CollectionLifecycle":
        """Default stub for domain-slice drivers that do not declare LIFECYCLE.

        Only :class:`CollectionCorePostgresqlDriver` overrides this with a
        real implementation — it owns the ``"{schema}".collections`` registry
        row.  All other subclasses (STAC, custom domain slices) must not
        declare ``EntityStoreCapability.LIFECYCLE`` and should never be
        called through this path.  Raising here surfaces a routing
        misconfiguration loudly rather than returning a silently wrong value.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not declare LIFECYCLE; "
            "only CollectionCorePostgresqlDriver owns the registry row."
        )


class CollectionCorePostgresqlDriver(
    TypedDriver[CollectionCorePostgresqlDriverConfig], _PgCollectionCoreBase,
):
    """Primary driver for CORE collection metadata (``title``, ``description``, …).

    Backs ``{schema}.collection_core``.  Declares ``SEARCH`` because
    CORE fields carry the human-readable text the ``/search?q=…`` endpoint
    matches against.  Active path — :mod:`~dynastore.modules.catalog.
    collection_router` fans out WRITEs across this driver plus
    :class:`CollectionStacPostgresqlDriver` and merges both slices on
    READ.  Each driver filters the unified payload to its own domain's
    columns and no-ops when its filtered slice is empty.
    """

    _table: ClassVar[str] = "collection_core"
    _columns: ClassVar[Tuple[str, ...]] = _COLLECTION_CORE_COLUMNS
    teardown_lane: ClassVar[TeardownLane] = TeardownLane.INLINE_TXN

    # Collection metadata fallback for SEARCH (PG serves the
    # query-fallback path when ES is unavailable / not registered).
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset({Operation.SEARCH})

    capabilities: FrozenSet[str] = frozenset({
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SEARCH,
        EntityStoreCapability.SEARCH_EXACT,
        EntityStoreCapability.SOFT_DELETE,
        EntityStoreCapability.PHYSICAL_ADDRESSING,
        EntityStoreCapability.QUERY_FALLBACK_SOURCE,
        EntityStoreCapability.LIFECYCLE,
    })

    async def get_lifecycle(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
        **kwargs: Any,
    ) -> "CollectionLifecycle":
        """Return the authoritative lifecycle state from the registry row.

        Reads ``deleted_at`` + ``lifecycle_status`` from ``"{schema}".collections``
        — the system-of-record table — bypassing secondary indexes and cached
        model reads.  A transitional overlay (``lifecycle_status``) outranks
        ``deleted_at`` (#2066):

            no row                  -> MISSING
            status='deleting'       -> DELETING       (hard-delete purge in flight)
            status='provisioning'   -> PROVISIONING   (async init in flight)
            deleted_at IS NOT NULL  -> TOMBSTONED     (soft-deleted)
            otherwise               -> ACTIVE

        Schema unresolvable → MISSING (catalog hard-deleted or never created).
        """
        engine = db_resource or _get_engine()
        if not engine:
            return CollectionLifecycle.MISSING
        async with managed_transaction(engine) as conn:
            phys = await _resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys:
                return CollectionLifecycle.MISSING
            row = await DQLQuery(
                f'SELECT deleted_at, lifecycle_status FROM "{phys}".collections '
                "WHERE id = :id;",
                result_handler=ResultHandler.ONE_DICT,
            ).execute(conn, id=collection_id)
            if row is None:
                return CollectionLifecycle.MISSING
            status = row.get("lifecycle_status")
            if status == CollectionLifecycle.DELETING.value:
                return CollectionLifecycle.DELETING
            if status == CollectionLifecycle.PROVISIONING.value:
                return CollectionLifecycle.PROVISIONING
            if row["deleted_at"] is not None:
                return CollectionLifecycle.TOMBSTONED
            return CollectionLifecycle.ACTIVE

    async def search_metadata(
        self,
        catalog_id: str,
        *,
        q: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[str] = None,
        filter_cql: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Keyword search on title/description (CORE columns only).

        Listing visibility: translates the request's collection constraint
        (``RequestVisibility`` → ``resolve_collection_listing_ids``) into
        this driver's own predicate — ``c.id = ANY(...)`` ahead of the
        count and LIMIT/OFFSET — so both the listing page and its total
        reflect only what the caller may see. No constraint (background
        work, or no authorization layer) ⟹ unfiltered.
        """
        from dynastore.models.protocols.visibility import (
            resolve_collection_listing_ids,
        )

        visible_ids = await resolve_collection_listing_ids(catalog_id)
        if visible_ids is not None and not visible_ids:
            return [], 0
        engine = db_resource or _get_engine()
        if not engine:
            return [], 0
        async with managed_transaction(engine) as conn:
            phys = await _resolve_physical_schema(catalog_id, db_resource=conn)
            if not phys:
                return [], 0
            # Hide transient collections (#2194): a non-null ``lifecycle_status``
            # overlay (#2066) means the collection is mid-provisioning or
            # mid-hard-delete — write-gated and not yet (or no longer) a live
            # member of the catalog, so it must not surface in listings/search.
            # Only ACTIVE rows (overlay NULL) are visible. A direct GET-by-id
            # still resolves it so a client can watch its status. Applied to both
            # the COUNT and the data query below, so the page total stays honest.
            where_clauses = ["c.deleted_at IS NULL", "c.lifecycle_status IS NULL"]
            params: Dict[str, Any] = {"limit": limit, "offset": offset}
            if visible_ids is not None:
                where_clauses.append("c.id = ANY(:visible_ids)")
                params["visible_ids"] = list(visible_ids)
            if q:
                where_clauses.append(
                    "(c.id ILIKE :q OR m.title->>'en' ILIKE :q "
                    "OR m.description->>'en' ILIKE :q)"
                )
                params["q"] = f"%{q}%"
            where_sql = " AND ".join(where_clauses)
            count_sql = (
                f'SELECT COUNT(*) FROM "{phys}".collections c '
                f'LEFT JOIN "{phys}".collection_core m '
                f'ON m.collection_physical_id = c.physical_id WHERE {where_sql};'
            )
            total = await DQLQuery(
                count_sql, result_handler=ResultHandler.SCALAR,
            ).execute(conn, **params) or 0
            col_list = ", ".join(f"m.{c}" for c in self._columns)
            data_sql = (
                f'SELECT c.id, {col_list} '
                f'FROM "{phys}".collections c '
                f'LEFT JOIN "{phys}".collection_core m '
                f'ON m.collection_physical_id = c.physical_id '
                f'WHERE {where_sql} '
                f'ORDER BY c.created_at DESC LIMIT :limit OFFSET :offset;'
            )
            rows = await DQLQuery(
                data_sql, result_handler=ResultHandler.ALL_DICTS,
            ).execute(conn, **params)
            return [_deserialise_jsonb(dict(r), self._columns) for r in rows], total


# ---------------------------------------------------------------------------
# Catalog-tier drivers (global catalog.catalog_metadata_*)
# ---------------------------------------------------------------------------


class _PgCatalogCoreBase:
    """Shared implementation for the two catalog-tier ``CatalogStore`` drivers."""

    _table: ClassVar[str]              # e.g. "catalog_core"
    _columns: ClassVar[Tuple[str, ...]]  # columns this driver owns
    capabilities: FrozenSet[str]

    async def is_available(self) -> bool:
        return _get_engine() is not None

    async def get_catalog_metadata(
        self,
        catalog_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        engine = db_resource or _get_engine()
        if not engine:
            return None
        async with managed_transaction(engine) as conn:
            sql = (
                f"SELECT * FROM catalog.{self._table} "
                f"WHERE catalog_id = :id;"
            )
            row = await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, id=catalog_id)
            if not row:
                return None
            return _deserialise_jsonb(dict(row), self._columns)

    async def upsert_catalog_metadata(
        self,
        catalog_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Partial-update UPSERT — only columns the caller set are touched.

        Same "PATCH, not PUT" semantics as the collection-tier driver
        (see :meth:`_PgCollectionCoreBase.upsert_metadata`).
        Absent keys are never stamped as ``NULL``; partial updates
        preserve previously-populated columns.

        Default-fast invariant: when ``metadata`` filters down to zero
        domain-relevant columns, the driver no-ops — it does NOT land
        an empty row or bump ``updated_at`` for a domain that has no
        content.  A STAC-only payload reaching the CORE driver, for
        instance, skips entirely.  Freshness bumps happen when real
        content changes, not on cross-domain noise.
        """
        payload = _filter_payload(metadata, self._columns)
        if not payload:
            return
        engine = db_resource or _get_engine()
        if not engine:
            raise RuntimeError("DatabaseProtocol not available")
        async with managed_transaction(engine) as conn:
            await self._upsert_catalog_row(conn, catalog_id, payload)

    async def _upsert_catalog_row(
        self,
        conn: Any,
        catalog_id: str,
        payload: Dict[str, Any],
    ) -> None:
        supplied_cols: Tuple[str, ...] = tuple(payload.keys())
        params: Dict[str, Any] = {"id": catalog_id}
        for c in supplied_cols:
            params[c] = _to_json(payload[c])

        if not supplied_cols:
            sql = (
                f"INSERT INTO catalog.{self._table} "
                f"(catalog_id, updated_at) VALUES (:id, NOW()) "
                f"ON CONFLICT (catalog_id) DO UPDATE SET "
                f"updated_at = NOW();"
            )
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn, **params,
            )
            return

        col_list = ", ".join(supplied_cols)
        val_placeholders = ", ".join(f":{c}" for c in supplied_cols)
        update_list = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in supplied_cols
        )
        sql = (
            f"INSERT INTO catalog.{self._table} "
            f"(catalog_id, {col_list}, updated_at) "
            f"VALUES (:id, {val_placeholders}, NOW()) "
            f"ON CONFLICT (catalog_id) DO UPDATE SET "
            f"{update_list}, updated_at = NOW();"
        )
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, **params,
        )

    async def delete_catalog_metadata(
        self,
        catalog_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        engine = db_resource or _get_engine()
        if not engine:
            raise RuntimeError("DatabaseProtocol not available")
        async with managed_transaction(engine) as conn:
            if soft and "extra_metadata" in self._columns:
                sql = (
                    f"UPDATE catalog.{self._table} SET "
                    f"extra_metadata = jsonb_set("
                    f"  COALESCE(extra_metadata, '{{}}'::jsonb), "
                    f"  '{{_deleted_at}}', to_jsonb(now()::text)), "
                    f"updated_at = NOW() "
                    f"WHERE catalog_id = :id;"
                )
            else:
                sql = (
                    f"DELETE FROM catalog.{self._table} WHERE catalog_id = :id;"
                )
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn, id=catalog_id
            )

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        return None

    async def ensure_storage(
        self, catalog_id: Optional[str] = None, **kwargs: Any
    ) -> None:
        """No-op: the ``catalog.catalog_core`` / ``catalog.catalog_stac``
        tables are created by the per-tenant provisioning DDL, not per
        driver — same contract as :meth:`_PgCollectionCoreBase.ensure_storage`.
        """
        return None

    async def drop_storage(
        self, catalog_id: str, *, soft: bool = False
    ) -> None:
        """No-op counterpart to :meth:`ensure_storage`.

        Catalog-tier PG metadata lives in the per-tenant schema, which is
        dropped out-of-band (``DROP SCHEMA … CASCADE``) when the catalog is
        deprovisioned; individual rows are removed via
        :meth:`delete_catalog_metadata`.  Declaring it keeps the catalog PG
        drivers structurally conformant with the ``CatalogStore`` lifecycle
        contract.
        """
        return None


class CatalogCorePostgresqlDriver(
    TypedDriver[CatalogCorePostgresqlDriverConfig], _PgCatalogCoreBase,
):
    """Primary driver for CORE catalog metadata.

    Backs ``catalog.catalog_core``.  Scope: ``title``,
    ``description``, ``keywords``, ``license``, ``extra_metadata``.
    """

    _table: ClassVar[str] = "catalog_core"
    _columns: ClassVar[Tuple[str, ...]] = _CATALOG_CORE_COLUMNS
    teardown_lane: ClassVar[TeardownLane] = TeardownLane.INLINE_TXN

    capabilities: FrozenSet[str] = frozenset({
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SOFT_DELETE,
        EntityStoreCapability.QUERY_FALLBACK_SOURCE,
    })


# Catalog-tier metadata persistence is invoked directly by
# ``CatalogService.create_catalog`` through ``catalog_router.
# upsert_catalog_metadata`` — no lifecycle-decorator indirection.  Each
# driver enforces its own default-fast skip inside
# ``upsert_catalog_metadata`` (empty filtered payload → no write), so
# a STAC-only payload reaching the CORE driver is a no-op and
# vice-versa.  See the router in
# :mod:`dynastore.modules.catalog.catalog_router`.
