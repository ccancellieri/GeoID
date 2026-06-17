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
PostgreSQL asset storage driver.

Owns the DDL and all SQL operations for:
- ``{schema}.assets``   — partitioned by ``collection_id``
- ``{schema}.asset_references`` — cascade-delete coordination table

Collection-level metadata is no longer this driver's responsibility.
Callers go through :mod:`dynastore.modules.catalog.collection_router`
which fans out across registered ``CollectionStore`` drivers
(``CollectionPostgresqlDriver`` — the composition wrapper that owns
the collection_core + collection_stac sidecar fan-out — in the default PG
deployment).  The asset driver handles asset-level CRUD only.

Lifecycle
~~~~~~~~~
On every catalog creation the ``_pg_asset_driver_init_tenant`` hook (priority 5)
calls ``ensure_storage()`` to create the tables.  This replaces the DDL that
was previously split between ``catalog_service.py`` and ``asset_service.py``.

Reference guard
~~~~~~~~~~~~~~~
``check_blocking_references()`` queries the partial index
``WHERE cascade_delete = FALSE`` to find assets that cannot be hard-deleted.
``AssetService.delete_assets()`` calls this method before executing DELETE so
the ``AssetsProtocol`` contract is unchanged for all callers.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, FrozenSet, List, Optional

from sqlalchemy import text

from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.models.query_builder import AssetFilter
from dynastore.modules.tools.asset_filters import build_pg_where
from dynastore.modules.storage.driver_config import AssetPostgresqlDriverConfig
from dynastore.modules.storage.hints import Hint
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.db_config.locking_tools import safe_drop_relation
from dynastore.tools.json import CustomJSONEncoder

logger = logging.getLogger(__name__)


class AssetPostgresqlDriver(TypedDriver[AssetPostgresqlDriverConfig]):
    """PostgreSQL implementation of ``AssetStore``.

    Owns all DDL and SQL for asset storage in the tenant schema.
    Registered via ``register_plugin(AssetPostgresqlDriver(engine=...))`` in
    ``CatalogModule.lifespan()``.
    """

    capabilities: FrozenSet[str] = frozenset({
        Capability.READ,
        Capability.WRITE,
        Capability.STREAMING,
        Capability.QUERY_FALLBACK_SOURCE,
        Capability.BULK_COPY,
    })
    preferred_for: FrozenSet[Hint] = frozenset({Hint.DEFAULT, Hint.METADATA})
    supported_hints: FrozenSet[Hint] = frozenset({Hint.METADATA})

    def __init__(self, engine: Optional[DbResource] = None) -> None:
        self.engine = engine

    def is_available(self) -> bool:
        return self.engine is not None

    def location(self, catalog_id: str, collection_id: Optional[str] = None):
        """Return physical addressing for this driver's asset table."""
        from dynastore.modules.storage.storage_location import StorageLocation
        table = "assets"
        schema_hint = f"<schema({catalog_id})>"
        uri = f"postgresql://{schema_hint}.{table}"
        identifiers = {"catalog_id": catalog_id, "table": table}
        if collection_id:
            identifiers["collection_id"] = collection_id
        return StorageLocation(
            backend="postgresql",
            canonical_uri=uri,
            identifiers=identifiers,
            display_label=f"PG assets: {schema_hint}.{table}",
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _resolve_schema(
        self,
        catalog_id: str,
        db_resource: Optional[DbResource] = None,
        *,
        allow_missing: bool = False,
    ) -> Optional[str]:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return None
        conn = db_resource or self.engine
        return await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=allow_missing
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Idempotently create ``assets`` (partitioned) and ``asset_references``.

        Called:
        - During catalog creation via ``_pg_asset_driver_init_tenant`` (priority 5).
        - On-demand when a new driver is activated for an existing catalog.
        """
        db_resource: Optional[DbResource] = kwargs.get("db_resource")
        # Accept pre-resolved schema (e.g. from lifecycle hook where catalog row
        # may not be committed yet, so _resolve_schema would fail).
        schema: Optional[str] = kwargs.get("schema") or await self._resolve_schema(
            catalog_id, db_resource
        )
        if not schema:
            logger.warning(
                "AssetPostgresqlDriver.ensure_storage: cannot resolve schema for catalog=%s",
                catalog_id,
            )
            return

        # New asset shape: discriminated by ``kind`` (physical/virtual) with
        # ``status`` lifecycle; identity columns split into ``filename`` (set
        # for physical) and ``href`` (set for virtual). NULL ``collection_id``
        # represents catalog-tier rows; we provision a DEFAULT partition to
        # land them. Partial unique indexes guard filename per collection scope
        # and href per catalog scope, both ignoring soft-deleted rows.
        #
        # We can't use a classic PRIMARY KEY here because ``collection_id`` is
        # nullable for the catalog tier and PG requires NOT NULL on PK columns.
        # Instead we declare a UNIQUE NULLS NOT DISTINCT index over the same
        # tuple (PG 15+) — semantically a primary identity, while permitting
        # NULL collection_id rows to coexist.
        schema_tag = schema.replace('.', '_')
        assets_ddl = f"""
        CREATE TABLE IF NOT EXISTS "{schema}".assets (
            asset_id      VARCHAR      NOT NULL,
            catalog_id    VARCHAR      NOT NULL,
            collection_id VARCHAR,
            asset_type    VARCHAR      NOT NULL,
            kind          VARCHAR      NOT NULL,
            status        VARCHAR      NOT NULL DEFAULT 'pending',
            filename      VARCHAR,
            href          TEXT,
            uri           TEXT,
            -- content_hash is stored as a tagged scalar "<algo>:<value>"
            -- (e.g. "md5:abc==", "sha256:0a1b..."). Length budget covers
            -- algo prefix + base64-encoded MD5 (~24 chars) and hex-encoded
            -- SHA-256 (64 chars). The CONTENT_HASH probe matches the
            -- tagged form verbatim — payloads MUST submit "<algo>:<raw>".
            content_hash  VARCHAR(96),
            size_bytes    BIGINT,
            metadata      JSONB        DEFAULT '{{}}'::jsonb,
            owned_by      VARCHAR,
            created_at    TIMESTAMPTZ  DEFAULT NOW(),
            updated_at    TIMESTAMPTZ,
            CONSTRAINT assets_kind_check
                CHECK (kind IN ('physical','virtual')),
            CONSTRAINT assets_status_check
                CHECK (status IN ('pending','active','failed','deleted')),
            CONSTRAINT assets_kind_identity_check
                CHECK ((kind = 'physical' AND filename IS NOT NULL)
                    OR (kind = 'virtual'  AND href     IS NOT NULL)),
            CONSTRAINT assets_identity_uq
                UNIQUE NULLS NOT DISTINCT (catalog_id, collection_id, asset_id)
        ) PARTITION BY LIST (collection_id);
        CREATE TABLE IF NOT EXISTS "{schema}".assets_catalog_tier
            PARTITION OF "{schema}".assets
            FOR VALUES IN (NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS assets_uq_filename_{schema_tag}
            ON "{schema}".assets (catalog_id, collection_id, filename)
            WHERE kind = 'physical' AND status <> 'deleted';
        CREATE UNIQUE INDEX IF NOT EXISTS assets_uq_href_{schema_tag}
            ON "{schema}".assets (catalog_id, collection_id, href)
            WHERE kind = 'virtual' AND status <> 'deleted';
        CREATE INDEX IF NOT EXISTS assets_status_idx_{schema_tag}
            ON "{schema}".assets (status);
        CREATE INDEX IF NOT EXISTS assets_pending_idx_{schema_tag}
            ON "{schema}".assets (catalog_id, collection_id, filename)
            WHERE status = 'pending';
        CREATE INDEX IF NOT EXISTS idx_assets_created_at_{schema_tag}
            ON "{schema}".assets (created_at);
        CREATE INDEX IF NOT EXISTS idx_assets_metadata_gin_{schema_tag}
            ON "{schema}".assets USING GIN (metadata jsonb_path_ops);
        """

        refs_ddl = f"""
        CREATE TABLE IF NOT EXISTS "{schema}".asset_references (
            asset_id       VARCHAR     NOT NULL,
            catalog_id     VARCHAR     NOT NULL,
            ref_type       VARCHAR     NOT NULL,
            ref_id         VARCHAR     NOT NULL,
            cascade_delete BOOLEAN     NOT NULL DEFAULT TRUE,
            created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            -- valid_until: NULL means the reference is currently active.
            -- Stamped by soft-delete and NEW_VERSION archive paths so a
            -- stale reference cannot block hard-deletion of a successor
            -- asset that re-uses the same asset_id. Audit trail preserved.
            valid_until    TIMESTAMPTZ DEFAULT NULL,
            PRIMARY KEY (catalog_id, asset_id, ref_type, ref_id)
        );
        -- Active blocking references: only rows that are non-cascade AND
        -- still valid (valid_until IS NULL) actually block hard-delete.
        CREATE INDEX IF NOT EXISTS idx_asset_refs_blocking_{schema.replace('.', '_')}
            ON "{schema}".asset_references (catalog_id, asset_id)
            WHERE cascade_delete = FALSE AND valid_until IS NULL;
        """

        async with managed_transaction(db_resource or self.engine) as conn:
            await DDLQuery(assets_ddl).execute(conn)
            await DDLQuery(refs_ddl).execute(conn)

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Drop the assets partition for ``collection_id``.

        If ``collection_id`` is None, drops ``asset_references`` rows for the
        catalog but does NOT drop the parent ``assets`` table — that lives
        with the schema itself.

        ``soft=True`` is a no-op (assets carry soft-delete via ``status='deleted'``).
        """
        if soft:
            return

        # Idempotent cleanup: the cascade hard-delete runs this AFTER the catalog
        # row (and its per-tenant schema, which owns the ``assets`` table) is
        # already gone, so an absent catalog means there is nothing left to drop.
        # ``allow_missing=True`` makes schema resolution return None in that case
        # instead of raising "Catalog not found" — which otherwise propagates as
        # a RETRY and, after exhausting the per-ref retries, a permanent DEAD
        # cascade failure. Every sibling routing-driven driver already tolerates
        # the deleted catalog; this aligns the asset driver with them.
        schema = await self._resolve_schema(catalog_id, db_resource, allow_missing=True)
        if not schema:
            return

        if collection_id:
            partition_name = f"assets_{catalog_id}_{collection_id}"
            # Hot-table DROP — bound AccessExclusiveLock wait with lock_timeout
            # + retry so concurrent ingest DML can't pile us up into a deadlock.
            _drop_conn = db_resource or self.engine
            if _drop_conn is None:
                return
            await safe_drop_relation(
                _drop_conn,
                schema,
                partition_name,
                kind="table",
            )
        else:
            async with managed_transaction(db_resource or self.engine) as conn:
                # Remove all asset_references for this catalog
                await DQLQuery(
                    f'DELETE FROM "{schema}".asset_references WHERE catalog_id = :catalog_id',
                    result_handler=ResultHandler.ROWCOUNT,
                ).execute(conn, catalog_id=catalog_id)

    # ------------------------------------------------------------------
    # Asset CRUD
    # ------------------------------------------------------------------

    async def index_asset(
        self,
        catalog_id: str,
        asset_doc: Dict[str, Any],
        *,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Upsert a single asset document.

        Existing call sites (Stage 2) pass ``kind="physical"`` /
        ``status="active"`` defaults so ingestion paths keep working until
        Stage 4 wires the policy-gated PENDING insert.
        """
        from dynastore.modules.db_config.partition_tools import (
            ensure_partition_exists as ensure_partition_tool,
        )

        collection_id = asset_doc.get("collection_id")
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            raise ValueError(
                f"AssetPostgresqlDriver.index_asset: catalog '{catalog_id}' not found."
            )

        async with managed_transaction(db_resource or self.engine) as conn:
            # NULL collection_id rows land in the default partition created in
            # ensure_storage; only non-NULL values need a per-value partition.
            if collection_id is not None:
                await ensure_partition_tool(
                    conn,
                    table_name="assets",
                    strategy="LIST",
                    partition_value=collection_id,
                    schema=schema,
                    parent_table_name="assets",
                    parent_table_schema=schema,
                )

            now = datetime.now(timezone.utc)
            kind_val = asset_doc.get("kind") or "physical"
            status_val = asset_doc.get("status") or "active"
            # ``ON CONFLICT ON CONSTRAINT assets_identity_uq`` resolves the
            # upsert against the NULLS-NOT-DISTINCT unique constraint, so
            # catalog-tier rows (collection_id IS NULL) participate in the
            # upsert just like collection-scoped rows.
            sql = text(f"""
                INSERT INTO "{schema}".assets
                    (asset_id, catalog_id, collection_id, asset_type, kind, status,
                     filename, href, uri, content_hash, size_bytes,
                     created_at, updated_at, metadata, owned_by)
                VALUES
                    (:asset_id, :catalog_id, :collection_id, :asset_type, :kind, :status,
                     :filename, :href, :uri, :content_hash, :size_bytes,
                     :created_at, :updated_at, :metadata, :owned_by)
                ON CONFLICT ON CONSTRAINT assets_identity_uq DO UPDATE SET
                    asset_type   = EXCLUDED.asset_type,
                    kind         = EXCLUDED.kind,
                    status       = EXCLUDED.status,
                    filename     = EXCLUDED.filename,
                    href         = EXCLUDED.href,
                    uri          = EXCLUDED.uri,
                    content_hash = EXCLUDED.content_hash,
                    size_bytes   = EXCLUDED.size_bytes,
                    metadata     = EXCLUDED.metadata,
                    owned_by     = EXCLUDED.owned_by,
                    updated_at   = EXCLUDED.updated_at
            """)
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                asset_id=asset_doc["asset_id"],
                catalog_id=catalog_id,
                collection_id=collection_id,
                asset_type=asset_doc.get("asset_type", "ASSET"),
                kind=kind_val,
                status=status_val,
                filename=asset_doc.get("filename"),
                href=asset_doc.get("href"),
                uri=asset_doc.get("uri"),
                content_hash=asset_doc.get("content_hash"),
                size_bytes=asset_doc.get("size_bytes"),
                created_at=asset_doc.get("created_at") or now,
                updated_at=asset_doc.get("updated_at") or now,
                metadata=json.dumps(
                    asset_doc.get("metadata", {}), cls=CustomJSONEncoder
                ),
                owned_by=asset_doc.get("owned_by"),
            )

    async def delete_asset(
        self,
        catalog_id: str,
        asset_id: str,
        *,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Hard-delete a single asset document by ID."""
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return

        sql = text(f"""
            DELETE FROM "{schema}".assets
            WHERE asset_id = :asset_id
              AND catalog_id = :catalog_id
              AND collection_id IS NOT DISTINCT FROM :collection_id
        """)
        async with managed_transaction(db_resource or self.engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                asset_id=asset_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )

    async def get_asset(
        self,
        catalog_id: str,
        asset_id: str,
        *,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return a single non-deleted asset document as a dict, or None.

        Direct-get visibility contract: an asset the caller has no visibility
        grant for is indistinguishable from a missing one — returns None so
        the HTTP layer renders 404, never 403 or 200-with-data.
        """
        from dynastore.models.protocols.visibility import resolve_asset_listing_ids

        # Enforce the direct-get visibility contract: resolve the visible-id
        # set for this caller. None = IAM off (unfiltered). A non-None set that
        # does not contain asset_id means this asset is hidden for this caller;
        # return None so the caller cannot distinguish it from a genuine miss.
        visible_ids = await resolve_asset_listing_ids(catalog_id, collection_id)
        if visible_ids is not None and asset_id not in visible_ids:
            return None

        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return None

        # ``collection_id`` is None means: caller wants the catalog-tier row
        # (NULL collection_id). Use IS NOT DISTINCT FROM so a NULL parameter
        # matches a NULL column. Without scoping at all, the search route is
        # used; this method is the single-row lookup so the parameter value
        # (None or a string) drives the scope.
        sql = f"""
            SELECT asset_id, catalog_id, collection_id, asset_type, kind, status,
                   filename, href, uri, content_hash, size_bytes,
                   created_at, updated_at, metadata, owned_by
            FROM "{schema}".assets
            WHERE asset_id = :asset_id
              AND catalog_id = :catalog_id
              AND collection_id IS NOT DISTINCT FROM :collection_id
              AND status <> 'deleted'
            LIMIT 1
        """
        params: Dict[str, Any] = {
            "asset_id": asset_id,
            "catalog_id": catalog_id,
            "collection_id": collection_id,
        }

        async with managed_transaction(db_resource or self.engine) as conn:
            return await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, **params)

    async def search_assets(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        filters: Optional[List[AssetFilter]] = None,
        limit: int = 100,
        offset: int = 0,
        all_collections: bool = False,
        db_resource: Optional[DbResource] = None,
    ) -> List[Dict[str, Any]]:
        """Return asset dicts matching the filters.

        ``filters`` is an optional list of :class:`AssetFilter`. The supported
        operator set and predicate translation live in
        :func:`dynastore.modules.tools.asset_filters.build_pg_where`. JSONB paths
        use dot notation (``metadata.provider``, ``metadata.sensor.name``);
        ``eq`` filters on those paths fold into a single GIN-indexable ``@>``
        containment predicate, while comparison/text operators use the ``#>>``
        accessor.

        Collection scope: ``collection_id IS NOT DISTINCT FROM :collection_id``
        matches a single collection (or the catalog tier when ``None``).
        When ``all_collections`` is True the predicate is dropped so the
        search spans every collection plus the catalog tier under the catalog.

        Asset listing visibility: when the request published a caller snapshot,
        the listing is transparently narrowed to the asset ids that caller may
        see — applied before pagination, fail-closed when the visible set is
        empty. Background/CLI work (no snapshot) lists unfiltered.
        """
        from dynastore.models.protocols.visibility import resolve_asset_listing_ids

        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return []

        # Resolve the asset visibility constraint for this caller.
        # ``None`` means no authorization layer is active — list unfiltered.
        # An empty frozenset means the caller may see no assets — short-circuit.
        # When ``all_collections`` is True the constraint is per-catalog (no
        # collection pin) so we pass collection_id=None in that case.
        vis_collection_id = None if all_collections else collection_id
        visible_ids = await resolve_asset_listing_ids(catalog_id, vis_collection_id)
        if visible_ids is not None and not visible_ids:
            return []

        where_parts = [
            "catalog_id = :catalog_id",
            "status <> 'deleted'",
        ]
        params: Dict[str, Any] = {
            "catalog_id": catalog_id,
            "limit": limit,
            "offset": offset,
        }
        if not all_collections:
            where_parts.append("collection_id IS NOT DISTINCT FROM :collection_id")
            params["collection_id"] = collection_id

        if visible_ids is not None:
            where_parts.append("asset_id = ANY(:visible_asset_ids)")
            params["visible_asset_ids"] = list(visible_ids)

        if filters:
            filter_parts, filter_params = build_pg_where(filters)
            where_parts.extend(filter_parts)
            params.update(filter_params)

        sql = (
            f'SELECT asset_id, catalog_id, collection_id, asset_type, kind, status, '
            f'filename, href, uri, content_hash, size_bytes, '
            f'created_at, updated_at, metadata, owned_by '
            f'FROM "{schema}".assets '
            f'WHERE {" AND ".join(where_parts)} '
            f'ORDER BY created_at DESC LIMIT :limit OFFSET :offset'
        )

        async with managed_transaction(db_resource or self.engine) as conn:
            rows = await DQLQuery(
                sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, **params)
            return rows or []

    # ------------------------------------------------------------------
    # Reference guard (PG-only coordination mechanism)
    # ------------------------------------------------------------------

    async def check_blocking_references(
        self,
        asset_ids: List[str],
        catalog_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> List[Any]:
        """Return cascade_delete=False references for the given asset IDs.

        Uses the partial index on
        ``(catalog_id, asset_id) WHERE cascade_delete=FALSE AND valid_until IS NULL``
        for O(1) per-asset lookup. Invalidated references (``valid_until``
        stamped by a prior soft-delete or NEW_VERSION archive) do NOT count
        as blocking — the asset they pointed at is gone, so the new asset
        re-using the same id is free to be hard-deleted.

        Called by ``AssetService.delete_assets()`` before hard-deleting owned
        assets.  Returns a list of ``AssetReference``-compatible dicts.
        """
        if not asset_ids:
            return []

        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return []

        placeholders = ", ".join(f":aid_{i}" for i in range(len(asset_ids)))
        params: Dict[str, Any] = {"catalog_id": catalog_id}
        for i, aid in enumerate(asset_ids):
            params[f"aid_{i}"] = aid

        sql = text(f"""
            SELECT asset_id, catalog_id, ref_type, ref_id, cascade_delete, created_at
            FROM "{schema}".asset_references
            WHERE catalog_id = :catalog_id
              AND asset_id IN ({placeholders})
              AND cascade_delete = FALSE
              AND valid_until IS NULL
            ORDER BY asset_id, created_at ASC
        """)

        async with managed_transaction(db_resource or self.engine) as conn:
            rows = await DQLQuery(
                sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, **params)
            return rows or []

    async def delete_assets_bulk(
        self,
        catalog_id: str,
        *,
        asset_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        hard: bool = False,
        db_resource: Optional[DbResource] = None,
    ) -> tuple:
        """Bulk delete/soft-delete assets matching the given criteria.

        Handles candidate lookup, reference guard (hard-delete only), and
        the actual DELETE/UPDATE in a single transaction.

        Returns:
            ``(rowcount, matched_rows)`` where ``matched_rows`` is a list of
            dicts with ``asset_id``, ``catalog_id``, ``collection_id``,
            ``owned_by``, ``uri`` for each matched asset. ``uri`` lets
            hard-delete subscribers reap the backing object (e.g. the GCS
            blob) after the row is gone.
        """
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return 0, []

        now = datetime.now(timezone.utc)

        # Build WHERE.
        # - Always filter by catalog_id.
        # - When asset_id is given, also pin to a single (collection_id) scope
        #   using IS NOT DISTINCT FROM so NULL (catalog-tier) matches NULL.
        # - When asset_id is absent and collection_id is provided, filter by
        #   that collection only; if both are absent, the operation spans the
        #   whole catalog.
        where_clauses = ["catalog_id = :cat"]
        params: Dict[str, Any] = {"cat": catalog_id, "now": now}
        if asset_id:
            where_clauses.append("asset_id = :aid")
            params["aid"] = asset_id
            where_clauses.append("collection_id IS NOT DISTINCT FROM :coll")
            params["coll"] = collection_id
        elif collection_id is not None:
            where_clauses.append("collection_id IS NOT DISTINCT FROM :coll")
            params["coll"] = collection_id

        where_stmt = " AND ".join(where_clauses)

        async with managed_transaction(db_resource or self.engine) as conn:
            fetch_sql = text(
                f'SELECT asset_id, catalog_id, collection_id, owned_by, uri '
                f'FROM "{schema}".assets WHERE {where_stmt}'
            )
            asset_rows = await DQLQuery(
                fetch_sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, **params)

            if not asset_rows:
                return 0, []

            # Reference guard (hard-delete only)
            if hard:
                owned_ids = [a["asset_id"] for a in asset_rows if a.get("owned_by")]
                if owned_ids:
                    blocking = await self.check_blocking_references(
                        owned_ids, catalog_id, db_resource=conn,
                    )
                    if blocking:
                        # Return blocking rows so caller can raise the appropriate error
                        return -1, blocking

            prefix = (
                f'DELETE FROM "{schema}".assets'
                if hard
                else f"UPDATE \"{schema}\".assets SET status = 'deleted', updated_at = :now"
            )
            final_sql = text(f"{prefix} WHERE {where_stmt}")
            rowcount = await DQLQuery(
                final_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(conn, **params)

            # Stamp any active asset_references for the soft-deleted assets
            # so future hard-deletes of a successor (re-using the same
            # asset_id via NEW_VERSION) don't get blocked by stale rows.
            # On hard-delete, the rows are already gone; we don't bother
            # cleaning up references — the partial unique index guards the
            # next insert anyway, and the audit trail is preserved.
            if not hard and asset_rows:
                deleted_ids = [a["asset_id"] for a in asset_rows]
                ref_placeholders = ", ".join(
                    f":raid_{i}" for i in range(len(deleted_ids))
                )
                ref_params: Dict[str, Any] = {"cat": catalog_id, "now": now}
                for i, aid in enumerate(deleted_ids):
                    ref_params[f"raid_{i}"] = aid
                ref_sql = text(
                    f'UPDATE "{schema}".asset_references '
                    f"SET valid_until = :now "
                    f"WHERE catalog_id = :cat "
                    f"AND asset_id IN ({ref_placeholders}) "
                    f"AND valid_until IS NULL"
                )
                await DQLQuery(
                    ref_sql, result_handler=ResultHandler.ROWCOUNT
                ).execute(conn, **ref_params)

        return rowcount, asset_rows

    async def add_asset_reference(
        self,
        asset_id: str,
        catalog_id: str,
        ref_type: Any,
        ref_id: str,
        cascade_delete: bool = True,
        db_resource: Optional[DbResource] = None,
    ) -> Dict[str, Any]:
        """Insert or update an asset reference row."""
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            raise ValueError(
                f"AssetPostgresqlDriver.add_asset_reference: catalog '{catalog_id}' not found."
            )

        now = datetime.now(timezone.utc)
        ref_type_val = ref_type.value if hasattr(ref_type, "value") else str(ref_type)
        sql = text(f"""
            INSERT INTO "{schema}".asset_references
                (asset_id, catalog_id, ref_type, ref_id, cascade_delete, created_at)
            VALUES (:asset_id, :catalog_id, :ref_type, :ref_id, :cascade_delete, :created_at)
            ON CONFLICT (catalog_id, asset_id, ref_type, ref_id) DO UPDATE SET
                cascade_delete = EXCLUDED.cascade_delete,
                valid_until    = NULL
            RETURNING asset_id, catalog_id, ref_type, ref_id, cascade_delete, created_at
        """)
        async with managed_transaction(db_resource or self.engine) as conn:
            row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
                conn,
                asset_id=asset_id,
                catalog_id=catalog_id,
                ref_type=ref_type_val,
                ref_id=ref_id,
                cascade_delete=cascade_delete,
                created_at=now,
            )
        return row

    async def remove_asset_reference(
        self,
        asset_id: str,
        catalog_id: str,
        ref_type: Any,
        ref_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Delete an asset reference row."""
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return

        ref_type_val = ref_type.value if hasattr(ref_type, "value") else str(ref_type)
        sql = text(f"""
            DELETE FROM "{schema}".asset_references
            WHERE catalog_id = :catalog_id
              AND asset_id   = :asset_id
              AND ref_type   = :ref_type
              AND ref_id     = :ref_id
        """)
        async with managed_transaction(db_resource or self.engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                catalog_id=catalog_id,
                asset_id=asset_id,
                ref_type=ref_type_val,
                ref_id=ref_id,
            )

    async def list_asset_references(
        self,
        asset_id: str,
        catalog_id: str,
        db_resource: Optional[DbResource] = None,
        *,
        include_invalidated: bool = False,
    ) -> List[Dict[str, Any]]:
        """Return references for an asset.

        By default returns only currently-valid rows (``valid_until IS NULL``).
        Pass ``include_invalidated=True`` to also surface rows stamped by a
        prior soft-delete or NEW_VERSION archive — useful for audit /
        forensics views.
        """
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return []

        valid_clause = (
            "" if include_invalidated else " AND valid_until IS NULL"
        )
        sql = f"""
            SELECT asset_id, catalog_id, ref_type, ref_id, cascade_delete, created_at, valid_until
            FROM "{schema}".asset_references
            WHERE catalog_id = :catalog_id AND asset_id = :asset_id{valid_clause}
            ORDER BY created_at ASC
        """
        async with managed_transaction(db_resource or self.engine) as conn:
            rows = await DQLQuery(
                sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, catalog_id=catalog_id, asset_id=asset_id)
            return rows or []

    async def list_assets_for_reference(
        self,
        catalog_id: str,
        ref_type: Any,
        ref_id: str,
        db_resource: Optional[DbResource] = None,
        *,
        include_invalidated: bool = False,
    ) -> List[str]:
        """Return asset IDs that carry a given reference (inverse lookup).

        By default returns only currently-valid rows (``valid_until IS NULL``).
        Pass ``include_invalidated=True`` to also surface rows stamped by a
        prior soft-delete — useful for audit / forensics views.
        """
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return []

        ref_type_val = ref_type.value if hasattr(ref_type, "value") else str(ref_type)
        valid_clause = (
            "" if include_invalidated else " AND valid_until IS NULL"
        )
        sql = f"""
            SELECT asset_id
            FROM "{schema}".asset_references
            WHERE catalog_id = :catalog_id
              AND ref_type = :ref_type
              AND ref_id = :ref_id{valid_clause}
        """
        async with managed_transaction(db_resource or self.engine) as conn:
            rows = await DQLQuery(
                sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, catalog_id=catalog_id, ref_type=ref_type_val, ref_id=ref_id)
            return [r["asset_id"] for r in (rows or [])]

    # Collection-metadata CRUD has moved to the
    # CollectionStore protocol +
    # :mod:`dynastore.modules.catalog.collection_router`.  The
    # asset driver no longer owns collection metadata — callers invoke
    # the router, which delegates to the registered
    # CollectionStore implementers (the PG-tier wrapper
    # CollectionPostgresqlDriver in the default deployment).


# ==============================================================================
# Lifecycle registration
# ==============================================================================

from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry  # noqa: E402
from dynastore.models.driver_context import DriverContext


@lifecycle_registry.sync_catalog_initializer(priority=5)
async def _pg_asset_driver_init_tenant(
    conn: DbResource, schema: str, catalog_id: str
) -> None:
    """Create ``assets`` and ``asset_references`` during catalog creation.

    Priority 5 runs before module-specific hooks (stats, tiles at priority 50).
    This is the sole DDL path for asset tables — ``TENANT_ASSETS_DDL`` has been
    removed from ``catalog_service.py``.
    """
    driver = AssetPostgresqlDriver()
    driver.engine = conn  # use the in-transaction connection directly
    await driver.ensure_storage(catalog_id, db_resource=conn, schema=schema)
