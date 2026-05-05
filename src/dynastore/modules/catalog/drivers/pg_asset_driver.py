#    Copyright 2025 FAO
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

from dynastore.models.protocols.asset_driver import AssetStore
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.typed_driver import TypedDriver
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

_CATALOG_LEVEL_COLLECTION_ID = "_catalog_"


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
        self, catalog_id: str, db_resource: Optional[DbResource] = None
    ) -> Optional[str]:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return None
        conn = db_resource or self.engine
        return await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn))

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

        assets_ddl = f"""
        CREATE TABLE IF NOT EXISTS "{schema}".assets (
            asset_id      VARCHAR      NOT NULL,
            catalog_id    VARCHAR      NOT NULL,
            collection_id VARCHAR      NOT NULL DEFAULT '_catalog_',
            asset_type    VARCHAR      NOT NULL,
            uri           TEXT         NOT NULL,
            created_at    TIMESTAMPTZ  DEFAULT NOW(),
            deleted_at    TIMESTAMPTZ  DEFAULT NULL,
            metadata      JSONB        DEFAULT '{{}}'::jsonb,
            owned_by      VARCHAR      DEFAULT NULL,
            PRIMARY KEY (collection_id, asset_id)
        ) PARTITION BY LIST (collection_id);
        CREATE INDEX IF NOT EXISTS idx_assets_created_at_{schema.replace('.', '_')}
            ON "{schema}".assets (created_at);
        """

        refs_ddl = f"""
        CREATE TABLE IF NOT EXISTS "{schema}".asset_references (
            asset_id       VARCHAR     NOT NULL,
            catalog_id     VARCHAR     NOT NULL,
            ref_type       VARCHAR     NOT NULL,
            ref_id         VARCHAR     NOT NULL,
            cascade_delete BOOLEAN     NOT NULL DEFAULT TRUE,
            created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_id, asset_id, ref_type, ref_id)
        );
        CREATE INDEX IF NOT EXISTS idx_asset_refs_blocking_{schema.replace('.', '_')}
            ON "{schema}".asset_references (catalog_id, asset_id)
            WHERE cascade_delete = FALSE;
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

        ``soft=True`` is a no-op (assets have soft-delete via ``deleted_at``).
        """
        if soft:
            return

        schema = await self._resolve_schema(catalog_id, db_resource)
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
        """Upsert a single asset document (dict representation of ``AssetBase``)."""
        from dynastore.modules.db_config.partition_tools import (
            ensure_partition_exists as ensure_partition_tool,
        )

        collection_id = asset_doc.get("collection_id") or _CATALOG_LEVEL_COLLECTION_ID
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            raise ValueError(
                f"AssetPostgresqlDriver.index_asset: catalog '{catalog_id}' not found."
            )

        async with managed_transaction(db_resource or self.engine) as conn:
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
            sql = text(f"""
                INSERT INTO "{schema}".assets
                    (asset_id, catalog_id, collection_id, asset_type, uri,
                     created_at, metadata, owned_by)
                VALUES
                    (:asset_id, :catalog_id, :collection_id, :asset_type, :uri,
                     :created_at, :metadata, :owned_by)
                ON CONFLICT (collection_id, asset_id) DO UPDATE SET
                    uri        = EXCLUDED.uri,
                    metadata   = EXCLUDED.metadata,
                    owned_by   = EXCLUDED.owned_by,
                    deleted_at = NULL
            """)
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                asset_id=asset_doc["asset_id"],
                catalog_id=catalog_id,
                collection_id=collection_id,
                asset_type=asset_doc.get("asset_type", "ASSET"),
                uri=asset_doc["uri"],
                created_at=asset_doc.get("created_at") or now,
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

        target_col = collection_id or _CATALOG_LEVEL_COLLECTION_ID
        sql = text(f"""
            DELETE FROM "{schema}".assets
            WHERE asset_id = :asset_id
              AND catalog_id = :catalog_id
              AND collection_id = :collection_id
        """)
        async with managed_transaction(db_resource or self.engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                asset_id=asset_id,
                catalog_id=catalog_id,
                collection_id=target_col,
            )

    async def get_asset(
        self,
        catalog_id: str,
        asset_id: str,
        *,
        collection_id: Optional[str] = None,
        db_resource: Optional[DbResource] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return a single non-deleted asset document as a dict, or None."""
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return None

        # Without collection_id, search across all partitions via the parent table.
        if collection_id:
            sql = f"""
                SELECT asset_id, catalog_id, collection_id, asset_type, uri,
                       created_at, deleted_at, metadata, owned_by
                FROM "{schema}".assets
                WHERE asset_id = :asset_id
                  AND catalog_id = :catalog_id
                  AND collection_id = :collection_id
                  AND deleted_at IS NULL
                LIMIT 1
            """
            params: Dict[str, Any] = {
                "asset_id": asset_id,
                "catalog_id": catalog_id,
                "collection_id": collection_id,
            }
        else:
            sql = f"""
                SELECT asset_id, catalog_id, collection_id, asset_type, uri,
                       created_at, deleted_at, metadata, owned_by
                FROM "{schema}".assets
                WHERE asset_id = :asset_id
                  AND catalog_id = :catalog_id
                  AND deleted_at IS NULL
                LIMIT 1
            """
            params = {"asset_id": asset_id, "catalog_id": catalog_id}

        async with managed_transaction(db_resource or self.engine) as conn:
            return await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, **params)

    async def search_assets(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        query: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[DbResource] = None,
    ) -> List[Dict[str, Any]]:
        """Return asset dicts matching the query.

        ``query`` is an optional dict of ``{field: value}`` equality filters.
        JSONB paths are supported via dot notation: ``{"metadata.provider": "ESA"}``.
        """
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return []

        target_col = collection_id or _CATALOG_LEVEL_COLLECTION_ID
        where_parts = [
            "catalog_id = :catalog_id",
            "collection_id = :collection_id",
            "deleted_at IS NULL",
        ]
        params: Dict[str, Any] = {
            "catalog_id": catalog_id,
            "collection_id": target_col,
            "limit": limit,
            "offset": offset,
        }

        if query:
            for i, (field, value) in enumerate(query.items()):
                key = f"qval_{i}"
                if field.startswith("metadata."):
                    parts = field.split(".")
                    path = "->".join(f"'{p}'" for p in parts[1:-1])
                    leaf = f"->>'{parts[-1]}'"
                    expr = f"metadata{'->' + path if path else ''}{leaf}"
                else:
                    from dynastore.tools.db import validate_sql_identifier
                    validate_sql_identifier(field)
                    expr = f'"{field}"'
                where_parts.append(f"{expr} = :{key}")
                params[key] = value

        sql = (
            f'SELECT asset_id, catalog_id, collection_id, asset_type, uri, '
            f'created_at, deleted_at, metadata, owned_by '
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

        Uses the partial index on ``(catalog_id, asset_id) WHERE
        cascade_delete = FALSE`` for O(1) per-asset lookup.

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
            ``owned_by`` for each matched asset.
        """
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return 0, []

        now = datetime.now(timezone.utc)

        where_clauses = ["catalog_id = :cat"]
        params: Dict[str, Any] = {"cat": catalog_id, "now": now}
        if asset_id:
            where_clauses.append("asset_id = :aid")
            params["aid"] = asset_id
        if collection_id:
            where_clauses.append("collection_id = :coll")
            params["coll"] = collection_id
        elif asset_id:
            where_clauses.append("collection_id = :coll")
            params["coll"] = _CATALOG_LEVEL_COLLECTION_ID

        where_stmt = " AND ".join(where_clauses)

        async with managed_transaction(db_resource or self.engine) as conn:
            fetch_sql = text(
                f'SELECT asset_id, catalog_id, collection_id, owned_by '
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
                else f'UPDATE "{schema}".assets SET deleted_at = :now'
            )
            final_sql = text(f"{prefix} WHERE {where_stmt}")
            rowcount = await DQLQuery(
                final_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(conn, **params)

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
                cascade_delete = EXCLUDED.cascade_delete
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
    ) -> List[Dict[str, Any]]:
        """Return all active references for an asset."""
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return []

        sql = f"""
            SELECT asset_id, catalog_id, ref_type, ref_id, cascade_delete, created_at
            FROM "{schema}".asset_references
            WHERE catalog_id = :catalog_id AND asset_id = :asset_id
            ORDER BY created_at ASC
        """
        async with managed_transaction(db_resource or self.engine) as conn:
            rows = await DQLQuery(
                sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, catalog_id=catalog_id, asset_id=asset_id)
            return rows or []

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
