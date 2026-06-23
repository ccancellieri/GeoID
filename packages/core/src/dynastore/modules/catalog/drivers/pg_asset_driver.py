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
- ``{schema}.assets``   — partitioned by ``collection_physical_id``
- ``{schema}.asset_references`` — cascade-delete coordination table

Partition key
~~~~~~~~~~~~~
The ``assets`` table is partitioned by ``collection_physical_id`` — the
immutable physical id (``c_…`` token) stored in
``{schema}.collections.physical_id``.  This is *distinct* from:

* ``asset_physical_id`` — the asset's own UUIDv7 (column already present).

``catalog_id`` and ``collection_id`` are NOT stored in the ``assets`` table —
they are mutable labels that go stale on a rename.  The schema already
scopes rows to one catalog; logical ids are injected at read time from
method parameters or via ``CatalogsProtocol.resolve_logical_id``.

A collection rename only updates ``{schema}.collections.id``; it touches
zero rows in ``assets`` because no key or index references the logical id.

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
``WHERE cascade_delete = FALSE AND valid_until IS NULL`` to find assets that
cannot be hard-deleted.  ``asset_references`` is keyed on the immutable
``asset_physical_id`` (UUIDv7) so a rename (``assets.asset_id`` label change)
needs zero propagation in the references table.  ``AssetService.delete_assets()``
calls this method before executing DELETE so the ``AssetsProtocol`` contract
is unchanged for all callers.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional

from sqlalchemy import text

from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.protocols.teardown_lane import TeardownLane
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


def _get_catalogs():
    """Single accessor for the ``CatalogsProtocol`` implementer (or ``None``).

    Centralizes the discovery import + lookup so every physical/logical id
    resolution site in this driver stays a one-liner instead of repeating the
    same four-line boilerplate.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.catalogs import CatalogsProtocol

    return get_protocol(CatalogsProtocol)


class AssetPostgresqlDriver(TypedDriver[AssetPostgresqlDriverConfig]):
    """PostgreSQL implementation of ``AssetStore``.

    Owns all DDL and SQL for asset storage in the tenant schema.
    Registered via ``register_plugin(AssetPostgresqlDriver(engine=...))`` in
    ``CatalogModule.lifespan()``.
    """

    # Asset rows are deleted inline inside the delete transaction; the async
    # cascade must not re-drop them.
    teardown_lane: ClassVar[TeardownLane] = TeardownLane.INLINE_TXN

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
        catalogs = _get_catalogs()
        if not catalogs:
            return None
        conn = db_resource or self.engine
        return await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=allow_missing
        )

    async def _resolve_collection_physical_id(
        self,
        catalog_id: str,
        collection_id: Optional[str],
        db_resource: Optional[DbResource] = None,
    ) -> Optional[str]:
        """Return the immutable physical id for the given collection.

        Returns ``None`` when ``collection_id`` is ``None`` (catalog-tier
        rows) or when the collection cannot be found.  Callers treat a ``None``
        result the same as a catalog-tier NULL partition key.
        """
        if collection_id is None:
            return None
        catalogs = _get_catalogs()
        if not catalogs:
            return None
        conn = db_resource or self.engine
        return await catalogs.resolve_physical_id(
            catalog_id,
            collection_id,
            ctx=DriverContext(db_resource=conn),
            allow_missing=True,
        )

    async def _attach_logical_ids(
        self,
        rows: Any,
        catalog_id: str,
        collection_id: Optional[str],
        *,
        all_collections: bool = False,
    ) -> Any:
        """Inject ``catalog_id`` and ``collection_id`` into row dict(s).

        ``catalog_id`` always comes from the method parameter (the schema
        already scopes the catalog; storing it in the row was redundant).

        ``collection_id`` is either the method parameter (single-collection
        reads — a plain assignment, no lookup) or reverse-resolved per-row via
        ``CatalogsProtocol.resolve_logical_id`` when the read spans multiple
        collections (``all_collections=True``).  The reverse path uses the
        central cached resolver (no connection): asset listings read committed
        state, so the TTL cache is correct and avoids an O(N) per-row DB
        round-trip — distinct collections per page are few and hot in cache.

        Accepts a single dict, a list of dicts, or None and returns the same
        shape with the two keys set in-place.
        """
        if rows is None:
            return rows

        single = isinstance(rows, dict)
        items: list = [rows] if single else list(rows)

        if not all_collections:
            for row in items:
                row["catalog_id"] = catalog_id
                row["collection_id"] = collection_id
            return rows if single else items

        catalogs = _get_catalogs()
        for row in items:
            row["catalog_id"] = catalog_id
            coll_phys = row.get("collection_physical_id")
            if coll_phys is None or catalogs is None:
                row["collection_id"] = None
            else:
                row["collection_id"] = await catalogs.resolve_logical_id(
                    catalog_id, coll_phys
                )

        return rows if single else items

    async def _ref_id_to_physical(
        self, catalog_id: str, ref_type_val: str, ref_id: str, conn: Any
    ) -> str:
        """Translate a COLLECTION reference's logical ``ref_id`` to the physical id.

        ``asset_references`` stores the *physical* id of whatever an asset points
        at, so a parent rename never invalidates a reference.  For COLLECTION
        refs the caller-supplied ``ref_id`` is the mutable logical
        ``collection_id``; resolve it to the immutable ``collection_physical_id``
        before any store/match.  All other ref kinds (ITEM=geoid, duckdb:table,
        …) are already physical/stable and pass through untouched.
        """
        from dynastore.models.shared_models import CoreAssetReferenceType

        if ref_type_val != CoreAssetReferenceType.COLLECTION.value:
            return ref_id
        catalogs = _get_catalogs()
        if not catalogs:
            return ref_id
        phys = await catalogs.resolve_physical_id(
            catalog_id, ref_id, ctx=DriverContext(db_resource=conn), allow_missing=True
        )
        if not phys:
            # Collection physical id unresolved (e.g. ref added before the
            # collection row is visible). Fall back to the logical id so the
            # write still lands; log so an ordering bug is observable.
            logger.warning(
                "asset_references: COLLECTION ref_id '%s' has no physical id in "
                "catalog '%s'; storing logical id (rename-stale).",
                ref_id, catalog_id,
            )
            return ref_id
        return phys

    async def _ref_id_to_logical(
        self, catalog_id: str, ref_type_val: str, ref_id: str, conn: Any
    ) -> str:
        """Reverse of :meth:`_ref_id_to_physical` for output projection.

        COLLECTION refs are stored as ``collection_physical_id``; re-attach the
        current logical ``collection_id`` so ``AssetReference.ref_id`` shows the
        user-facing value, never the internal physical token.  Falls back to the
        stored value when the collection is gone (audit views on a deleted
        collection).
        """
        from dynastore.models.shared_models import CoreAssetReferenceType

        if ref_type_val != CoreAssetReferenceType.COLLECTION.value:
            return ref_id
        catalogs = _get_catalogs()
        if not catalogs:
            return ref_id
        logical = await catalogs.resolve_logical_id(
            catalog_id, ref_id, ctx=DriverContext(db_resource=conn)
        )
        return logical or ref_id

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

        # Asset table partitioned by collection_physical_id — the immutable
        # physical id of the owning collection (c_… token).  This decouples
        # partition names from the mutable logical collection_id so a rename
        # touches zero asset rows.
        #
        # Two "id" columns on this table identify the owning scope:
        #   asset_physical_id      — the asset's own UUIDv7 (P2a, existing).
        #   collection_physical_id — the collection's immutable physical id;
        #                            NULL for catalog-tier (no collection) rows.
        #
        # catalog_id and collection_id are NOT stored — they are mutable labels
        # that go stale on a rename.  The schema already scopes every row to
        # one catalog; the logical ids are injected at read time from method
        # parameters or via CatalogsProtocol.resolve_logical_id so callers
        # never see a stale snapshot.
        #
        # Uniqueness and all read filters key on collection_physical_id so a
        # rename touches zero asset rows.
        #
        # UNIQUE NULLS NOT DISTINCT (PG 15+): NULL collection_physical_id rows
        # (catalog tier) participate in the identity constraint so they can be
        # upserted the same way as collection-scoped rows.
        schema_tag = schema.replace('.', '_')
        assets_ddl = f"""
        CREATE TABLE IF NOT EXISTS "{schema}".assets (
            asset_id                VARCHAR      NOT NULL,
            collection_physical_id  VARCHAR,
            asset_type              VARCHAR      NOT NULL,
            kind                    VARCHAR      NOT NULL,
            status                  VARCHAR      NOT NULL DEFAULT 'pending',
            filename                VARCHAR,
            href                    TEXT,
            uri                     TEXT,
            -- content_hash is stored as a tagged scalar "<algo>:<value>"
            -- (e.g. "md5:abc==", "sha256:0a1b..."). Length budget covers
            -- algo prefix + base64-encoded MD5 (~24 chars) and hex-encoded
            -- SHA-256 (64 chars). The CONTENT_HASH probe matches the
            -- tagged form verbatim — payloads MUST submit "<algo>:<raw>".
            content_hash            VARCHAR(96),
            size_bytes              BIGINT,
            metadata                JSONB        DEFAULT '{{}}'::jsonb,
            owned_by                VARCHAR,
            created_at              TIMESTAMPTZ  DEFAULT NOW(),
            updated_at              TIMESTAMPTZ,
            -- asset_physical_id (#2296): immutable UUIDv7 minted once at
            -- asset creation.  Stable across asset_id renames and soft-delete/
            -- reclaim cycles — the durable join key for asset_references
            -- and all denormalized surfaces.  NOT NULL: every insert path
            -- now mints it via generate_geoid() so no legacy NULL rows
            -- reach this table in a clean-break deployment.
            asset_physical_id       UUID         NOT NULL,
            CONSTRAINT assets_kind_check
                CHECK (kind IN ('physical','virtual')),
            CONSTRAINT assets_status_check
                CHECK (status IN ('pending','active','failed','deleted')),
            CONSTRAINT assets_kind_identity_check
                CHECK ((kind = 'physical' AND filename IS NOT NULL)
                    OR (kind = 'virtual'  AND href     IS NOT NULL)),
            CONSTRAINT assets_identity_uq
                UNIQUE NULLS NOT DISTINCT (collection_physical_id, asset_id),
            -- Immutable physical-id uniqueness scoped to the partition key.
            -- A bare UNIQUE (asset_physical_id) is rejected by Postgres on a
            -- partitioned table; the partition key must be included.
            CONSTRAINT assets_physical_uq
                UNIQUE (collection_physical_id, asset_physical_id)
        ) PARTITION BY LIST (collection_physical_id);
        CREATE TABLE IF NOT EXISTS "{schema}".assets_catalog_tier
            PARTITION OF "{schema}".assets
            FOR VALUES IN (NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS assets_uq_filename_{schema_tag}
            ON "{schema}".assets (collection_physical_id, filename)
            WHERE kind = 'physical' AND status <> 'deleted';
        CREATE UNIQUE INDEX IF NOT EXISTS assets_uq_href_{schema_tag}
            ON "{schema}".assets (collection_physical_id, href)
            WHERE kind = 'virtual' AND status <> 'deleted';
        CREATE INDEX IF NOT EXISTS assets_status_idx_{schema_tag}
            ON "{schema}".assets (status);
        CREATE INDEX IF NOT EXISTS assets_pending_idx_{schema_tag}
            ON "{schema}".assets (collection_physical_id, filename)
            WHERE status = 'pending';
        CREATE INDEX IF NOT EXISTS idx_assets_created_at_{schema_tag}
            ON "{schema}".assets (created_at);
        CREATE INDEX IF NOT EXISTS idx_assets_metadata_gin_{schema_tag}
            ON "{schema}".assets USING GIN (metadata jsonb_path_ops);
        """

        refs_ddl = f"""
        CREATE TABLE IF NOT EXISTS "{schema}".asset_references (
            -- asset_physical_id: the immutable UUIDv7 join key from
            -- assets.asset_physical_id.  Keying on asset_physical_id means a
            -- rename (assets.asset_id label change) needs zero propagation
            -- here — the key never changes.
            asset_physical_id    UUID        NOT NULL,
            -- No catalog_id column: the schema namespace
            -- (s_<catalog_physical_id>) already scopes every row to its
            -- catalog, and ref_id carries the immutable collection_physical_id
            -- for COLLECTION refs. A stored logical catalog_id would be a
            -- mutable echo that goes stale on a catalog rename; the logical
            -- label is injected at read time from the method parameter.
            ref_type             VARCHAR     NOT NULL,
            ref_id               VARCHAR     NOT NULL,
            cascade_delete       BOOLEAN     NOT NULL DEFAULT TRUE,
            created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
            -- valid_until: NULL means the reference is currently active.
            -- Stamped by soft-delete and NEW_VERSION archive paths so a
            -- stale reference cannot block hard-deletion of a successor
            -- asset that re-uses the same asset_id. Audit trail preserved.
            valid_until          TIMESTAMPTZ DEFAULT NULL,
            PRIMARY KEY (asset_physical_id, ref_type, ref_id)
        );
        -- Active blocking references: only rows that are non-cascade AND
        -- still valid (valid_until IS NULL) actually block hard-delete.
        CREATE INDEX IF NOT EXISTS idx_asset_refs_blocking_{schema.replace('.', '_')}
            ON "{schema}".asset_references (asset_physical_id)
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
            # Partitions are named after the collection's physical id, not the
            # logical id. Resolve the physical id first; if the collection is
            # already gone (allow_missing=True above for schema), fall back to
            # a no-op: the partition either never existed or was already dropped.
            coll_physical_id = await self._resolve_collection_physical_id(
                catalog_id, collection_id, db_resource
            )
            if not coll_physical_id:
                return
            partition_name = f"assets_p_{coll_physical_id}"
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
                # Remove all asset_references for this catalog (schema scopes the catalog).
                await DQLQuery(
                    f'DELETE FROM "{schema}".asset_references',
                    result_handler=ResultHandler.ROWCOUNT,
                ).execute(conn)

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

        # Resolve the collection's immutable physical id. NULL for catalog-tier
        # rows (collection_id is None); these land in the assets_catalog_tier
        # partition whose FOR VALUES IN (NULL) already covers them.
        # Use the caller-supplied value when present (e.g. re-index paths that
        # already resolved it), otherwise look it up.
        collection_physical_id: Optional[str] = asset_doc.get("collection_physical_id")
        if collection_physical_id is None and collection_id is not None:
            collection_physical_id = await self._resolve_collection_physical_id(
                catalog_id, collection_id, db_resource
            )

        async with managed_transaction(db_resource or self.engine) as conn:
            # NULL collection_physical_id rows land in assets_catalog_tier.
            # Only non-NULL values need a named per-value partition.
            if collection_physical_id is not None:
                await ensure_partition_tool(
                    conn,
                    table_name="assets",
                    strategy="LIST",
                    partition_value=collection_physical_id,
                    schema=schema,
                    parent_table_name="assets",
                    parent_table_schema=schema,
                )

            now = datetime.now(timezone.utc)
            kind_val = asset_doc.get("kind") or "physical"
            status_val = asset_doc.get("status") or "active"
            # asset_physical_id (P2a): mint a UUIDv7 on INSERT; preserved (never
            # overwritten) on conflict-update so it stays immutable for the
            # lifetime of the asset row.
            from dynastore.tools.identifiers import generate_geoid as _gen_geoid
            physical_id_val = asset_doc.get("asset_physical_id") or asset_doc.get("physical_id") or _gen_geoid()
            # ``ON CONFLICT ON CONSTRAINT assets_identity_uq`` resolves the
            # upsert against the NULLS-NOT-DISTINCT unique constraint, so
            # catalog-tier rows (collection_physical_id IS NULL) participate
            # in the upsert just like collection-scoped rows.
            # catalog_id and collection_id are NOT stored — they are injected
            # at read time from method parameters / resolve_logical_id.
            sql = text(f"""
                INSERT INTO "{schema}".assets
                    (asset_id, collection_physical_id,
                     asset_type, kind, status,
                     filename, href, uri, content_hash, size_bytes,
                     created_at, updated_at, metadata, owned_by, asset_physical_id)
                VALUES
                    (:asset_id, :collection_physical_id,
                     :asset_type, :kind, :status,
                     :filename, :href, :uri, :content_hash, :size_bytes,
                     :created_at, :updated_at, :metadata, :owned_by, :physical_id)
                ON CONFLICT ON CONSTRAINT assets_identity_uq DO UPDATE SET
                    asset_type             = EXCLUDED.asset_type,
                    kind                   = EXCLUDED.kind,
                    status                 = EXCLUDED.status,
                    filename               = EXCLUDED.filename,
                    href                   = EXCLUDED.href,
                    uri                    = EXCLUDED.uri,
                    content_hash           = EXCLUDED.content_hash,
                    size_bytes             = EXCLUDED.size_bytes,
                    metadata               = EXCLUDED.metadata,
                    owned_by               = EXCLUDED.owned_by,
                    updated_at             = EXCLUDED.updated_at
            """)
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                asset_id=asset_doc["asset_id"],
                collection_physical_id=collection_physical_id,
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
                physical_id=physical_id_val,
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

        collection_physical_id = await self._resolve_collection_physical_id(
            catalog_id, collection_id, db_resource
        )
        sql = text(f"""
            DELETE FROM "{schema}".assets
            WHERE asset_id = :asset_id
              AND collection_physical_id IS NOT DISTINCT FROM :collection_physical_id
        """)
        async with managed_transaction(db_resource or self.engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                asset_id=asset_id,
                collection_physical_id=collection_physical_id,
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

        # Resolve the collection's physical id so the query can prune to the
        # correct partition. IS NOT DISTINCT FROM handles the NULL (catalog-tier)
        # case: a None collection_physical_id matches the NULL partition value.
        collection_physical_id = await self._resolve_collection_physical_id(
            catalog_id, collection_id, db_resource
        )
        sql = f"""
            SELECT asset_id, collection_physical_id,
                   asset_type, kind, status,
                   filename, href, uri, content_hash, size_bytes,
                   created_at, updated_at, metadata, owned_by,
                   asset_physical_id::text AS asset_physical_id
            FROM "{schema}".assets
            WHERE asset_id = :asset_id
              AND collection_physical_id IS NOT DISTINCT FROM :collection_physical_id
              AND status <> 'deleted'
            LIMIT 1
        """
        params: Dict[str, Any] = {
            "asset_id": asset_id,
            "collection_physical_id": collection_physical_id,
        }

        async with managed_transaction(db_resource or self.engine) as conn:
            row = await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, **params)
            return await self._attach_logical_ids(
                row, catalog_id, collection_id
            )

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

        Collection scope: ``collection_physical_id IS NOT DISTINCT FROM :collection_physical_id``
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
            "status <> 'deleted'",
        ]
        params: Dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }
        if not all_collections:
            # Resolve the physical id for the partition predicate.
            collection_physical_id = await self._resolve_collection_physical_id(
                catalog_id, collection_id, db_resource
            )
            where_parts.append(
                "collection_physical_id IS NOT DISTINCT FROM :collection_physical_id"
            )
            params["collection_physical_id"] = collection_physical_id

        if visible_ids is not None:
            where_parts.append("asset_id = ANY(:visible_asset_ids)")
            params["visible_asset_ids"] = list(visible_ids)

        if filters:
            filter_parts, filter_params = build_pg_where(filters)
            where_parts.extend(filter_parts)
            params.update(filter_params)

        sql = (
            f'SELECT asset_id, collection_physical_id, '
            f'asset_type, kind, status, '
            f'filename, href, uri, content_hash, size_bytes, '
            f'created_at, updated_at, metadata, owned_by, '
            f'asset_physical_id::text AS asset_physical_id '
            f'FROM "{schema}".assets '
            f'WHERE {" AND ".join(where_parts)} '
            f'ORDER BY created_at DESC LIMIT :limit OFFSET :offset'
        )

        # collection_physical_id was resolved above when not all_collections;
        # for all_collections it was never set in params — look it up from the
        # local variable (None when spanning multiple collections).
        _inject_collection_id = None if all_collections else collection_id  # type: ignore[possibly-undefined]

        async with managed_transaction(db_resource or self.engine) as conn:
            rows = await DQLQuery(
                sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, **params)
            rows = await self._attach_logical_ids(
                rows or [],
                catalog_id,
                _inject_collection_id,
                all_collections=all_collections,
            )
            return rows

    # ------------------------------------------------------------------
    # Reference guard (PG-only coordination mechanism)
    # ------------------------------------------------------------------

    async def check_blocking_references(
        self,
        physical_ids: List[str],
        catalog_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> List[Any]:
        """Return cascade_delete=False references for the given asset physical_ids.

        Uses the partial index on
        ``(physical_id) WHERE cascade_delete=FALSE AND valid_until IS NULL``
        for O(1) per-asset lookup. Invalidated references (``valid_until``
        stamped by a prior soft-delete or NEW_VERSION archive) do NOT count
        as blocking — the asset they pointed at is gone, so the new asset
        re-using the same id is free to be hard-deleted.

        JOINs ``assets`` to return the current live ``asset_id`` alongside
        ``physical_id`` so ``AssetReference``-compatible dicts can be
        built by callers without a separate lookup.

        Called by ``AssetService.delete_assets()`` before hard-deleting owned
        assets.  Returns a list of ``AssetReference``-compatible dicts.
        """
        if not physical_ids:
            return []

        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return []

        placeholders = ", ".join(f":phid_{i}::uuid" for i in range(len(physical_ids)))
        params: Dict[str, Any] = {}
        for i, phid in enumerate(physical_ids):
            params[f"phid_{i}"] = phid

        sql = text(f"""
            SELECT
                a.asset_id,
                r.ref_type,
                r.ref_id,
                r.cascade_delete,
                r.created_at
            FROM "{schema}".asset_references r
            JOIN "{schema}".assets a
              ON a.asset_physical_id = r.asset_physical_id
             AND a.status <> 'deleted'
            WHERE r.asset_physical_id IN ({placeholders})
              AND r.cascade_delete = FALSE
              AND r.valid_until IS NULL
            ORDER BY r.asset_physical_id, r.created_at ASC
        """)

        async with managed_transaction(db_resource or self.engine) as conn:
            rows = await DQLQuery(
                sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, **params)
            # Surface the logical collection_id in the blocking-reference report
            # (raised as AssetReferencedError), not the internal physical token.
            # catalog_id is injected from the param (not stored on the row).
            for r in rows or []:
                r["catalog_id"] = catalog_id
                r["ref_id"] = await self._ref_id_to_logical(
                    catalog_id, r.get("ref_type"), r.get("ref_id"), conn
                )
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
        # Schema scopes the catalog; catalog_id is a mutable label, omitted here
        # to stay rename-robust.
        # - When asset_id is given, also pin to a single collection scope using
        #   collection_physical_id IS NOT DISTINCT FROM so NULL (catalog-tier)
        #   matches NULL.
        # - When asset_id is absent and collection_id is provided, filter by
        #   that collection only; if both are absent, the operation spans the
        #   whole catalog.
        where_clauses: List[str] = []
        params: Dict[str, Any] = {"now": now}

        # Resolve collection_physical_id once for both the asset_id pin and the
        # collection-only scan so callers never have to pass it explicitly.
        collection_physical_id: Optional[str] = None
        if asset_id or collection_id is not None:
            collection_physical_id = await self._resolve_collection_physical_id(
                catalog_id, collection_id, db_resource
            )

        if asset_id:
            where_clauses.append("asset_id = :aid")
            params["aid"] = asset_id
            where_clauses.append(
                "collection_physical_id IS NOT DISTINCT FROM :coll_phys"
            )
            params["coll_phys"] = collection_physical_id
        elif collection_id is not None:
            where_clauses.append(
                "collection_physical_id IS NOT DISTINCT FROM :coll_phys"
            )
            params["coll_phys"] = collection_physical_id

        where_stmt = " AND ".join(where_clauses) if where_clauses else None

        # When asset_id or collection_id is provided the query is pinned to a
        # single collection; otherwise it spans all collections under the schema.
        _bulk_all_collections = asset_id is None and collection_id is None
        _bulk_inject_collection_id = None if _bulk_all_collections else collection_id

        async with managed_transaction(db_resource or self.engine) as conn:
            fetch_sql = text(
                f'SELECT asset_id, collection_physical_id, '
                f'owned_by, uri, asset_physical_id::text AS asset_physical_id '
                f'FROM "{schema}".assets'
                + (f' WHERE {where_stmt}' if where_stmt else '')
            )
            asset_rows = await DQLQuery(
                fetch_sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, **params)

            if not asset_rows:
                return 0, []

            # Inject catalog_id and collection_id into each fetched row so
            # matched_rows callers (e.g. GCS blob reaping) keep both keys.
            asset_rows = await self._attach_logical_ids(
                asset_rows,
                catalog_id,
                _bulk_inject_collection_id,
                all_collections=_bulk_all_collections,
            )

            # Reference guard (hard-delete only).
            # asset_references keys on asset_physical_id (immutable) so we pass
            # asset_physical_ids directly — no stale-read risk across renames.
            if hard:
                owned_phys_ids = [
                    a["asset_physical_id"]
                    for a in asset_rows
                    if a.get("owned_by") and a.get("asset_physical_id")
                ]
                # Surface any legacy owned row missing asset_physical_id: post-#2296
                # all rows mint it (NOT NULL), so a NULL here means a pre-reset
                # row whose blocking references would be skipped silently.
                legacy_owned = [
                    a.get("asset_id")
                    for a in asset_rows
                    if a.get("owned_by") and not a.get("asset_physical_id")
                ]
                if legacy_owned:
                    logger.warning(
                        "delete_assets: hard-delete skipping reference guard for "
                        "legacy owned assets %s in '%s' — no asset_physical_id "
                        "(row pre-dates #2296; expected only on a non-reset schema)",
                        legacy_owned, schema,
                    )
                if owned_phys_ids:
                    blocking = await self.check_blocking_references(
                        owned_phys_ids, catalog_id, db_resource=conn,
                    )
                    if blocking:
                        # Return blocking rows so caller can raise the appropriate error
                        return -1, blocking

            prefix = (
                f'DELETE FROM "{schema}".assets'
                if hard
                else f"UPDATE \"{schema}\".assets SET status = 'deleted', updated_at = :now"
            )
            final_sql = text(
                f"{prefix}" + (f" WHERE {where_stmt}" if where_stmt else "")
            )
            rowcount = await DQLQuery(
                final_sql, result_handler=ResultHandler.ROWCOUNT
            ).execute(conn, **params)

            # Stamp any active asset_references for the soft-deleted assets
            # so future hard-deletes of a successor (re-using the same
            # asset_id via NEW_VERSION) don't get blocked by stale rows.
            # Key on asset_physical_id (immutable) — safe across renames.
            # On hard-delete the rows are already gone; no stamp needed.
            if not hard and asset_rows:
                deleted_phys_ids = [
                    a["asset_physical_id"]
                    for a in asset_rows
                    if a.get("asset_physical_id")
                ]
                if deleted_phys_ids:
                    ref_placeholders = ", ".join(
                        f":rphid_{i}::uuid" for i in range(len(deleted_phys_ids))
                    )
                    ref_params: Dict[str, Any] = {"now": now}
                    for i, phid in enumerate(deleted_phys_ids):
                        ref_params[f"rphid_{i}"] = phid
                    ref_sql = text(
                        f'UPDATE "{schema}".asset_references '
                        f"SET valid_until = :now "
                        f"WHERE asset_physical_id IN ({ref_placeholders}) "
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
        """Insert or update an asset reference row.

        Resolves the asset's immutable ``physical_id`` from the ``assets``
        table and stores it as the primary key so a rename (``asset_id`` label
        change) never invalidates existing references.  The returned dict
        includes the current live ``asset_id`` (read back from the assets row)
        so callers that build ``AssetReference`` objects get a consistent view.
        """
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            raise ValueError(
                f"AssetPostgresqlDriver.add_asset_reference: catalog '{catalog_id}' not found."
            )

        now = datetime.now(timezone.utc)
        ref_type_val = ref_type.value if hasattr(ref_type, "value") else str(ref_type)

        # Resolve physical_id inside the caller's transaction so uncommitted
        # asset rows are visible (e.g. ingest paths that write the asset and
        # the reference in one shot).
        async with managed_transaction(db_resource or self.engine) as conn:
            physical_id_val = await DQLQuery(
                f'SELECT asset_physical_id::text FROM "{schema}".assets '
                "WHERE asset_id = :asset_id AND status <> 'deleted' "
                "ORDER BY created_at DESC LIMIT 1",
                result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
            ).execute(conn, asset_id=asset_id)
            if physical_id_val is None:
                raise ValueError(
                    f"AssetPostgresqlDriver.add_asset_reference: "
                    f"asset '{asset_id}' not found in catalog '{catalog_id}'."
                )

            # COLLECTION refs store the immutable collection_physical_id so a
            # collection rename never orphans them; other ref kinds pass through.
            stored_ref_id = await self._ref_id_to_physical(
                catalog_id, ref_type_val, ref_id, conn
            )

            sql = text(f"""
                INSERT INTO "{schema}".asset_references
                    (asset_physical_id, ref_type, ref_id, cascade_delete, created_at)
                VALUES (:physical_id::uuid, :ref_type, :ref_id,
                        :cascade_delete, :created_at)
                ON CONFLICT (asset_physical_id, ref_type, ref_id) DO UPDATE SET
                    cascade_delete = EXCLUDED.cascade_delete,
                    valid_until    = NULL
                RETURNING asset_physical_id::text AS asset_physical_id,
                          ref_type, ref_id, cascade_delete, created_at
            """)
            row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
                conn,
                physical_id=physical_id_val,
                ref_type=ref_type_val,
                ref_id=stored_ref_id,
                cascade_delete=cascade_delete,
                created_at=now,
            )
        # Callers build AssetReference from this dict; inject the current
        # live asset_id so the model field is populated correctly. The INSERT
        # ... RETURNING always yields a row, so a None here is a hard error.
        if row is None:
            raise ValueError(
                f"AssetPostgresqlDriver.add_asset_reference: INSERT returned no "
                f"row for asset '{asset_id}' / {ref_type_val}:{ref_id}."
            )
        row = dict(row)
        row["asset_id"] = asset_id
        # catalog_id is not stored on the row (the schema scopes it); inject the
        # caller's logical label so the AssetReference model field is populated.
        row["catalog_id"] = catalog_id
        # Echo the caller's logical ref_id back, not the stored physical token,
        # so AssetReference.ref_id stays user-facing (collection_id).
        row["ref_id"] = ref_id
        return row

    async def remove_asset_reference(
        self,
        asset_id: str,
        catalog_id: str,
        ref_type: Any,
        ref_id: str,
        db_resource: Optional[DbResource] = None,
    ) -> None:
        """Delete an asset reference row.

        Resolves the asset's ``physical_id`` first so the delete targets the
        immutable PK; a rename between the add and the remove leaves the
        physical_id unchanged and this lookup still finds the right row.
        """
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return

        ref_type_val = ref_type.value if hasattr(ref_type, "value") else str(ref_type)
        async with managed_transaction(db_resource or self.engine) as conn:
            # Resolve asset_physical_id regardless of status: an asset that has
            # been soft-deleted must still have its references removable (the old
            # asset_id-keyed DELETE fired unconditionally). The immutable
            # asset_physical_id is valid for any row state.
            physical_id_val = await DQLQuery(
                f'SELECT asset_physical_id::text FROM "{schema}".assets '
                "WHERE asset_id = :asset_id "
                "ORDER BY created_at DESC LIMIT 1",
                result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
            ).execute(conn, asset_id=asset_id)
            if physical_id_val is None:
                # Asset not found — nothing to remove; idempotent no-op.
                return

            # Match the stored physical ref_id for COLLECTION refs (the row was
            # written with collection_physical_id, not the logical collection_id).
            stored_ref_id = await self._ref_id_to_physical(
                catalog_id, ref_type_val, ref_id, conn
            )
            sql = text(f"""
                DELETE FROM "{schema}".asset_references
                WHERE asset_physical_id = :physical_id::uuid
                  AND ref_type          = :ref_type
                  AND ref_id            = :ref_id
            """)
            await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn,
                physical_id=physical_id_val,
                ref_type=ref_type_val,
                ref_id=stored_ref_id,
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

        Resolves the ``physical_id`` for the given logical ``asset_id``, then
        queries ``asset_references`` by ``physical_id``.  The live
        ``asset_id`` is read back from the ``assets`` JOIN so callers always
        see the current label, not a stale copy.

        By default returns only currently-valid rows (``valid_until IS NULL``).
        Pass ``include_invalidated=True`` to also surface rows stamped by a
        prior soft-delete or NEW_VERSION archive — useful for audit /
        forensics views.
        """
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return []

        valid_clause = (
            "" if include_invalidated else " AND r.valid_until IS NULL"
        )
        async with managed_transaction(db_resource or self.engine) as conn:
            # Resolve asset_physical_id regardless of status so references on a
            # soft-deleted asset remain listable (audit / forensics).
            physical_id_val = await DQLQuery(
                f'SELECT asset_physical_id::text FROM "{schema}".assets '
                "WHERE asset_id = :asset_id "
                "ORDER BY created_at DESC LIMIT 1",
                result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
            ).execute(conn, asset_id=asset_id)
            if physical_id_val is None:
                return []

            sql = f"""
                SELECT
                    a.asset_id,
                    r.ref_type,
                    r.ref_id,
                    r.cascade_delete,
                    r.created_at,
                    r.valid_until
                FROM "{schema}".asset_references r
                JOIN "{schema}".assets a
                  ON a.asset_physical_id = r.asset_physical_id
                 AND a.status <> 'deleted'
                WHERE r.asset_physical_id = :physical_id::uuid{valid_clause}
                ORDER BY r.created_at ASC
            """
            rows = await DQLQuery(
                sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, physical_id=physical_id_val)
            # Project COLLECTION ref_ids back to the logical collection_id so
            # callers never see the internal physical token. catalog_id is
            # injected from the param (not stored on the row).
            for r in rows or []:
                r["catalog_id"] = catalog_id
                r["ref_id"] = await self._ref_id_to_logical(
                    catalog_id, r.get("ref_type"), r.get("ref_id"), conn
                )
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

        JOINs ``assets`` on ``asset_physical_id`` to return the current live
        ``asset_id`` — never a stale stored copy — so callers always see
        the post-rename label without a separate lookup.

        By default returns only currently-valid rows (``valid_until IS NULL``).
        Pass ``include_invalidated=True`` to also surface rows stamped by a
        prior soft-delete — useful for audit / forensics views.
        """
        schema = await self._resolve_schema(catalog_id, db_resource)
        if not schema:
            return []

        ref_type_val = ref_type.value if hasattr(ref_type, "value") else str(ref_type)
        valid_clause = (
            "" if include_invalidated else " AND r.valid_until IS NULL"
        )
        sql = f"""
            SELECT a.asset_id
            FROM "{schema}".asset_references r
            JOIN "{schema}".assets a
              ON a.asset_physical_id = r.asset_physical_id
             AND a.status <> 'deleted'
            WHERE r.ref_type = :ref_type
              AND r.ref_id   = :ref_id{valid_clause}
        """
        async with managed_transaction(db_resource or self.engine) as conn:
            # Match the stored physical ref_id for COLLECTION refs.
            stored_ref_id = await self._ref_id_to_physical(
                catalog_id, ref_type_val, ref_id, conn
            )
            rows = await DQLQuery(
                sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(conn, ref_type=ref_type_val, ref_id=stored_ref_id)
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
from dynastore.models.driver_context import DriverContext  # noqa: E402


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
