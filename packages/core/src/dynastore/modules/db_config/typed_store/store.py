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

"""PostgreSQL implementation of :class:`TypedStore`.

Dispatches on :class:`Scope` type to the appropriate physical table:

* :class:`PlatformScope`  → ``configs.platform_configs``
* :class:`CatalogScope`   → ``"<tenant_schema>".typed_catalog_configs``
* :class:`CollectionScope`→ ``"<tenant_schema>".typed_collection_configs``

The tenant schema is resolved via :class:`CatalogsProtocol` at query time,
so a single store instance handles every tenant.

Read resolution (:meth:`get`) returns the row whose ``class_key`` is either
``base.class_key()`` exactly or a subclass thereof — exact match wins,
ties broken by ``updated_at DESC``. Implemented as one indexed query per
call thanks to the real ``class_key`` column.

Cycle F.4c.1 introduced ``ref_key`` as the operator-chosen instance name
and the new primary key.  This store writes ``ref_key = class_key`` for
single-instance configs (every config today); the multi-instance API
extension lands in F.4c.2.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Type

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.db_config.typed_store.ddl import (
    CONFIGS_SCHEMA,
    PLATFORM_SCHEMAS_DDL,
    tenant_configs_ddl,
)
from dynastore.tools.typed_store.base import PersistentModel
from dynastore.tools.typed_store.migrations import migrate
from dynastore.tools.typed_store.registry import TypedModelRegistry
from dynastore.tools.typed_store.scope import (
    CatalogScope,
    CollectionScope,
    PlatformScope,
    Scope,
)

logger = logging.getLogger(__name__)


class PostgresTypedStore:
    """Concrete ``TypedStore`` backed by PostgreSQL.

    Constructed once at startup with a DB engine and a tenant-schema
    resolver. All scope dispatch happens inside this class so call sites
    remain scope-agnostic.
    """

    def __init__(
        self,
        engine: Any,
        *,
        tenant_schema_resolver: Any,
    ) -> None:
        """Create a new store.

        :param engine: SQLAlchemy async engine or connection factory.
        :param tenant_schema_resolver: async callable
            ``(catalog_id, db_resource) -> str`` returning the PG schema
            name for a catalog. Typically
            ``CatalogsProtocol.resolve_physical_schema``.
        """
        self._engine = engine
        self._resolve_schema = tenant_schema_resolver

    # -----------------------------------------------------------------
    # DDL bootstrap
    # -----------------------------------------------------------------

    async def ensure_platform_schema(self, *, db_resource: Any = None) -> None:
        """Create the global ``configs`` schema and its two tables (idempotent)."""
        engine = db_resource or self._engine
        async with managed_transaction(engine) as conn:
            await DDLQuery(PLATFORM_SCHEMAS_DDL).execute(conn)

    async def ensure_tenant_schema(
        self, tenant_schema: str, *, db_resource: Any = None
    ) -> None:
        """Create per-tenant config tables inside ``tenant_schema`` (idempotent)."""
        engine = db_resource or self._engine
        async with managed_transaction(engine) as conn:
            await DDLQuery(tenant_configs_ddl(tenant_schema)).execute(conn)

    # -----------------------------------------------------------------
    # Scope → physical location
    # -----------------------------------------------------------------

    async def _locate(
        self, scope: Scope, *, conn: Any
    ) -> Optional[Dict[str, Any]]:
        """Translate a Scope into ``(schema, table, pk_filter, pk_values)``.

        Returns ``None`` when the tenant schema cannot be resolved (missing
        catalog). Callers should treat that as "nothing stored at this scope".
        """
        if isinstance(scope, PlatformScope):
            return {
                "schema": CONFIGS_SCHEMA,
                "table": "platform_configs",
                "pk_filter": "",
                "pk_values": {},
            }
        if isinstance(scope, CatalogScope):
            ts = await self._resolve_schema(scope.catalog_id, db_resource=conn)
            if not ts:
                return None
            return {
                "schema": ts,
                "table": "catalog_configs",
                "pk_filter": "",
                "pk_values": {},
            }
        if isinstance(scope, CollectionScope):
            ts = await self._resolve_schema(scope.catalog_id, db_resource=conn)
            if not ts:
                return None
            return {
                "schema": ts,
                "table": "collection_configs",
                "pk_filter": "collection_id = :collection_id AND ",
                "pk_values": {"collection_id": scope.collection_id},
            }
        raise TypeError(f"Unsupported scope: {type(scope).__name__}")

    # -----------------------------------------------------------------
    # TypedStore contract
    # -----------------------------------------------------------------

    async def get(
        self, scope: Scope, base: Type[PersistentModel]
    ) -> Optional[PersistentModel]:
        """Return the compatible row for ``base`` at ``scope``, else ``None``.

        Exact class_key match wins; otherwise the most recently updated
        subclass row is returned. One indexed query.
        """
        async with managed_transaction(self._engine) as conn:
            loc = await self._locate(scope, conn=conn)
            if loc is None:
                return None

            keys = [m.class_key() for m in TypedModelRegistry.subclasses_of(base)]
            if not keys:
                # base itself isn't registered (unlikely) — fall back to exact key
                keys = [base.class_key()]

            sql = f'''
                SELECT class_key, schema_id, config_data
                  FROM "{loc["schema"]}".{loc["table"]}
                 WHERE {loc["pk_filter"]}class_key = ANY(:keys)
                 ORDER BY (class_key = :exact) DESC, updated_at DESC
                 LIMIT 1
            '''
            row = await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT
            ).execute(
                conn,
                keys=keys,
                exact=base.class_key(),
                **loc["pk_values"],
            )
            if not row:
                return None

            cls = TypedModelRegistry.get(row["class_key"])
            if cls is None:
                logger.warning(
                    "PostgresTypedStore.get: stored class_key %r not in registry",
                    row["class_key"],
                )
                return None

            data = row["config_data"]
            if isinstance(data, str):
                data = json.loads(data)
            stored_sid = row.get("schema_id")
            current_sid = cls.schema_id()
            if stored_sid and stored_sid != current_sid:
                data = migrate(data, source=stored_sid, target=current_sid)
            return cls.model_validate(data)

    async def set(self, scope: Scope, instance: PersistentModel) -> None:
        """Upsert ``instance`` at ``scope`` keyed by its ``class_key``."""
        async with managed_transaction(self._engine) as conn:
            loc = await self._locate(scope, conn=conn)
            if loc is None:
                raise RuntimeError(
                    f"Cannot set {type(instance).__name__}: scope not resolvable"
                )
            class_key = type(instance).class_key()
            schema_id = type(instance).schema_id()
            # ``secret_mode="db"`` tells every ``Secret`` field to serialize as
            # its encrypted Fernet envelope before we write jsonb. Without this
            # mode Secrets would dump as ``"***"`` (the mask) and the plaintext
            # would be lost on the next load. See tools/secrets.py.
            payload = json.dumps(
                instance.model_dump(mode="json", context={"secret_mode": "db"}),
                cls=CustomJSONEncoder,
            )

            if isinstance(scope, CollectionScope):
                sql = f'''
                    INSERT INTO "{loc["schema"]}".{loc["table"]}
                        (collection_id, ref_key, class_key, schema_id, config_data, updated_at)
                    VALUES (:collection_id, :ref_key, :class_key, :schema_id, :config_data::jsonb, NOW())
                    ON CONFLICT (collection_id, ref_key) DO UPDATE SET
                        class_key   = EXCLUDED.class_key,
                        schema_id   = EXCLUDED.schema_id,
                        config_data = EXCLUDED.config_data,
                        updated_at  = NOW();
                '''
            else:
                sql = f'''
                    INSERT INTO "{loc["schema"]}".{loc["table"]}
                        (ref_key, class_key, schema_id, config_data, updated_at)
                    VALUES (:ref_key, :class_key, :schema_id, :config_data::jsonb, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        class_key   = EXCLUDED.class_key,
                        schema_id   = EXCLUDED.schema_id,
                        config_data = EXCLUDED.config_data,
                        updated_at  = NOW();
                '''
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                ref_key=class_key,
                class_key=class_key,
                schema_id=schema_id,
                config_data=payload,
                **loc["pk_values"],
            )

    async def delete(self, scope: Scope, cls: Type[PersistentModel]) -> None:
        """Delete the row for ``cls`` at ``scope`` (no cascade to subclasses)."""
        async with managed_transaction(self._engine) as conn:
            loc = await self._locate(scope, conn=conn)
            if loc is None:
                return
            sql = (
                f'DELETE FROM "{loc["schema"]}".{loc["table"]} '
                f'WHERE {loc["pk_filter"]}ref_key = :ref_key;'
            )
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn, ref_key=cls.class_key(), **loc["pk_values"]
            )

    async def list(
        self,
        scope: Scope,
        base: Type[PersistentModel],
        limit: int = 100,
        offset: int = 0,
    ) -> List[PersistentModel]:
        """Return every subclass-of-``base`` row at ``scope``, newest first."""
        async with managed_transaction(self._engine) as conn:
            loc = await self._locate(scope, conn=conn)
            if loc is None:
                return []
            keys = [m.class_key() for m in TypedModelRegistry.subclasses_of(base)]
            if not keys:
                return []
            sql = f'''
                SELECT class_key, schema_id, config_data
                  FROM "{loc["schema"]}".{loc["table"]}
                 WHERE {loc["pk_filter"]}class_key = ANY(:keys)
                 ORDER BY updated_at DESC
                 LIMIT :limit OFFSET :offset
            '''
            rows = await DQLQuery(
                sql, result_handler=ResultHandler.ALL_DICTS
            ).execute(
                conn,
                keys=keys,
                limit=limit,
                offset=offset,
                **loc["pk_values"],
            )

            out: List[PersistentModel] = []
            for row in rows:
                cls = TypedModelRegistry.get(row["class_key"])
                if cls is None:
                    continue
                data = row["config_data"]
                if isinstance(data, str):
                    data = json.loads(data)
                stored_sid = row.get("schema_id")
                current_sid = cls.schema_id()
                if stored_sid and stored_sid != current_sid:
                    data = migrate(data, source=stored_sid, target=current_sid)
                out.append(cls.model_validate(data))
            return out

    # -----------------------------------------------------------------
    # Schema registry helpers
    # -----------------------------------------------------------------

    async def register_schema(
        self,
        model: Type[PersistentModel],
        *,
        created_by: Optional[str] = None,
        db_resource: Any = None,
    ) -> None:
        """Upsert ``model``'s current schema into ``configs.schemas`` (idempotent)."""
        engine = db_resource or self._engine
        async with managed_transaction(engine) as conn:
            schema_json = json.dumps(model.model_json_schema(), sort_keys=True)
            sql = f"""
                INSERT INTO {CONFIGS_SCHEMA}.schemas
                    (schema_id, class_key, schema_json, created_by)
                VALUES (:schema_id, :class_key, :schema_json::jsonb, :created_by)
                ON CONFLICT (schema_id) DO NOTHING;
            """
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                schema_id=model.schema_id(),
                class_key=model.class_key(),
                schema_json=schema_json,
                created_by=created_by,
            )

    async def register_all_schemas(
        self,
        *,
        created_by: Optional[str] = None,
        db_resource: Any = None,
    ) -> int:
        """Register every currently-imported :class:`PersistentModel` class.

        Returns the number of classes processed. Call after all modules
        are imported (i.e. after plugin discovery) so the registry is
        fully populated.
        """
        count = 0
        for model in TypedModelRegistry.all().values():
            await self.register_schema(
                model, created_by=created_by, db_resource=db_resource
            )
            count += 1
        return count
