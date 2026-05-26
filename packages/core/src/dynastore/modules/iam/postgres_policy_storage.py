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

# File: dynastore/modules/iam/postgres_policy_storage.py

from typing import Any, Dict, Optional, List
from uuid import UUID
import json

from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, ResultHandler, DbResource, managed_transaction, DbConnection
from dynastore.modules import get_protocol
from dynastore.models.protocols import DatabaseProtocol

from .models import Policy
from .policy_storage import AbstractPolicyStorage

# --- Queries ---

CREATE_POLICIES_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.policies (
        id VARCHAR(128) NOT NULL,
        version VARCHAR(16) DEFAULT '1.0',
        description TEXT,
        effect VARCHAR(16) DEFAULT 'ALLOW',
        priority INTEGER NOT NULL DEFAULT 0,
        actions JSONB NOT NULL DEFAULT '[]'::jsonb,
        resources JSONB DEFAULT '["*"]'::jsonb,
        conditions JSONB DEFAULT '[]'::jsonb,
        partition_key VARCHAR(64) NOT NULL DEFAULT 'global',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (id, partition_key)
    ) PARTITION BY LIST (partition_key);
""")


CREATE_PARTITION_GLOBAL = DDLQuery('CREATE TABLE IF NOT EXISTS {schema}.policies_global PARTITION OF {schema}.policies FOR VALUES IN (\'global\');')
CREATE_PARTITION_DEFAULT = DDLQuery('CREATE TABLE IF NOT EXISTS {schema}.policies_default PARTITION OF {schema}.policies DEFAULT;')

INSERT_POLICY = DQLQuery(
    """
    INSERT INTO {schema}.policies (id, version, description, effect, priority, actions, resources, conditions, partition_key)
    VALUES (:id, :version, :description, :effect, :priority, :actions, :resources, :conditions, :partition_key)
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Policy(**row) if row else None
)

UPSERT_POLICY = DQLQuery(
    """
    INSERT INTO {schema}.policies (id, version, description, effect, priority, actions, resources, conditions, partition_key)
    VALUES (:id, :version, :description, :effect, :priority, :actions, :resources, :conditions, :partition_key)
    ON CONFLICT (id, partition_key) DO UPDATE
    SET version = EXCLUDED.version,
        description = EXCLUDED.description,
        effect = EXCLUDED.effect,
        priority = EXCLUDED.priority,
        actions = EXCLUDED.actions,
        resources = EXCLUDED.resources,
        conditions = EXCLUDED.conditions
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Policy(**row) if row else None
)

GET_POLICY = DQLQuery(
    # (id, partition_key) is the table's PRIMARY KEY. Filtering by id alone
    # would return whichever row PG happens to scan first when the same id
    # exists in multiple partitions, leaking rows across tenants and
    # producing the cross-partition update bug fixed in this module's
    # update_policy.
    "SELECT * FROM {schema}.policies WHERE id = :id AND partition_key = :partition_key;",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Policy(**row) if row else None
)



DELETE_POLICY = DQLQuery(
    # See GET_POLICY note: filter by full PK so admin deletes can never
    # cascade across partitions.
    "DELETE FROM {schema}.policies WHERE id = :id AND partition_key = :partition_key;",
    result_handler=ResultHandler.ROWCOUNT
)

DELETE_USAGE_COUNTERS_FOR_POLICY = DQLQuery(
    # Cleanup orphan rate-limit / lifetime-quota rows when a policy is
    # dropped. usage_counters has no FK, the windowed reaper skips
    # ``expires_at IS NULL`` rows, so without this lifetime-quota rows
    # would linger indefinitely. ``usage_counters`` is not partitioned
    # by tenant — policy_id is globally unique across partitions, so
    # filtering on the policy_id alone is correct.
    "DELETE FROM {schema}.usage_counters WHERE policy_id = :policy_id;",
    result_handler=ResultHandler.ROWCOUNT,
)

LIST_POLICIES = DQLQuery(
    """
    SELECT * FROM {schema}.policies 
    ORDER BY created_at DESC 
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [Policy(**row) for row in rows]
)

LIST_POLICIES_BY_PARTITION = DQLQuery(
    """
    SELECT * FROM {schema}.policies 
    WHERE partition_key = :partition_key
    ORDER BY created_at DESC 
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [Policy(**row) for row in rows]
)

# --- Enhanced Search Query ---

def build_search_policies_query(resource_pattern: Optional[str], action_pattern: Optional[str], limit: int, offset: int, schema: str = "iam"):
    clauses = []
    params: Dict[str, Any] = {"limit": limit, "offset": offset}

    if resource_pattern:
        clauses.append("resources::text LIKE :res_search")
        params["res_search"] = f"%{resource_pattern}%"

    if action_pattern:
        clauses.append("actions::text LIKE :act_search")
        params["act_search"] = f"%{action_pattern}%"

    where_clause = " AND ".join(clauses) if clauses else "1=1"
    
    sql = f"""
        SELECT * FROM {{schema}}.policies 
        WHERE {where_clause}
        ORDER BY created_at DESC 
        LIMIT :limit OFFSET :offset;
    """
    return DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS, post_processor=lambda rows: [Policy(**row) for row in rows]), params


class PostgresPolicyStorage(AbstractPolicyStorage):
    engine: Optional[DbResource] = None

    def __init__(self, app_state: Optional[object] = None) -> None:
        db = get_protocol(DatabaseProtocol)
        self.engine = db.engine if db else None

    async def initialize(self, conn: DbResource, schema: str = "iam"):
        """Compatibility alias for _initialize_schema."""
        return await self._initialize_schema(conn, schema=schema)

    async def _initialize_schema(self, conn: DbResource, schema: str = "iam"):
        schema = schema.strip('"')

        # 0. Ensure Schema
        await maintenance_tools.ensure_schema_exists(conn, schema)

        # 1. Base Table (auto-inferred existence check via {schema})
        await CREATE_POLICIES_TABLE.execute(conn, schema=schema)

        # 2. Partitions (IF NOT EXISTS in SQL handles idempotency)
        await CREATE_PARTITION_GLOBAL.execute(conn, schema=schema)
        await CREATE_PARTITION_DEFAULT.execute(conn, schema=schema)

    async def ensure_policy_partition(self, conn: DbResource, partition_key: str, schema: str = "iam"):
        from dynastore.tools.db import validate_sql_identifier
        schema = schema.strip('"')
        validate_sql_identifier(partition_key)
        partition_table = f"policies_{partition_key}"
        safe_key = partition_key.replace("'", "''")
        # Quote partition table name to handle dashes in partition keys (e.g., catalog IDs with dashes)
        ddl = f'CREATE TABLE IF NOT EXISTS {{schema}}."{partition_table}" PARTITION OF {{schema}}.policies FOR VALUES IN (\'{safe_key}\');'
        await DDLQuery(ddl).execute(conn, schema=schema)

    async def _bump_binding_version(self, schema: str) -> None:
        """Best-effort invalidation of the phantom-token cache (#1343).

        No-op unless the phantom cache is active (Valkey + flag); never raises.
        """
        from dynastore.modules.iam.phantom_token import bump_binding_version

        await bump_binding_version(schema)

    async def create_policy(self, policy: Policy, conn: Optional[DbResource] = None, schema: str = "iam") -> Policy:
        async with managed_transaction(conn or self.engine) as db:
            result = await INSERT_POLICY.execute(
                db,
                schema=schema.strip('"'),
                id=policy.id,
                version=policy.version,
                description=policy.description,
                effect=policy.effect,
                priority=policy.priority,
                actions=json.dumps(policy.actions),
                resources=json.dumps(policy.resources),
                conditions=json.dumps([c.model_dump() for c in policy.conditions]) if policy.conditions else "[]",
                partition_key=policy.partition_key or "global"
            )
        await self._bump_binding_version(schema)
        return result



    async def get_policy(self, policy_id: str, conn: Optional[DbResource] = None, schema: str = "iam", partition_key: str = "global") -> Optional[Policy]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_POLICY.execute(db, schema=schema.strip('"'), id=policy_id, partition_key=partition_key)

    async def update_policy(self, policy: Policy, conn: Optional[DbResource] = None, schema: str = "iam") -> Optional[Policy]:
        async with managed_transaction(conn or self.engine) as db:
            # (id, partition_key) is the table's PRIMARY KEY — distinct
            # partitions are independent rows. UPSERT_POLICY's
            # ON CONFLICT (id, partition_key) DO UPDATE handles both the
            # "row exists in this partition" and "row does not exist"
            # cases. Crucially, no DELETE: a row identified by
            # (id, partition_key) cannot semantically "move partitions" —
            # callers that genuinely intend to relocate a policy must
            # delete the old row and create a new one explicitly. The
            # earlier implementation's GET-then-DELETE-then-INSERT branch
            # was load-bearing for the IAM-outage class of bugs: under
            # multi-service boot the same default policy IDs ping-pong
            # between partition_keys, the unfiltered DELETE wiped every
            # partition's copy, and concurrent reads saw Deny-by-Default.
            result = await UPSERT_POLICY.execute(
                db,
                schema=schema.strip('"'),
                id=policy.id,
                version=policy.version,
                description=policy.description,
                effect=policy.effect,
                priority=policy.priority,
                actions=json.dumps(policy.actions),
                resources=json.dumps(policy.resources),
                conditions=json.dumps([c.model_dump() for c in policy.conditions]) if policy.conditions else "[]",
                partition_key=policy.partition_key or "global"
            )
        await self._bump_binding_version(schema)
        return result

    async def delete_policy(self, policy_id: str, conn: Optional[DbResource] = None, schema: str = "iam", partition_key: str = "global") -> bool:
        async with managed_transaction(conn or self.engine) as db:
            await DELETE_USAGE_COUNTERS_FOR_POLICY.execute(
                db, schema=schema.strip('"'), policy_id=policy_id
            )
            count = await DELETE_POLICY.execute(db, schema=schema.strip('"'), id=policy_id, partition_key=partition_key)
        await self._bump_binding_version(schema)
        return count > 0

    async def list_policies(self, partition_key: Optional[str] = None, limit: int = 100, offset: int = 0, conn: Optional[DbResource] = None, schema: str = "iam") -> List[Policy]:
        async with managed_transaction(conn or self.engine) as db:
            if partition_key:
                return await LIST_POLICIES_BY_PARTITION.execute(db, schema=schema.strip('"'), partition_key=partition_key, limit=limit, offset=offset)
            else:
                return await LIST_POLICIES.execute(db, schema=schema.strip('"'), limit=limit, offset=offset)

    async def search_policies(self, resource_pattern: Optional[str] = None, action_pattern: Optional[str] = None, limit: int = 100, offset: int = 0, conn: Optional[DbResource] = None, schema: str = "iam") -> List[Policy]:
        query, params = build_search_policies_query(resource_pattern, action_pattern, limit, offset, schema=schema.strip('"'))
        async with managed_transaction(conn or self.engine) as db:
            return await query.execute(db, **params)