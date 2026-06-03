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

# File: dynastore/modules/iam/postgres_iam_storage.py

from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID
import json
import logging
import time

from dynastore.modules.db_config.exceptions import TableNotFoundError
from dynastore.modules.db_config.query_executor import (
    DDLBatch,
    DDLQuery,
    DQLQuery,
    ResultHandler,
    DbResource,
    managed_transaction,
)
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.db_config import maintenance_tools
from .models import (
    Principal,
    Role,
    RefreshToken,
    IdentityLink,
)
from .iam_storage import AbstractIamStorage
from .interfaces import AuthorizationStorageProtocol

logger = logging.getLogger(__name__)

from .iam_queries import *  # noqa: F401,F403


# Subject/object kind constants — string-typed to mirror DB columns;
# kept centralized so callers don't sprinkle magic strings.
SUBJECT_PRINCIPAL = "principal"
SUBJECT_CATALOG = "catalog"
SUBJECT_COLLECTION = "collection"
SUBJECT_ITEM = "item"
SUBJECT_ASSET = "asset"

OBJECT_ROLE = "role"
OBJECT_POLICY = "policy"

EFFECT_ALLOW = "allow"
EFFECT_DENY = "deny"


def _reject_platform_schema(
    catalog_schema: str, action: str, role_name: str, principal_id: "UUID"
) -> None:
    """Refuse a catalog-scoped role ``action`` aimed at the platform schema.

    Catalog grants/revokes are inherently tenant-scoped, so ``"iam"`` is
    never a valid target — it only arises when tenant-schema resolution
    silently fell back (see #1698). Raising here turns that silent
    misroute into a loud, debuggable error instead of a 204 over an empty
    write.
    """
    if catalog_schema == "iam":
        raise ValueError(
            f"Refusing to {action} catalog-scoped role against the platform "
            f"'iam' schema (role={role_name!r}, principal={principal_id}). "
            "The tenant schema failed to resolve — resolve it with "
            "strict=True before calling (see #1698)."
        )


class PostgresIamStorage(AbstractIamStorage, AuthorizationStorageProtocol):
    engine: Optional[DbResource] = None

    _ROLE_HIERARCHY_TTL = 60  # seconds

    def __init__(self, app_state: Optional[object] = None) -> None:
        super().__init__()
        self.engine = get_engine()
        self._known_partitions = set()
        self._role_hierarchy_cache: Dict[tuple, tuple] = {}  # (roles_key, schema) -> (result, timestamp)

    async def initialize(self, conn: DbResource, schema: str = "iam"):
        """Compatibility alias for _initialize_schema."""
        return await self._initialize_schema(conn, schema=schema)

    async def _initialize_schema(self, conn: DbResource, schema: str = "iam"):
        """Initialize the IAM storage backend for a specific schema.

        Platform schema (`iam`) gets: principals, identity_links, roles +
        role_hierarchy (platform role registry), unified `grants` table,
        refresh tokens, policies (partitioned), audit log, and the
        nightly prune cron job.

        Tenant schemas only get the per-scope IAM tables — `roles`,
        `role_hierarchy`, `grants`, and the partitioned `policies` table
        (consumed via `PolicyService._resolve_schema(catalog_id)`).
        `principals` / `identity_links` / `refresh_tokens` / `audit_log` /
        `usage_counters` live platform-only: every read/write path pins
        `schema="iam"`, so the tenant copies were dead weight. The
        catalog provisioning lifecycle hook
        (`catalog_service._build_tenant_core_ddl_batch`) bootstraps the
        tenant subset; this method overlaps with it (idempotent CREATE
        TABLE IF NOT EXISTS) and adds the `policies` table that the
        catalog hook doesn't.

        Uses DDLBatch sentinel check — on warm start, 1 query confirms
        all tables exist.
        """
        # Strip quotes just in case, to prevent double quoting.
        schema = schema.strip('"')

        logger.info(
            f"Initializing PostgresIamStorage schemas and tables for '{schema}'..."
        )

        # 0. Ensure Schema
        await maintenance_tools.ensure_schema_exists(conn, schema)

        # 1. Base Tables — DDLBatch checks the sentinel once; if it exists
        #    the entire batch is skipped in 1 query.
        #
        # Sentinel = CREATE_GRANTS_TABLE: the unified grants table is the
        # newest addition (Option B hard cut). Picking this as the sentinel
        # means existing dev DBs from before the cut still trigger a full
        # re-run of the (idempotent CREATE TABLE IF NOT EXISTS) batch, so
        # they pick up the new table without requiring `docker compose
        # down -v`. All other steps are no-ops on warm DBs.
        # Sentinel bumped to CREATE_USAGE_COUNTERS_TABLE: it is the newest
        # platform table; older dev DBs (created before the counter table
        # landed) re-run the idempotent CREATE TABLE IF NOT EXISTS batch
        # to pick it up. Existing tables are no-ops on warm DBs.
        # usage_counters sink: flat by default; HASH-partitioned by
        # principal_key on a fresh platform schema when the operator opts in
        # via IAM_USAGE_COUNTER_HASH_PARTITIONS (#1344). Partitioning only
        # in the ``iam`` schema (where the counter driver reads/writes) and
        # only on a fresh table — the sentinel below skips the whole batch
        # once the table exists, so existing flat tables stay flat.
        from .scale_config import usage_counter_hash_partitions

        # Split platform-only DDL from per-tenant DDL. Tenant schemas only
        # need the per-scope IAM tables (`roles`, `role_hierarchy`,
        # `grants`, `policies`); identity / token / audit / counter tables
        # are platform-global and live exclusively in `iam`. Historically
        # this batch ran unconditionally for every schema, which left a
        # set of empty `{tenant}.principals` / `identity_links` /
        # `refresh_tokens` / `audit_log` / `usage_counters` tables in
        # every catalog schema — never written, never read (every code
        # path pins `schema="iam"`). Splitting here eliminates them on
        # fresh DBs.
        is_platform = schema == "iam"

        tenant_steps = [
            CREATE_ROLES_TABLE,
            CREATE_ROLE_HIERARCHY_TABLE,
            CREATE_GRANTS_TABLE,
            CREATE_POLICIES_TABLE,
        ]

        if is_platform:
            _hash_parts = usage_counter_hash_partitions()
            _usage_counter_steps = build_usage_counters_steps(_hash_parts)
            platform_steps = [
                CREATE_PRINCIPALS_TABLE,
                CREATE_IDENTITY_LINKS_TABLE,
                CREATE_REFRESH_TOKENS_TABLE,
                CREATE_AUDIT_LOG_TABLE,
                *_usage_counter_steps,
            ]
            sentinel = CREATE_USAGE_COUNTERS_TABLE
        else:
            platform_steps = []
            # Tenant sentinel: `grants` is the newest of the per-scope
            # tables; if it exists the whole batch can be skipped on warm
            # DBs (mirrors the cold-cut rationale above, scoped to the
            # tenant set).
            sentinel = CREATE_GRANTS_TABLE

        await DDLBatch(
            sentinel=sentinel,
            steps=[*tenant_steps, *platform_steps],
        ).execute(conn, schema=schema)

        # 1b. Grants indexes — the unique index expresses the (subject,
        # object, effect, resource) constraint and the partial index
        # covers the resource-scoped lookup path. The DROP cleans up a
        # pre-resource-scope auto-named UNIQUE constraint if one is
        # carried in from an older snapshot; on a fresh DB it finds no
        # matching constraint and is a no-op. The DROP is a str.format
        # template (its {schema} sits in a SQL string literal that the
        # DDLQuery identifier-quoter would mangle), wrapped in a fresh
        # DDLQuery — same pattern as the prune-function DDL below.
        await DDLQuery(DROP_OLD_GRANTS_UNIQUE.format(schema=schema)).execute(conn)
        await CREATE_GRANTS_UNIQUE_WITH_RESOURCE.execute(conn, schema=schema)
        await CREATE_IDX_GRANTS_RESOURCE.execute(conn, schema=schema)

        # Partition tables (IF NOT EXISTS in SQL handles idempotency)
        if schema in ["iam"]:
            await CREATE_PARTITION_GLOBAL.execute(conn, schema=schema)

        # 2. Maintenance (System Schema Only)
        if schema == "iam":
            # Register nightly pruning of expired rows in flat tables
            _prune_job_name = f"prune_expired_{schema}"
            _prune_func_name = f"prune_expired_rows_{schema}"

            async def _check_prune_job_exists():
                from dynastore.modules.db_config.locking_tools import check_cron_job_exists
                return await check_cron_job_exists(conn, _prune_job_name)

            # Function body is refreshed on every boot — ``CREATE OR
            # REPLACE FUNCTION`` is idempotent and cheap, and gating it
            # behind the cron-job existence check would freeze warm DBs
            # on the old body when the prune logic changes (e.g. a new
            # ``DELETE`` line for a newly-added table).
            #
            # ``usage_counters`` reaper lines are sourced from SSOT
            # constants in ``iam_queries`` — the in-process
            # ``PostgresUsageCounter`` driver uses the same strings so
            # the two paths cannot drift on grace-period or join tweaks.
            _reap_usage_counters_sql = REAP_EXPIRED_USAGE_COUNTERS_SQL.format(
                schema=schema
            )
            _reap_orphan_usage_counters_sql = REAP_ORPHAN_USAGE_COUNTERS_SQL.format(
                schema=schema
            )
            _prune_function_ddl = f"""
            CREATE OR REPLACE FUNCTION "{schema}"."{_prune_func_name}"() RETURNS void AS $$
            BEGIN
                -- Expired refresh tokens
                DELETE FROM "{schema}".refresh_tokens WHERE expires_at < NOW();

                -- Expired OAuth2 authorisation codes (short-lived, ~10 min)
                DELETE FROM "{schema}".oauth_codes WHERE expires_at < NOW();

                -- Expired OAuth2 access/bearer tokens
                DELETE FROM "{schema}".oauth_tokens WHERE expires_at < NOW();

                -- Expired grants (D13 — cleanup runs even though the
                -- resolver already filters by valid_until at query time;
                -- prevents unbounded grants-table growth).
                DELETE FROM "{schema}".grants
                  WHERE valid_until IS NOT NULL AND valid_until < NOW();

                -- Expired usage counter buckets (rate-limit windows past
                -- their grace period). Lifetime counters
                -- (expires_at IS NULL) are kept indefinitely.
                {_reap_usage_counters_sql}

                -- Orphan lifetime usage counters whose parent policy is
                -- gone. Defence-in-depth — the transactional path on
                -- DELETE_POLICY (gap #2) already drops these rows; this
                -- catches non-transactional orphans (manual SQL, partial
                -- backup restore). last_seen_at intentionally NOT used —
                -- lifetime means lifetime; a quiet principal must keep
                -- their counter.
                {_reap_orphan_usage_counters_sql}
            END;
            $$ LANGUAGE plpgsql;
            """

            await DDLQuery(_prune_function_ddl).execute(conn)

            # Cron registration is idempotent (unschedule-if-exists
            # then re-schedule) and the command never changes, so the
            # job-exists check_query can safely short-circuit warm
            # boots once the schedule entry is in place.
            _cron_schedule_ddl = f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{_prune_job_name}') THEN
                    PERFORM cron.unschedule('{_prune_job_name}');
                END IF;
            END;
            $$;

            SELECT cron.schedule('{_prune_job_name}', '0 4 * * *',
                $CMD$SELECT "{schema}"."{_prune_func_name}"()$CMD$);
            """

            await DDLQuery(
                _cron_schedule_ddl,
                check_query=_check_prune_job_exists,
            ).execute(conn)

        logger.info(f"PostgresIamStorage initialization complete for '{schema}'.")

    # ------------------------------------------------------------------
    # Principal CRUD — platform-global (lives only in `iam` schema).
    # `schema=` is hardcoded internally per D12; callers do not pass it.
    # ------------------------------------------------------------------

    async def create_principal(
        self,
        principal: Principal,
        conn: Optional[DbResource] = None,
    ) -> Principal:
        async with managed_transaction(conn or self.engine) as db:
            # Logic: If principal has provider AND subject_id, we use
            # "provider:subject_id" as the identifier for backward
            # compatibility with systems that expect a single string
            # identifier. Otherwise we fall back to the explicit
            # identifier attribute or the display_name.
            if principal.provider and principal.subject_id:
                identifier = f"{principal.provider}:{principal.subject_id}"
            else:
                identifier = getattr(principal, "identifier", principal.display_name)

            # Role grants are no longer stored on the principal row.
            # `principal.roles` (if present) is honored by the higher-level
            # service via grant_platform_role / grant_catalog_role at the
            # right scope; storage never persists it on `principals`.
            return await INSERT_PRINCIPAL.execute(
                db,
                schema="iam",
                id=principal.id,
                identifier=identifier,
                display_name=principal.display_name,
                is_active=principal.is_active,
                valid_until=principal.valid_until,
                metadata=json.dumps(getattr(principal, "metadata", {})),
                custom_policies=json.dumps(
                    [p.model_dump() for p in principal.custom_policies]
                ),
                attributes=json.dumps(principal.attributes),
            )

    async def get_principal(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_ID.execute(db, schema="iam", id=principal_id)

    async def update_principal(
        self,
        principal: Principal,
        conn: Optional[DbResource] = None,
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            result = await UPDATE_PRINCIPAL.execute(
                db,
                schema="iam",
                id=principal.id,
                identifier=getattr(principal, "identifier", principal.display_name),
                display_name=principal.display_name,
                is_active=principal.is_active,
                metadata=json.dumps(getattr(principal, "metadata", {})),
                custom_policies=json.dumps(
                    [p.model_dump() for p in principal.custom_policies]
                ),
                attributes=json.dumps(principal.attributes),
            )
        # A principal edit (is_active flip, custom_policies, expiry) is a
        # binding-affecting mutation: invalidate the phantom-token cache so a
        # deactivation / expiry propagates on the next request, not only at TTL.
        # Principals live in the platform ("iam") schema, whose version is part
        # of every resolution key, so this invalidates every schema's entries.
        await self._bump_binding_version("iam")
        return result

    async def get_principal_by_identifier(
        self, identifier: str, conn: Optional[DbResource] = None,
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_IDENTIFIER.execute(
                db, schema="iam", identifier=identifier
            )

    async def delete_principal(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_PRINCIPAL.execute(db, schema="iam", id=principal_id)
            return count > 0

    async def get_principal_id_by_identifier(
        self, identifier: str, conn: Optional[DbResource] = None,
    ) -> Optional[UUID]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_ID_BY_IDENTIFIER.execute(
                db, schema="iam", identifier=identifier
            )

    async def list_principals(
        self,
        offset: int,
        limit: int,
        conn: Optional[DbResource] = None,
    ) -> List[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_PRINCIPALS.execute(
                db, schema="iam", offset=offset, limit=limit
            )

    # --- Enhanced Methods ---

    async def search_principals(
        self,
        identifier: Optional[str] = None,
        role: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> List[Principal]:
        """Search principals by identifier and/or role.

        `schema` selects which `grants` table the role-filter joins
        through (platform `iam.grants` vs. a tenant's). Principals
        themselves are always read from `iam.principals`.
        """
        query, params = build_search_principals_query(
            identifier, role, limit, offset, schema=schema
        )
        async with managed_transaction(conn or self.engine) as db:
            return await query.execute(db, **params)

    async def add_role_hierarchy(
        self,
        parent_role: str,
        child_role: str,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ):
        async with managed_transaction(conn or self.engine) as db:
            await INSERT_ROLE_HIERARCHY.execute(
                db, schema=schema, parent_role=parent_role, child_role=child_role
            )
            self.invalidate_role_hierarchy_cache(schema)
        await self._bump_binding_version(schema)

    async def remove_role_hierarchy(
        self,
        parent_role: str,
        child_role: str,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_ROLE_HIERARCHY.execute(
                db, schema=schema, parent_role=parent_role, child_role=child_role
            )
            self.invalidate_role_hierarchy_cache(schema)
        await self._bump_binding_version(schema)
        return count > 0

    def invalidate_role_hierarchy_cache(self, schema: str = "iam") -> None:
        """Clear role hierarchy cache entries for a schema (call on role CRUD)."""
        self._role_hierarchy_cache = {
            k: v for k, v in self._role_hierarchy_cache.items() if k[1] != schema
        }

    async def get_role_hierarchy(
        self,
        role_names: List[str],
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> List[str]:
        if not role_names:
            return []

        cache_key = (tuple(sorted(role_names)), schema)
        cached = self._role_hierarchy_cache.get(cache_key)
        if cached:
            result, ts = cached
            if time.monotonic() - ts < self._ROLE_HIERARCHY_TTL:
                return list(result)

        async with managed_transaction(conn or self.engine) as db:
            children = await GET_FULL_ROLE_HIERARCHY.execute(
                db, schema=schema, role_names=role_names
            )
            merged = list(set(role_names + children))
            self._role_hierarchy_cache[cache_key] = (merged, time.monotonic())
            return merged

    async def list_role_hierarchy_edges(
        self,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> List[tuple[str, str]]:
        """Return all (parent_role, child_role) edges stored in the hierarchy table."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_ROLE_HIERARCHY_EDGES.execute(db, schema=schema)

    async def create_refresh_token(
        self,
        token: RefreshToken,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> RefreshToken:
        async with managed_transaction(conn or self.engine) as db:
            return await INSERT_REFRESH_TOKEN.execute(
                db,
                schema=schema,
                id=token.id,
                key_hash=token.key_hash,
                principal_id=token.principal_id,
                family_id=token.family_id,
                expires_at=token.expires_at,
            )

    async def get_refresh_token(
        self, token_id: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Optional[RefreshToken]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_REFRESH_TOKEN.execute(db, schema=schema, id=token_id)

    async def invalidate_refresh_token(
        self, token_id: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            result = await INVALIDATE_REFRESH_TOKEN.execute(
                db, schema=schema, id=token_id
            )
            return result is not None

    async def invalidate_token_family(
        self, family_id: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> int:
        """Invalidate all active refresh tokens in a family (reuse detection)."""
        async with managed_transaction(conn or self.engine) as db:
            rows = await INVALIDATE_REFRESH_TOKEN_FAMILY.execute(
                db, schema=schema, family_id=family_id
            )
            return len(rows) if rows else 0

    async def log_audit_event(
        self,
        event_type: str,
        principal_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        detail: Optional[Dict[str, Any]] = None,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> None:
        """Write a structured audit log entry."""
        try:
            async with managed_transaction(conn or self.engine) as db:
                await INSERT_AUDIT_EVENT.execute(
                    db,
                    schema=schema,
                    event_type=event_type,
                    principal_id=principal_id,
                    ip_address=ip_address,
                    detail=json.dumps(detail or {}),
                )
        except Exception:
            logger.debug("audit log write failed (table may not exist yet)", exc_info=True)

    async def run_maintenance(
        self, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> dict:
        """Runs storage-specific maintenance (e.g. pruning expired tokens)."""
        logger.info(
            f"Running maintenance for PostgresIamStorage (schema: {schema})..."
        )
        async with managed_transaction(conn or self.engine) as db:
            pruned_tokens = await PRUNE_EXPIRED_REFRESH_TOKENS.execute(
                db, schema=schema
            )

            return {
                "pruned_refresh_tokens": pruned_tokens,
            }

    # ------------------------------------------------------------------
    # SPI V2 — identity → principal resolution.
    # ------------------------------------------------------------------

    async def get_principal_by_identity(
        self,
        provider: str,
        subject_id: str,
        conn: Optional[DbResource] = None,
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_IDENTITY.execute(
                db, schema="iam", provider=provider, subject_id=subject_id
            )

    async def create_principal_link(
        self,
        principal: Principal,
        identity: Dict[str, Any],
        conn: Optional[DbResource] = None,
    ) -> Principal:
        """Insert a Principal + identity_link in one transaction. Both
        live in `iam`; tenant scopes never get principal/link rows.
        """
        async with managed_transaction(conn or self.engine) as db:
            p = await INSERT_PRINCIPAL.execute(
                db,
                schema="iam",
                id=principal.id,
                display_name=principal.display_name,
                is_active=principal.is_active,
                valid_until=principal.valid_until,
                custom_policies=json.dumps(
                    [p.model_dump() for p in principal.custom_policies]
                ),
                attributes=json.dumps(principal.attributes),
                identifier=getattr(
                    principal, "identifier", principal.display_name
                ),
                metadata=json.dumps(getattr(principal, "metadata", {})),
            )

            await INSERT_IDENTITY_LINK.execute(
                db,
                schema="iam",
                provider=identity.get("provider"),
                subject_id=identity.get("sub"),
                principal_id=principal.id,
                email=identity.get("email"),
            )
            return p

    async def get_effective_roles(
        self,
        principal_id: str,
        catalog_schema: Optional[str] = None,
        conn: Optional[DbResource] = None,
    ) -> List[str]:
        """Effective role list for a principal — direct grants only.

        Platform roles unioned with catalog-scoped roles when
        `catalog_schema` is provided. Role-hierarchy expansion is
        handled separately by `get_role_hierarchy` because hierarchy
        lives next to the role definitions, which are now per-tenant.
        """
        pid = UUID(principal_id) if isinstance(principal_id, str) else principal_id
        platform = await self.list_platform_roles(principal_id=pid, conn=conn)
        catalog: List[str] = []
        if catalog_schema and catalog_schema != "iam":
            catalog = await self.list_catalog_roles(
                principal_id=pid, catalog_schema=catalog_schema, conn=conn
            )
        return list({*platform, *catalog})

    # ------------------------------------------------------------------
    # Identity Link Management — platform-only (D12).
    # ------------------------------------------------------------------

    async def create_identity_link(
        self,
        principal_id: UUID,
        provider: str,
        subject_id: str,
        email: Optional[str] = None,
        conn: Optional[DbResource] = None,
    ) -> bool:
        """Create an identity link for a principal in `iam.identity_links`."""
        async with managed_transaction(conn or self.engine) as db:
            count = await INSERT_IDENTITY_LINK.execute(
                db,
                schema="iam",
                provider=provider,
                subject_id=subject_id,
                principal_id=principal_id,
                email=email,
            )
            return count > 0

    async def list_identity_links(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
    ) -> List[IdentityLink]:
        """List all identity links for a principal."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_IDENTITY_LINKS.execute(
                db, schema="iam", principal_id=principal_id
            )

    async def delete_identity_link(
        self,
        provider: str,
        subject_id: str,
        conn: Optional[DbResource] = None,
    ) -> bool:
        """Delete an identity link from `iam.identity_links`."""
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_IDENTITY_LINK.execute(
                db, schema="iam", provider=provider, subject_id=subject_id
            )
            return count > 0

    async def _bump_binding_version(self, schema: str) -> None:
        """Best-effort invalidation of the phantom-token cache (#1343).

        No-op unless the phantom cache is active (Valkey + flag); never raises.
        """
        from dynastore.modules.iam.phantom_token import bump_binding_version

        await bump_binding_version(schema)

    # ------------------------------------------------------------------
    # Role definitions — per-scope. `schema=` here denotes the role
    # registry (iam for platform, tenant schema for catalog roles).
    # ------------------------------------------------------------------

    async def create_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Role:
        async with managed_transaction(conn or self.engine) as db:
            result = await INSERT_ROLE.execute(
                db,
                schema=schema,
                id=role.id,
                name=role.name,
                description=role.description,
                metadata=json.dumps(role.metadata),
                policies=json.dumps(role.policies),
            )
        await self._bump_binding_version(schema)
        return result

    async def get_role(
        self, role_id: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Optional[Role]:
        try:
            async with managed_transaction(conn or self.engine) as db:
                return await GET_ROLE.execute(db, schema=schema, name=role_id)
        except TableNotFoundError:
            return None

    async def list_roles(
        self, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> List[Role]:
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_ROLES.execute(db, schema=schema)

    async def update_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Optional[Role]:
        async with managed_transaction(conn or self.engine) as db:
            result = await UPDATE_ROLE.execute(
                db,
                schema=schema,
                name=role.name,
                description=role.description,
                metadata=json.dumps(role.metadata),
                policies=json.dumps(role.policies),
            )
        await self._bump_binding_version(schema)
        return result

    async def bind_policy_to_role(
        self, role_name: str, policy_entry: Dict[str, Any], schema: str = "iam"
    ) -> None:
        policy_id: str = policy_entry["id"]
        async with managed_transaction(self.engine) as db:
            await BIND_POLICY_TO_ROLE.execute(
                db, schema=schema, role_name=role_name, policy_id=policy_id,
            )
        await self._bump_binding_version(schema)

    async def unbind_policy_from_role(
        self, role_name: str, policy_id: str, schema: str = "iam"
    ) -> None:
        async with managed_transaction(self.engine) as db:
            await UNBIND_POLICY_FROM_ROLE.execute(
                db, schema=schema, role_name=role_name, policy_id=policy_id,
            )
        await self._bump_binding_version(schema)

    async def delete_role(
        self, role_id: str, cascade: bool = False, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            await DELETE_ROLE.execute(db, schema=schema, name=role_id)
        await self._bump_binding_version(schema)
        return True

    # ------------------------------------------------------------------
    # Principal-backed compatibility methods (replaced IAG tables in v1.0)
    # ------------------------------------------------------------------

    async def _resolve_principal_by_identity(
        self, provider: str, subject_id: str, conn: Optional[DbResource] = None,
    ) -> Optional["Principal"]:
        """Resolve (provider, subject_id) → Principal via identity_links.

        Identity links live on the platform `iam` schema by design —
        principals are platform-global; their grants are scope-specific
        and live in the per-scope `grants` tables.
        """
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_IDENTITY.execute(
                conn=db, schema="iam", provider=provider, subject_id=subject_id,
            )

    async def resolve_identity(
        self, email: str, conn: Optional[DbResource] = None,
    ) -> tuple[str, str]:
        """Resolve an email to ``(provider, subject_id)``.

        Looks up identity_links.email first; falls back to the linked
        principal's identifier / display_name so principals created with
        an email identifier but no email on the link still resolve.
        Raises ``ValueError`` when no identity matches — the admin API
        surfaces this as 404 via ``http_errors({ValueError: 404})``.
        """
        async with managed_transaction(conn or self.engine) as db:
            row = await RESOLVE_IDENTITY_BY_EMAIL.execute(
                conn=db, schema="iam", email=email,
            )
        if not row:
            raise ValueError(f"No identity found for email: {email}")
        return row["provider"], row["subject_id"]

    # ==================================================================
    # Unified grants surface — single source of truth for "who can do
    # what". The generic primitives (`grant`, `revoke`, `revoke_by_match`,
    # `list_grants_for_subject`, `list_grants_for_object`) operate on the
    # raw `{schema}.grants` table; the role-shaped facades wrap them with
    # the right (subject_kind=principal, object_kind=role, effect=allow)
    # combination so existing callers don't need to know about the
    # broader model.
    # ==================================================================

    # ---- Generic primitives ----

    async def grant(
        self,
        scope_schema: str,
        subject_kind: str,
        subject_ref: str,
        object_kind: str,
        object_ref: str,
        effect: str = EFFECT_ALLOW,
        valid_from: Optional[datetime] = None,
        valid_until: Optional[datetime] = None,
        conditions: Optional[Dict[str, Any]] = None,
        quota: Optional[Dict[str, Any]] = None,
        granted_by: Optional[UUID] = None,
        resource_kind: Optional[str] = None,
        resource_ref: Optional[str] = None,
        attribute_predicates: Optional[List[Dict[str, Any]]] = None,
        conn: Optional[DbResource] = None,
    ) -> Optional[UUID]:
        """Insert (or refresh) a grant row in `{scope_schema}.grants`.

        Returns the grant id. Idempotent on
        (subject_kind, subject_ref, object_kind, object_ref, effect,
        resource_kind, resource_ref): a second call with the same tuple
        updates the time/condition/quota/attribute_predicates columns and
        bumps `granted_at`.

        `resource_kind`/`resource_ref` scope the grant to a specific
        resource within the scope schema (e.g. ``"collection"`` + a
        collection id). ``None``/``None`` (the default) is a whole-catalog
        grant — the historical behaviour.

        `attribute_predicates` is a list of ABAC predicate dicts applied at
        query time (document-level row filter); ``None`` leaves the column
        at its ``'[]'::jsonb`` default (no additional restriction — the grant
        behaves as a plain RBAC binding). See ``attribute_predicates.py``
        for the supported ops; refs #1443.
        """
        async with managed_transaction(conn or self.engine) as db:
            result = await INSERT_GRANT.execute(
                db,
                schema=scope_schema,
                subject_kind=subject_kind,
                subject_ref=subject_ref,
                object_kind=object_kind,
                object_ref=object_ref,
                effect=effect,
                valid_from=valid_from,
                valid_until=valid_until,
                conditions=json.dumps(conditions) if conditions is not None else None,
                quota=json.dumps(quota) if quota is not None else None,
                granted_by=granted_by,
                resource_kind=resource_kind,
                resource_ref=resource_ref,
                attribute_predicates=(
                    json.dumps(attribute_predicates)
                    if attribute_predicates is not None
                    else None
                ),
            )
        await self._bump_binding_version(scope_schema)
        return result

    async def revoke(
        self,
        grant_id: UUID,
        scope_schema: str,
        conn: Optional[DbResource] = None,
    ) -> bool:
        """Delete a grant by id."""
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_GRANT_BY_ID.execute(
                db, schema=scope_schema, id=grant_id
            )
        await self._bump_binding_version(scope_schema)
        return count > 0

    async def revoke_by_match(
        self,
        scope_schema: str,
        subject_kind: str,
        subject_ref: str,
        object_kind: str,
        object_ref: str,
        effect: str = EFFECT_ALLOW,
        resource_kind: Optional[str] = None,
        resource_ref: Optional[str] = None,
        conn: Optional[DbResource] = None,
    ) -> int:
        """Delete every grant matching (subject_kind, subject_ref,
        object_kind, object_ref, effect, resource_kind, resource_ref).
        Returns rows deleted.

        ``resource_kind``/``resource_ref`` default to ``None`` (whole-catalog
        row); a scoped revoke removes only the matching scoped row and leaves
        the whole-catalog row intact.
        """
        async with managed_transaction(conn or self.engine) as db:
            result = await DELETE_GRANTS_BY_MATCH.execute(
                db,
                schema=scope_schema,
                subject_kind=subject_kind,
                subject_ref=subject_ref,
                object_kind=object_kind,
                object_ref=object_ref,
                effect=effect,
                resource_kind=resource_kind,
                resource_ref=resource_ref,
            )
        await self._bump_binding_version(scope_schema)
        return result

    async def list_grants_for_subject(
        self,
        scope_schema: str,
        subject_kind: str,
        subject_ref: str,
        conn: Optional[DbResource] = None,
    ) -> List[Dict[str, Any]]:
        """Every grant where this subject is the actor."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_GRANTS_FOR_SUBJECT.execute(
                db,
                schema=scope_schema,
                subject_kind=subject_kind,
                subject_ref=subject_ref,
            ) or []

    async def list_grants_for_object(
        self,
        scope_schema: str,
        object_kind: str,
        object_ref: str,
        conn: Optional[DbResource] = None,
    ) -> List[Dict[str, Any]]:
        """Every grant referencing this object (used by the
        `?cascade=true` precheck on role/policy delete).
        """
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_GRANTS_FOR_OBJECT.execute(
                db,
                schema=scope_schema,
                object_kind=object_kind,
                object_ref=object_ref,
            ) or []

    async def list_grants_for_resource(
        self,
        scope_schema: str,
        resource_kind: str,
        resource_ref: str,
        conn: Optional[DbResource] = None,
    ) -> List[Dict[str, Any]]:
        """Every grant scoped to a specific resource (the Admin UI reverse
        "who has access to collection C" view, #1342). Whole-catalog grants
        (``resource_kind IS NULL``) are not included — this is the
        per-resource view.
        """
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_GRANTS_FOR_RESOURCE.execute(
                db,
                schema=scope_schema,
                resource_kind=resource_kind,
                resource_ref=resource_ref,
            ) or []

    async def count_grants_for_object(
        self,
        scope_schema: str,
        object_kind: str,
        object_ref: str,
        conn: Optional[DbResource] = None,
    ) -> int:
        """Count grants referencing an object (used by 409
        `object_in_use` checks before role/policy delete).
        """
        async with managed_transaction(conn or self.engine) as db:
            n = await COUNT_GRANTS_FOR_OBJECT.execute(
                db,
                schema=scope_schema,
                object_kind=object_kind,
                object_ref=object_ref,
            )
            return int(n or 0)

    # ---- Role-shaped facades over the unified table ----

    async def grant_platform_role(
        self,
        principal_id: UUID,
        role_name: str,
        granted_by: Optional[UUID] = None,
        conn: Optional[DbResource] = None,
    ) -> None:
        """Grant a platform-scope role to a principal (writes `iam.grants`)."""
        await self.grant(
            scope_schema="iam",
            subject_kind=SUBJECT_PRINCIPAL,
            subject_ref=str(principal_id),
            object_kind=OBJECT_ROLE,
            object_ref=role_name,
            granted_by=granted_by,
            conn=conn,
        )

    async def revoke_platform_role(
        self,
        principal_id: UUID,
        role_name: str,
        conn: Optional[DbResource] = None,
    ) -> bool:
        """Revoke a platform-scope role from a principal."""
        count = await self.revoke_by_match(
            scope_schema="iam",
            subject_kind=SUBJECT_PRINCIPAL,
            subject_ref=str(principal_id),
            object_kind=OBJECT_ROLE,
            object_ref=role_name,
            conn=conn,
        )
        return count > 0

    async def list_platform_roles(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
    ) -> List[str]:
        """List platform-scope roles granted to a principal (active only)."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_ROLE_NAMES_FOR_PRINCIPAL.execute(
                db, schema="iam", principal_id=str(principal_id)
            ) or []

    async def grant_catalog_role(
        self,
        principal_id: UUID,
        role_name: str,
        catalog_schema: str,
        granted_by: Optional[UUID] = None,
        collection_id: Optional[str] = None,
        conn: Optional[DbResource] = None,
    ) -> None:
        """Grant a catalog-scope role to a principal in `catalog_schema`.

        ``collection_id`` (optional) scopes the grant to a single
        collection within the catalog; ``None`` (default) is a
        whole-catalog grant.
        """
        # Defense-in-depth for the #1698 silent-misroute class: a
        # catalog-scoped grant resolved to the platform 'iam' schema would
        # be written where the catalog never reads it back, while the
        # endpoint still returns 204. Refuse it loudly. The caller is
        # expected to resolve the tenant schema with ``strict=True``; this
        # guard catches any path that did not.
        _reject_platform_schema(catalog_schema, "grant", role_name, principal_id)
        resource_kind = SUBJECT_COLLECTION if collection_id else None
        await self.grant(
            scope_schema=catalog_schema,
            subject_kind=SUBJECT_PRINCIPAL,
            subject_ref=str(principal_id),
            object_kind=OBJECT_ROLE,
            object_ref=role_name,
            granted_by=granted_by,
            resource_kind=resource_kind,
            resource_ref=collection_id,
            conn=conn,
        )

    async def revoke_catalog_role(
        self,
        principal_id: UUID,
        role_name: str,
        catalog_schema: str,
        collection_id: Optional[str] = None,
        conn: Optional[DbResource] = None,
    ) -> bool:
        """Revoke a catalog-scope role from a principal.

        ``collection_id`` (optional) targets a collection-scoped grant;
        ``None`` (default) targets the whole-catalog grant.
        """
        # Symmetric guard with ``grant_catalog_role`` (see #1698): a
        # catalog revoke that resolved to 'iam' would target the wrong
        # table and silently report success against no rows.
        _reject_platform_schema(catalog_schema, "revoke", role_name, principal_id)
        resource_kind = SUBJECT_COLLECTION if collection_id else None
        count = await self.revoke_by_match(
            scope_schema=catalog_schema,
            subject_kind=SUBJECT_PRINCIPAL,
            subject_ref=str(principal_id),
            object_kind=OBJECT_ROLE,
            object_ref=role_name,
            resource_kind=resource_kind,
            resource_ref=collection_id,
            conn=conn,
        )
        return count > 0

    async def list_catalog_roles(
        self,
        principal_id: UUID,
        catalog_schema: str,
        conn: Optional[DbResource] = None,
    ) -> List[str]:
        """List catalog-scope roles granted to a principal."""
        try:
            async with managed_transaction(conn or self.engine) as db:
                return await LIST_ROLE_NAMES_FOR_PRINCIPAL.execute(
                    db, schema=catalog_schema, principal_id=str(principal_id)
                ) or []
        except TableNotFoundError:
            # New catalog still provisioning, or stale schema — caller
            # decides whether to retry or treat as empty.
            return []

    async def get_identity_roles(
        self,
        provider: str,
        subject_id: str,
        catalog_schema: Optional[str] = None,
        conn: Optional[DbResource] = None,
    ) -> List[str]:
        """Effective roles for an identity = platform ∪ catalog-roles.

        - `catalog_schema=None` returns platform roles only.
        - `catalog_schema=<tenant>` returns platform ∪ catalog-roles for
          that tenant. The two scopes are unioned (additive); there is
          no inheritance across scopes (per design — sysadmin bypass is
          a middleware-level rule, not a role-graph rule).
        """
        async with managed_transaction(conn or self.engine) as db:
            platform = await LIST_ROLE_NAMES_FOR_IDENTITY.execute(
                db, schema="iam", provider=provider, subject_id=subject_id
            ) or []

            catalog: List[str] = []
            if catalog_schema and catalog_schema != "iam":
                try:
                    catalog = await LIST_ROLE_NAMES_FOR_IDENTITY.execute(
                        db,
                        schema=catalog_schema,
                        provider=provider,
                        subject_id=subject_id,
                    ) or []
                except TableNotFoundError:
                    # Catalog still provisioning — platform roles still
                    # apply, just no catalog-scoped grants yet.
                    catalog = []

            return list({*platform, *catalog})

    # ---- Resolver (skeleton) ----

    async def resolve_effective_grants(
        self,
        principal_id: UUID,
        catalog_schema: Optional[str] = None,
        request_path: Optional[str] = None,
        collection_id: Optional[str] = None,
        conn: Optional[DbResource] = None,
    ) -> List[Dict[str, Any]]:
        """Return the time-active direct grants for a principal across
        platform + (optional) catalog scopes.

        When ``collection_id`` is set, the catalog-schema grants are read
        through the resource-scoped query: whole-catalog grants
        (``resource_kind IS NULL``) PLUS grants scoped to that collection
        are returned (allows additive; deny precedence applied by the
        caller). When ``collection_id`` is ``None`` the historical
        unscoped query is used, surfacing every catalog grant regardless
        of resource scope. Platform (``iam``) grants always use the
        unscoped query — platform grants are never collection-scoped.

        PR-1 skeleton otherwise unchanged: only direct principal grants
        are computed. Deny precedence (D9), conditions (D-PR2), and quota
        (D-PR2) are evaluated by the caller (PolicyService) using the
        raw rows returned here. ``request_path`` is accepted now so
        callers don't have to change signatures when scope cascade lands.
        """
        out: List[Dict[str, Any]] = []
        async with managed_transaction(conn or self.engine) as db:
            platform = await LIST_TIMEACTIVE_GRANTS_FOR_PRINCIPAL.execute(
                db, schema="iam", principal_id=str(principal_id)
            ) or []
            out.extend(platform)

            if catalog_schema and catalog_schema != "iam":
                try:
                    if collection_id is not None:
                        catalog = await LIST_TIMEACTIVE_SCOPED_GRANTS_FOR_PRINCIPAL.execute(
                            db,
                            schema=catalog_schema,
                            principal_id=str(principal_id),
                            resource_kind=SUBJECT_COLLECTION,
                            resource_ref=collection_id,
                        ) or []
                    else:
                        catalog = await LIST_TIMEACTIVE_GRANTS_FOR_PRINCIPAL.execute(
                            db, schema=catalog_schema, principal_id=str(principal_id)
                        ) or []
                    out.extend(catalog)
                except TableNotFoundError:
                    pass

        return out

    # ------------------------------------------------------------------
    # Identity-side authorization helpers (used by middleware /
    # request-scoped permission checks).
    # ------------------------------------------------------------------

    async def get_identity_authorization(
        self,
        provider: str,
        subject_id: str,
        conn: Optional[DbResource] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get authorization metadata for an identity.

        Principals live exclusively in the platform `iam` schema, so
        this no longer takes a `schema` parameter — there is no
        per-tenant principals table to look at.
        """
        principal = await self._resolve_principal_by_identity(provider, subject_id, conn)
        if not principal:
            return None
        return {
            "provider": provider,
            "subject_id": subject_id,
            "display_name": principal.display_name,
            "is_active": principal.is_active,
            "valid_from": getattr(principal, "valid_from", None),
            "valid_until": principal.valid_until,
            "attributes": principal.attributes or {},
        }

    async def get_identity_policies(
        self,
        provider: str,
        subject_id: str,
        conn: Optional[DbResource] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """Get custom policies for an identity.

        Custom policies live on the platform `principals` row (one row
        per principal across the platform); the `schema` parameter is
        gone for the same reason as above.
        """
        principal = await self._resolve_principal_by_identity(provider, subject_id, conn)
        if not principal:
            return None
        policies = getattr(principal, "custom_policies", None) or getattr(principal, "policies", None)
        if not policies:
            return []
        if isinstance(policies, str):
            return json.loads(policies)
        return policies

    async def get_catalogs_for_identity(
        self, provider: str, subject_id: str
    ) -> List[str]:
        """Get catalog IDs where the identity has at least one grant.

        Thin wrapper around ``get_catalog_roles_for_identity`` for callers
        that only need the membership flag, not the role list.
        """
        roles_map = await self.get_catalog_roles_for_identity(provider, subject_id)
        return list(roles_map.keys())

    async def get_catalog_roles_for_identity(
        self, provider: str, subject_id: str,
    ) -> Dict[str, List[str]]:
        """Get ``{catalog_id: [role_name, ...]}`` for an identity.

        Resolves the principal through ``iam.identity_links`` and then
        iterates active catalogs, querying each tenant's ``grants`` for
        the principal's roles. Catalogs that aren't fully provisioned
        (no ``grants`` table) are skipped via TableNotFoundError.

        For deployments with very many catalogs this remains O(N), but
        the per-catalog query is O(1) on the (subject_kind, subject_ref)
        index, so the overall cost is bounded and avoids any role-name
        probing.
        """
        principal = await self._resolve_principal_by_identity(provider, subject_id)
        if not principal or principal.id is None:
            return {}
        principal_uuid = principal.id if isinstance(principal.id, UUID) else UUID(str(principal.id))

        async with managed_transaction(self.engine) as db:
            rows = await DQLQuery(
                "SELECT id, physical_schema FROM catalog.catalogs "
                "WHERE deleted_at IS NULL ORDER BY id;",
                result_handler=ResultHandler.ALL_DICTS,
            ).execute(conn=db)

        result: Dict[str, List[str]] = {}
        for row in rows or []:
            cid = row.get("id")
            schema = row.get("physical_schema") or row.get("schema")
            if not cid or not schema:
                continue
            try:
                grants = await self.list_catalog_roles(
                    principal_id=principal_uuid, catalog_schema=schema
                )
            except TableNotFoundError:
                continue
            if grants:
                result[cid] = list(grants)
        return result

    async def get_catalog_users(self, catalog_schema: str) -> List[Dict[str, Any]]:
        """List principals with at least one grant in `catalog_schema`.

        Replaces the old `schema=` form that always returned every
        principal globally. The returned shape preserves the previous
        keys (`provider`, `subject_id`, `display_name`, `is_active`)
        plus `id`, so admin endpoints can identify users for catalog
        management.
        """
        async with managed_transaction(self.engine) as db:
            return await LIST_CATALOG_USERS.execute(
                conn=db, schema=catalog_schema
            )
