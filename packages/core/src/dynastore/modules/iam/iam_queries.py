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

from typing import Any, Dict, Optional
from uuid import UUID

from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, ResultHandler
from .models import Principal, Role, RefreshToken, IdentityLink

# --- Queries (IAM Tables) ---

# Principals — platform-global (lives only in `iam` schema).
#
# Role grants no longer live on this table. They live in a single
# unified `grants` table per scope: platform grants in `iam.grants`,
# catalog grants in `{catalog_schema}.grants`. The old `roles JSONB`
# column has been removed (hard cut, no migration).
CREATE_PRINCIPALS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.principals (
        id UUID PRIMARY KEY,
        -- NOT NULL: the JIT-auth upsert dedupes ON CONFLICT (identifier);
        -- a NULL identifier never conflicts (SQL NULLs are pairwise
        -- distinct), so each request would mint a duplicate principal row.
        identifier VARCHAR(512) NOT NULL,
        display_name VARCHAR(255),
        is_active BOOLEAN DEFAULT TRUE,
        valid_from TIMESTAMPTZ DEFAULT NOW(),
        valid_until TIMESTAMPTZ,
        custom_policies JSONB DEFAULT '[]'::jsonb,
        attributes JSONB DEFAULT '{}'::jsonb,
        metadata JSONB DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(identifier)
    );
""")

# Identity Links — platform-only. Lives in `iam.identity_links`.
# Tenant schemas never carry their own copy; identity is global,
# only grants are scope-specific.
CREATE_IDENTITY_LINKS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.identity_links (
        provider VARCHAR(64) NOT NULL,
        subject_id VARCHAR(255) NOT NULL,
        principal_id UUID NOT NULL REFERENCES {schema}.principals(id) ON DELETE CASCADE,
        email VARCHAR(255),
        last_login TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (provider, subject_id)
    );
""")

# Roles — per-scope registry. Exists in `iam` (platform roles) and
# in every catalog schema (tenant-owned roles). Same shape; tenants
# may define, rename, delete their own roles freely after bootstrap.
CREATE_ROLES_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.roles (
        id VARCHAR(128) PRIMARY KEY,
        name VARCHAR(128) NOT NULL,
        description TEXT,
        policies JSONB DEFAULT '[]'::jsonb,
        metadata JSONB DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
""")

CREATE_ROLE_HIERARCHY_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.role_hierarchy (
        parent_role VARCHAR(128) NOT NULL REFERENCES {schema}.roles(id) ON DELETE CASCADE,
        child_role VARCHAR(128) NOT NULL REFERENCES {schema}.roles(id) ON DELETE CASCADE,
        PRIMARY KEY (parent_role, child_role),
        -- Self-loops are the degenerate hierarchy cycle; longer cycles
        -- terminate at read time (GET_FULL_ROLE_HIERARCHY recurses with
        -- UNION, which dedupes), but a direct self-edge is always a data
        -- error and is rejected at the source.
        CHECK (parent_role <> child_role)
    );
""")

# Unified grants table — single source of truth for "who can do what".
#
# Created in every scope: `iam.grants` (platform) and
# `{catalog_schema}.grants` (per tenant). Same shape; only the set of
# valid `subject_kind` values differs (catalogs may scope to
# collection/item/asset; the platform schema only sees principal/catalog).
#
# Columns:
#   subject_kind  who/what is being granted (principal | catalog
#                 | collection | item | asset)
#   subject_ref   the subject's stable id (UUID-as-text for principal,
#                 catalog_id/collection_id/... otherwise)
#   object_kind   what is granted (role | policy)
#   object_ref    the object's stable id (role_name | policy_id)
#   effect        allow | deny — D9 deny precedence
#   valid_from    grant becomes active at this time (default: NOW())
#   valid_until   grant becomes inactive at this time (NULL = never)
#   conditions    JSONB — predicate to evaluate at request time (PR-2+)
#   quota         JSONB — quota / rate-limit spec (PR-2+, no-op in PR-1)
#   granted_by    UUID of the principal who issued the grant
#   granted_at    timestamp the grant was issued
#
# No FKs into `roles` or `policies`: platform grants may reference
# platform roles, catalog grants may reference catalog roles, and
# resolution-time validation logs and skips dangling object refs.
# Cross-schema FKs would block `DROP SCHEMA … CASCADE` on tenant
# eviction; we trade FK safety for tenant-cleanup simplicity.
# resource_kind / resource_ref scope a grant to a specific resource within
# the scope schema (e.g. resource_kind='collection', resource_ref=<id>).
# NULL/NULL = whole-catalog grant (the historical behaviour). Enforcement:
# explicit DENY at any scope wins; ALLOWs are additive across scopes.
#
# Uniqueness is expressed by the functional unique index below
# (``uq_grants_subject_object_resource`` — see
# CREATE_GRANTS_UNIQUE_WITH_RESOURCE). It uses COALESCE so two NULL
# resource columns collapse to a single key, giving the (NULL, NULL)
# whole-catalog row a single-row uniqueness while distinct collections
# each get their own row.
CREATE_GRANTS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.grants (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        subject_kind VARCHAR(32) NOT NULL,
        subject_ref VARCHAR(256) NOT NULL,
        object_kind VARCHAR(32) NOT NULL,
        object_ref VARCHAR(256) NOT NULL,
        effect VARCHAR(8) NOT NULL DEFAULT 'allow'
            CHECK (effect IN ('allow','deny')),
        valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        valid_until TIMESTAMPTZ,
        conditions JSONB,
        quota JSONB,
        granted_by UUID,
        granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        resource_kind VARCHAR(32),
        resource_ref VARCHAR(256),
        attribute_predicates JSONB NOT NULL DEFAULT '[]'
    );
    CREATE INDEX IF NOT EXISTS idx_grants_subject
        ON {schema}.grants (subject_kind, subject_ref);
    CREATE INDEX IF NOT EXISTS idx_grants_object
        ON {schema}.grants (object_kind, object_ref);
    CREATE INDEX IF NOT EXISTS idx_grants_validity
        ON {schema}.grants (valid_until) WHERE valid_until IS NOT NULL;
""")

# Lookup index for resource-scoped grants (partial — only rows that carry a
# resource scope are indexed; whole-catalog rows already covered by
# idx_grants_subject).
CREATE_IDX_GRANTS_RESOURCE = DDLQuery(
    "CREATE INDEX IF NOT EXISTS idx_grants_subject_resource "
    "ON {schema}.grants (subject_kind, subject_ref, resource_kind, resource_ref) "
    "WHERE resource_kind IS NOT NULL;"
)

# Functional unique index replacing the old inline UNIQUE constraint.
# COALESCE collapses NULL resource columns to '' so the whole-catalog
# (NULL/NULL) row keeps its single-row uniqueness while distinct
# collections each get their own row.
CREATE_GRANTS_UNIQUE_WITH_RESOURCE = DDLQuery(
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_grants_subject_object_resource "
    "ON {schema}.grants "
    "(subject_kind, subject_ref, object_kind, object_ref, effect, "
    "COALESCE(resource_kind, ''), COALESCE(resource_ref, ''));"
)

# Policies — platform-only for PR-1 (lives in `iam.policies`).
#
# Per-tenant policy registries (D11) are deferred to PR-2: changing
# the partitioned policies layout has 24-file blast radius and the
# plan explicitly flags this as revisitable. Tenant roles can still
# reference platform policies, which covers the PR-1 surface.
CREATE_POLICIES_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.policies (
        id VARCHAR(128) NOT NULL,
        version VARCHAR(16) DEFAULT '1.0',
        description TEXT,
        effect VARCHAR(16) DEFAULT 'ALLOW' CHECK (effect IN ('ALLOW', 'DENY')),
        priority INTEGER NOT NULL DEFAULT 0,
        actions JSONB NOT NULL DEFAULT '[]'::jsonb,
        resources JSONB DEFAULT '["*"]'::jsonb,
        conditions JSONB DEFAULT '[]'::jsonb,
        partition_key VARCHAR(64) NOT NULL DEFAULT 'global',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (id, partition_key)
    ) PARTITION BY LIST (partition_key);
""")

CREATE_PARTITION_GLOBAL = DDLQuery(
    "CREATE TABLE IF NOT EXISTS {schema}.policies_global PARTITION OF {schema}.policies FOR VALUES IN ('global');"
)

CREATE_REFRESH_TOKENS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.refresh_tokens (
        id VARCHAR(128) PRIMARY KEY,
        key_hash VARCHAR(64) NOT NULL,
        principal_id UUID NOT NULL REFERENCES {schema}.principals(id) ON DELETE CASCADE,
        family_id VARCHAR(128),
        is_active BOOLEAN DEFAULT TRUE,
        expires_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_refresh_tokens_family
        ON {schema}.refresh_tokens (family_id) WHERE family_id IS NOT NULL;
""")

# --- Usage Counters Table ---
#
# Shared atomic counter store consumed by the IAM `rate_limit` /
# `max_count` policy conditions. One row per
# ``(policy_id, principal_key, window_start)`` bucket.
#
#   * window_seconds set on the policy → window_start truncated to that
#     window, expires_at = window_start + 2*window_seconds. Rows reaped
#     by the nightly prune cron (see _initialize_schema below).
#   * window_seconds NULL → lifetime quota; window_start fixed at
#     ``to_timestamp(0)``, expires_at NULL.
#
# Designed so a Valkey-backed driver can later short-circuit reads
# while flushing deltas here for durability (see
# ``dynastore.models.protocols.usage_counter.UsageCounterProtocol``).
#
# ``last_seen_at`` is the last-write timestamp, refreshed on every
# ``incr`` / ``incr_if_below``. Not read by PR-A1 — reserved for:
#   * admin diagnostics ("stale lifetime quotas" — principals that
#     have not hit their quota in N days);
#   * soft-reaper criterion for lifetime rows (``expires_at IS NULL``)
#     whose ``policy_id`` is no longer registered, since the windowed
#     prune cron skips them.
CREATE_USAGE_COUNTERS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.usage_counters (
        policy_id      VARCHAR(128) NOT NULL,
        principal_key  VARCHAR(256) NOT NULL,
        window_start   TIMESTAMPTZ NOT NULL,
        count          BIGINT NOT NULL DEFAULT 0,
        last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at     TIMESTAMPTZ,
        PRIMARY KEY (policy_id, principal_key, window_start)
    );
    CREATE INDEX IF NOT EXISTS idx_usage_counters_expiry
        ON {schema}.usage_counters (expires_at) WHERE expires_at IS NOT NULL;
""")


# Hash-partitioned variant of the durable usage-counter sink (#1344).
#
# At extreme scale the flat ``usage_counters`` table becomes a write
# hot-spot for the lifetime-quota write-through and the rate-window flush.
# Partitioning by HASH(principal_key) spreads writes across N child tables
# while keeping the PK and the expiry index intact (PG cascades the index
# to every partition). ``principal_key`` is part of the PK, so it is a
# valid partition key.
#
# Applied ONLY on a fresh schema (the table must not already exist as a
# flat table) and ONLY in the platform ``iam`` schema where the counter
# driver actually reads/writes. Converting an existing flat table is a
# destructive operation reserved for the cleanup migration. The partition
# count is read at schema-init from ``IAM_USAGE_COUNTER_HASH_PARTITIONS``
# (see ``IamScaleConfig.usage_counter_hash_partitions``).
CREATE_USAGE_COUNTERS_TABLE_HASH = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.usage_counters (
        policy_id      VARCHAR(128) NOT NULL,
        principal_key  VARCHAR(256) NOT NULL,
        window_start   TIMESTAMPTZ NOT NULL,
        count          BIGINT NOT NULL DEFAULT 0,
        last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at     TIMESTAMPTZ,
        PRIMARY KEY (policy_id, principal_key, window_start)
    ) PARTITION BY HASH (principal_key);
    CREATE INDEX IF NOT EXISTS idx_usage_counters_expiry
        ON {schema}.usage_counters (expires_at) WHERE expires_at IS NOT NULL;
""")


def build_usage_counters_steps(hash_partitions: int) -> "list[DDLQuery]":
    """Return the DDL steps that create the ``usage_counters`` sink.

    ``hash_partitions <= 1`` → the historical flat table (single step).
    ``> 1`` → the HASH-partitioned parent plus one ``PARTITION OF`` child
    per modulus bucket. All steps are ``IF NOT EXISTS`` so re-running on a
    warm DB is a no-op (and the surrounding ``DDLBatch`` sentinel skips
    them entirely once the table exists).
    """
    if hash_partitions <= 1:
        return [CREATE_USAGE_COUNTERS_TABLE]
    steps: "list[DDLQuery]" = [CREATE_USAGE_COUNTERS_TABLE_HASH]
    for remainder in range(hash_partitions):
        steps.append(
            DDLQuery(
                f"CREATE TABLE IF NOT EXISTS "
                f"{{schema}}.usage_counters_p{remainder} "
                f"PARTITION OF {{schema}}.usage_counters "
                f"FOR VALUES WITH (MODULUS {hash_partitions}, "
                f"REMAINDER {remainder});"
            )
        )
    return steps


# SSOT for "what counts as an expired usage-counter row" — the WHERE
# predicate, factored out so every reaper shares one definition. Consumed by:
#   * the in-process `PostgresUsageCounter.reap_expired` driver method, and
#   * the leader-elected `MaintenanceSupervisor._run_iam_prune`, which wraps
#     this predicate in its bounded-batch `ctid … LIMIT` loop.
# Keeping the WHERE clause in one place is gap #6 of #800 — without it, a
# grace-period tweak (e.g. `expires_at < NOW() - INTERVAL '1 hour'`) would
# have to be applied at every site and silently drift if one is forgotten.
#
# The canonical reaper in production is the MaintenanceSupervisor running in
# the always-on catalog pod (it replaced the former plpgsql + pg_cron job,
# deleted in #1911 / #1927). `PostgresUsageCounter.reap_expired` remains the
# in-process entry point that satisfies `UsageCounterProtocol`.
REAP_EXPIRED_USAGE_COUNTERS_WHERE = "expires_at IS NOT NULL AND expires_at < NOW()"
REAP_EXPIRED_USAGE_COUNTERS_SQL = (
    'DELETE FROM "{schema}".usage_counters '
    "WHERE " + REAP_EXPIRED_USAGE_COUNTERS_WHERE + ";"
)

# Safety-net reaper for lifetime quotas whose policy is gone.
#
# Lifetime rows (``expires_at IS NULL``) are intentionally skipped by the
# windowed reaper above — they encode "use this policy at most N times,
# ever" semantics and must survive indefinitely. But if the parent
# policy is deleted, those rows become permanent orphans:
# ``usage_counters`` carries no FK to ``policies`` (intentional —
# counters are hot-path; a FK would force a row lock on the policies row
# for every incr).
#
# The transactional path on ``DELETE_POLICY`` already drops the rows in
# the same transaction (gap #2 / PR #828), so this reaper is a
# defence-in-depth safety net for rows orphaned via non-transactional
# paths — manual SQL, partial restore from backup, schema-level cascades
# disabled in some envs. Without it, a backup-restore that drops the
# policies row but keeps usage_counters silently strands lifetime rows
# under a now-nonexistent policy_id.
#
# We deliberately do NOT use ``last_seen_at`` as a reap criterion —
# lifetime means lifetime; a quiet principal must keep their counter.
# ``last_seen_at`` is reserved for admin-sort in ``list_for_policy``.
#
# As with the windowed predicate above, the WHERE clause is the SSOT shared
# by the in-process driver and the MaintenanceSupervisor's IAM prune job.
REAP_ORPHAN_USAGE_COUNTERS_WHERE = (
    "u.expires_at IS NULL "
    "  AND NOT EXISTS ("
    '    SELECT 1 FROM "{schema}".policies p WHERE p.id = u.policy_id'
    "  )"
)
REAP_ORPHAN_USAGE_COUNTERS_SQL = (
    'DELETE FROM "{schema}".usage_counters u '
    "WHERE " + REAP_ORPHAN_USAGE_COUNTERS_WHERE + ";"
)

# --- Audit Log Table ---

CREATE_AUDIT_LOG_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.audit_log (
        id BIGSERIAL PRIMARY KEY,
        event_type VARCHAR(64) NOT NULL,
        principal_id VARCHAR(255),
        ip_address VARCHAR(45),
        detail JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_audit_log_event_type
        ON {schema}.audit_log (event_type, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_audit_log_principal
        ON {schema}.audit_log (principal_id, created_at DESC)
        WHERE principal_id IS NOT NULL;
""")

INSERT_AUDIT_EVENT = DQLQuery(
    """INSERT INTO {schema}.audit_log (event_type, principal_id, ip_address, detail)
       VALUES (:event_type, :principal_id, :ip_address, CAST(:detail AS jsonb))
       RETURNING id;""",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)




# --- DML / DQL ---

# Legacy Principal Queries

INSERT_PRINCIPAL = DQLQuery(
    # ON CONFLICT on `identifier` (not `id`) because JIT auto-registration
    # generates a fresh UUID on every request — the logical key for
    # "is this user already known?" is the identifier. Keyed-on-id conflicts
    # would never fire for the repeat-auth case and instead surface the
    # duplicate-identifier unique-constraint as a 401.
    """
    INSERT INTO {schema}.principals
    (id, identifier, display_name, is_active, valid_until, custom_policies, attributes, metadata)
    VALUES
    (:id, :identifier, :display_name, :is_active, :valid_until, :custom_policies, :attributes, :metadata)
    ON CONFLICT (identifier) DO UPDATE SET
        display_name = EXCLUDED.display_name,
        is_active = EXCLUDED.is_active,
        valid_until = EXCLUDED.valid_until,
        custom_policies = EXCLUDED.custom_policies,
        attributes = EXCLUDED.attributes,
        metadata = EXCLUDED.metadata,
        updated_at = NOW()
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Principal(**row) if row else None,
)

UPDATE_PRINCIPAL = DQLQuery(
    """
    UPDATE {schema}.principals
    SET identifier = :identifier,
        display_name = :display_name,
        is_active = :is_active,
        metadata = :metadata,
        custom_policies = :custom_policies,
        attributes = :attributes
    WHERE id = :id
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Principal(**row) if row else None,
)

GET_PRINCIPAL_ID_BY_IDENTIFIER = DQLQuery(
    "SELECT id FROM {schema}.principals WHERE identifier = :identifier;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
    post_processor=lambda row: UUID(str(row)) if row else None,
)

GET_PRINCIPAL_BY_ID = DQLQuery(
    """SELECT p.*, l.provider, l.subject_id
    FROM {schema}.principals p
    LEFT JOIN {schema}.identity_links l ON p.id = l.principal_id
    WHERE p.id = :id;""",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Principal(**row) if row else None,
)

GET_PRINCIPAL_BY_IDENTIFIER = DQLQuery(
    "SELECT * FROM {schema}.principals WHERE identifier = :identifier;",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Principal(**row) if row else None,
)

GET_PRINCIPAL_BY_IDENTITY = DQLQuery(
    """
    SELECT p.* FROM {schema}.principals p
    JOIN {schema}.identity_links l ON p.id = l.principal_id
    WHERE l.provider = :provider AND l.subject_id = :subject_id;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Principal(**row) if row else None,
)

INSERT_IDENTITY_LINK = DQLQuery(
    """
    INSERT INTO {schema}.identity_links
    (provider, subject_id, principal_id, email)
    VALUES
    (:provider, :subject_id, :principal_id, :email)
    ON CONFLICT (provider, subject_id) DO UPDATE
        SET principal_id = EXCLUDED.principal_id,
            email = COALESCE(EXCLUDED.email, identity_links.email);
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

RESOLVE_IDENTITY_BY_EMAIL = DQLQuery(
    """
    SELECT l.provider, l.subject_id
    FROM {schema}.identity_links l
    LEFT JOIN {schema}.principals p ON p.id = l.principal_id
    WHERE l.email = :email
       OR p.identifier = :email
       OR p.display_name = :email
    ORDER BY l.created_at ASC
    LIMIT 1;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

LIST_PRINCIPALS = DQLQuery(
    """SELECT p.*, l.provider, l.subject_id
    FROM {schema}.principals p
    LEFT JOIN {schema}.identity_links l ON p.id = l.principal_id
    ORDER BY p.created_at DESC LIMIT :limit OFFSET :offset;""",
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [Principal(**row) for row in rows],
)

DELETE_PRINCIPAL = DQLQuery(
    "DELETE FROM {schema}.principals WHERE id = :id;",
    result_handler=ResultHandler.ROWCOUNT,
)



# --- Role & Hierarchy Queries ---

# ON CONFLICT semantics are ADDITIVE on ``policies`` (closes geoid#902).
# The cold-boot path seeds the catalog-tier ``unauthenticated`` role via
# this query with ``policies=["public_access"]``. PolicyContributorPreset
# instances (web, stac, records, maps, edr, …) later call ``update_role``
# (REPLACE — operator intent) so that each preset applies its own policy
# union idempotently via the preset lifecycle (apply/revoke).
#
# ``create_role`` is reachable from multiple racing paths during cold
# boot (per-service ``PolicyService.provision_default_policies`` for both
# ``catalog_id=None`` and ``catalog_id="_system_"``). If two of those
# paths race past the ``get_role`` check concurrently, the second INSERT
# used to hit ON CONFLICT and replace ``policies`` with EXCLUDED —
# silently dropping bindings the first path already wrote. The
# replace-on-conflict semantics surfaced as the ``/health`` 403 in
# geoid#902 (the seed-declared ``public_access`` was the binding being
# clobbered).
#
# Fix: on conflict, ``policies`` is recomputed as the UNION of the
# existing JSONB array and EXCLUDED's. Operator REPLACE remains
# available via UPDATE_ROLE (used by ``PATCH /admin/roles`` and
# ``update_role`` callers such as PolicyContributorPreset.apply and
# the ES private driver's all_users role binding); the additive
# semantics only kick in when two callers ``create_role`` the same name.
INSERT_ROLE = DQLQuery(
    """
    INSERT INTO {schema}.roles (id, name, description, metadata, policies)
    VALUES (:id, :name, :description, :metadata, :policies)
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        description = EXCLUDED.description,
        metadata = EXCLUDED.metadata,
        policies = (
            SELECT COALESCE(jsonb_agg(DISTINCT value), '[]'::jsonb)
            FROM jsonb_array_elements_text(
                COALESCE(roles.policies, '[]'::jsonb)
                || COALESCE(EXCLUDED.policies, '[]'::jsonb)
            ) AS arr(value)
        )
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Role(**row) if row else None,
)

GET_ROLE = DQLQuery(
    "SELECT * FROM {schema}.roles WHERE id = :name OR name = :name;",  # Support ID or Name lookup
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Role(**row) if row else None,
)

LIST_ROLES = DQLQuery(
    "SELECT * FROM {schema}.roles ORDER BY name ASC;",
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [Role(**row) for row in rows],
)

INSERT_ROLE_HIERARCHY = DQLQuery(
    "INSERT INTO {schema}.role_hierarchy (parent_role, child_role) VALUES (:parent_role, :child_role) ON CONFLICT DO NOTHING;",
    result_handler=ResultHandler.ROWCOUNT,
)

# Note: the historical ``SEED_TENANT_DEFAULT_ROLES_SQL`` and
# ``SEED_TENANT_ROLE_HIERARCHY_SQL`` constants were retired in geoid#643.
# Catalog-tier role seeding is now config-driven via
# ``IamRolesConfig.catalog_roles`` and ``RoleSeed.parent``, executed by
# ``PolicyService.provision_default_policies(catalog_id, ...)`` from the
# IamModule's ``initialize_iam_tenant`` lifecycle hook.

DELETE_ROLE_HIERARCHY = DQLQuery(
    "DELETE FROM {schema}.role_hierarchy WHERE parent_role = :parent_role AND child_role = :child_role;",
    result_handler=ResultHandler.ROWCOUNT,
)

LIST_ROLE_HIERARCHY_EDGES = DQLQuery(
    "SELECT parent_role, child_role FROM {schema}.role_hierarchy ORDER BY parent_role, child_role;",
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [(row["parent_role"], row["child_role"]) for row in rows],
)

UPDATE_ROLE = DQLQuery(
    """
    UPDATE {schema}.roles
    SET description = :description,
        metadata = :metadata,
        policies = :policies
    WHERE id = :name OR name = :name
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Role(**row) if row else None,
)

DELETE_ROLE = DQLQuery(
    "DELETE FROM {schema}.roles WHERE id = :name OR name = :name;",
    result_handler=ResultHandler.ROWCOUNT,
)

# Atomic policy bind — appends policy_id string to roles.policies, replacing
# any existing equal entry. roles.policies is a JSONB array of plain strings.
# Single SQL UPDATE: no Python read-modify-write, no race window.
# jsonb_agg over zero rows returns NULL → COALESCE to empty array.
BIND_POLICY_TO_ROLE = DQLQuery(
    """
    UPDATE {schema}.roles
    SET policies = COALESCE(
        (
            SELECT jsonb_agg(elem)
            FROM jsonb_array_elements_text(COALESCE(policies, '[]'::jsonb)) AS elem
            WHERE elem != :policy_id
        ),
        '[]'::jsonb
    ) || jsonb_build_array(:policy_id)
    WHERE id = :role_name OR name = :role_name;
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

# Atomic policy unbind — removes all string entries equal to policy_id.
# NULL jsonb_agg (zero matching rows) is coalesced to empty array.
UNBIND_POLICY_FROM_ROLE = DQLQuery(
    """
    UPDATE {schema}.roles
    SET policies = COALESCE(
        (
            SELECT jsonb_agg(elem)
            FROM jsonb_array_elements_text(COALESCE(policies, '[]'::jsonb)) AS elem
            WHERE elem != :policy_id
        ),
        '[]'::jsonb
    )
    WHERE id = :role_name OR name = :role_name;
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

# Recursive CTE to find all child roles (descendants)
GET_FULL_ROLE_HIERARCHY = DQLQuery(
    """
    WITH RECURSIVE hierarchy AS (
        -- Base case: the initial roles
        SELECT child_role FROM {schema}.role_hierarchy WHERE parent_role = ANY(:role_names)
        UNION
        -- Recursive step: find children of the roles already found
        SELECT rh.child_role
        FROM {schema}.role_hierarchy rh
        JOIN hierarchy h ON rh.parent_role = h.child_role
    )
    SELECT DISTINCT child_role FROM hierarchy;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [row["child_role"] for row in rows],
)

# --- Unified grants DML ---
#
# These primitives operate on `{schema}.grants`. Catalog-scoped grants
# pass the tenant schema; platform-scoped grants pass `iam`. Storage
# facades wrap these to expose role-friendly entry points
# (grant_platform_role, grant_catalog_role, …).

INSERT_GRANT = DQLQuery(
    """
    INSERT INTO {schema}.grants (
        subject_kind, subject_ref, object_kind, object_ref, effect,
        valid_from, valid_until, conditions, quota, granted_by,
        resource_kind, resource_ref, attribute_predicates
    )
    VALUES (
        :subject_kind, :subject_ref, :object_kind, :object_ref, :effect,
        COALESCE(:valid_from, NOW()), :valid_until,
        CAST(:conditions AS jsonb), CAST(:quota AS jsonb), :granted_by,
        :resource_kind, :resource_ref,
        COALESCE(CAST(:attribute_predicates AS jsonb), '[]'::jsonb)
    )
    ON CONFLICT (subject_kind, subject_ref, object_kind, object_ref, effect,
                 COALESCE(resource_kind, ''), COALESCE(resource_ref, ''))
    DO UPDATE SET
        valid_from           = EXCLUDED.valid_from,
        valid_until          = EXCLUDED.valid_until,
        conditions           = EXCLUDED.conditions,
        quota                = EXCLUDED.quota,
        granted_by           = EXCLUDED.granted_by,
        granted_at           = NOW(),
        attribute_predicates = EXCLUDED.attribute_predicates
    RETURNING id;
    """,
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

DELETE_GRANT_BY_ID = DQLQuery(
    "DELETE FROM {schema}.grants WHERE id = :id;",
    result_handler=ResultHandler.ROWCOUNT,
)

DELETE_GRANTS_BY_MATCH = DQLQuery(
    """
    DELETE FROM {schema}.grants
    WHERE subject_kind = :subject_kind
      AND subject_ref  = :subject_ref
      AND object_kind  = :object_kind
      AND object_ref   = :object_ref
      AND effect       = :effect
      AND COALESCE(resource_kind, '') = COALESCE(:resource_kind, '')
      AND COALESCE(resource_ref, '')  = COALESCE(:resource_ref, '');
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

LIST_GRANTS_FOR_SUBJECT = DQLQuery(
    """
    SELECT id, subject_kind, subject_ref, object_kind, object_ref, effect,
           valid_from, valid_until, conditions, quota, granted_by, granted_at,
           resource_kind, resource_ref
    FROM {schema}.grants
    WHERE subject_kind = :subject_kind AND subject_ref = :subject_ref
    ORDER BY object_kind, object_ref;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

LIST_GRANTS_FOR_OBJECT = DQLQuery(
    """
    SELECT id, subject_kind, subject_ref, object_kind, object_ref, effect,
           valid_from, valid_until, conditions, quota, granted_by, granted_at
    FROM {schema}.grants
    WHERE object_kind = :object_kind AND object_ref = :object_ref
    ORDER BY subject_kind, subject_ref;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

# Reverse "who has access to this resource" lookup for the Admin UI
# bindings tab (#1342): every grant scoped to a specific resource
# (e.g. resource_kind='collection', resource_ref=<collection id>).
# Whole-catalog grants (resource_kind IS NULL) are intentionally NOT
# returned — this is the per-resource view, not the catalog-wide one.
LIST_GRANTS_FOR_RESOURCE = DQLQuery(
    """
    SELECT id, subject_kind, subject_ref, object_kind, object_ref, effect,
           valid_from, valid_until, conditions, quota, granted_by, granted_at,
           resource_kind, resource_ref
    FROM {schema}.grants
    WHERE resource_kind = :resource_kind AND resource_ref = :resource_ref
    ORDER BY subject_ref, object_kind, object_ref;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

# Used by the `?cascade=true` precheck on role/policy-definition delete.
COUNT_GRANTS_FOR_OBJECT = DQLQuery(
    """
    SELECT COUNT(*) FROM {schema}.grants
    WHERE object_kind = :object_kind AND object_ref = :object_ref;
    """,
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

# --- Role-scoped read facades over the unified grants table ---
#
# These are simple specializations of LIST_GRANTS_FOR_SUBJECT that
# project to a flat list of role names — preserving the prior public
# storage surface (`list_platform_roles`, `list_catalog_roles`).
LIST_ROLE_NAMES_FOR_PRINCIPAL = DQLQuery(
    """
    SELECT object_ref AS role_name
    FROM {schema}.grants
    WHERE subject_kind = 'principal'
      AND subject_ref  = :principal_id
      AND object_kind  = 'role'
      AND effect       = 'allow'
      AND valid_from <= NOW()
      AND (valid_until IS NULL OR NOW() < valid_until)
    ORDER BY object_ref;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [row["role_name"] for row in rows],
)

# Role lookup by identity. Joins the grants table against the
# *platform* identity_links (`iam.identity_links`) — principals are
# platform-global; only their grants are tenant-scoped.
LIST_ROLE_NAMES_FOR_IDENTITY = DQLQuery(
    """
    SELECT g.object_ref AS role_name
    FROM {schema}.grants g
    JOIN iam.identity_links l
      ON l.principal_id::text = g.subject_ref
    WHERE g.subject_kind = 'principal'
      AND g.object_kind  = 'role'
      AND g.effect       = 'allow'
      AND g.valid_from <= NOW()
      AND (g.valid_until IS NULL OR NOW() < g.valid_until)
      AND l.provider   = :provider
      AND l.subject_id = :subject_id
    ORDER BY g.object_ref;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [row["role_name"] for row in rows],
)

# Resolver — direct grants for a principal in one scope.
#
# PR-1 only exercises principal-as-subject + role/policy-as-object,
# but the projection includes every column needed to evaluate
# effect/time/conditions/quota in PR-2+. Resource-scoped subjects
# (catalog/collection/item/asset) are computed in Python by the
# storage layer using `LIST_GRANTS_FOR_SUBJECT` per prefix, so this
# query stays simple and reusable across scopes.
LIST_TIMEACTIVE_GRANTS_FOR_PRINCIPAL = DQLQuery(
    """
    SELECT id, subject_kind, subject_ref, object_kind, object_ref, effect,
           valid_from, valid_until, conditions, quota, granted_by, granted_at,
           resource_kind, resource_ref
    FROM {schema}.grants
    WHERE subject_kind = 'principal'
      AND subject_ref  = :principal_id
      AND valid_from <= NOW()
      AND (valid_until IS NULL OR NOW() < valid_until);
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

# Resource-scoped sibling of LIST_TIMEACTIVE_GRANTS_FOR_PRINCIPAL: returns
# whole-catalog grants (resource_kind IS NULL) PLUS grants scoped to the
# requested resource. Allows are additive across both scopes; deny precedence
# is applied by the caller.
LIST_TIMEACTIVE_SCOPED_GRANTS_FOR_PRINCIPAL = DQLQuery(
    """
    SELECT id, subject_kind, subject_ref, object_kind, object_ref, effect,
           valid_from, valid_until, conditions, quota, granted_by, granted_at,
           resource_kind, resource_ref
    FROM {schema}.grants
    WHERE subject_kind = 'principal'
      AND subject_ref  = :principal_id
      AND valid_from <= NOW()
      AND (valid_until IS NULL OR NOW() < valid_until)
      AND (resource_kind IS NULL
           OR (resource_kind = :resource_kind AND resource_ref = :resource_ref));
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

# Catalog users — distinct principals with at least one grant in the
# tenant's grants table. Joins against platform `iam.principals` /
# `iam.identity_links` to surface the same shape as before.
LIST_CATALOG_USERS = DQLQuery(
    """
    SELECT DISTINCT p.id, p.identifier, p.display_name, p.is_active,
                    l.provider, l.subject_id
    FROM {schema}.grants g
    JOIN iam.principals p ON p.id::text = g.subject_ref
    LEFT JOIN iam.identity_links l ON l.principal_id = p.id
    WHERE g.subject_kind = 'principal'
    ORDER BY p.display_name;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

INSERT_REFRESH_TOKEN = DQLQuery(
    "INSERT INTO {schema}.refresh_tokens (id, key_hash, principal_id, family_id, expires_at) VALUES (:id, :key_hash, :principal_id, :family_id, :expires_at) RETURNING *;",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: RefreshToken(**row) if row else None,
)

GET_REFRESH_TOKEN = DQLQuery(
    "SELECT * FROM {schema}.refresh_tokens WHERE id = :id;",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: RefreshToken(**row) if row else None,
)

INVALIDATE_REFRESH_TOKEN = DQLQuery(
    "UPDATE {schema}.refresh_tokens SET is_active = FALSE WHERE id = :id RETURNING is_active;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)

INVALIDATE_REFRESH_TOKEN_FAMILY = DQLQuery(
    "UPDATE {schema}.refresh_tokens SET is_active = FALSE WHERE family_id = :family_id AND is_active = TRUE RETURNING id;",
    result_handler=ResultHandler.ALL,
)

PRUNE_EXPIRED_REFRESH_TOKENS = DQLQuery(
    "DELETE FROM {schema}.refresh_tokens WHERE expires_at < NOW();",
    result_handler=ResultHandler.ROWCOUNT,
)

# Policy CRUD lives in ``PostgresPolicyStorage`` (partition-aware: every
# read/write/delete filters by the full ``(id, partition_key)`` primary
# key). The unfiltered ``WHERE id = :id`` duplicates that used to live here
# were callerless and could read/update across tenant partitions — removed.

# --- Identity Link Queries ---

DELETE_IDENTITY_LINK = DQLQuery(
    "DELETE FROM {schema}.identity_links WHERE provider = :provider AND subject_id = :subject_id;",
    result_handler=ResultHandler.ROWCOUNT,
)

LIST_IDENTITY_LINKS = DQLQuery(
    """
    SELECT provider, subject_id, principal_id, email, created_at
    FROM {schema}.identity_links
    WHERE principal_id = :principal_id
    ORDER BY created_at DESC;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [IdentityLink(**row) for row in rows],
)



def build_search_principals_query(
    identifier: Optional[str],
    role: Optional[str],
    limit: int,
    offset: int,
    schema: str = "iam",
):
    """Build a Principal search query.

    ``schema`` governs which ``grants`` table the optional role filter
    joins through (``iam.grants`` for platform scope, or a tenant
    schema's ``grants`` for catalog-scoped role filtering).

    Principals themselves always live in ``iam.principals`` — they are
    platform-global and are never replicated into tenant schemas.  The
    ``FROM`` clause is therefore hardcoded to ``iam.principals``
    regardless of the ``schema`` argument, preventing the
    ``42P01: relation "{tenant}.principals" does not exist`` error that
    arises when a catalog ``schema`` is passed alongside a role filter.
    """
    clauses = []
    params: Dict[str, Any] = {"limit": limit, "offset": offset}

    if identifier:
        clauses.append(
            "identifier LIKE :identifier_pattern OR display_name LIKE :identifier_pattern"
        )
        params["identifier_pattern"] = f"%{identifier}%"

    # Role filter via the unified grants table (replaces the dropped
    # JSONB column). Joins on subject_ref::text = principal_id.
    # ``schema`` here is the grants scope; principals are always in ``iam``.
    join_clause = ""
    if role:
        params["role"] = role
        join_clause = (
            f"JOIN {schema}.grants g "
            f"ON g.subject_kind = 'principal' "
            f"AND g.subject_ref = p.id::text "
            f"AND g.object_kind = 'role' "
            f"AND g.object_ref = :role"
        )

    where_clause = " AND ".join(clauses) if clauses else "1=1"

    sql = f"""
        SELECT DISTINCT p.* FROM iam.principals p
        {join_clause}
        WHERE {where_clause}
        ORDER BY p.created_at DESC
        LIMIT :limit OFFSET :offset;
    """
    return DQLQuery(
        sql,
        result_handler=ResultHandler.ALL_DICTS,
        post_processor=lambda rows: [Principal(**row) for row in rows],
    ), params
