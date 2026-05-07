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

from dynastore.modules.db_config.query_executor import DDLQuery, DDLBatch, DQLQuery, ResultHandler
from .models import Principal, Role, RefreshToken, IdentityLink, Policy

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
        identifier VARCHAR(512),
        display_name VARCHAR(255),
        is_active BOOLEAN DEFAULT TRUE,
        valid_from TIMESTAMPTZ DEFAULT NOW(),
        valid_until TIMESTAMPTZ,
        custom_policies JSONB DEFAULT '[]'::jsonb,
        attributes JSONB DEFAULT '{}'::jsonb,
        metadata JSONB DEFAULT '{}'::jsonb,
        policy JSONB,
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
        level INTEGER DEFAULT 0,
        parent_roles JSONB DEFAULT '[]'::jsonb,
        policies JSONB DEFAULT '[]'::jsonb,
        metadata JSONB DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
""")

CREATE_ROLE_HIERARCHY_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS {schema}.role_hierarchy (
        parent_role VARCHAR(128) NOT NULL REFERENCES {schema}.roles(id) ON DELETE CASCADE,
        child_role VARCHAR(128) NOT NULL REFERENCES {schema}.roles(id) ON DELETE CASCADE,
        PRIMARY KEY (parent_role, child_role)
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
        UNIQUE (subject_kind, subject_ref, object_kind, object_ref, effect)
    );
    CREATE INDEX IF NOT EXISTS idx_grants_subject
        ON {schema}.grants (subject_kind, subject_ref);
    CREATE INDEX IF NOT EXISTS idx_grants_object
        ON {schema}.grants (object_kind, object_ref);
    CREATE INDEX IF NOT EXISTS idx_grants_validity
        ON {schema}.grants (valid_until) WHERE valid_until IS NOT NULL;
""")

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
        effect VARCHAR(16) DEFAULT 'ALLOW',
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
    (id, identifier, display_name, is_active, valid_until, custom_policies, attributes, metadata, policy)
    VALUES
    (:id, :identifier, :display_name, :is_active, :valid_until, :custom_policies, :attributes, :metadata, :policy)
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
        metadata = :metadata,
        policy = :policy,
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
    ON CONFLICT (provider, subject_id) DO NOTHING;
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

INSERT_ROLE = DQLQuery(
    """
    INSERT INTO {schema}.roles (id, name, description, level, metadata, parent_roles, policies)
    VALUES (:id, :name, :description, :level, :metadata, :parent_roles, :policies)
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        description = EXCLUDED.description,
        level = EXCLUDED.level,
        metadata = EXCLUDED.metadata,
        parent_roles = EXCLUDED.parent_roles,
        policies = EXCLUDED.policies
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
    "SELECT * FROM {schema}.roles ORDER BY level DESC, name ASC;",
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [Role(**row) for row in rows],
)

INSERT_ROLE_HIERARCHY = DQLQuery(
    "INSERT INTO {schema}.role_hierarchy (parent_role, child_role) VALUES (:parent_role, :child_role) ON CONFLICT DO NOTHING;",
    result_handler=ResultHandler.ROWCOUNT,
)

# Tenant default role seeds. Inserted with ON CONFLICT DO NOTHING so a
# tenant admin can rename/restructure these without the next provisioning
# pass clobbering their changes (D3 — tenants own their role definitions).
#
# D5 chain of authority: admin → editor (admin inherits editor's policies
# via role_hierarchy). `allUsers` and `unauthenticated` are read-only
# floors — no inheritance, no policies seeded here. Per-permission policy
# wiring lives on `iam.policies` and ships in PR-2 with the per-tenant
# policy registry; PR-1 keeps the seed roles policy-free so the unified
# grants table is the only thing the resolver evaluates.
SEED_TENANT_DEFAULT_ROLES_SQL = """
    INSERT INTO {schema}.roles
        (id, name, description, level, parent_roles, policies, metadata)
    VALUES
        ('admin', 'admin',
         'Tenant administrator — manages roles, grants, and members.',
         100, '[]'::jsonb, '[]'::jsonb,
         '{"seed": true, "scope": "catalog"}'::jsonb),
        ('editor', 'editor',
         'Catalog editor — creates and updates content.',
         50,  '[]'::jsonb, '[]'::jsonb,
         '{"seed": true, "scope": "catalog"}'::jsonb),
        ('allUsers', 'allUsers',
         'Read-only floor for any authenticated user.',
         10,  '[]'::jsonb, '[]'::jsonb,
         '{"seed": true, "scope": "catalog"}'::jsonb),
        ('unauthenticated', 'unauthenticated',
         'Read-only floor for anonymous (unauthenticated) requests.',
         0,   '[]'::jsonb, '[]'::jsonb,
         '{"seed": true, "scope": "catalog"}'::jsonb)
    ON CONFLICT (id) DO NOTHING;
"""

SEED_TENANT_ROLE_HIERARCHY_SQL = """
    INSERT INTO {schema}.role_hierarchy (parent_role, child_role)
    VALUES ('admin', 'editor')
    ON CONFLICT DO NOTHING;
"""

DELETE_ROLE_HIERARCHY = DQLQuery(
    "DELETE FROM {schema}.role_hierarchy WHERE parent_role = :parent_role AND child_role = :child_role;",
    result_handler=ResultHandler.ROWCOUNT,
)

UPDATE_ROLE = DQLQuery(
    """
    UPDATE {schema}.roles
    SET description = :description,
        level = :level,
        metadata = :metadata,
        parent_roles = :parent_roles,
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
        valid_from, valid_until, conditions, quota, granted_by
    )
    VALUES (
        :subject_kind, :subject_ref, :object_kind, :object_ref, :effect,
        COALESCE(:valid_from, NOW()), :valid_until,
        CAST(:conditions AS jsonb), CAST(:quota AS jsonb), :granted_by
    )
    ON CONFLICT (subject_kind, subject_ref, object_kind, object_ref, effect)
    DO UPDATE SET
        valid_from  = EXCLUDED.valid_from,
        valid_until = EXCLUDED.valid_until,
        conditions  = EXCLUDED.conditions,
        quota       = EXCLUDED.quota,
        granted_by  = EXCLUDED.granted_by,
        granted_at  = NOW()
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
      AND effect       = :effect;
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

LIST_GRANTS_FOR_SUBJECT = DQLQuery(
    """
    SELECT id, subject_kind, subject_ref, object_kind, object_ref, effect,
           valid_from, valid_until, conditions, quota, granted_by, granted_at
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
           valid_from, valid_until, conditions, quota, granted_by, granted_at
    FROM {schema}.grants
    WHERE subject_kind = 'principal'
      AND subject_ref  = :principal_id
      AND valid_from <= NOW()
      AND (valid_until IS NULL OR NOW() < valid_until);
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

# --- Policy CRUD Queries ---

INSERT_POLICY = DQLQuery(
    """
    INSERT INTO {schema}.policies (id, version, description, effect, actions, resources, conditions, partition_key)
    VALUES (:id, :version, :description, :effect, :actions, :resources, :conditions, :partition_key)
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Policy(**row) if row else None,
)

GET_POLICY = DQLQuery(
    "SELECT * FROM {schema}.policies WHERE id = :id;",
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Policy(**row) if row else None,
)

LIST_POLICIES = DQLQuery(
    """SELECT * FROM {schema}.policies
    ORDER BY created_at DESC
    LIMIT :limit OFFSET :offset;""",
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [Policy(**row) for row in rows],
)

UPDATE_POLICY = DQLQuery(
    """
    UPDATE {schema}.policies
    SET version = :version,
        description = :description,
        effect = :effect,
        actions = :actions,
        resources = :resources,
        conditions = :conditions
    WHERE id = :id
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda row: Policy(**row) if row else None,
)

DELETE_POLICY = DQLQuery(
    "DELETE FROM {schema}.policies WHERE id = :id;",
    result_handler=ResultHandler.ROWCOUNT,
)

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

    The legacy `role` filter searched a JSONB column on `principals`
    that no longer exists. Role-based search now joins the unified
    grants table, projecting `subject_kind='principal' AND
    object_kind='role'`:

    - For schema == "iam" (or any schema with platform grants), we
      filter through `iam.grants`.
    - For a tenant schema, we filter through that schema's
      `grants` table.

    Mixing platform + catalog filtering in one search is a separate
    concern; admin search is per-scope today.
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
        SELECT DISTINCT p.* FROM {schema}.principals p
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
