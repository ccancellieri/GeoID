# Authentication & Authorization

This document describes the security architecture of AIP Catalog Services. The system implements a zero-trust model where every request is evaluated against a policy engine.

> See also: [Authentication](../authentication.md) for operator-facing OIDC / IdP configuration and cold-boot seeding.

## Architecture

```
Request → IamMiddleware → Identity Resolution → Policy Evaluation → Allow / Deny
```

### Identity Resolution

`IamMiddleware` (`extensions/iam/middleware.py`) intercepts every request and
resolves credentials by discovering an `AuthenticatorProtocol` implementation
at runtime via `get_protocol(AuthenticatorProtocol)`. The concrete
implementation is `IamModule` (`modules/iam/module.py`), which delegates to one
or more registered **Identity Providers** (`IdentityProviderProtocol`).

Credentials are extracted from:
1. `Authorization: Bearer <jwt_token>` header
2. Session cookie (set after `/auth/authorize` OIDC flow)

Identity providers are registered at startup from `IdpConfig` (class key
`idp_config`, configured via the Configs API) or from `KEYCLOAK_*` environment
variables when no `IdpConfig` record exists:

| Provider | Class | Validates |
|----------|-------|-----------|
| `OidcIdentityProvider` | `modules/iam/identity_providers/oidc_identity.py` | Keycloak-issued JWTs via JWKS endpoint; optional audience verification |

If no credentials are provided, or if token validation fails, the request is
treated as **unauthenticated** with the `unauthenticated` role (configurable
via `IamRolesConfig.anonymous_role_name`).

### Principal Model

Every authenticated request produces a `Principal` (defined in
`models/auth.py`):

```
Principal
├── id            — UUID of the authenticated entity
├── provider      — identity provider name (e.g. "oidc", "oidc:service_account")
├── subject_id    — provider-specific subject identifier
├── display_name  — human-readable name
├── roles         — ["sysadmin", "admin", ...] from grants table
├── custom_policies — inline Policy objects attached to the principal
└── attributes    — arbitrary key-value attributes from JWT claims
```

## Policy Engine

### Evaluation Flow

```
1. Collect roles from principal (subject_id → grants via iam.grants)
2. For each role, look up policy IDs (roles → policies mapping)
3. Fetch full Policy objects from iam.policies
4. Include custom_policies directly attached to the principal
5. For each policy: check action regex against HTTP method, resource regex against path
6. First matching DENY → deny immediately
7. First matching ALLOW → allow
8. No match → deny by default (implicit deny)
```

### Policy Model

```
Policy
├── id              — unique identifier (e.g. "sysadmin_full_access")
├── actions         — regex patterns matching HTTP methods ["GET", "POST", ".*"]
├── resources       — regex patterns matching URL paths ["/features/.*", "/admin/.*"]
├── effect          — "ALLOW" or "DENY"
├── conditions      — optional list of Condition objects (rate limits, IP, time)
├── partition_key   — "global" or catalog_id for tenant-scoped policies
└── description     — human-readable description
```

Wildcard `"*"` in actions/resources is auto-converted to `".*"` regex via
`transform_wildcards` validator.

### Default Roles and Policies

Provisioned by applying the `iam_baseline` and `default_roles_baseline` presets
(`extensions/iam/presets/iam_baseline.py`,
`modules/iam/presets/default_roles_baseline.py`):

| Role | Policies | Description |
|------|----------|-------------|
| `sysadmin` | `sysadmin_full_access`, `admin_authorization_api` | Unrestricted `.*` on `.*` |
| `admin` | `admin_authorization_api` | Manage principals, roles, and policies |
| `unauthenticated` | `self_service_authorization_api` (+ `public_access` if the `public_access_baseline` preset is applied) | Floor role for all requests |

Extensions register additional policies via the `PolicyContributorPreset`
mechanism. For example:
- `tiles_public_access` → `/tiles/.*` (GET/OPTIONS), contributed by `extensions/tiles/presets/__init__.py`
- `features_public_access` → `/features/.*`
- `stac_public_access` → `/stac/.*`

### Conditions

Policies support conditions evaluated at request time. The registered condition
handlers — and the authoritative spec for each one's config keys — live in
`modules/iam/conditions.py`:

| `type` | Config keys | Description |
|--------|-------------|-------------|
| `rate_limit` | `limit`, `window_seconds`, `scope` (+ optional `path_pattern`, `methods`, `mode`) | Per-window token-bucket limit. `scope`: `principal` (default), `role`, `client_ip`, `catalog`. `mode`: `graceful` (default, allow when no counter backend) or `strict` (fail-closed) |
| `max_count` | `limit`, `scope` (+ optional `path_pattern`, `methods`, `mode`) | Lifetime quota on matched requests. `mode`: same `graceful`/`strict` semantics as `rate_limit` |
| `query_match` | `param`, `pattern` | Require a query parameter to match a regex |
| `lookup_only_search` | _(none)_ | Allow `/search` only as retrieve-by-id — request must carry `geoid` or `external_id`; blocks enumeration |
| `time_window` | `start`, `end` | Absolute ISO-8601 validity window |
| `expiration` | `expires_at` | Deny once an absolute ISO-8601 instant has passed |
| `max_token_ttl` | `max_ttl_seconds` (default `3600`) | Reject token requests exceeding a TTL bound |
| `match` | `attribute`, `operator` (default `eq`), `value` | Compare a request/principal attribute against a value |
| `and` / `or` / `not` | `conditions` | Boolean combinators over nested conditions |
| `catalog_admin_required` | `required_roles` | Per-catalog admin delegation |
| `catalog_membership_required` | `allow_platform`, `allow_sysadmin`, `sysadmin_role` | Require membership in the target catalog |

## OAuth2 / OIDC Flow

The `auth` extension (`extensions/auth/authentication.py`) provides an
OAuth2 authorization code flow backed by the configured OIDC provider
(typically Keycloak):

```
GET  /auth/authorize            — redirect to IdP login
POST /auth/token                — exchange auth code for access token
POST /auth/refresh              — exchange refresh token for new access token
GET  /auth/userinfo             — return normalized profile from valid JWT
GET  /auth/logout               — clear session and redirect
```

The `iam` extension (`extensions/iam/service.py`) provides:

```
GET  /iam/me                    — effective permissions for the current principal
GET  /iam/me/roles              — roles of the current principal
GET  /iam/me/catalogs           — catalogs the current principal has access to
GET  /iam/jwks.json             — public JWKS for token verification
```

## Concurrency and Policy Registration

During startup, multiple extensions register policies for roles via
`IamModule.create_policy` / `IamModule.create_role`. The `PermissionProtocol`
implementation in `IamModule` serializes concurrent writes to the policy tables
via PostgreSQL transactions. Extensions discover the `PermissionProtocol`
implementation through `get_protocol(PermissionProtocol)` — there are no direct
module imports.

## Database Tables

All platform IAM tables live in the `iam` schema:

| Table | Purpose |
|-------|---------|
| `iam.principals` | Identity registry (UUID, provider, display_name) |
| `iam.identity_links` | Maps external identities (provider + subject_id) to principals |
| `iam.grants` | Role assignments scoped to platform or per-catalog |
| `iam.roles` | Role definitions |
| `iam.role_hierarchy` | Parent-child role relationships |
| `iam.policies` | Policy definitions (LIST-partitioned by partition_key) |
| `iam.refresh_tokens` | Refresh token storage |
| `iam.usage_counters` | Per-principal request counters (rate limiting) |

Per-catalog IAM (role grants for catalog members) lives in each catalog's
own schema under the same table names.

## Files

| Path | Purpose |
|------|---------|
| `src/dynastore/modules/iam/module.py` | `IamModule` — `AuthenticatorProtocol` + `PermissionProtocol` impl, lifespan |
| `src/dynastore/modules/iam/iam_service.py` | `IamService` — principal CRUD, JWT issuance, OIDC reconciliation |
| `src/dynastore/modules/iam/iam_queries.py` | SQL DDL and DML for all IAM tables |
| `src/dynastore/modules/iam/policies.py` | Policy evaluation helpers |
| `src/dynastore/modules/iam/conditions.py` | Condition handlers (rate_limit, max_count, time_window, catalog_admin_required, …) |
| `src/dynastore/modules/iam/identity_providers/oidc_identity.py` | `OidcIdentityProvider` — JWKS-backed JWT validation |
| `src/dynastore/modules/iam/presets/default_roles_baseline.py` | Default `sysadmin`, `admin`, `unauthenticated` roles |
| `src/dynastore/modules/iam/presets/public_access_baseline.py` | Optional `public_access` policy for anonymous discovery |
| `src/dynastore/extensions/iam/middleware.py` | `IamMiddleware` — request interception, principal resolution, policy evaluation |
| `src/dynastore/extensions/iam/service.py` | `IamExtension` — `/iam/` REST routes, JWKS endpoint |
| `src/dynastore/extensions/iam/presets/iam_baseline.py` | `sysadmin_full_access` policy and role bindings |
| `src/dynastore/extensions/auth/authentication.py` | `Authentication` extension — `/auth/` OAuth2/OIDC proxy routes |
| `src/dynastore/models/protocols/authentication.py` | `AuthenticatorProtocol` — identity-resolution contract |
| `src/dynastore/models/protocols/policies.py` | `PermissionProtocol`, `Policy`, `Principal`, `Role` |
| `src/dynastore/models/auth.py` | `Principal` runtime model |
