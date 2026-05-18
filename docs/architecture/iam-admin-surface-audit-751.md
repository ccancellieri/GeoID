# IAM / Admin / Self-Service HTTP Surface Audit (#751)

Phase 1 of [#751](https://github.com/un-fao/GeoID/issues/751). Audit-only —
no code changes here. Captures what the three surfaces actually look like
on `main` today (the issue body was filed earlier and several claims have
already been addressed by intervening PRs) and enumerates real consumers
so the consolidation PRs can land safely.

## TL;DR for reviewers

- The HTTP surface is already much closer to the Option A target than the
  issue body suggests. The duplicate admin endpoints under
  `/iam/admin/*`, the email-keyed admin mirror, and `IamExtension.gov_router`
  are **already gone**. `authorization_api.py` ships `me_router` only.
- The only DTO duplication still live is the historical RoleCreate /
  PolicyResponse / PrincipalResponse / AssignRoleRequest family in
  `packages/extensions/admin/.../models.py`. They are not duplicated
  elsewhere today — they are simply not shared. Consolidating them into
  `dynastore.models.protocols.policies` is single-site work.
- `IamExtension.stats_router` (mounted at `/iam/credentials/stats/*`)
  has **zero consumers** in this repo. Recommend deletion as part of the
  cleanup, separate from the DTO move.

## Current surface map

### `/admin/*` (canonical management surface)

File: `packages/extensions/admin/src/dynastore/extensions/admin/admin_service.py`
Router: `APIRouter(prefix="/admin", tags=["Authentication & Authorization"])`

Identifiers: `principal_id: UUID` end to end. Catalog scoping via
`?catalog_id=` query string or `/admin/catalogs/{catalog_id}/...` path.

Endpoint inventory:

| Route                                                                 | Verb   |
|-----------------------------------------------------------------------|--------|
| `/admin/principals`                                                        | GET, POST |
| `/admin/principals/{principal_id}`                                         | GET, PUT, DELETE |
| `/admin/platform/principals/{principal_id}/roles`                     | POST |
| `/admin/platform/principals/{principal_id}/roles/{role}`              | DELETE |
| `/admin/platform/principals/{principal_id}/roles`                     | GET |
| `/admin/catalogs/{catalog_id}/principals/{principal_id}/roles`        | POST, GET |
| `/admin/catalogs/{catalog_id}/principals/{principal_id}/roles/{role}` | DELETE |
| `/admin/catalogs/{catalog_id}`                                        | GET (provisioning view) |
| `/admin/catalogs/{catalog_id}/principals`                                  | GET |
| `/admin/roles`                                                        | GET, POST |
| `/admin/roles/{role_name}`                                            | GET, PUT, DELETE |
| `/admin/hierarchies`                                                  | POST, DELETE |
| `/admin/hierarchies/{role_name}`                                      | GET |
| `/admin/policies`                                                     | GET, POST |
| `/admin/policies/{policy_id}`                                         | GET, PUT, DELETE |
| `/admin/policies/{policy_id}/usage`                                   | GET, DELETE |
| `/admin/reset-defaults`                                               | POST |
| `/admin/rotate-jwt-secret`                                            | POST |

DTOs (all in `extensions/admin/.../models.py`):
`UserCreate`, `UserUpdate`, `UserResponse`,
`RoleCreate`, `RoleUpdate`, `RoleResponse`,
`PolicyCreate`, `PolicyUpdate`, `PolicyResponse`,
`UsageRow`, `UsagePage`, `UsageResetResponse`,
`AssignRoleRequest`, `CatalogRoleAssignment`, `PrincipalResponse`,
`ProvisioningTaskView`, `CatalogProvisioningView`.

### `/iam/me/*` (self-service)

File: `packages/extensions/iam/src/dynastore/extensions/iam/authorization_api.py`
Router: `me_router = APIRouter(prefix="/me", ...)` mounted under
`IamExtension.router` (prefix `/iam`), final paths `/iam/me/*`.

Identifiers: identity tuple `(provider, subject_id)` read from
`request.state.principal` / `request.state.identity`. The principal UUID
is only resolved internally to look up catalog grants; it is never in a
path or body.

Endpoint inventory:

| Route                                | Verb | Returns                              |
|--------------------------------------|------|--------------------------------------|
| `/iam/me`                            | GET  | `{principal, roles}`                 |
| `/iam/me/available-roles`            | GET  | platform or per-catalog role registry |
| `/iam/me/roles/global`               | GET  | `List[str]` platform roles           |
| `/iam/me/roles/catalogs/{catalog_id}`| GET  | `List[str]` catalog-scope roles      |
| `/iam/me/catalogs`                   | GET  | `List[CatalogAccessResponse]`        |
| `/iam/me/catalogs/{catalog_id}`      | GET  | `EffectiveAuthorizationResponse`     |

DTOs (only used here): `EffectiveAuthorizationResponse`,
`CatalogAccessResponse`.

### `/iam/*` (IAM extension residual: auth + stats)

File: `packages/extensions/iam/src/dynastore/extensions/iam/service.py`
Two sub-routers mounted under `IamExtension.router`:

- `auth_router` prefix `/auth` → exposes `/iam/auth/jwks.json` (`GET`).
  This is the JWKS-discovery surface that token validators hit.
- `stats_router` prefix `/credentials` → exposes
  `/iam/credentials/stats/summary` (`GET`) and
  `/iam/credentials/stats/logs` (`GET`).

The `gov_router` referenced in the issue body no longer exists; the
admin endpoints that were once exposed under `/iam/admin/*` and the
email-keyed mirror referenced in `service.py:398-399` of the issue body
have already been removed. The current `service.py:300` mounts
`me_router` only.

### `/auth/*` (OIDC bridge — separate extension)

File: `packages/extensions/auth/src/dynastore/extensions/auth/authentication.py`
Router prefix `/auth`. Endpoints:
`/auth/authorize`, `/auth/userinfo`, `/auth/token`, `/auth/refresh`,
`/auth/logout`, `/auth/debug`.

Out of scope for #751 (it is the OIDC adapter, not management).
Mentioned here only because the issue's tag-grouping commentary in
`build_iam_openapi_schema` groups it under the same
"Authentication & Authorization" Swagger tag.

## Consumer inventory

Concrete callers of `/admin/*` and `/iam/me/*` found via repo grep
(`'/admin/<segment>'`, `'/iam/me'`, `'/iam/auth'`, `'/iam/credentials'`).

### Web UI (`packages/extensions/web/.../static/`)

Single chokepoint: `common/api.js`. All `/admin/*` calls go through
`getJSON` / `postJSON` / `putJSON` / `deleteJSON` helpers — so a path
rename or DTO-shape change touches **one file**.

Calls observed:

- `fetchMe()` → `GET /iam/me`
- `fetchMyCatalogs()` → `GET /iam/me/catalogs`
- `listRoles / createRole / updateRole / deleteRole` → `/admin/roles[/{name}]`
- `listPolicies / createPolicy / updatePolicy / deletePolicy` → `/admin/policies[/{id}]`
- `searchPrincipals` → `GET /admin/principals?q=&role=&catalog_id=&limit=&offset=`
- `listCatalogUsers` → `GET /admin/catalogs/{cid}/principals`
- `assignGlobalRole / removeGlobalRole` → `/admin/platform/principals/{pid}/roles[/{role}]`
- `assignCatalogRole / removeCatalogRole` → `/admin/catalogs/{cid}/principals/{pid}/roles[/{role}]`
- `getCatalogProvisioning` → `GET /admin/catalogs/{cid}`
- `createStacCatalog / createStacCollection / postFeatures` → STAC, out of scope.

`static/admin/governance.html` + `governance.js` only display the path
strings (`PUT /admin/roles/${r.name}`) as label text; they do not
construct the requests themselves — those go through the helpers above.

### Notebooks (in-repo)

- `packages/core/src/dynastore/modules/iam/notebooks/iam01_provision_tenant_and_assign_roles.ipynb`
  — uses `/admin/principals`, `/admin/principals/{principal_id}`, `/admin/catalogs/{cid}/...`.
- `.../iam/notebooks/iam02_self_service_permission_introspection.ipynb`
  — uses `/iam/me/available-roles`, `/iam/me/catalogs`, `/iam/me/catalogs/{cid}`,
  `/iam/me/roles/catalogs/{cid}`, `/iam/me/roles/global`.
- `.../iam/notebooks/iam03_custom_roles_and_policies.ipynb`
  — uses `/admin/{users,roles,roles/{name},policies,policies/{id},catalogs/{cid}/...}`.
- `packages/core/src/dynastore/modules/catalog/notebooks/cat01_provision_tenant_catalog.ipynb`
  — `/admin/catalogs/{cid}/...`, `/admin/principals`.
- `packages/core/src/dynastore/modules/elasticsearch/notebooks/collection_vault_geoid_only.ipynb`
  — `/admin/{users,users/{pid},roles,roles/{name},policies,policies/{id}}`. Contains
  one prose mention of a long-removed `/iam/governance/hierarchies` endpoint
  in retrospective-analysis text — not an actual call; flag for prose cleanup
  separate from this audit.
- `packages/extensions/web/.../notebooks/uc_lookup_only_anonymous_write.ipynb`
  — `/admin/{policies,policies/{id},roles,roles/{name}}`.
- `notebooks/admin_boundaries_fixed_schema/walkthrough.ipynb`
  — no admin/iam/auth calls.

### Tests

- `tests/dynastore/extensions/admin/integration/test_admin_routes.py` —
  full coverage of the `/admin/*` surface above.
- `tests/dynastore/extensions/admin/unit/test_*.py` — DTO/projection unit
  tests.
- `tests/dynastore/extensions/iam/integration/test_authorization_api.py` —
  `/iam/me/roles/global`, `/iam/me/catalogs`, `/iam/me/roles/catalogs/{cid}`,
  plus a 401-when-anonymous assertion against `/iam/me/roles/global`.
- `tests/dynastore/extensions/iam/test_openapi_tag_grouping.py` — mounts
  every auth/iam/admin router and asserts they all carry the
  `Authentication & Authorization` Swagger tag.

### Sister repos

Not in scope for this audit — IAM HTTP surface is geoid-local. Per the
three-repo sync directive (`feedback_geoid_dynastore_catalog_sync.md`),
dynastore and fao-aip-catalog depend on geoid wheels but do not pin paths
into `/admin/*` or `/iam/me/*` themselves.

## Findings vs. issue body

| Issue body claim                                                       | Status on `main` |
|------------------------------------------------------------------------|------------------|
| `/iam/admin/*` (email-keyed admin mirror) duplicates `/admin/*`        | **Resolved** — `authorization_api.py` has only `me_router`. |
| `service.py:398-399` mounts `admin_router` + `me_router` under `/iam`  | **Resolved** — current code mounts only `me_router` (line 300). |
| `IamExtension.gov_router` ships and should be removed                  | **Resolved** — symbol no longer exists. |
| `list catalog users` duplicated in `admin_service.py:382` and `authorization_api.py:596` | **Resolved** — only `admin_service.py:566` remains. |
| Inconsistent identifiers `principal_id: UUID` vs `email: EmailStr`     | **Resolved** — `/admin/*` uses `principal_id: UUID`; `/iam/me/*` reads identity from request state (no email in path/body except as a returned display field). |
| `RoleCreate/Update/Response` etc. duplicated across `admin/models.py`, `iam/service.py`, `authorization_api.py` | **Partially resolved** — the duplicates in `service.py` / `authorization_api.py` are gone; the originals in `admin/models.py` still live there and are not yet exposed via `models/protocols/policies.py`. |
| Keep `auth_router` (`/iam/auth/jwks.json`)                             | Still present. |
| Keep `stats_router` (`/iam/credentials/stats/*`)                       | Still present, but **zero in-repo consumers** — recommend deletion. |

## Recommended next steps (each its own PR)

Ordered by blast radius — small, mergeable, reviewable.

1. **DTO promotion to `models/protocols/policies.py`** *(refactor PR)*
   Move `RoleCreate`, `RoleUpdate`, `RoleResponse`, `PolicyCreate`,
   `PolicyUpdate`, `PolicyResponse`, `AssignRoleRequest`,
   `CatalogRoleAssignment`, `PrincipalResponse` from
   `extensions/admin/.../models.py` to `dynastore.models.protocols.policies`.
   Keep the old import paths as thin re-exports for one cycle so external
   consumers (if any wheels pin them) don't break. Tests:
   `pytest packages/extensions/iam packages/extensions/admin`.

   Rationale: these are wire schemas for a permission management surface;
   `models/protocols/policies.py` already exposes the `Policy` / `Role` /
   `Principal` domain types alongside `PermissionProtocol`. The wire DTOs
   belong in the same module so a future second consumer (an IAM CLI,
   another extension, the planned admin SDK) doesn't have to re-import
   from an extension package.

2. **Delete `IamExtension.stats_router`** *(separate PR, refactor)*
   `/iam/credentials/stats/summary` and `/iam/credentials/stats/logs`
   have no in-repo consumers and the underlying `get_stats_summary` /
   `get_access_logs` reach into `dynastore.modules.stats.storage` (the
   existing TODO comment in `service.py` flags this as a layer
   violation). Audit external deployments first; if clean, remove the
   router and the layer-violating import.

3. **OpenAPI deprecation pass** *(docs/OpenAPI PR)*
   The issue body's phase 3 (deprecate duplicates in OpenAPI) is now a
   no-op for routes — there are no duplicates to deprecate. The
   remaining work is to annotate the DTOs renamed in step 1 with a
   `deprecated` flag on the old import paths (or skip if step 1 is a
   straight move with re-exports).

4. **Notebook prose cleanup** *(docs PR)*
   `collection_vault_geoid_only.ipynb` cell at line 294 narrates a
   prior architecture using `/iam/governance/hierarchies`. The endpoint
   is gone; the narrative remains useful as background but should be
   rephrased so a new reader doesn't try the endpoint.

## What stays deferred

- **Phase 3 / 4 of the issue** (OpenAPI deprecation and DTO-duplicate
  removal in production code) — not in this PR.
- **External-deployment consumer audit** for `stats_router` removal — the
  stats endpoints have zero in-repo consumers but production
  deployments/dashboards may still hit them. Confirm with operators
  before deleting.
- **Auth invariant scan** — `IamMiddleware` / `Policy` / `Role` /
  `ConditionHandler` are the enforcement path and are out of scope for
  #751 per the issue's closing note. This audit deliberately did not
  walk the enforcement code.
