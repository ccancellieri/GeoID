# Admin Extension

The admin extension provides system administration endpoints and dashboard pages for managing users, API keys, roles, and policies.

## Endpoints

### User & Key Management

| Method | Path | Description |
|--------|------|-------------|
| GET | `/admin/principals` | List or search principals (filterable by `provider`, `q`, `role`, `catalog_id`, `limit`, `offset`) |
| POST | `/admin/principals` | Create a new local user (username/password login) |
| GET | `/admin/principals/{id}` | Get principal details |
| PUT | `/admin/principals/{id}` | Update principal (roles, attributes) |
| DELETE | `/admin/principals/{id}` | Delete principal and all linked keys |
| GET | `/admin/keys` | Search API keys |
| POST | `/admin/keys` | Create API key for a principal |
| DELETE | `/admin/keys/{hash}` | Revoke an API key |

### Roles & Policies

| Method | Path | Description |
|--------|------|-------------|
| GET | `/admin/roles` | List all roles |
| POST | `/admin/roles` | Create a role |
| PUT | `/admin/roles/{name}` | Update role policies |
| DELETE | `/admin/roles/{name}` | Delete a role |
| GET | `/admin/policies` | List all policies |
| POST | `/admin/policies` | Create a policy |
| PUT | `/admin/policies/{id}` | Update a policy |
| DELETE | `/admin/policies/{id}` | Delete a policy |

### System Defaults

| Method | Path | Description |
|--------|------|-------------|
| POST | `/admin/reset-defaults` | Reprovision default policies/roles (sysadmin only) |
| POST | `/admin/rotate-jwt-secret` | Rotate the JWT signing secret (sysadmin only) |

## Authorization

All admin endpoints require the `sysadmin` or `admin` role. The `admin_access` policy grants access to `/admin/.*` routes. Endpoint-level enforcement is performed dynamically by `IamMiddleware` against the policy registry — there are no route-level `Depends` guards in `admin_service.py`.

## Dashboard Pages

The admin extension contributes one web page via `@expose_web_page`:

- **Admin Panel** (`admin_panel`) — user/key/role/policy management UI

## Files

| Path | Purpose |
|------|---------|
| `src/dynastore/extensions/admin/admin_service.py` | Main service — routes, CRUD endpoints |
| `src/dynastore/extensions/admin/policies.py` | Policy registration for admin routes |
| `src/dynastore/extensions/admin/static/admin_panel.html` | Admin dashboard page |
