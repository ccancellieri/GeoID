# admin — Admin Extension

The `admin` extension exposes management endpoints and a web dashboard for operators with `sysadmin` or `admin` roles. It is deployed as part of the `geospatial-catalog` service.

## Activation

The extension is loaded when `SCOPE` includes `api_catalog`:

```yaml
SCOPE: "api_catalog"
```

It registers under the `/admin` prefix via `AdminService`.

---

## Policies

All `/admin/*` routes and the admin dashboard page require the `admin_access` policy, which is granted to the `sysadmin` and `admin` roles only. Anonymous users and users with the `user` role are denied.

Endpoint authorization is enforced dynamically by `IamMiddleware`, which evaluates `PermissionProtocol.evaluate_access(principals, path, method)` against the policy registry for every request. When the IAM module is not loaded the fail-closed `DefaultAuthorizer` protects privileged paths.

Admin policies are declared (not registered directly) via the `PolicyContributor` Protocol:

```python
from dynastore.extensions.admin.policies import admin_policies, admin_role_bindings

# AdminService implements PolicyContributor:
def get_policies(self):
    return admin_policies()

def get_role_bindings(self):
    return admin_role_bindings()
```

The IAM extension iterates `get_protocols(PolicyContributor)` at lifespan and forwards declarations to `PermissionProtocol` centrally — `AdminService` itself never touches the enforcement implementation. See PR #308 for the architectural rule.

---

## API Endpoints

### User Management (`/admin/users`)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/users` | List local users |
| `POST` | `/admin/users` | Create a local user |
| `GET` | `/admin/users/{principal_id}` | Get user |
| `PUT` | `/admin/users/{principal_id}` | Update user |
| `DELETE` | `/admin/users/{principal_id}` | Delete user |

### Principal Management (`/admin/principals`)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/principals` | Search principals across all providers |
| `POST` | `/admin/principals/{principal_id}/roles` | Assign a global role |
| `DELETE` | `/admin/principals/{principal_id}/roles/{role_name}` | Remove a global role |
| `POST` | `/admin/principals/{principal_id}/catalogs/{catalog_id}/roles` | Assign a catalog-scoped role |
| `DELETE` | `/admin/principals/{principal_id}/catalogs/{catalog_id}/roles/{role_name}` | Remove a catalog-scoped role |
| `GET` | `/admin/catalogs/{catalog_id}/users` | List users with roles in a catalog |

### Role and Policy Management

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/roles` | List roles (optional `catalog_id`) |
| `POST` | `/admin/roles` | Create a role |
| `PUT` | `/admin/roles/{role_name}` | Update a role |
| `DELETE` | `/admin/roles/{role_name}` | Delete a role |
| `GET` | `/admin/policies` | List policies |
| `POST` | `/admin/policies` | Create a policy |
| `PUT` | `/admin/policies/{policy_id}` | Update a policy |
| `DELETE` | `/admin/policies/{policy_id}` | Delete a policy |

### System Defaults

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/admin/reset-defaults` | Reprovision default policies/roles (sysadmin only) |
| `POST` | `/admin/rotate-jwt-secret` | Rotate the JWT signing secret (sysadmin only) |

---

## Files

| Path | Purpose |
|------|---------|
| `admin_service.py` | `AdminService` class; route handlers; declares policies via `PolicyContributor` (`get_policies` / `get_role_bindings`) |
| `policies.py` | `admin_policies()` / `admin_role_bindings()` — pure declaration helpers |
| `static/admin_panel.html` | General admin panel (user management, etc.) |

---

## Adding a New Admin Endpoint

1. Add a route handler to `admin_service.py` using the shared `router` on `AdminService`.
2. Add the new resource path pattern to the `admin_access` policy in `policies.py` if it is not already covered by `/admin/.*`.
