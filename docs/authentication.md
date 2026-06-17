# DynaStore Authentication -- v2.0

> See also: [Authentication & Authorization architecture](components/auth.md) for the request-time policy engine, middleware, and identity resolution.

## Identity Provider: External OIDC (IdP-Agnostic)

DynaStore v2.0 delegates authentication to external OIDC-compliant identity providers (Keycloak, Auth0, Azure AD, etc.). The platform is IdP-agnostic — any provider implementing `IdentityProviderProtocol` can be registered.

### Configuration

| Environment Variable | Required | Description |
|---------------------|----------|-------------|
| `IDP_ISSUER_URL` (alias: `KEYCLOAK_ISSUER_URL`) | Yes* | IdP realm issuer URL (e.g., `https://keycloak.example.com/realms/dynastore`) |
| `IDP_CLIENT_ID` (alias: `KEYCLOAK_CLIENT_ID`) | Yes* | SPA / login client ID (e.g., `geoid-fe`). Used for the OAuth2 authorization-code redirect; **not** used for audience validation. |
| `IDP_AUDIENCE` (alias: `KEYCLOAK_AUDIENCE`) | Recommended | API audience client ID (e.g., `geoid-be`) — the value PyJWT enforces against the token's `aud` claim. May be left unset in single-client setups, where the provider validates tokens against `IDP_CLIENT_ID`. |
| `IDP_CLIENT_SECRET` (alias: `KEYCLOAK_CLIENT_SECRET`) | If client is confidential | OAuth2 client secret for the SPA / login client. |
| `IDP_PUBLIC_URL` (alias: `KEYCLOAK_PUBLIC_URL`) | No | Browser-reachable IdP URL (if different from internal `IDP_ISSUER_URL`). |
| `IDP_ROLES_CLAIM_PATH` | No | Dotted JSON path used to locate roles inside the JWT. Defaults to `resource_access.${IDP_AUDIENCE}.roles`. See "Role claim path" below. |
| `SESSION_SECRET_KEY` | Recommended | Signs the Starlette session cookie. Auto-generated per-process if unset (inconsistent across pods — set it). Also the **last-resort source** for the config secret-encryption key (see "Secret encryption key" below). |

*Required when using Keycloak. Other IdP implementations may use different env vars.

### Bootstrap vs steady state — `IDP_*` env is a one-time seed

The `IDP_*` variables are read **only to seed the platform `idp_config` row on a
cold boot where none exists**. The cold-boot seed never overwrites an existing
row, so once the first boot has materialised `idp_config` in the platform-config
store, that DB row is the single source of truth and the `IDP_*` env becomes
redundant.

Practical consequence for deployments: keep `IDP_*` set for the **first** boot of
a fresh database, then you may remove `IDP_ISSUER_URL` / `IDP_CLIENT_ID` /
`IDP_AUDIENCE` / `IDP_CLIENT_SECRET` from the runtime environment. Change the live
configuration afterwards via the Configs API (`platform_configs`, class key
`idp_config`) — no restart and no env edit required. To re-seed from env on a
fresh database, simply delete the `idp_config` row first.

The minimum to cold-boot a working sysadmin on a fresh DB is therefore just
`IDP_ISSUER_URL` (plus the `geoid.sysadmin` realm role on the operator's account,
which `OidcRoleSyncConfig` maps to the internal `sysadmin` grant). `IDP_CLIENT_ID`
defaults to `dynastore-api`; `IDP_CLIENT_SECRET` is only needed for confidential
OAuth2 flows — bearer-token validation does not use it.

### Secret encryption key

Config fields typed as secrets (e.g. `idp_config.client_secret`) are encrypted at
rest with a key derived, in order, from `DYNASTORE_SECRET_KEY` → `JWT_SECRET` →
`SESSION_SECRET_KEY`. Provisioning any one of them satisfies encryption; a
deployment that already sets `SESSION_SECRET_KEY` needs no separate key. Provision
a dedicated `DYNASTORE_SECRET_KEY` when you need to rotate the session key
independently of stored secrets — rotating the active source orphans anything
encrypted under it. If no source is set, the `idp_config` cold-boot seed still
registers a working (public-client) OIDC provider without the secret, so
token-authenticated login is never blocked purely by a missing encryption key.

### Choosing the audience

`IDP_CLIENT_ID` identifies the OAuth2 login client (used in the `/authorize`
redirect); `IDP_AUDIENCE` is the JWT `aud` claim the resource server
validates. They differ whenever the SPA login client and the API audience are
separate Keycloak clients (the standard two-client layout).

- **Single-client setup** (one client used for both login and as the API
  audience): `IDP_AUDIENCE` may be left unset — the provider validates
  tokens against `IDP_CLIENT_ID`.
- **Two-client setup** (recommended): set `IDP_CLIENT_ID=geoid-fe` (the
  public PKCE login client) and `IDP_AUDIENCE=geoid-be` (the bearer-only
  API audience). The frontend obtains a token via `geoid-fe` audienced for
  `geoid-be`; the API enforces `aud == geoid-be`.

### Role claim path

`IDP_ROLES_CLAIM_PATH` selects exactly one location in the JWT for role
extraction — there is no silent merge across paths. Three common values:

| Path | When to use it |
|------|----------------|
| `resource_access.${IDP_AUDIENCE}.roles` (default) | Roles assigned to the API audience client in Keycloak. Standard pattern. |
| `resource_access.account.roles` | Roles sit on Keycloak's built-in `account` client (current FAO realm setup, where `sysadmin` was assigned there). |
| `realm_access.roles` | Roles are assigned at the realm level rather than per-client. |

The decoded identity dict exposes the configured-path result as `roles`;
`realm_roles` and `client_roles` remain available as separate entries for
backward compatibility, but downstream consumers should migrate to `roles`.

### Auth Flow

1. Client calls `GET /auth/authorize` with OAuth2 parameters
2. DynaStore redirects to IdP authorization endpoint
3. After IdP login, callback returns with authorization code
4. Client exchanges code for tokens at IdP's token endpoint
5. Client calls DynaStore APIs with `Authorization: Bearer <access_token>`
6. DynaStore validates JWT via IdP's JWKS endpoint

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/auth/authorize` | GET | OAuth2 authorization redirect to IdP |
| `/auth/userinfo` | GET | Returns user profile from IdP token (OIDC spec) |
| `/iam/me` | GET | Principal + platform roles (app-level "who am I") |
| `/auth/logout` | GET | Clears session, optional `redirect_uri` |
| `/auth/debug` | GET | Auth state inspection (requires valid token) |

## Principal Model

All authenticated identities resolve to a `Principal`:

- **Table**: `iam.principals`
- **Key fields**: `id` (UUID), `identifier`, `display_name`, `roles` (JSONB), `is_active`, `valid_from`
- External identities linked via `iam.identity_links` (provider + subject_id -> principal_id)

## Authorization

Authorization uses the `PermissionProtocol` with RBAC + ABAC:

- **Roles**: Defined in `iam.roles` with hierarchical inheritance
- **Policies**: ALLOW/DENY rules with action/resource/condition matching
- **Registration**: Extensions register policies during lifespan via `PermissionProtocol.register_policy()`

### Sysadmin Role

The `sysadmin` role provides full platform administration. It is assigned via the external IdP (e.g., Keycloak realm role) and resolved to a local Principal with elevated privileges.

## On-Premise Deployment

For deployments without internet access to a cloud IdP instance:

1. Deploy Keycloak (or another OIDC provider) alongside DynaStore (Docker Compose or K8s)
2. Create a realm and the two clients for DynaStore (`geoid-fe` for login, `geoid-be` for the API audience)
3. Set `IDP_ISSUER_URL`, `IDP_CLIENT_ID=geoid-fe`, and `IDP_AUDIENCE=geoid-be`
4. Configure user federation (LDAP, Active Directory) in the IdP if needed

## Programmatic Access (Service-to-Service)

For machine-to-machine authentication, use OAuth2 Client Credentials flow:

1. Create a confidential client in your IdP (e.g., Keycloak)
2. Request a token using client credentials:
   ```bash
   curl -X POST "${IDP_ISSUER_URL}/protocol/openid-connect/token" \
     -d "grant_type=client_credentials" \
     -d "client_id=my-service" \
     -d "client_secret=my-service-secret"
   ```
3. Use the returned `access_token` as a Bearer token in DynaStore API calls:
   ```bash
   curl -H "Authorization: Bearer ${ACCESS_TOKEN}" \
     http://localhost/catalogs
   ```

The IdP issues short-lived JWTs; no long-lived API keys are stored in DynaStore.
