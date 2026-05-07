# DynaStore Authentication -- v2.0

## Identity Provider: External OIDC (IdP-Agnostic)

DynaStore v2.0 delegates authentication to external OIDC-compliant identity providers (Keycloak, Auth0, Azure AD, etc.). The platform is IdP-agnostic — any provider implementing `IdentityProviderProtocol` can be registered.

### Configuration

| Environment Variable | Required | Description |
|---------------------|----------|-------------|
| `IDP_ISSUER_URL` (alias: `KEYCLOAK_ISSUER_URL`) | Yes* | IdP realm issuer URL (e.g., `https://keycloak.example.com/realms/dynastore`) |
| `IDP_CLIENT_ID` (alias: `KEYCLOAK_CLIENT_ID`) | Yes* | SPA / login client ID (e.g., `geoid-fe`). Used for the OAuth2 authorization-code redirect; **not** used for audience validation. |
| `IDP_AUDIENCE` (alias: `KEYCLOAK_AUDIENCE`) | Recommended | API audience client ID (e.g., `geoid-be`) — the value PyJWT enforces against the token's `aud` claim. Falls back to `IDP_CLIENT_ID` for legacy single-client setups (with a deprecation warning). |
| `IDP_CLIENT_SECRET` (alias: `KEYCLOAK_CLIENT_SECRET`) | If client is confidential | OAuth2 client secret for the SPA / login client. |
| `IDP_PUBLIC_URL` (alias: `KEYCLOAK_PUBLIC_URL`) | No | Browser-reachable IdP URL (if different from internal `IDP_ISSUER_URL`). |
| `IDP_ROLES_CLAIM_PATH` | No | Dotted JSON path used to locate roles inside the JWT. Defaults to `resource_access.${IDP_AUDIENCE}.roles`. See "Role claim path" below. |
| `SESSION_SECRET_KEY` | Recommended | Secret for session cookie encryption. Auto-generated if not set. |

*Required when using Keycloak. Other IdP implementations may use different env vars.

### Migrating from `IDP_CLIENT_ID`-as-audience to `IDP_AUDIENCE`

Earlier releases used `IDP_CLIENT_ID` for both the OAuth2 client identifier
(in the `/authorize` redirect) **and** for the JWT `aud` claim that the
resource server validates. That conflation breaks when the SPA login client
and the API audience are separate Keycloak clients (the standard two-client
layout).

- **Single-client legacy setup** (one client used for both login and as the
  API audience): set `IDP_AUDIENCE=$IDP_CLIENT_ID` to silence the
  deprecation warning while keeping today's behaviour. Leaving `IDP_AUDIENCE`
  unset still works during the deprecation window — the resource server
  falls back to `IDP_CLIENT_ID` and logs a warning at startup.
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
| `/auth/userinfo` | GET | Returns user profile from IdP token |
| `/auth/me` | GET | Alias for `/auth/userinfo` |
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
