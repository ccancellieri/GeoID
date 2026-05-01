# Keycloak Setup Guide for GeoID

Configuration recipe for the **Keycloak admin** running the IdP that GeoID will authenticate against.

GeoID is a vanilla OIDC RP — any OIDC-compliant IdP (Okta, Auth0, Azure AD, Google) works. This guide focuses on Keycloak because it is what every GeoID deployment ships against by default. The local-dev realm export at `src/dynastore/docker/keycloak/realm-export.json` is the working reference; this document explains *what to recreate in your remote Keycloak* and *what to send back to the GeoID admin*.

---

## What the GeoID admin needs from you

Send back the following four values once the realm is configured. They become `IDP_*` env vars on the GeoID side.

| Send | Description | Example |
|---|---|---|
| **Issuer URL (backend-internal)** | URL the GeoID API services use to reach Keycloak for OIDC discovery, JWKS, token validation, userinfo. Often a private/internal hostname. | `https://keycloak.internal/realms/geoid` |
| **Issuer URL (browser-facing)** | URL browsers redirect to during the auth code flow. Often a public hostname behind your edge proxy. | `https://login.example.org/realms/geoid` |
| **Client ID** | The confidential client GeoID API services authenticate with. | `geoid-api` |
| **Client secret** | The shared secret of that confidential client. Treat as a credential. | `<32+ chars>` |

Optional: a separate **public** client id (e.g. `geoid-web`) for the browser-facing dashboard / JupyterLite redirect flow. This client is `publicClient=true` (no secret).

GeoID consumes these as:

```bash
IDP_TYPE=oidc
IDP_ISSUER_URL=https://keycloak.internal/realms/geoid
IDP_PUBLIC_URL=https://login.example.org/realms/geoid
IDP_CLIENT_ID=geoid-api
IDP_CLIENT_SECRET=<the-secret>
# IDP_AUDIENCE defaults to IDP_CLIENT_ID. Override only if your tokens
# carry a different `aud` claim.
```

---

## What you (Keycloak admin) need to create

### 1. Realm

- Realm name: anything; convention is `geoid`. **The realm name appears in the issuer URL**, so coordinate with the GeoID admin.
- `enabled = true`
- `sslRequired`: production should be `external` or `all`. Dev/local can use `none`.
- Default token lifespans are fine; defaults to 5-minute access tokens, 30-minute refresh.

### 2. Realm roles

GeoID maps Keycloak roles to its internal `DefaultRole` enum (`models/protocols/authorization.py:34`). Create at least these realm-level roles:

| Realm role | GeoID `DefaultRole` | Notes |
|---|---|---|
| `sysadmin` | `SYSADMIN` | Full access. Use for break-glass admins only. |
| `admin` | `ADMIN` | Operational admin. |
| `user` | `USER` | Authenticated end user. |
| `viewer` | (read-only consumer) | Optional; deployment-specific. |

> The names above are the convention shipped in the local realm export. They are case-sensitive. GeoID's middleware (`extensions/iam/middleware.py:169`) explicitly checks `DefaultRole.SYSADMIN.value in principal_role`, so the literal string `sysadmin` matters.

Anonymous (unauthenticated) requests are mapped to `DefaultRole.ANONYMOUS` automatically — do not create a `anonymous` role in Keycloak.

### 3. Client roles (per client)

For finer-grained authorization, GeoID also reads **client roles** out of `resource_access.{client_id}.roles`. The local realm uses `catalog_admin` on `geoid-api` for the `testadmin` user; it gates catalog-admin operations. Create whichever client roles your deployment needs and assign them to users (or groups) the same way you assign realm roles.

### 4. Clients

#### `geoid-api` (confidential, used by GeoID API services)

| Setting | Value | Notes |
|---|---|---|
| Client ID | `geoid-api` (or whatever you send to the GeoID admin) | |
| Protocol | `openid-connect` | |
| `publicClient` | `false` | Confidential — client_secret required. |
| `bearerOnly` | `false` | Must accept token introspection. |
| `directAccessGrantsEnabled` | `true` | Enables `grant_type=password` for service-account-style flows + the test users; turn off in production if you don't need the password grant. |
| `standardFlowEnabled` | `true` | Authorization Code flow. |
| `serviceAccountsEnabled` | `true` | Lets GeoID service principals act as themselves. |
| `redirectUris` | All API hostnames the OIDC redirect can land at (path-suffix wildcards OK). E.g. `https://api.example.org/*`, `https://api.example.org:8080/*`. | Internal-only hosts can be omitted if no browser redirect lands there. |
| `webOrigins` | Same hosts as redirect URIs (sans path), or `+` to inherit from redirectUris. | CORS for the browser auth flow. |
| **Default client scopes** | `web-origins`, `acr`, `profile`, `roles`, `email` | The `roles` scope is **load-bearing** — it places `realm_access.roles` and `resource_access.{client_id}.roles` into the access token. Without it GeoID sees an empty role list. |
| Optional client scopes | (none required) | |

#### `geoid-web` (public, used by the browser dashboard / JupyterLite)

| Setting | Value | Notes |
|---|---|---|
| Client ID | `geoid-web` | |
| Protocol | `openid-connect` | |
| `publicClient` | `true` | No secret — uses PKCE. |
| `directAccessGrantsEnabled` | `false` | Browser flow only. |
| `standardFlowEnabled` | `true` | Authorization Code + PKCE. |
| `serviceAccountsEnabled` | `false` | |
| `redirectUris` | The browser-facing GeoID web URL (e.g. `https://app.example.org/*`). | |
| `webOrigins` | Same. | |
| Default client scopes | Same as `geoid-api`. | |

### 5. Users

Production users are provisioned however your IdP normally provisions them (federation, manual, JIT). For every user assign:

- the appropriate **realm role** (`sysadmin` / `admin` / `user`) so GeoID's middleware maps them to a `DefaultRole`;
- any **client roles** under `geoid-api` your deployment uses (e.g. `catalog_admin`).

For smoke-testing parity with the local stack, the convention is:

| Username | Realm roles | `geoid-api` client roles |
|---|---|---|
| `testadmin` | `admin` | `catalog_admin` |
| `testuser` | `user` | (none) |
| `testviewer` | `viewer` | (none) |

Set passwords as you see fit; communicate them to the GeoID admin out-of-band only.

---

## What GeoID expects to find in the access token

GeoID validates every incoming `Authorization: Bearer <jwt>` header against the IdP's JWKS. The validation logic lives in `oidc_identity.py:169 validate_token`. It checks:

- **Signature** — verified against the JWKS resolved from `<IDP_ISSUER_URL>/.well-known/openid-configuration`. RS256 is the default Keycloak signing algorithm; HS256 is **not** supported.
- **`exp`** (expiry) — must be in the future.
- **`iss`** (issuer) — must equal `IDP_ISSUER_URL`.
- **`aud`** (audience) — must include `IDP_AUDIENCE` (defaults to `IDP_CLIENT_ID`). If your Keycloak emits tokens with `aud=account` by default, configure an **Audience protocol mapper** on the `geoid-api` client to add the client_id to `aud` (Keycloak admin console → Client Scopes → `roles` → Add mapper → Audience → "Included Client Audience" = `geoid-api`). This is one of the most common gotchas.
- **`iss` (issuer) URL stability** — Keycloak by default emits `iss` matching the **request URL** (browser-visible vs container-internal differ). If your callers reach Keycloak via different hostnames (browser via `https://login.example.org`; backend services via `http://keycloak:8080` on a private network), tokens carry different `iss` values and fail the catalog's `iss == IDP_ISSUER_URL` check from one path or the other. **Fix in Keycloak 26**: set `KC_HOSTNAME=<the-canonical-issuer-url>` AND `KC_HOSTNAME_STRICT=true` on the Keycloak server. Pre-26 the variable was `KC_HOSTNAME_URL`. The local docker-compose fragment at `src/dynastore/docker/compose.keycloak.yml` already does this. Symptom when missing: every authenticated request 403s with "Deny by Default — No matching ALLOW policy found".
- **`sub` claim missing** — Keycloak normally populates `sub` from the user's UUID via the standard subject mapper. If the realm export omits the mapper or it's been disabled, the token carries `sub: null` and geoid cannot identify the principal (every request resolves to ANONYMOUS). Verify in Keycloak admin console → Client Scopes → `profile` → Mappers → "subject" (should be enabled). If absent: add a "User Property" mapper with property `id`, token claim `sub`, claim type `String`, "Add to access token" enabled.

Then GeoID extracts (from `oidc_identity.py:262-266`):

```jsonc
{
  "sub": "<keycloak user id, immutable>",
  "email": "...",
  "name": "...",
  "realm_roles":   claims["realm_access"]["roles"],
  "client_roles":  claims["resource_access"]["<IDP_CLIENT_ID>"]["roles"]
}
```

If the `roles` client scope is missing, both `realm_roles` and `client_roles` will be empty and every request resolves to `ANONYMOUS` even with a valid token. This is the second-most-common gotcha.

---

## Quick verification (smoke test)

After you finish provisioning, the GeoID admin can run this from any host that can reach the issuer URL:

```bash
TOKEN=$(curl -s -X POST "$IDP_ISSUER_URL/protocol/openid-connect/token" \
  -d "grant_type=password" \
  -d "client_id=$IDP_CLIENT_ID" \
  -d "client_secret=$IDP_CLIENT_SECRET" \
  -d "username=testadmin" \
  -d "password=<password>" \
  | jq -r .access_token)

# Decode the payload (no signature check) and confirm the load-bearing claims are there
echo "$TOKEN" | cut -d. -f2 | base64 -d 2>/dev/null \
  | jq '{iss, aud, exp, sub, email, realm_access, resource_access}'
```

The output must show:

- `iss` exactly equal to `IDP_ISSUER_URL`
- `aud` containing `IDP_CLIENT_ID` (or your override `IDP_AUDIENCE`)
- `realm_access.roles` listing the realm roles (`admin` / `user` / etc.)
- `resource_access.<IDP_CLIENT_ID>.roles` listing the client roles (`catalog_admin`, etc.)

If any of these are missing or wrong, fix the realm/client configuration before pointing GeoID at it.

---

## Reference — the local realm export

`src/dynastore/docker/keycloak/realm-export.json` is the literal Keycloak export that the docker-compose dev stack imports. It is the authoritative source for the schema of every setting above. When in doubt, diff your remote configuration against that file.

The file is for local-dev only — secrets are placeholders. **Do not import it into your production realm verbatim.** Treat it as a reference for *which fields to set*.

---

## Related

- Geoid-side env vars + provider-pluggability: `README.md` (this directory).
- The OIDC RP implementation: `oidc_identity.py`.
- IAM middleware that consumes the validated token: `src/dynastore/extensions/iam/middleware.py:92`.
- Default-role mapping: `src/dynastore/models/protocols/authorization.py:34`.
