# Keycloak Setup Guide for GeoID

Configuration recipe for the **Keycloak admin** running the IdP that GeoID will authenticate against.

GeoID is a vanilla OIDC RP — any OIDC-compliant IdP (Okta, Auth0, Azure AD, Google) works. This guide focuses on Keycloak because it is what every GeoID deployment ships against by default. The local-dev realm export at `src/dynastore/docker/keycloak/realm-export.json` is the working reference; this document explains *what to recreate in your remote Keycloak* and *what to send back to the GeoID admin*.

---

## TL;DR — three deployment profiles

| Profile | Keycloak | Realm + users | Secrets | Override mechanism |
|---|---|---|---|---|
| **Local on-premise / dev** | Bundled `compose.keycloak.yml` (Keycloak 26 in docker) | Auto-imported from `keycloak/realm-export.json` (sysadmin, admin, user, viewer + legacy test* users; passwords match usernames) | Committed dev fixtures in `docker/.env` (`IDP_CLIENT_SECRET=geoid-api-secret`, `KEYCLOAK_ADMIN_PASSWORD=admin`) | None needed — `docker compose up` works out of the box. |
| **Review / staging** | Shared remote Keycloak (e.g. internal cluster) | Realm provisioned by Keycloak admin once; you receive issuer URLs, client id, client secret | Cloud Run env vars or shared K8s secrets | Set `IDP_*` env vars in the deployment manifest (`apps.base.yml` / `apps.review.yml`) — they override any value baked into the image. |
| **Production** | Same shape as review but with production realm + production secrets | Same as review | Cloud Run secrets (preferably referenced from a secret manager) | Same as review — never commit production values; use the per-environment override mechanism. |

Local defaults are committed deliberately. Anything you would commit for a **real** environment goes in your override layer (Cloud Run env vars, Astronomer connections, Kubernetes secrets, …) — never into `docker/.env` or `realm-export.json`.

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
| `viewer` | `VIEWER` | Read-only; assigned to newly auto-registered principals. |

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
- **`iss`** (issuer) — must equal `IDP_ISSUER_URL` **or** `IDP_PUBLIC_URL` (`oidc_identity.py:200` accepts both). This matters whenever the browser and the backend reach Keycloak via different hostnames.
- **`aud`** (audience) — must include `IDP_AUDIENCE` (defaults to `IDP_CLIENT_ID`). If your Keycloak emits tokens with `aud=account` by default, configure an **Audience protocol mapper** on the `geoid-api` client to add the client_id to `aud` (Keycloak admin console → Client Scopes → `roles` → Add mapper → Audience → "Included Client Audience" = `geoid-api`). This is one of the most common gotchas.
- **Frontchannel / backchannel hostname split** — Keycloak by default emits every URL it generates (login form `action`, `iss`, redirects, `token_endpoint`, `jwks_uri`) using the **request URL**. When the browser reaches Keycloak via one hostname (e.g. `https://login.example.org` or `http://localhost:8180`) and backend services reach it via another (e.g. `http://keycloak:8080` on a private docker / k8s network), this default produces broken login pages: the browser submits the login form to the internal hostname, which it cannot resolve.

  **Recommended Keycloak 26+ configuration**:
  ```
  KC_HOSTNAME=<browser-facing-url>          # e.g. http://localhost:8180  or https://login.example.org
  KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true       # backend URLs derived from request Host
  KC_HOSTNAME_STRICT=true                    # required for KC_HOSTNAME to take effect
  ```
  Pre-26 the variable was `KC_HOSTNAME_URL`; `KC_HOSTNAME_BACKCHANNEL_DYNAMIC` is 24+.

  Result: front-channel URLs (login form action, authorization endpoint, `iss`) use the public URL; back-channel URLs (token, userinfo, jwks) use the Host header — so the API container keeps using its private hostname to reach Keycloak. The local docker-compose fragment at `src/dynastore/docker/compose.keycloak.yml` follows this pattern.

  Symptom when **front/back channels are not split**: after entering credentials at `http://localhost:8180/...`, the browser is redirected to `http://keycloak:8080/realms/<realm>/login-actions/authenticate?...` and DNS resolution fails on the host machine.
  Symptom when **HOSTNAME pins are missing entirely**: every authenticated request 403s with "Deny by Default — No matching ALLOW policy found" because tokens issued via different paths carry different `iss` values.
- **`sub` claim missing** — Keycloak 26 does NOT add a `sub` claim to access tokens by default; the standard `profile` client scope only populates id-token claims, not access-token claims. Without an explicit `oidc-sub-mapper` on each client, the access token carries `sub: null` and geoid falls back to `preferred_username` for principal identity (`oidc_identity.py:238`). The local `realm-export.json` includes the mapper on `geoid-api` and `geoid-web`. To add manually via the Keycloak admin console: select the client → Client scopes → Dedicated mappers → Add mapper → "Subject (sub)" → enable "Add to access token". Or via API: `POST /admin/realms/{realm}/clients/{client_uuid}/protocol-mappers/models` with `{"name":"subject","protocolMapper":"oidc-sub-mapper","protocol":"openid-connect","config":{"access.token.claim":"true","id.token.claim":"true","userinfo.token.claim":"true","introspection.token.claim":"true"}}`.

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

## Migration warnings — read before deploying changes to a non-fresh environment

### Adding the `oidc-sub-mapper` to an environment that already has principals

If your environment was running with `sub: null` (the Keycloak 26 default before the mapper was added), `oidc_identity.py:238` was deriving the principal identifier from `preferred_username` — recorded in `iam.principals.identifier` as `oidc:<username>` (e.g. `oidc:alice`).

Once you add the `oidc-sub-mapper`, NEW tokens carry a real `sub` (the user's Keycloak UUID) and the catalog creates a NEW `iam.principals` row with identifier `oidc:<uuid>` (e.g. `oidc:0d9a7e33-ab84-4c92-8195-3e9f424dad8c`). **Any existing role grants in `iam.grants` bound to the OLD identifier are orphaned** — the new principal record has no grants, and every authenticated request from that user resolves to the role-less new principal.

The fallback in `oidc_identity.py:238` only fires when `sub` is absent in the token, so once you flip the mapper there is no automatic reconciliation.

**Mitigation options** in priority order:

1. **Re-trigger IAM seeding** — if your auto-grant code runs on every login (resolves the JWT's `realm_access.roles` and creates fresh grants for the new principal), the new principal will get its grants on first login post-deploy. Most local dev stacks behave this way.
2. **Reconcile via SQL** — for environments where grants were created manually, find each `oidc:<username>` principal and re-create the grants under the new `oidc:<uuid>` row. Match via `iam.identity_links` if present, or via `display_name`.
3. **Defer the mapper** — if (1) and (2) are not feasible, leave the realm's old behavior intact and rely on the `preferred_username` fallback. Document this and revisit when downstream tooling requires the canonical OIDC `sub`.

Symptom of the orphan state: every authenticated request returns `403 Deny by Default — No matching ALLOW policy found` from the moment the mapper takes effect, until the new principal accumulates grants. The `iam.principals` table will show a new row with `identifier = 'oidc:<uuid>'` and zero rows in `iam.grants` for that `subject_ref`.

### Vault-collection notebook IAM bundle pollution risk

The vault-collection notebook (`src/dynastore/modules/elasticsearch/notebooks/collection_vault_geoid_only.ipynb`, registered via `register_platform_notebook`) creates a `vault-{cat}` Role and binds it as parent of `sysadmin/admin/user/anonymous` via `POST /iam/governance/hierarchies`. If the notebook errors out before the cleanup cell runs, the hierarchy edges remain and the vault role's DENY policies stay attached to the four default roles — **every user gets the vault DENYs applied globally**. Symptom: catalog/collection writes start returning 403 across unrelated catalogs.

Manual recovery:

```bash
# List + remove leftover hierarchy edges
for child in sysadmin admin user anonymous; do
  curl -s -X DELETE "$BASE/iam/governance/hierarchies?parent=vault-<cat>&child=$child" \
    -H "Authorization: Bearer $TOKEN"
done
curl -s -X DELETE "$BASE/iam/governance/roles/vault-<cat>" -H "Authorization: Bearer $TOKEN"
# Then delete each vault-<cat>-* policy if any remain
```

A future hardening of the notebook should bind the vault role to a single test principal instead of the global default-role hierarchy, so kernel death can't pollute global IAM state.

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
