# Identity Providers

This directory contains the concrete implementations of `IdentityProviderProtocol`.
The active provider is selected at startup via the `IDP_TYPE` environment variable.

---

## Protocol Contract

Every provider must implement `IdentityProviderProtocol`
(`src/dynastore/modules/iam/interfaces.py`):

| Method | Purpose |
|--------|---------|
| `get_provider_id() -> str` | Unique string tag stored in `identity_links.provider` (e.g. `"oidc"`) |
| `validate_token(token) -> dict \| None` | Validate an access token and return a normalised identity dict |
| `get_authorization_url(redirect_uri, state) -> str` | Build the browser login redirect URL |
| `exchange_code_for_token(code, redirect_uri) -> dict` | Exchange auth code for tokens |
| `get_user_info(access_token) -> dict` | Fetch user profile from the IdP |

The identity dict returned by `validate_token` must include at minimum:
```python
{
    "provider": "oidc",           # must match get_provider_id()
    "sub": "<immutable user id>", # used as the stable external identity key
    "email": "...",
    "name": "...",
    "realm_roles": [...],         # flat list of role names
    "client_roles": [...],
    "raw_claims": {...},          # full token payload for audit
}
```

---

## Current Implementation: `OidcIdentityProvider`

**File:** `oidc_identity.py`  
**Selected by:** `IDP_TYPE=oidc` (default)

Uses [RFC 8414](https://www.rfc-editor.org/rfc/rfc8414) / OIDC Discovery
(`/.well-known/openid-configuration`) to resolve all endpoints at runtime.
This makes it compatible with any standards-compliant IdP without code changes.

Tested with: **Keycloak**, **Okta**, **Auth0**, **Azure AD**, **Google**.

> **Setting up Keycloak?** See [`KEYCLOAK_SETUP.md`](./KEYCLOAK_SETUP.md) — recipe for the **Keycloak admin** covering realm / role / client setup, the load-bearing `roles` scope + audience mapper, and a smoke-test command.

### Environment Variables

| Variable | Alias (legacy) | Description |
|----------|---------------|-------------|
| `IDP_TYPE` | — | Set to `oidc` (default) |
| `IDP_ISSUER_URL` | `KEYCLOAK_ISSUER_URL` | Backend-internal issuer URL (used for discovery, token validation, userinfo). Example: `http://keycloak:8080/realms/myrealm` |
| `IDP_PUBLIC_URL` | `KEYCLOAK_PUBLIC_URL` | Browser-facing URL for auth redirects. Example: `http://localhost:8180/realms/myrealm`. Defaults to `IDP_ISSUER_URL`. |
| `IDP_CLIENT_ID` | `KEYCLOAK_CLIENT_ID` | OAuth2 client ID |
| `IDP_CLIENT_SECRET` | `KEYCLOAK_CLIENT_SECRET` | OAuth2 client secret (confidential clients) |
| `IDP_AUDIENCE` | `KEYCLOAK_AUDIENCE` | Expected `aud` claim. Defaults to `IDP_CLIENT_ID`. |

> **Note:** The `KEYCLOAK_*` aliases are read as fallbacks so existing
> deployments keep working without changes.

---

## Adding a New Provider

1. **Create** `<type>_identity.py` implementing `IdentityProviderProtocol`.

2. **Add** an `elif` branch in `IamModule.lifespan()`
   (`src/dynastore/modules/iam/module.py`):
   ```python
   elif idp_type == "saml2":
       from .identity_providers.saml2_identity import Saml2IdentityProvider
       register_plugin(Saml2IdentityProvider(
           metadata_url=os.environ.get("IDP_METADATA_URL"),
           sp_entity_id=os.environ.get("IDP_SP_ENTITY_ID"),
           ...
       ))
   ```

3. **Export** the class in `__init__.py`.

4. **Document** the new `IDP_*` variables in `docker/.env`.

---

## SAML2 (Planned)

SAML2 support is not yet implemented but the env-var structure is reserved:

| Variable | Description |
|----------|-------------|
| `IDP_TYPE=saml2` | Activates the SAML2 provider |
| `IDP_METADATA_URL` | URL of the IdP SAML metadata XML |
| `IDP_SP_ENTITY_ID` | Service Provider entity ID |
| `IDP_SP_CERT` | SP signing certificate (PEM) |
| `IDP_SP_KEY` | SP private key (PEM) |

The `IdentityProviderProtocol` interface is intentionally protocol-agnostic
so the same IAM core handles both OIDC and SAML2 sessions without changes.
