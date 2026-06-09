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

"""Runtime config for the identity-provider (IdP) factory.

Replaces the ``IDP_*`` / ``KEYCLOAK_*`` environment-variable reads that the
``IamModule`` lifespan used to perform. Per the project rule, system settings
live in :class:`PluginConfig` (runtime-editable, reload-without-restart),
never in ``os.environ``.

Notes on field-type choices that differ from the issue text (geoid#1500):

- ``client_secret`` is a :class:`dynastore.tools.secrets.Secret`, not a
  pydantic ``SecretStr``. ``Secret`` is the codebase's single persisted-secret
  convention: it serializes to a Fernet-encrypted envelope under
  ``secret_mode="db"`` (what the platform-config persistence layer passes) and
  masks (``***``) in API responses and logs. A bare ``SecretStr`` would dump
  to ``"**********"`` on write and corrupt the stored secret.
- ``issuer_url`` / ``public_url`` are plain ``str``, not ``HttpUrl``. The OIDC
  ``iss`` claim must match the configured issuer *exactly* (RFC 8414);
  ``HttpUrl`` normalization (e.g. appending a trailing slash) would silently
  break discovery and issuer matching. We keep the raw operator-supplied
  string.

The IdP is considered *configured* only when ``type == "oidc"`` and
``issuer_url`` is set — mirroring the historical ENV gate
(``if idp_type == "oidc" and idp_issuer``). When unconfigured, the lifespan
falls back to the deprecated ENV path for one release.
"""

from typing import ClassVar, Literal, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.tools.secrets import Secret


class IdpConfig(PluginConfig):
    """Identity-provider factory configuration (platform scope).

    A default-constructed ``IdpConfig`` is the *unconfigured* state
    (``issuer_url is None``) — required so ``get_config`` can resolve a
    zero-arg default when no row exists, and so the lifespan can detect
    "not configured here" and fall back to the deprecated ENV path.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "iam", "idp")

    type: Mutable[Literal["oidc", "saml2"]] = Field(
        default="oidc",
        description=(
            "Identity-provider backend. 'oidc' is implemented; 'saml2' is a "
            "reserved placeholder (no provider is registered for it yet)."
        ),
    )
    issuer_url: Mutable[Optional[str]] = Field(
        default=None,
        description=(
            "OIDC issuer URL (the 'iss' claim value / discovery base). Stored "
            "verbatim — must match the token issuer exactly (RFC 8414). When "
            "unset the IdP is treated as not configured."
        ),
    )
    client_id: Mutable[str] = Field(
        default="dynastore-api",
        description="OIDC client_id used for token-audience / introspection.",
    )
    client_secret: Mutable[Optional[Secret]] = Field(
        default=None,
        description=(
            "OIDC client secret. Persisted encrypted, masked ('***') in "
            "responses and logs. Optional for public clients."
        ),
    )
    audience: Mutable[Optional[str]] = Field(
        default=None,
        description="Expected token audience ('aud' claim). Optional.",
    )
    public_url: Mutable[Optional[str]] = Field(
        default=None,
        description=(
            "Externally reachable issuer URL when it differs from the "
            "in-cluster 'issuer_url' (browser-facing redirects). Stored "
            "verbatim. Optional."
        ),
    )
    roles_claim_path: Mutable[str] = Field(
        default="realm_access.roles",
        description=(
            "Dotted path into the token claims where the role list lives "
            "(Keycloak default: 'realm_access.roles')."
        ),
    )

    @property
    def is_configured(self) -> bool:
        """True when this config selects an implemented, fully-addressed IdP.

        Mirrors the historical ENV gate: only OIDC with an issuer is wired.
        """
        return self.type == "oidc" and bool(self.issuer_url)
