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

"""Unit tests for the ``IdpConfig`` PluginConfig (geoid#1500).

Pins the contract that replaces the ``IDP_*`` / ``KEYCLOAK_*`` environment
reads in ``IamModule.lifespan``:

- a default-constructed config is the *unconfigured* state (required so
  ``get_config`` can resolve a zero-arg default and the lifespan can fall back
  to the deprecated ENV path);
- ``is_configured`` mirrors the historical ENV gate (oidc + issuer set);
- ``client_secret`` is a :class:`Secret`, not pydantic ``SecretStr``: it
  serializes to an encrypted envelope under ``secret_mode="db"`` and masks by
  default — so it round-trips through the platform-config jsonb store without
  corrupting the secret;
- the config auto-registers under class_key ``idp_config``.
"""

import os

import pytest

# Secret encryption derives its key from JWT_SECRET / DYNASTORE_SECRET_KEY.
# Pin one so the db-envelope round-trip is deterministic under test.
os.environ.setdefault(
    "JWT_SECRET", "test-secret-padded-to-enough-chars-for-fernet-xx"
)

from dynastore.modules.iam.idp_config import IdpConfig
from dynastore.tools.secrets import Secret, is_encrypted_envelope


def test_address_is_platform_iam_idp() -> None:
    assert IdpConfig._address == ("platform", "modules", "iam", "idp")


def test_class_key_is_idp_config() -> None:
    assert IdpConfig.class_key() == "idp_config"


def test_registered_as_plugin_config() -> None:
    from dynastore.models.plugin_config import list_registered_configs

    reg = list_registered_configs()
    assert reg.get("idp_config") is IdpConfig


def test_defaults_are_unconfigured() -> None:
    cfg = IdpConfig()
    assert cfg.type == "oidc"
    assert cfg.issuer_url is None
    assert cfg.client_id == "dynastore-api"
    assert cfg.client_secret is None
    assert cfg.audience is None
    assert cfg.public_url is None
    assert cfg.roles_claim_path == "realm_access.roles"
    # Zero-arg construction must succeed — get_config falls back to cls().
    assert cfg.is_configured is False


def test_is_configured_true_for_oidc_with_issuer() -> None:
    assert IdpConfig(issuer_url="https://kc/realms/x").is_configured is True


def test_is_configured_false_when_issuer_missing() -> None:
    assert IdpConfig(type="oidc", issuer_url=None).is_configured is False


def test_is_configured_false_for_saml2() -> None:
    # saml2 is a reserved placeholder — never auto-registers a provider.
    assert IdpConfig(type="saml2", issuer_url="https://kc/x").is_configured is False


def test_invalid_type_rejected() -> None:
    with pytest.raises(ValueError):
        IdpConfig(type="ldap")  # not in Literal["oidc", "saml2"]


def test_client_secret_accepts_plaintext_string() -> None:
    cfg = IdpConfig(issuer_url="https://kc/x", client_secret="hunter2")
    assert isinstance(cfg.client_secret, Secret)
    assert cfg.client_secret.reveal() == "hunter2"


def test_client_secret_masks_by_default() -> None:
    """Default dump (API response / log) must never expose the plaintext."""
    cfg = IdpConfig(issuer_url="https://kc/x", client_secret="hunter2")
    dumped = cfg.model_dump(mode="json")
    assert dumped["client_secret"] == "***"
    assert "hunter2" not in repr(cfg)


def test_client_secret_db_envelope_round_trips() -> None:
    """secret_mode='db' emits an encrypted envelope that decrypts back to the
    same plaintext — the persistence contract the platform config store uses."""
    cfg = IdpConfig(issuer_url="https://kc/x", client_secret="hunter2")
    dumped = cfg.model_dump(
        mode="json", context={"secret_mode": "db"}, exclude_unset=True
    )
    assert is_encrypted_envelope(dumped["client_secret"])
    restored = IdpConfig.model_validate(dumped)
    assert restored.client_secret is not None
    assert restored.client_secret.reveal() == "hunter2"


def test_public_client_has_no_secret() -> None:
    cfg = IdpConfig(issuer_url="https://kc/x")
    assert cfg.client_secret is None
    assert cfg.is_configured is True


def test_issuer_url_stored_verbatim_no_normalization() -> None:
    """Plain str (not HttpUrl): the exact issuer string is preserved so the
    OIDC 'iss' exact-match (RFC 8414) is not broken by URL normalization."""
    raw = "https://kc.example.org/realms/fao"  # no trailing slash
    cfg = IdpConfig(issuer_url=raw)
    assert cfg.issuer_url == raw
