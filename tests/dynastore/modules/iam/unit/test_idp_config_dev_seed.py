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

"""Unit tests for the dev-compose ``idp-config.json`` local override.

geoid's tracked ``catalog``/``tools``/``maps`` defaults ship no
``idp-config.json`` (the onprem baseline is the neutral ``IdpConfig``
pydantic default — unconfigured). ``docker-compose.dev.yml`` layers a single
local-only override, ``docker/config/local/defaults/idp-config.json``, on top
of each auth-fronting service's mounted config dir so local dev keeps a
working IdP (see ``config/example/README.md`` "Pitfall" section, option 2).

Validates that this override:
- has the correct ``class_key`` (resolves to IdpConfig via the registry);
- has a ``value`` that parses via ``IdpConfig.model_validate`` without error;
- produces ``is_configured == True`` (i.e. type=oidc and issuer_url is set),
  confirming the first-boot chicken-and-egg issue (geoid#2042) is addressed.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

os.environ.setdefault(
    "JWT_SECRET", "test-secret-padded-to-enough-chars-for-fernet-xx"
)

from dynastore.models.plugin_config import resolve_config_class
from dynastore.modules.iam.idp_config import IdpConfig

# Path to the docker/config dir under the package (sibling of this test's repo root).
_DOCKER_CONFIG_DIR = (
    Path(__file__).parents[5]  # repo root
    / "packages" / "core" / "src" / "dynastore" / "docker" / "config"
)

_SEED_PATH = _DOCKER_CONFIG_DIR / "local" / "defaults" / "idp-config.json"


def _load_payload() -> dict:
    return json.loads(_SEED_PATH.read_text())


def test_seed_file_exists() -> None:
    assert _SEED_PATH.exists(), (
        f"Missing dev seed: {_SEED_PATH}. "
        "docker-compose.dev.yml mounts this file for the local IdP override."
    )


def test_seed_class_key_resolves_to_idp_config() -> None:
    payload = _load_payload()
    class_key = payload.get("class_key")
    assert class_key == "idp_config", f"expected class_key='idp_config', got {class_key!r}"
    cls = resolve_config_class(class_key)
    assert cls is IdpConfig, f"resolve_config_class({class_key!r}) returned {cls!r}"


def test_seed_value_validates_and_is_configured() -> None:
    payload = _load_payload()
    cfg = IdpConfig.model_validate(payload["value"])
    assert cfg.is_configured, (
        "IdpConfig.is_configured is False after model_validate — "
        "check that type='oidc' and issuer_url is set in the seed file."
    )


def test_seed_issuer_url_is_internal_keycloak() -> None:
    payload = _load_payload()
    cfg = IdpConfig.model_validate(payload["value"])
    assert cfg.issuer_url == "http://keycloak:8080/realms/geoid", (
        "issuer_url must point to the in-cluster Keycloak address"
    )


def test_seed_public_url_matches_host_port() -> None:
    payload = _load_payload()
    cfg = IdpConfig.model_validate(payload["value"])
    # HOST_PORT_KEYCLOAK=8180 in docker/.env; compose default is 8181 but .env wins.
    assert cfg.public_url == "http://localhost:8180/realms/geoid", (
        "public_url must match HOST_PORT_KEYCLOAK=8180 from docker/.env"
    )


def test_seed_no_client_secret() -> None:
    """geoid-web is a public PKCE client — no client_secret should be seeded."""
    payload = _load_payload()
    value = payload["value"]
    assert "client_secret" not in value, (
        "client_secret must not appear in the dev seed (public PKCE client)"
    )
