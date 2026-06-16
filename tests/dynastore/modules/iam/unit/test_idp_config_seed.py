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

"""Unit tests for ``_seed_idp_config`` — cold-boot ENV bridge for IdpConfig.

The seed function lives in ``dynastore.modules.iam.module``.  All DB calls
are mocked; these tests require no real database.

Coverage:
- Seeds IdpConfig from IDP_ISSUER_URL + IDP_CLIENT_ID when NO row exists in
  the platform-configs table (presence checked via ``list_configs``).
- Skips seeding when ANY row already exists — including a deliberately
  ``type='saml2'`` / unconfigured operator row (never overwrite operator data).
- Logs a WARNING and returns without writing when no ENV issuer is set
  (fail-closed; no anonymous IdP registration).
- Cold-boot race: first ``list_configs`` raises, ``initialize_storage`` is
  called once, second ``list_configs`` succeeds, seed is written.
"""

from __future__ import annotations

import logging
import os
from unittest.mock import AsyncMock, patch

import pytest

# Ensure the Fernet/Secret layer can initialize in tests.
os.environ.setdefault(
    "JWT_SECRET", "test-secret-padded-to-enough-chars-for-fernet-xx"
)

from dynastore.modules.iam.idp_config import IdpConfig


async def _seed(engine=None):
    """Call the standalone seed function."""
    from dynastore.modules.iam.module import _seed_idp_config
    await _seed_idp_config(engine)


# ---------------------------------------------------------------------------
# Test: seeds from ENV when no row exists
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_seeds_idp_config_from_env_when_no_row(monkeypatch, caplog):
    """When list_configs returns no IdpConfig row, the seed persists a
    fully-configured IdpConfig built from IDP_ISSUER_URL and IDP_CLIENT_ID.
    client_secret.reveal() must return the original plaintext.
    """
    monkeypatch.setenv("IDP_ISSUER_URL", "https://keycloak.example.com/realms/test")
    monkeypatch.setenv("IDP_CLIENT_ID", "my-api-client")
    monkeypatch.setenv("IDP_CLIENT_SECRET", "supersecret")
    # Remove any KEYCLOAK_* variants to avoid interference.
    monkeypatch.delenv("KEYCLOAK_ISSUER_URL", raising=False)
    monkeypatch.delenv("KEYCLOAK_CLIENT_ID", raising=False)
    monkeypatch.delenv("KEYCLOAK_CLIENT_SECRET", raising=False)

    configs = AsyncMock()
    # No IdpConfig row in the DB.
    configs.list_configs = AsyncMock(return_value={})
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ), caplog.at_level(logging.INFO, logger="dynastore.modules.iam.module"):
        await _seed()

    configs.set_config.assert_awaited_once()
    cls_arg, cfg_arg = configs.set_config.await_args.args
    assert cls_arg is IdpConfig
    assert isinstance(cfg_arg, IdpConfig)
    assert cfg_arg.is_configured is True
    assert cfg_arg.issuer_url == "https://keycloak.example.com/realms/test"
    assert cfg_arg.client_id == "my-api-client"
    assert cfg_arg.client_secret is not None
    assert cfg_arg.client_secret.reveal() == "supersecret"
    assert any("Seeded IdpConfig" in rec.getMessage() for rec in caplog.records)


# ---------------------------------------------------------------------------
# Test: skips when a configured row already exists
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_skips_when_configured_row_exists(monkeypatch):
    """When a configured IdpConfig row exists, set_config must NOT be called —
    the operator/seed-file row wins, even if ENV is also set.
    """
    monkeypatch.setenv("IDP_ISSUER_URL", "https://keycloak.example.com/realms/test")

    existing = IdpConfig(type="oidc", issuer_url="https://existing.example.com/realms/prod")
    assert existing.is_configured is True

    configs = AsyncMock()
    configs.list_configs = AsyncMock(return_value={IdpConfig: existing})
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ):
        await _seed()

    configs.set_config.assert_not_awaited()


# ---------------------------------------------------------------------------
# Test: respects an operator's deliberately-unconfigured (saml2) row
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_respects_existing_unconfigured_saml2_row(monkeypatch):
    """A row that exists but is NOT is_configured (e.g. type='saml2', chosen by
    an operator to disable OIDC) must be left untouched, NOT overwritten by the
    ENV seed — even though IDP_ISSUER_URL is present.
    """
    monkeypatch.setenv("IDP_ISSUER_URL", "https://keycloak.example.com/realms/test")
    monkeypatch.setenv("IDP_CLIENT_ID", "my-api-client")

    existing = IdpConfig(type="saml2")
    assert existing.is_configured is False  # saml2 is not a wired OIDC provider

    configs = AsyncMock()
    configs.list_configs = AsyncMock(return_value={IdpConfig: existing})
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ):
        await _seed()

    configs.set_config.assert_not_awaited()


# ---------------------------------------------------------------------------
# Test: fail-closed when no row and no ENV issuer
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fail_closed_when_no_row_and_no_env(monkeypatch, caplog):
    """When no IdpConfig row exists and no IDP_ISSUER_URL / KEYCLOAK_ISSUER_URL
    is in the environment, the seed must NOT write anything and must log a
    WARNING that mentions IDP_ISSUER_URL.
    """
    monkeypatch.delenv("IDP_ISSUER_URL", raising=False)
    monkeypatch.delenv("KEYCLOAK_ISSUER_URL", raising=False)
    monkeypatch.delenv("IDP_CLIENT_ID", raising=False)
    monkeypatch.delenv("KEYCLOAK_CLIENT_ID", raising=False)
    monkeypatch.delenv("IDP_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("KEYCLOAK_CLIENT_SECRET", raising=False)

    configs = AsyncMock()
    configs.list_configs = AsyncMock(return_value={})
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ), caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.module"):
        await _seed()

    configs.set_config.assert_not_awaited()
    assert any(
        "IDP_ISSUER_URL" in rec.getMessage()
        for rec in caplog.records
        if rec.levelno >= logging.WARNING
    ), "Expected a WARNING mentioning IDP_ISSUER_URL when no issuer is configured"


# ---------------------------------------------------------------------------
# Test: cold-boot race — initialize_storage called on first failure, retried
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cold_boot_initializes_storage_and_retries(monkeypatch, caplog):
    """When the first list_configs raises (relation does not exist),
    initialize_storage must be called once and the seed must succeed on the
    second list_configs call.
    """
    monkeypatch.setenv("IDP_ISSUER_URL", "https://keycloak.example.com/realms/cold")
    monkeypatch.setenv("IDP_CLIENT_ID", "cold-boot-client")
    monkeypatch.delenv("KEYCLOAK_ISSUER_URL", raising=False)
    monkeypatch.delenv("IDP_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("KEYCLOAK_CLIENT_SECRET", raising=False)

    engine_sentinel = object()

    configs = AsyncMock()
    configs.list_configs = AsyncMock(
        side_effect=[
            RuntimeError('relation "configs.platform_configs" does not exist'),
            {},  # second call: table now exists, no IdpConfig row
        ]
    )
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ), patch(
        "dynastore.modules.db_config.platform_config_service."
        "PlatformConfigService.initialize_storage",
        new=AsyncMock(return_value=None),
    ) as init_storage, caplog.at_level(logging.INFO, logger="dynastore.modules.iam.module"):
        await _seed(engine=engine_sentinel)

    init_storage.assert_awaited_once()
    assert configs.list_configs.await_count == 2
    configs.set_config.assert_awaited_once()
    cls_arg, cfg_arg = configs.set_config.await_args.args
    assert cls_arg is IdpConfig
    assert isinstance(cfg_arg, IdpConfig)
    assert cfg_arg.is_configured is True
    assert cfg_arg.issuer_url == "https://keycloak.example.com/realms/cold"


# ---------------------------------------------------------------------------
# Test: #2210 defense-in-depth — persist WITHOUT client_secret when secret
# encryption is unavailable, so the OIDC provider still registers.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_persists_without_secret_when_encryption_unavailable(monkeypatch, caplog):
    """When the secret-bearing seed cannot persist (set_config raises because
    no Fernet key is provisioned to encrypt client_secret — the exact #2210
    dev/review failure), the seed retries WITHOUT client_secret so an OIDC
    provider still registers and bearer-token validation works.

    Asserts: set_config is called twice; the second (succeeding) call carries
    an IdpConfig that is_configured (issuer present) but has client_secret=None;
    a WARNING explains the secret was dropped.
    """
    monkeypatch.setenv("IDP_ISSUER_URL", "https://keycloak.example.com/realms/nokeys")
    monkeypatch.setenv("IDP_CLIENT_ID", "nokeys-client")
    monkeypatch.setenv("IDP_CLIENT_SECRET", "cannot-encrypt-me")
    monkeypatch.delenv("KEYCLOAK_ISSUER_URL", raising=False)
    monkeypatch.delenv("KEYCLOAK_CLIENT_ID", raising=False)
    monkeypatch.delenv("KEYCLOAK_CLIENT_SECRET", raising=False)

    configs = AsyncMock()
    configs.list_configs = AsyncMock(return_value={})
    # First persist (with secret) raises as if Fernet key is missing; the
    # retry (without secret) succeeds.
    configs.set_config = AsyncMock(
        side_effect=[
            RuntimeError("Error calling _serialize: secret encryption unavailable"),
            None,
        ]
    )

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ), caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.module"):
        await _seed()

    assert configs.set_config.await_count == 2
    # The first attempt carried the secret; the second dropped it.
    first_cfg = configs.set_config.await_args_list[0].args[1]
    second_cfg = configs.set_config.await_args_list[1].args[1]
    assert first_cfg.client_secret is not None
    assert second_cfg.client_secret is None
    assert second_cfg.is_configured is True
    assert second_cfg.issuer_url == "https://keycloak.example.com/realms/nokeys"
    assert any(
        "without client_secret" in rec.getMessage().lower()
        for rec in caplog.records
        if rec.levelno >= logging.WARNING
    ), "Expected a WARNING that client_secret was dropped"


@pytest.mark.asyncio
async def test_no_secret_retry_when_no_client_secret(monkeypatch):
    """A public-client seed (no IDP_CLIENT_SECRET) that fails to persist must
    NOT trigger the secret-less retry — there is no secret to drop, so a single
    failed attempt is correct (avoids a misleading double write)."""
    monkeypatch.setenv("IDP_ISSUER_URL", "https://keycloak.example.com/realms/pub")
    monkeypatch.setenv("IDP_CLIENT_ID", "public-client")
    monkeypatch.delenv("IDP_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("KEYCLOAK_ISSUER_URL", raising=False)
    monkeypatch.delenv("KEYCLOAK_CLIENT_ID", raising=False)
    monkeypatch.delenv("KEYCLOAK_CLIENT_SECRET", raising=False)

    configs = AsyncMock()
    configs.list_configs = AsyncMock(return_value={})
    configs.set_config = AsyncMock(side_effect=RuntimeError("boom"))

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ):
        await _seed()

    configs.set_config.assert_awaited_once()
