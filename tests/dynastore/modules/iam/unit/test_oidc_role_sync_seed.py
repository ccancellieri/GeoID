"""Regression for geoid#908 / geoid#907: ``_seed_oidc_role_sync_config``
must seed ``OidcRoleSyncConfig(reconcile_enabled=True)`` into platform
configs on first cold boot, idempotently.

Without the seed, a freshly-deployed auth-enforcing instance keeps the
Pydantic ``reconcile_enabled=False`` default and never maps Keycloak's
``geoid.sysadmin`` realm role to the internal ``sysadmin`` grant —
producing a 403 on every /admin/* call even for tokens that ship the
correct realm role.

After PR-5 the seed lives in the standalone ``_seed_oidc_role_sync_config``
function in ``dynastore.modules.iam.module`` (previously it was a method on
``PolicyService``).
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.iam.oidc_role_sync_config import OidcRoleSyncConfig


async def _seed(engine=None):
    """Call the new standalone seed function."""
    from dynastore.modules.iam.module import _seed_oidc_role_sync_config
    await _seed_oidc_role_sync_config(engine)


@pytest.mark.asyncio
async def test_seed_writes_default_when_no_row_exists(caplog):
    configs = AsyncMock()
    configs.list_configs = AsyncMock(return_value={})
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ), caplog.at_level(logging.INFO, logger="dynastore.modules.iam.module"):
        await _seed()

    configs.set_config.assert_awaited_once()
    cls_arg, cfg_arg = configs.set_config.await_args.args
    assert cls_arg is OidcRoleSyncConfig
    assert isinstance(cfg_arg, OidcRoleSyncConfig)
    assert cfg_arg.reconcile_enabled is True
    assert any(
        "Seeded OidcRoleSyncConfig" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_seed_skips_when_row_already_exists():
    persisted = OidcRoleSyncConfig(reconcile_enabled=False)
    configs = AsyncMock()
    configs.list_configs = AsyncMock(
        return_value={OidcRoleSyncConfig: persisted}
    )
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ):
        await _seed()

    configs.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_skips_when_platform_configs_protocol_absent(caplog):
    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=None,
    ), caplog.at_level(logging.DEBUG, logger="dynastore.modules.iam.module"):
        await _seed()

    assert any(
        "PlatformConfigsProtocol" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_seed_warns_when_enabled_without_issuer_whitelist(caplog):
    configs = AsyncMock()
    configs.list_configs = AsyncMock(return_value={})
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ), caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.module"):
        await _seed()

    assert any(
        "issuer_whitelist" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_seed_warns_on_existing_row_with_enabled_and_empty_whitelist(caplog):
    """Even when the row already exists, the misconfiguration warning
    must still fire so operators see the gap on every cold boot."""
    persisted = OidcRoleSyncConfig(reconcile_enabled=True, issuer_whitelist=None)
    configs = AsyncMock()
    configs.list_configs = AsyncMock(
        return_value={OidcRoleSyncConfig: persisted}
    )
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ), caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.module"):
        await _seed()

    configs.set_config.assert_not_awaited()
    assert any(
        "issuer_whitelist" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_seed_no_warning_when_whitelist_set(caplog):
    persisted = OidcRoleSyncConfig(
        reconcile_enabled=True,
        issuer_whitelist=["https://keycloak.example.com/realms/geoid"],
    )
    configs = AsyncMock()
    configs.list_configs = AsyncMock(
        return_value={OidcRoleSyncConfig: persisted}
    )
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ), caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.module"):
        await _seed()

    assert not any(
        "issuer_whitelist" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_seed_self_heals_when_configs_table_missing(caplog):
    """#1209: the seed can run before DatastoreModule creates
    ``configs.platform_configs``, so the first ``list_configs`` raises.
    It must ensure storage and retry once, then seed."""
    engine_sentinel = object()
    configs = AsyncMock()
    configs.list_configs = AsyncMock(
        side_effect=[
            RuntimeError('relation "configs.platform_configs" does not exist'),
            {},
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
    ) as init_storage, caplog.at_level(
        logging.INFO, logger="dynastore.modules.iam.module"
    ):
        await _seed(engine=engine_sentinel)

    init_storage.assert_awaited_once()
    assert configs.list_configs.await_count == 2
    configs.set_config.assert_awaited_once()
    cls_arg, cfg_arg = configs.set_config.await_args.args
    assert cls_arg is OidcRoleSyncConfig
    assert cfg_arg.reconcile_enabled is True


@pytest.mark.asyncio
async def test_seed_swallows_persistent_storage_error(caplog):
    """If ``list_configs`` still raises after the ensure-storage retry
    (DB genuinely down at boot), the seed must NOT propagate."""
    configs = AsyncMock()
    configs.list_configs = AsyncMock(side_effect=RuntimeError("DB down"))
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ), patch(
        "dynastore.modules.db_config.platform_config_service."
        "PlatformConfigService.initialize_storage",
        new=AsyncMock(return_value=None),
    ), caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.module"):
        await _seed()  # must not raise

    configs.set_config.assert_not_awaited()
    assert any(
        "platform configs storage" in rec.getMessage().lower()
        for rec in caplog.records
    )
