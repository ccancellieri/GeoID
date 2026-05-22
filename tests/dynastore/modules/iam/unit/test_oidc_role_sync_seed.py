"""Regression for geoid#908 / geoid#907: ``provision_default_policies``
must seed ``OidcRoleSyncConfig(reconcile_enabled=True)`` into platform
configs on first cold boot, idempotently.

Without the seed, a freshly-deployed auth-enforcing instance keeps the
Pydantic ``reconcile_enabled=False`` default and never maps Keycloak's
``geoid.sysadmin`` realm role to the internal ``sysadmin`` grant —
producing a 403 on every /admin/* call even for tokens that ship the
correct realm role.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.iam.oidc_role_sync_config import OidcRoleSyncConfig
from dynastore.modules.iam.policies import PolicyService


def _service() -> PolicyService:
    return PolicyService.__new__(PolicyService)


@pytest.mark.asyncio
async def test_seed_writes_default_when_no_row_exists(caplog):
    svc = _service()
    configs = AsyncMock()
    configs.list_configs = AsyncMock(return_value={})
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.policies.get_protocol",
        return_value=configs,
    ), caplog.at_level(logging.INFO, logger="dynastore.modules.iam.policies"):
        await svc._seed_oidc_role_sync_default()

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
    svc = _service()
    persisted = OidcRoleSyncConfig(reconcile_enabled=False)
    configs = AsyncMock()
    configs.list_configs = AsyncMock(
        return_value={OidcRoleSyncConfig: persisted}
    )
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.policies.get_protocol",
        return_value=configs,
    ):
        await svc._seed_oidc_role_sync_default()

    configs.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_skips_when_platform_configs_protocol_absent(caplog):
    svc = _service()

    with patch(
        "dynastore.modules.iam.policies.get_protocol",
        return_value=None,
    ), caplog.at_level(logging.DEBUG, logger="dynastore.modules.iam.policies"):
        await svc._seed_oidc_role_sync_default()

    assert any(
        "PlatformConfigsProtocol" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_seed_warns_when_enabled_without_issuer_whitelist(caplog):
    svc = _service()
    configs = AsyncMock()
    configs.list_configs = AsyncMock(return_value={})
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.policies.get_protocol",
        return_value=configs,
    ), caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.policies"):
        await svc._seed_oidc_role_sync_default()

    assert any(
        "issuer_whitelist" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_seed_warns_on_existing_row_with_enabled_and_empty_whitelist(caplog):
    """Even when the row already exists, the misconfiguration warning
    must still fire so operators see the gap on every cold boot."""
    svc = _service()
    persisted = OidcRoleSyncConfig(reconcile_enabled=True, issuer_whitelist=None)
    configs = AsyncMock()
    configs.list_configs = AsyncMock(
        return_value={OidcRoleSyncConfig: persisted}
    )
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.policies.get_protocol",
        return_value=configs,
    ), caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.policies"):
        await svc._seed_oidc_role_sync_default()

    configs.set_config.assert_not_awaited()
    assert any(
        "issuer_whitelist" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_seed_no_warning_when_whitelist_set(caplog):
    svc = _service()
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
        "dynastore.modules.iam.policies.get_protocol",
        return_value=configs,
    ), caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.policies"):
        await svc._seed_oidc_role_sync_default()

    assert not any(
        "issuer_whitelist" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_seed_self_heals_when_configs_table_missing(caplog):
    """#1209: ``provision_default_policies`` (IamModule lifespan) can run
    before ``DatastoreModule``'s ``ensure_init_db`` creates
    ``configs.platform_configs``, so the first ``list_configs`` raises
    "relation does not exist". The seed used to skip one-shot and never
    retry — leaving ``reconcile_enabled`` defaulted ``False`` so the
    sysadmin JWT resolved to user-tier and every protected route 403'd
    until an unrelated redeploy. It must instead ensure the (idempotent)
    platform-configs storage and retry once, then seed."""
    svc = _service()
    svc._engine = object()  # sentinel; initialize_storage is mocked
    configs = AsyncMock()
    # First list: table missing. Second list (post ensure-storage): empty.
    configs.list_configs = AsyncMock(
        side_effect=[
            RuntimeError('relation "configs.platform_configs" does not exist'),
            {},
        ]
    )
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.policies.get_protocol",
        return_value=configs,
    ), patch(
        "dynastore.modules.db_config.platform_config_service."
        "PlatformConfigService.initialize_storage",
        new=AsyncMock(return_value=None),
    ) as init_storage, caplog.at_level(
        logging.INFO, logger="dynastore.modules.iam.policies"
    ):
        await svc._seed_oidc_role_sync_default()

    init_storage.assert_awaited_once()
    assert configs.list_configs.await_count == 2
    configs.set_config.assert_awaited_once()
    cls_arg, cfg_arg = configs.set_config.await_args.args
    assert cls_arg is OidcRoleSyncConfig
    assert cfg_arg.reconcile_enabled is True


@pytest.mark.asyncio
async def test_seed_swallows_persistent_storage_error(caplog):
    """If ``list_configs`` still raises after the ensure-storage retry
    (DB genuinely down at boot), the seed must NOT propagate — it's
    best-effort; the next cold boot or operator PATCH recovers. It now
    warns (was a silent debug) so the skipped bootstrap is observable."""
    svc = _service()
    svc._engine = object()
    configs = AsyncMock()
    configs.list_configs = AsyncMock(side_effect=RuntimeError("DB down"))
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.policies.get_protocol",
        return_value=configs,
    ), patch(
        "dynastore.modules.db_config.platform_config_service."
        "PlatformConfigService.initialize_storage",
        new=AsyncMock(return_value=None),
    ), caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.policies"):
        await svc._seed_oidc_role_sync_default()  # must not raise

    configs.set_config.assert_not_awaited()
    assert any(
        "platform configs storage" in rec.getMessage().lower()
        for rec in caplog.records
    )
