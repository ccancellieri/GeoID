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

geoid#2227: the seed must be a true one-time cold-boot operation.  Runtime
changes made via the configs API (e.g. patching ``role_mapping`` to an
environment-specific Keycloak role name) must survive subsequent redeploys.
The skip-if-exists guard in ``_seed_oidc_role_sync_config`` provides this —
see ``test_seed_idempotent_patched_role_mapping_survives`` for the full
lifecycle regression.
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
async def test_seed_persists_full_config_including_role_mapping():
    """#2210: the seeded row must be self-describing — it has to include
    ``role_mapping`` (and the other fields), not just
    ``{"reconcile_enabled": true}``.

    ``PlatformConfigService.set_config`` serializes the config with
    ``model_dump(exclude_unset=True)``, so the seed object must have *every*
    field marked as set. Otherwise the stored row drops ``role_mapping`` and
    an operator inspecting/PATCHing ``configs.platform_configs`` — or a future
    raw-dict merge over the row — sees a misleadingly partial config.
    """
    configs = AsyncMock()
    configs.list_configs = AsyncMock(return_value={})
    configs.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs,
    ):
        await _seed()

    configs.set_config.assert_awaited_once()
    cls_arg, cfg_arg = configs.set_config.await_args.args
    assert cls_arg is OidcRoleSyncConfig

    # Reproduce exactly how set_config persists the row (platform_config_service
    # line ~732): mode="json", db secret context, exclude_unset=True.
    stored = cfg_arg.model_dump(
        mode="json", context={"secret_mode": "db"}, exclude_unset=True
    )
    assert "role_mapping" in stored, (
        "seeded row dropped role_mapping — would persist only "
        f"{sorted(stored)}, leaving the OIDC reconciler unable to map "
        "geoid.sysadmin from a stored-then-merged row"
    )
    assert stored["role_mapping"] == {"geoid.sysadmin": "sysadmin"}
    assert stored["reconcile_enabled"] is True
    # ttl_seconds is part of the self-describing row too.
    assert "ttl_seconds" in stored


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


@pytest.mark.asyncio
async def test_seed_idempotent_patched_role_mapping_survives():
    """geoid#2227: full idempotency lifecycle — first cold-boot seeds the
    hardcoded default; a subsequent call (simulating a redeploy after an
    operator API patch) must NOT overwrite the operator-patched row.

    Specifically validates the environment-specific ``role_mapping`` scenario:
    an operator sets a realm-specific OIDC role name (e.g. one matching a
    non-default Keycloak realm) via the configs API and expects that value
    to survive all future redeploys without requiring manual re-patching.

    The ``issuer_whitelist`` patch is also preserved — operators configure
    the trusted issuer URL once and it is never clobbered by the seed.
    """
    # --- First cold-boot: empty DB, seed creates the default row. ---
    configs_first = AsyncMock()
    configs_first.list_configs = AsyncMock(return_value={})
    configs_first.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs_first,
    ):
        await _seed()

    configs_first.set_config.assert_awaited_once()
    _, seeded = configs_first.set_config.await_args.args
    assert isinstance(seeded, OidcRoleSyncConfig)
    assert seeded.reconcile_enabled is True
    # Default mapping is hardcoded — backward-compat guarantee.
    assert seeded.role_mapping == {"geoid.sysadmin": "sysadmin"}

    # --- Operator patches: env-specific role name + trusted issuer. ---
    operator_patch = OidcRoleSyncConfig(
        reconcile_enabled=True,
        role_mapping={"geoid-dev.sysadmin": "sysadmin"},
        issuer_whitelist=["https://keycloak.example.com/realms/geoid-dev"],
    )

    # --- Redeploy: DB already has the operator-patched row. ---
    configs_redeploy = AsyncMock()
    configs_redeploy.list_configs = AsyncMock(
        return_value={OidcRoleSyncConfig: operator_patch}
    )
    configs_redeploy.set_config = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.module.get_protocol",
        return_value=configs_redeploy,
    ):
        await _seed()

    # Seed must not overwrite; operator values must remain intact.
    configs_redeploy.set_config.assert_not_awaited()
