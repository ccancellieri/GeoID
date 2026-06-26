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

"""Unit tests for connection health configuration.

These exercise the *live snapshot* contract: ``resolve_*`` reads the module
snapshot, and the apply handlers (fired by the configs store on a ``PUT``)
replace that snapshot in place — no environment variables, no restart.
"""

from __future__ import annotations

import pytest

import dynastore.modules.db_config.connection_health_config as chc
from dynastore.modules.db_config.connection_health_config import (
    ConnectionRetryConfig,
    ProvisioningRetryConfig,
    LeadershipConfig,
    ConnectionHealthConfig,
    resolve_connection_retry_config,
    resolve_provisioning_retry_config,
    resolve_leadership_config,
    resolve_slow_pool_acquire_threshold,
    _apply_retry_config,
    _apply_provisioning_config,
    _apply_leadership_config,
    _apply_health_config,
)


@pytest.fixture(autouse=True)
def _restore_snapshot():
    """Each test mutates module-global snapshots; restore them afterwards so
    tests cannot leak config state into one another."""
    saved = (
        chc._retry_config,
        chc._provisioning_config,
        chc._leadership_config,
        chc._health_config,
    )
    yield
    (
        chc._retry_config,
        chc._provisioning_config,
        chc._leadership_config,
        chc._health_config,
    ) = saved


class TestConfigClasses:
    """Defaults, addresses, and validation bounds on the PluginConfig classes."""

    def test_retry_defaults_and_address(self):
        config = ConnectionRetryConfig()
        assert config.max_retries == 5
        assert config.base_delay_seconds == 0.5
        assert config.max_delay_seconds == 8.0
        assert config.jitter == 0.25
        assert ConnectionRetryConfig._address == ("platform", "db", "retry")

    def test_provisioning_defaults_and_address(self):
        config = ProvisioningRetryConfig()
        assert config.max_attempts == 3
        assert config.lock_backoff_seconds == 1.0
        assert ProvisioningRetryConfig._address == ("platform", "db", "provisioning_retry")

    def test_leadership_defaults_and_address(self):
        config = LeadershipConfig()
        assert config.lock_acquire_timeout_seconds == 30
        assert config.dismiss_force_delete_after_seconds == 600
        assert config.leadership_interval_seconds == 20.0
        assert config.visibility_extend_seconds == 300
        assert config.unknown_grace_seconds == 180
        assert LeadershipConfig._address == ("platform", "db", "leadership")

    def test_health_defaults_and_address(self):
        config = ConnectionHealthConfig()
        assert config.slow_pool_acquire_threshold_seconds == 0.5
        assert ConnectionHealthConfig._address == ("platform", "db", "health")

    def test_validation_bounds_enforced(self):
        # Bounds are enforced because the configs store always instantiates the
        # class — there is no env bypass that could skip Pydantic validation.
        with pytest.raises(ValueError):
            ConnectionRetryConfig(max_retries=0)
        with pytest.raises(ValueError):
            ConnectionRetryConfig(max_retries=25)
        with pytest.raises(ValueError):
            ConnectionRetryConfig(base_delay_seconds=0.05)
        with pytest.raises(ValueError):
            ConnectionRetryConfig(max_delay_seconds=65.0)


class TestResolveReadsSnapshot:
    """``resolve_*`` returns the validated class defaults out of the box."""

    def test_retry_defaults(self):
        assert resolve_connection_retry_config() == (5, 0.5, 8.0, 0.25)

    def test_provisioning_defaults(self):
        assert resolve_provisioning_retry_config() == (3, 1.0)

    def test_leadership_defaults(self):
        assert resolve_leadership_config() == (30, 600, 20.0, 300, 180)

    def test_health_default(self):
        assert resolve_slow_pool_acquire_threshold() == 0.5


class TestApplyHandlersHotReload:
    """The apply handlers swap the live snapshot; ``resolve_*`` reflects it
    immediately — this is the configs-API hot-reload path."""

    @pytest.mark.asyncio
    async def test_apply_retry_updates_resolve(self):
        await _apply_retry_config(
            ConnectionRetryConfig(max_retries=10, base_delay_seconds=1.0), None, None, None
        )
        max_retries, base_delay, _, _ = resolve_connection_retry_config()
        assert max_retries == 10
        assert base_delay == 1.0

    @pytest.mark.asyncio
    async def test_apply_provisioning_updates_resolve(self):
        await _apply_provisioning_config(
            ProvisioningRetryConfig(max_attempts=7, lock_backoff_seconds=2.0), None, None, None
        )
        assert resolve_provisioning_retry_config() == (7, 2.0)

    @pytest.mark.asyncio
    async def test_apply_leadership_updates_resolve(self):
        await _apply_leadership_config(
            LeadershipConfig(lock_acquire_timeout_seconds=60, leadership_interval_seconds=30.0),
            None, None, None,
        )
        lock_timeout, _, interval, _, _ = resolve_leadership_config()
        assert lock_timeout == 60
        assert interval == 30.0

    @pytest.mark.asyncio
    async def test_apply_health_updates_resolve(self):
        await _apply_health_config(
            ConnectionHealthConfig(slow_pool_acquire_threshold_seconds=1.5), None, None, None
        )
        assert resolve_slow_pool_acquire_threshold() == 1.5

    @pytest.mark.asyncio
    async def test_apply_ignores_wrong_type(self):
        # A handler must no-op on a foreign config instance (defensive: the
        # registry keys by class, but isinstance guards belt-and-suspenders).
        await _apply_retry_config(LeadershipConfig(), None, None, None)
        assert resolve_connection_retry_config() == (5, 0.5, 8.0, 0.25)


class TestApplyHandlerRegistration:
    """register/unregister round-trips through the PluginConfig registry."""

    def test_register_then_unregister(self):
        chc.register_connection_health_apply_handlers()
        try:
            assert _apply_retry_config in ConnectionRetryConfig.get_apply_handlers()
            assert _apply_health_config in ConnectionHealthConfig.get_apply_handlers()
        finally:
            chc.unregister_connection_health_apply_handlers()
        assert _apply_retry_config not in ConnectionRetryConfig.get_apply_handlers()
        assert _apply_health_config not in ConnectionHealthConfig.get_apply_handlers()
