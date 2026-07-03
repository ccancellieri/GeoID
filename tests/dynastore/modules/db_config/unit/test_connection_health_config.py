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

These exercise the module-global snapshot contract: ``resolve_*`` reads the
module globals, and tests that need a non-default value set the global
directly (matching the integration test pattern in
``tests/dynastore/modules/db_config/integration/test_configurable_retry.py``).
"""

from __future__ import annotations

import pytest

from dynastore.modules.db_config.connection_health_config import (
    ConnectionRetryConfig,
    ProvisioningRetryConfig,
    LeadershipConfig,
    _ConnectionHealthInfraConfig,
    ConnectionHealthConfig,
    resolve_connection_retry_config,
    resolve_provisioning_retry_config,
    resolve_leadership_config,
    resolve_slow_pool_acquire_threshold,
    resolve_max_concurrent_connection_retries,
    resolve_pool_acquire_warn_seconds,
    resolve_pool_saturation_retry_after_seconds,
)


class TestConfigClasses:
    """Dataclass defaults for the infra dimensioning classes."""

    def test_retry_defaults(self):
        config = ConnectionRetryConfig()
        assert config.max_retries == 5
        assert config.base_delay_seconds == 0.5
        assert config.max_delay_seconds == 8.0
        assert config.jitter == 0.25

    def test_provisioning_defaults(self):
        config = ProvisioningRetryConfig()
        assert config.max_attempts == 3
        assert config.lock_backoff_seconds == 1.0

    def test_leadership_defaults(self):
        config = LeadershipConfig()
        assert config.lock_acquire_timeout_seconds == 30
        assert config.dismiss_force_delete_after_seconds == 600
        assert config.leadership_interval_seconds == 20.0
        assert config.visibility_extend_seconds == 300
        assert config.unknown_grace_seconds == 180

    def test_health_infra_defaults(self):
        config = _ConnectionHealthInfraConfig()
        assert config.slow_pool_acquire_threshold_seconds == 0.5

    def test_connection_health_plugin_config_defaults(self):
        config = ConnectionHealthConfig()
        assert config.leader_liveness_probe_enabled is True
        assert config.leader_liveness_probe_timeout_seconds == 2.0

    def test_max_concurrent_connection_retries_default(self):
        config = ConnectionHealthConfig()
        assert config.max_concurrent_connection_retries == 3

    def test_max_concurrent_connection_retries_bounds(self):
        """ge=1 and le=32 are enforced by Pydantic."""
        with pytest.raises(Exception):
            ConnectionHealthConfig(max_concurrent_connection_retries=0)
        with pytest.raises(Exception):
            ConnectionHealthConfig(max_concurrent_connection_retries=33)

    def test_max_concurrent_connection_retries_valid_range(self):
        assert ConnectionHealthConfig(max_concurrent_connection_retries=1).max_concurrent_connection_retries == 1
        assert ConnectionHealthConfig(max_concurrent_connection_retries=32).max_concurrent_connection_retries == 32

    def test_pool_saturation_retry_after_seconds_default(self):
        config = ConnectionHealthConfig()
        assert config.pool_saturation_retry_after_seconds == 5

    def test_pool_saturation_retry_after_seconds_bounds(self):
        """ge=1 and le=300 are enforced by Pydantic."""
        with pytest.raises(Exception):
            ConnectionHealthConfig(pool_saturation_retry_after_seconds=0)
        with pytest.raises(Exception):
            ConnectionHealthConfig(pool_saturation_retry_after_seconds=301)

    def test_pool_saturation_retry_after_seconds_valid_range(self):
        assert ConnectionHealthConfig(pool_saturation_retry_after_seconds=1).pool_saturation_retry_after_seconds == 1
        assert ConnectionHealthConfig(pool_saturation_retry_after_seconds=300).pool_saturation_retry_after_seconds == 300

    def test_pool_acquire_warn_seconds_default(self):
        config = ConnectionHealthConfig()
        assert config.pool_acquire_warn_seconds == 5.0

    def test_pool_acquire_warn_seconds_bounds(self):
        """ge=0.5 and le=60.0 are enforced by Pydantic, matching the sibling
        foreground_pool_acquire_timeout_s bounds pattern."""
        with pytest.raises(Exception):
            ConnectionHealthConfig(pool_acquire_warn_seconds=0.4)
        with pytest.raises(Exception):
            ConnectionHealthConfig(pool_acquire_warn_seconds=60.1)

    def test_pool_acquire_warn_seconds_valid_range(self):
        assert ConnectionHealthConfig(pool_acquire_warn_seconds=0.5).pool_acquire_warn_seconds == 0.5
        assert ConnectionHealthConfig(pool_acquire_warn_seconds=60.0).pool_acquire_warn_seconds == 60.0


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

    def test_connection_health_config_address(self):
        assert ConnectionHealthConfig._address == ("platform", "db", "health")

    def test_max_concurrent_connection_retries_default(self):
        assert resolve_max_concurrent_connection_retries() == 3

    def test_max_concurrent_connection_retries_reads_module_global(self):
        import dynastore.modules.db_config.connection_health_config as chc
        original = chc._max_concurrent_connection_retries
        try:
            chc._max_concurrent_connection_retries = 7
            assert resolve_max_concurrent_connection_retries() == 7
        finally:
            chc._max_concurrent_connection_retries = original

    def test_pool_saturation_retry_after_seconds_default(self):
        assert resolve_pool_saturation_retry_after_seconds() == 5

    def test_pool_saturation_retry_after_seconds_reads_module_global(self):
        import dynastore.modules.db_config.connection_health_config as chc
        original = chc._pool_saturation_retry_after_seconds
        try:
            chc._pool_saturation_retry_after_seconds = 10
            assert resolve_pool_saturation_retry_after_seconds() == 10
        finally:
            chc._pool_saturation_retry_after_seconds = original

    def test_pool_acquire_warn_seconds_default(self):
        assert resolve_pool_acquire_warn_seconds() == 5.0

    def test_pool_acquire_warn_seconds_reads_module_global(self):
        import dynastore.modules.db_config.connection_health_config as chc
        original = chc._pool_acquire_warn_seconds
        try:
            chc._pool_acquire_warn_seconds = 12.0
            assert resolve_pool_acquire_warn_seconds() == 12.0
        finally:
            chc._pool_acquire_warn_seconds = original
