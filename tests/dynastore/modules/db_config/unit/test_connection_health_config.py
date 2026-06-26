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
