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

"""Unit tests for connection health configuration."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from dynastore.modules.db_config.connection_health_config import (
    ConnectionRetryConfig,
    ProvisioningRetryConfig,
    LeadershipConfig,
    ConnectionHealthConfig,
    resolve_connection_retry_config,
    resolve_provisioning_retry_config,
    resolve_leadership_config,
    resolve_connection_health_config,
)


class TestConnectionRetryConfig:
    """Tests for ConnectionRetryConfig."""

    def test_default_values(self):
        """Test that default values are correctly set."""
        config = ConnectionRetryConfig()
        assert config.max_retries == 5
        assert config.base_delay_seconds == 0.5
        assert config.max_delay_seconds == 8.0
        assert config.jitter == 0.25

    def test_address(self):
        """Test that address is correctly set."""
        assert ConnectionRetryConfig._address == ("platform", "db", "retry")

    def test_validation_ge(self):
        """Test that validation rejects values below minimum."""
        with pytest.raises(ValueError):
            ConnectionRetryConfig(max_retries=0)
        with pytest.raises(ValueError):
            ConnectionRetryConfig(base_delay_seconds=0.05)

    def test_validation_le(self):
        """Test that validation rejects values above maximum."""
        with pytest.raises(ValueError):
            ConnectionRetryConfig(max_retries=25)
        with pytest.raises(ValueError):
            ConnectionRetryConfig(max_delay_seconds=65.0)


class TestProvisioningRetryConfig:
    """Tests for ProvisioningRetryConfig."""

    def test_default_values(self):
        """Test that default values are correctly set."""
        config = ProvisioningRetryConfig()
        assert config.max_attempts == 3
        assert config.lock_backoff_seconds == 1.0

    def test_address(self):
        """Test that address is correctly set."""
        assert ProvisioningRetryConfig._address == ("platform", "db", "provisioning_retry")


class TestLeadershipConfig:
    """Tests for LeadershipConfig."""

    def test_default_values(self):
        """Test that default values are correctly set."""
        config = LeadershipConfig()
        assert config.lock_acquire_timeout_seconds == 30
        assert config.dismiss_force_delete_after_seconds == 600
        assert config.leadership_interval_seconds == 20.0
        assert config.visibility_extend_seconds == 300
        assert config.unknown_grace_seconds == 180

    def test_address(self):
        """Test that address is correctly set."""
        assert LeadershipConfig._address == ("platform", "db", "leadership")


class TestConnectionHealthConfig:
    """Tests for ConnectionHealthConfig."""

    def test_default_values(self):
        """Test that default values are correctly set."""
        config = ConnectionHealthConfig()
        assert config.slow_pool_acquire_threshold_seconds == 0.5
        assert config.advisory_lock_validation_enabled is False
        assert config.connection_health_check_interval_seconds == 30
        assert config.circuit_breaker_threshold == 5
        assert config.circuit_breaker_recovery_seconds == 60

    def test_address(self):
        """Test that address is correctly set."""
        assert ConnectionHealthConfig._address == ("platform", "db", "health")


class TestResolveFunctions:
    """Tests for config resolution functions."""

    def test_resolve_connection_retry_config_defaults(self):
        """Test that resolve_connection_retry_config returns defaults when no env vars set."""
        with patch.dict(os.environ, {}, clear=True):
            max_retries, base_delay, max_delay, jitter = resolve_connection_retry_config()
            assert max_retries == 5
            assert base_delay == 0.5
            assert max_delay == 8.0
            assert jitter == 0.25

    def test_resolve_connection_retry_config_env_override(self):
        """Test that env vars override defaults."""
        env = {
            "DB_RETRY_MAX_RETRIES": "10",
            "DB_RETRY_BASE_DELAY_SECONDS": "1.0",
            "DB_RETRY_MAX_DELAY_SECONDS": "15.0",
            "DB_RETRY_JITTER": "0.5",
        }
        with patch.dict(os.environ, env, clear=True):
            max_retries, base_delay, max_delay, jitter = resolve_connection_retry_config()
            assert max_retries == 10
            assert base_delay == 1.0
            assert max_delay == 15.0
            assert jitter == 0.5

    def test_resolve_provisioning_retry_config_defaults(self):
        """Test that resolve_provisioning_retry_config returns defaults when no env vars set."""
        with patch.dict(os.environ, {}, clear=True):
            attempts, lock_backoff = resolve_provisioning_retry_config()
            assert attempts == 3
            assert lock_backoff == 1.0

    def test_resolve_provisioning_retry_config_env_override(self):
        """Test that env vars override defaults."""
        env = {
            "DB_PROVISIONING_RETRY_ATTEMPTS": "5",
            "DB_PROVISIONING_LOCK_BACKOFF_SECONDS": "2.0",
        }
        with patch.dict(os.environ, env, clear=True):
            attempts, lock_backoff = resolve_provisioning_retry_config()
            assert attempts == 5
            assert lock_backoff == 2.0

    def test_resolve_leadership_config_defaults(self):
        """Test that resolve_leadership_config returns defaults when no env vars set."""
        with patch.dict(os.environ, {}, clear=True):
            lock_timeout, dismiss_force_delete, interval, visibility, unknown_grace = resolve_leadership_config()
            assert lock_timeout == 30
            assert dismiss_force_delete == 600
            assert interval == 20.0
            assert visibility == 300
            assert unknown_grace == 180

    def test_resolve_leadership_config_env_override(self):
        """Test that env vars override defaults."""
        env = {
            "DB_LEADERSHIP_LOCK_TIMEOUT_SECONDS": "60",
            "DB_LEADERSHIP_DISMISS_FORCE_DELETE_SECONDS": "1200",
            "DB_LEADERSHIP_INTERVAL_SECONDS": "30.0",
            "DB_LEADERSHIP_VISIBILITY_EXTEND_SECONDS": "600",
            "DB_LEADERSHIP_UNKNOWN_GRACE_SECONDS": "300",
        }
        with patch.dict(os.environ, env, clear=True):
            lock_timeout, dismiss_force_delete, interval, visibility, unknown_grace = resolve_leadership_config()
            assert lock_timeout == 60
            assert dismiss_force_delete == 1200
            assert interval == 30.0
            assert visibility == 600
            assert unknown_grace == 300

    def test_resolve_connection_health_config_defaults(self):
        """Test that resolve_connection_health_config returns defaults when no env vars set."""
        with patch.dict(os.environ, {}, clear=True):
            slow_threshold, advisory_validation, check_interval, cb_threshold, cb_recovery = resolve_connection_health_config()
            assert slow_threshold == 0.5
            assert advisory_validation is False
            assert check_interval == 30
            assert cb_threshold == 5
            assert cb_recovery == 60

    def test_resolve_connection_health_config_env_override(self):
        """Test that env vars override defaults."""
        env = {
            "DB_HEALTH_SLOW_POOL_ACQUIRE_SECONDS": "1.0",
            "DB_HEALTH_ADVISORY_LOCK_VALIDATION_ENABLED": "true",
            "DB_HEALTH_CHECK_INTERVAL_SECONDS": "60",
            "DB_HEALTH_CIRCUIT_BREAKER_THRESHOLD": "10",
            "DB_HEALTH_CIRCUIT_BREAKER_RECOVERY_SECONDS": "120",
        }
        with patch.dict(os.environ, env, clear=True):
            slow_threshold, advisory_validation, check_interval, cb_threshold, cb_recovery = resolve_connection_health_config()
            assert slow_threshold == 1.0
            assert advisory_validation is True
            assert check_interval == 60
            assert cb_threshold == 10
            assert cb_recovery == 120

    def test_resolve_connection_health_config_bool_parsing(self):
        """Test that boolean env var parsing handles various true values."""
        for true_val in ("true", "TRUE", "True", "1", "yes", "YES"):
            with patch.dict(os.environ, {"DB_HEALTH_ADVISORY_LOCK_VALIDATION_ENABLED": true_val}, clear=True):
                _, advisory_validation, _, _, _ = resolve_connection_health_config()
                assert advisory_validation is True, f"Failed for value: {true_val}"

        for false_val in ("false", "FALSE", "False", "0", "no", "NO"):
            with patch.dict(os.environ, {"DB_HEALTH_ADVISORY_LOCK_VALIDATION_ENABLED": false_val}, clear=True):
                _, advisory_validation, _, _, _ = resolve_connection_health_config()
                assert advisory_validation is False, f"Failed for value: {false_val}"

    def test_resolve_invalid_int_env_uses_default(self):
        """Test that invalid int env values fall back to defaults."""
        with patch.dict(os.environ, {"DB_RETRY_MAX_RETRIES": "invalid"}, clear=True):
            max_retries, _, _, _ = resolve_connection_retry_config()
            assert max_retries == 5  # Default value

    def test_resolve_invalid_float_env_uses_default(self):
        """Test that invalid float env values fall back to defaults."""
        with patch.dict(os.environ, {"DB_RETRY_BASE_DELAY_SECONDS": "invalid"}, clear=True):
            _, base_delay, _, _ = resolve_connection_retry_config()
            assert base_delay == 0.5  # Default value
