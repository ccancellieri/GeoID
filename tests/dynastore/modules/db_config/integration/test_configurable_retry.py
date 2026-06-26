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

"""Integration tests for configurable retry behavior."""

from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from dynastore.modules.db_config.query_executor import retry_on_transient_connect, provisioning_write_with_retry


class TestRetryOnTransientConnectConfigurable:
    """Tests that retry_on_transient_connect uses configurable values."""

    @pytest.mark.asyncio
    async def test_uses_env_var_max_retries(self):
        """Test that retry_on_transient_connect respects DB_RETRY_MAX_RETRIES env var."""
        call_count = 0

        @retry_on_transient_connect()
        async def failing_func():
            nonlocal call_count
            call_count += 1
            raise OperationalError("connection failed", {}, None)

        env = {"DB_RETRY_MAX_RETRIES": "3"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(OperationalError):
                await failing_func()

        assert call_count == 3, f"Expected 3 attempts, got {call_count}"

    @pytest.mark.asyncio
    async def test_uses_env_var_base_delay(self):
        """Test that retry_on_transient_connect respects DB_RETRY_BASE_DELAY_SECONDS env var."""
        import time

        call_count = 0

        @retry_on_transient_connect(max_retries=2)
        async def failing_func():
            nonlocal call_count
            call_count += 1
            raise OperationalError("connection failed", {}, None)

        env = {"DB_RETRY_BASE_DELAY_SECONDS": "0.1"}
        with patch.dict(os.environ, env, clear=True):
            start = time.monotonic()
            with pytest.raises(OperationalError):
                await failing_func()
            elapsed = time.monotonic() - start

        # With base_delay=0.1 and 2 retries: first attempt, 0.1s delay, second attempt
        # Total delay should be ~0.1s (not 0.5s default)
        assert elapsed < 0.3, f"Expected fast retries (<0.3s), got {elapsed:.2f}s"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_parameter_overrides_env_var(self):
        """Test that explicit parameters override env vars."""
        call_count = 0

        @retry_on_transient_connect(max_retries=2)
        async def failing_func():
            nonlocal call_count
            call_count += 1
            raise OperationalError("connection failed", {}, None)

        env = {"DB_RETRY_MAX_RETRIES": "10"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(OperationalError):
                await failing_func()

        # Parameter (2) should override env var (10)
        assert call_count == 2, f"Expected 2 attempts (param override), got {call_count}"

    @pytest.mark.asyncio
    async def test_succeeds_before_max_retries(self):
        """Test that function succeeds if retry succeeds before max attempts."""
        call_count = 0

        @retry_on_transient_connect(max_retries=5)
        async def eventually_succeeding_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise OperationalError("connection failed", {}, None)
            return "success"

        result = await eventually_succeeding_func()
        assert result == "success"
        assert call_count == 3


class TestProvisioningWriteWithRetryConfigurable:
    """Tests that provisioning_write_with_retry uses configurable values."""

    @pytest.mark.asyncio
    async def test_uses_env_var_attempts(self):
        """Test that provisioning_write_with_retry respects DB_PROVISIONING_RETRY_ATTEMPTS env var."""
        from unittest.mock import MagicMock, patch

        call_count = 0

        async def failing_fn(conn):
            nonlocal call_count
            call_count += 1
            from sqlalchemy.exc import OperationalError
            exc = OperationalError("connection lost", {}, None)
            exc.connection_invalidated = True  # Mark as transient
            raise exc

        mock_engine = MagicMock()

        env = {"DB_PROVISIONING_RETRY_ATTEMPTS": "4"}
        with patch.dict(os.environ, env, clear=True):
            with patch("dynastore.modules.db_config.query_executor.managed_transaction") as mock_txn:
                mock_txn.side_effect = lambda engine: self._mock_context(failing_fn)
                with pytest.raises(OperationalError):
                    await provisioning_write_with_retry(mock_engine, failing_fn)

        assert call_count == 4, f"Expected 4 attempts, got {call_count}"

    def _mock_context(self, fn):
        """Helper to create a mock async context manager."""
        class MockContext:
            async def __aenter__(self):
                return "mock_connection"

            async def __aexit__(self, *args):
                pass

        return MockContext()

    @pytest.mark.asyncio
    async def test_parameter_overrides_env_var(self):
        """Test that explicit parameters override env vars."""
        from unittest.mock import MagicMock, patch

        call_count = 0

        async def failing_fn(conn):
            nonlocal call_count
            call_count += 1
            from sqlalchemy.exc import OperationalError
            exc = OperationalError("connection lost", {}, None)
            exc.connection_invalidated = True  # Mark as transient
            raise exc

        mock_engine = MagicMock()

        env = {"DB_PROVISIONING_RETRY_ATTEMPTS": "10"}
        with patch.dict(os.environ, env, clear=True):
            with patch("dynastore.modules.db_config.query_executor.managed_transaction") as mock_txn:
                mock_txn.side_effect = lambda engine: self._mock_context(failing_fn)
                with pytest.raises(OperationalError):
                    await provisioning_write_with_retry(mock_engine, failing_fn, attempts=2)

        # Parameter (2) should override env var (10)
        assert call_count == 2, f"Expected 2 attempts (param override), got {call_count}"


class TestLeadershipConfigIntegration:
    """Tests that leadership settings are used in actual code."""

    def test_acquire_startup_lock_uses_configurable_timeout(self):
        """Test that acquire_startup_lock uses configurable timeout."""
        from dynastore.modules.db_config.locking_tools import acquire_startup_lock

        # This is more of a smoke test - we verify the function accepts None timeout
        # and uses the resolved config internally
        import inspect
        sig = inspect.signature(acquire_startup_lock)
        timeout_param = sig.parameters.get("timeout")
        assert timeout_param is not None
        assert timeout_param.default is None  # Should default to None (resolves from config)

    def test_sync_acquire_startup_lock_uses_configurable_timeout(self):
        """Test that sync_acquire_startup_lock uses configurable timeout."""
        from dynastore.modules.db_config.locking_tools import sync_acquire_startup_lock

        import inspect
        sig = inspect.signature(sync_acquire_startup_lock)
        timeout_param = sig.parameters.get("timeout")
        assert timeout_param is not None
        assert timeout_param.default is None  # Should default to None (resolves from config)


class TestGcpLivenessReconcilerConfigurable:
    """Tests that GcpLivenessReconciler uses configurable values."""

    def test_uses_configurable_defaults(self):
        """Test that GcpLivenessReconciler uses configurable defaults when no params provided."""
        from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler
        from unittest.mock import MagicMock

        mock_engine = MagicMock()

        env = {
            "DB_LEADERSHIP_INTERVAL_SECONDS": "15.0",
            "DB_LEADERSHIP_VISIBILITY_EXTEND_SECONDS": "250",
            "DB_LEADERSHIP_UNKNOWN_GRACE_SECONDS": "150",
        }
        with patch.dict(os.environ, env, clear=True):
            reconciler = GcpLivenessReconciler(mock_engine)

        assert reconciler.cadence_seconds == 15.0
        assert reconciler._extend_visibility_seconds == 250
        assert reconciler._unknown_grace_seconds == 150

    def test_parameter_overrides_config(self):
        """Test that explicit parameters override configurable defaults."""
        from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler
        from unittest.mock import MagicMock

        mock_engine = MagicMock()

        env = {"DB_LEADERSHIP_INTERVAL_SECONDS": "15.0"}
        with patch.dict(os.environ, env, clear=True):
            reconciler = GcpLivenessReconciler(
                mock_engine,
                interval_seconds=25.0,
                extend_visibility_seconds=400,
                unknown_grace_seconds=200,
            )

        # Parameters should override env vars
        assert reconciler.cadence_seconds == 25.0
        assert reconciler._extend_visibility_seconds == 400
        assert reconciler._unknown_grace_seconds == 200
