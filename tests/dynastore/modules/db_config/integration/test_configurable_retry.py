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

"""Integration tests for configurable retry behavior.

The retry/leadership knobs are backed by module-global dataclass instances.
Tests override those globals directly to drive retry counts, delays, and
leadership intervals without restarting; the ``_restore_snapshot`` fixture
undoes the mutations after each test. Explicit call-time parameters still
win over the module globals.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

import dynastore.modules.db_config.connection_health_config as chc
from dynastore.modules.db_config.connection_health_config import (
    ConnectionRetryConfig,
    ProvisioningRetryConfig,
    LeadershipConfig,
)
from dynastore.modules.db_config.query_executor import (
    retry_on_transient_connect,
    provisioning_write_with_retry,
)


@pytest.fixture(autouse=True)
def _restore_snapshot():
    """Restore the live config snapshot after each test."""
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


def _mock_context():
    """A mock async context manager for managed_transaction."""
    class MockContext:
        async def __aenter__(self):
            return "mock_connection"

        async def __aexit__(self, *args):
            return False

    return MockContext()


class TestRetryOnTransientConnectConfigurable:
    """retry_on_transient_connect honours the live ConnectionRetryConfig."""

    @pytest.mark.asyncio
    async def test_uses_configured_max_retries(self):
        chc._retry_config = ConnectionRetryConfig(max_retries=3, base_delay_seconds=0.1)
        call_count = 0

        @retry_on_transient_connect()
        async def failing_func():
            nonlocal call_count
            call_count += 1
            raise OperationalError("connection failed", {}, None)

        with pytest.raises(OperationalError):
            await failing_func()

        assert call_count == 3, f"Expected 3 attempts, got {call_count}"

    @pytest.mark.asyncio
    async def test_uses_configured_base_delay(self):
        import time

        chc._retry_config = ConnectionRetryConfig(base_delay_seconds=0.1)
        call_count = 0

        @retry_on_transient_connect(max_retries=2)
        async def failing_func():
            nonlocal call_count
            call_count += 1
            raise OperationalError("connection failed", {}, None)

        start = time.monotonic()
        with pytest.raises(OperationalError):
            await failing_func()
        elapsed = time.monotonic() - start

        # base_delay=0.1, 2 attempts → ~0.1s of total sleep (well under the 0.5s default).
        assert elapsed < 0.3, f"Expected fast retries (<0.3s), got {elapsed:.2f}s"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_parameter_overrides_config(self):
        # Config says 10, the explicit decorator parameter says 2 — parameter wins.
        chc._retry_config = ConnectionRetryConfig(max_retries=10, base_delay_seconds=0.1)
        call_count = 0

        @retry_on_transient_connect(max_retries=2)
        async def failing_func():
            nonlocal call_count
            call_count += 1
            raise OperationalError("connection failed", {}, None)

        with pytest.raises(OperationalError):
            await failing_func()

        assert call_count == 2, f"Expected 2 attempts (param override), got {call_count}"

    @pytest.mark.asyncio
    async def test_succeeds_before_max_retries(self):
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
    """provisioning_write_with_retry honours the live ProvisioningRetryConfig."""

    @pytest.mark.asyncio
    async def test_uses_configured_attempts(self):
        chc._provisioning_config = ProvisioningRetryConfig(max_attempts=4)
        call_count = 0

        async def failing_fn(conn):
            nonlocal call_count
            call_count += 1
            exc = OperationalError("connection lost", {}, None)
            exc.connection_invalidated = True  # Mark as transient
            raise exc

        mock_engine = MagicMock()
        with patch(
            "dynastore.modules.db_config.query_executor.managed_transaction"
        ) as mock_txn:
            mock_txn.side_effect = lambda engine: _mock_context()
            with pytest.raises(OperationalError):
                await provisioning_write_with_retry(mock_engine, failing_fn)

        assert call_count == 4, f"Expected 4 attempts, got {call_count}"

    @pytest.mark.asyncio
    async def test_parameter_overrides_config(self):
        chc._provisioning_config = ProvisioningRetryConfig(max_attempts=10)
        call_count = 0

        async def failing_fn(conn):
            nonlocal call_count
            call_count += 1
            exc = OperationalError("connection lost", {}, None)
            exc.connection_invalidated = True  # Mark as transient
            raise exc

        mock_engine = MagicMock()
        with patch(
            "dynastore.modules.db_config.query_executor.managed_transaction"
        ) as mock_txn:
            mock_txn.side_effect = lambda engine: _mock_context()
            with pytest.raises(OperationalError):
                await provisioning_write_with_retry(mock_engine, failing_fn, attempts=2)

        assert call_count == 2, f"Expected 2 attempts (param override), got {call_count}"


class TestLeadershipConfigIntegration:
    """Leadership timeout resolves from config when no parameter is given."""

    def test_acquire_startup_lock_uses_configurable_timeout(self):
        from dynastore.modules.db_config.locking_tools import acquire_startup_lock

        import inspect
        sig = inspect.signature(acquire_startup_lock)
        timeout_param = sig.parameters.get("timeout")
        assert timeout_param is not None
        assert timeout_param.default is None  # Defaults to None → resolves from config

    def test_sync_acquire_startup_lock_uses_configurable_timeout(self):
        from dynastore.modules.db_config.locking_tools import sync_acquire_startup_lock

        import inspect
        sig = inspect.signature(sync_acquire_startup_lock)
        timeout_param = sig.parameters.get("timeout")
        assert timeout_param is not None
        assert timeout_param.default is None  # Defaults to None → resolves from config


class TestGcpLivenessReconcilerConfigurable:
    """GcpLivenessReconciler reads the live LeadershipConfig at construction."""

    def test_uses_configured_defaults(self):
        from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

        chc._leadership_config = LeadershipConfig(
            leadership_interval_seconds=15.0,
            visibility_extend_seconds=250,
            unknown_grace_seconds=150,
        )
        reconciler = GcpLivenessReconciler(MagicMock())

        assert reconciler.cadence_seconds == 15.0
        assert reconciler._extend_visibility_seconds == 250
        assert reconciler._unknown_grace_seconds == 150

    def test_parameter_overrides_config(self):
        from dynastore.modules.gcp.liveness_reconciler import GcpLivenessReconciler

        chc._leadership_config = LeadershipConfig(leadership_interval_seconds=15.0)
        reconciler = GcpLivenessReconciler(
            MagicMock(),
            interval_seconds=25.0,
            extend_visibility_seconds=400,
            unknown_grace_seconds=200,
        )

        assert reconciler.cadence_seconds == 25.0
        assert reconciler._extend_visibility_seconds == 400
        assert reconciler._unknown_grace_seconds == 200
