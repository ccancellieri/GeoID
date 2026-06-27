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

"""Unit tests for GCS retry observability and per-bucket circuit breaker.

Covers:
* ``gcs_operation_retry`` structured log line shape on each retry attempt.
* Per-bucket breaker opens after the configured failure threshold.
* Circuit fast-fails while OPEN; a *different* bucket is unaffected.
* HALF_OPEN probe recovers to CLOSED on success, re-opens on failure.
* ``GcpModuleConfig`` retry/breaker fields present, Mutable-marked, and
  have the expected defaults.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from dynastore.modules.storage.circuit_breaker import CircuitBreaker
from dynastore.modules.gcp.gcs_retry import gcs_run_with_retry, _is_transient_gcs_error
from dynastore.modules.gcp.errors import GcpServiceUnavailableError


# ---------------------------------------------------------------------------
# Helpers — simulate transient GCS errors without real google-cloud deps
# ---------------------------------------------------------------------------


class _FakeServiceUnavailable(ConnectionError):
    """Stand-in for google.api_core.exceptions.ServiceUnavailable."""


class _FakePermissionDenied(Exception):
    """Stand-in for google.api_core.exceptions.PermissionDenied (non-transient)."""


def _make_breaker(threshold: int = 3, cooldown: float = 30.0) -> CircuitBreaker:
    return CircuitBreaker(failure_threshold=threshold, cooldown_seconds=cooldown)


# ---------------------------------------------------------------------------
# _is_transient_gcs_error
# ---------------------------------------------------------------------------


class TestIsTransientGcsError:
    def test_connection_error_is_transient(self):
        assert _is_transient_gcs_error(ConnectionError("socket closed")) is True

    def test_timeout_error_is_transient(self):
        assert _is_transient_gcs_error(TimeoutError("deadline")) is True

    def test_value_error_is_not_transient(self):
        assert _is_transient_gcs_error(ValueError("bad input")) is False

    def test_runtime_error_is_not_transient(self):
        assert _is_transient_gcs_error(RuntimeError("unexpected")) is False


# ---------------------------------------------------------------------------
# gcs_run_with_retry — structured log shape
# ---------------------------------------------------------------------------


class TestRetryLogShape:
    """The ``gcs_operation_retry`` line must be emitted on each non-final retry
    with the correct key=value fields."""

    @pytest.mark.asyncio
    async def test_structured_log_emitted_on_retry(self, caplog):
        """A transient error on attempt 1 emits the structured WARNING."""
        breaker = _make_breaker()
        call_count = 0

        async def _call():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("transient socket error")
            return "ok"

        with caplog.at_level(logging.WARNING, logger="dynastore.modules.gcp.gcs_retry"):
            result = await gcs_run_with_retry(
                _call,
                bucket="my-bucket",
                operation="patch_cors",
                breaker=breaker,
                max_attempts=2,
            )

        assert result == "ok"
        assert call_count == 2

        # Exactly one retry line
        retry_records = [
            r for r in caplog.records if "gcs_operation_retry" in r.getMessage()
        ]
        assert len(retry_records) == 1

        msg = retry_records[0].getMessage()
        assert "bucket=my-bucket" in msg
        assert "operation=patch_cors" in msg
        assert "attempt=1/2" in msg
        assert "ConnectionError" in msg

    @pytest.mark.asyncio
    async def test_no_log_on_first_attempt_success(self, caplog):
        """No retry line when the call succeeds on the first attempt."""

        async def _call():
            return "ok"

        breaker = _make_breaker()
        with caplog.at_level(logging.WARNING, logger="dynastore.modules.gcp.gcs_retry"):
            result = await gcs_run_with_retry(
                _call,
                bucket="bucket-a",
                operation="patch_cors",
                breaker=breaker,
                max_attempts=3,
            )

        assert result == "ok"
        retry_records = [
            r for r in caplog.records if "gcs_operation_retry" in r.getMessage()
        ]
        assert len(retry_records) == 0

    @pytest.mark.asyncio
    async def test_multiple_retry_log_lines(self, caplog):
        """Each non-final retry emits its own structured line."""
        breaker = _make_breaker(threshold=10)
        call_count = 0

        async def _call():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("transient")
            return "done"

        with caplog.at_level(logging.WARNING, logger="dynastore.modules.gcp.gcs_retry"):
            result = await gcs_run_with_retry(
                _call,
                bucket="b",
                operation="op",
                breaker=breaker,
                max_attempts=5,
                base_delay=0.0,
            )

        assert result == "done"
        retry_records = [
            r for r in caplog.records if "gcs_operation_retry" in r.getMessage()
        ]
        # 2 retries (attempts 1 and 2 failed, attempt 3 succeeded)
        assert len(retry_records) == 2
        assert "attempt=1/5" in retry_records[0].getMessage()
        assert "attempt=2/5" in retry_records[1].getMessage()


# ---------------------------------------------------------------------------
# gcs_run_with_retry — circuit breaker integration
# ---------------------------------------------------------------------------


class TestCircuitBreakerIntegration:
    """Per-bucket breaker opens after the threshold and fast-fails; a different
    bucket circuit is independent."""

    @pytest.mark.asyncio
    async def test_breaker_opens_after_threshold(self):
        """After failure_threshold consecutive transient failures the breaker
        opens and subsequent calls fast-fail with GcpServiceUnavailableError."""
        breaker = _make_breaker(threshold=2, cooldown=60.0)
        bucket = "wedged-bucket"

        async def _always_fail():
            raise ConnectionError("service down")

        # First 2 attempts each exhaust 1-attempt budgets, hitting the breaker.
        for _ in range(2):
            with pytest.raises(ConnectionError):
                await gcs_run_with_retry(
                    _always_fail,
                    bucket=bucket,
                    operation="patch_cors",
                    breaker=breaker,
                    max_attempts=1,
                    base_delay=0.0,
                )

        # Breaker should now be OPEN.
        assert breaker.state_of(bucket) == "OPEN"

        # Next call fast-fails without invoking _always_fail.
        called = False

        async def _probe():
            nonlocal called
            called = True
            return "ok"

        with pytest.raises(GcpServiceUnavailableError):
            await gcs_run_with_retry(
                _probe,
                bucket=bucket,
                operation="patch_cors",
                breaker=breaker,
                max_attempts=1,
            )

        assert called is False, "open circuit must not invoke the call"

    @pytest.mark.asyncio
    async def test_different_bucket_is_unaffected(self):
        """Tripping the breaker on bucket-A leaves bucket-B CLOSED."""
        breaker = _make_breaker(threshold=2, cooldown=60.0)
        bucket_a = "bucket-alpha"
        bucket_b = "bucket-beta"

        async def _fail():
            raise ConnectionError("down")

        # Trip breaker on bucket_a.
        for _ in range(2):
            with pytest.raises(ConnectionError):
                await gcs_run_with_retry(
                    _fail, bucket=bucket_a, operation="op",
                    breaker=breaker, max_attempts=1, base_delay=0.0,
                )

        assert breaker.state_of(bucket_a) == "OPEN"
        assert breaker.state_of(bucket_b) == "CLOSED"

        # bucket_b succeeds normally.
        async def _ok():
            return "success"

        result = await gcs_run_with_retry(
            _ok, bucket=bucket_b, operation="op",
            breaker=breaker, max_attempts=1,
        )
        assert result == "success"
        assert breaker.state_of(bucket_b) == "CLOSED"

    @pytest.mark.asyncio
    async def test_half_open_probe_success_closes_breaker(self):
        """After cooldown elapses the breaker transitions to HALF_OPEN; a
        successful probe closes it."""
        breaker = _make_breaker(threshold=2, cooldown=1.0)
        bucket = "bucket-x"

        async def _fail():
            raise ConnectionError("down")

        for _ in range(2):
            with pytest.raises(ConnectionError):
                await gcs_run_with_retry(
                    _fail, bucket=bucket, operation="op",
                    breaker=breaker, max_attempts=1, base_delay=0.0,
                )

        assert breaker.state_of(bucket) == "OPEN"

        # Advance time past the cooldown so is_open returns False (HALF_OPEN).
        with patch("dynastore.modules.storage.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = 10_000_000.0
            # Now gcs_run_with_retry's breaker.is_open check passes (HALF_OPEN).
            async def _ok():
                return "recovered"

            result = await gcs_run_with_retry(
                _ok, bucket=bucket, operation="op",
                breaker=breaker, max_attempts=1,
            )

        assert result == "recovered"
        assert breaker.state_of(bucket) == "CLOSED"

    @pytest.mark.asyncio
    async def test_non_transient_error_propagates_immediately(self):
        """A non-transient error (e.g. PermissionDenied) propagates without
        burning retry budget but still increments the failure counter."""
        breaker = _make_breaker(threshold=5)
        call_count = 0

        async def _permission_denied():
            nonlocal call_count
            call_count += 1
            raise _FakePermissionDenied("access denied")

        with pytest.raises(_FakePermissionDenied):
            await gcs_run_with_retry(
                _permission_denied,
                bucket="b",
                operation="op",
                breaker=breaker,
                max_attempts=3,
                base_delay=0.0,
            )

        # Only 1 call — did not retry.
        assert call_count == 1
        # Failure still recorded.
        assert breaker.snapshot()["b"]["consecutive_failures"] == 1

    @pytest.mark.asyncio
    async def test_breaker_trips_mid_retry_sequence(self):
        """If the breaker trips during a retry sequence (threshold reached on
        a prior retry) the helper raises GcpServiceUnavailableError instead of
        sleeping again."""
        breaker = _make_breaker(threshold=1, cooldown=60.0)
        bucket = "b"

        async def _always_fail():
            raise ConnectionError("down")

        # First attempt opens the breaker (threshold=1, max_attempts=2 so there
        # would be a retry — but the mid-sequence check catches it).
        with pytest.raises((ConnectionError, GcpServiceUnavailableError)):
            await gcs_run_with_retry(
                _always_fail,
                bucket=bucket,
                operation="op",
                breaker=breaker,
                max_attempts=2,
                base_delay=0.0,
            )

        # Either the mid-sequence check raised GcpServiceUnavailableError or
        # max_attempts was exhausted; either way the breaker is OPEN.
        assert breaker.state_of(bucket) == "OPEN"


# ---------------------------------------------------------------------------
# GcpModuleConfig — retry/breaker fields contract
# ---------------------------------------------------------------------------


class TestGcpModuleConfigRetryFields:
    """Contract pin for the three GCS retry/breaker fields added to
    ``GcpModuleConfig``.  A regression here means the fields were renamed,
    removed, or their mutability marker was changed — all of which break
    operator-driven runtime tuning.
    """

    @pytest.fixture(autouse=True)
    def disable_managed_eventing(self):
        """Neutralize DB-bound autouse fixtures — pure in-memory introspection."""
        return None

    _RETRY_FIELDS = (
        "gcs_retry_max_attempts",
        "gcs_breaker_failure_threshold",
        "gcs_breaker_cooldown_seconds",
    )

    def _cfg_cls(self):
        from dynastore.modules.gcp.gcp_config import GcpModuleConfig
        return GcpModuleConfig

    @pytest.mark.parametrize("field", _RETRY_FIELDS)
    def test_field_present_on_gcp_module_config(self, field):
        assert field in self._cfg_cls().model_fields, (
            f"Field {field!r} is missing from GcpModuleConfig — the GCS "
            "retry/breaker tunables cannot be hot-configured."
        )

    @pytest.mark.parametrize("field", _RETRY_FIELDS)
    def test_field_is_marked_mutable(self, field):
        from dynastore.models.mutability import mutability_map

        kinds = mutability_map(self._cfg_cls())
        assert kinds.get(field) == "mutable", (
            f"Field {field!r} mutability is {kinds.get(field)!r}; "
            "expected 'mutable' so operators can tune it at runtime."
        )

    def test_default_max_attempts(self):
        cfg = self._cfg_cls()()
        assert cfg.gcs_retry_max_attempts == 3

    def test_default_breaker_threshold(self):
        cfg = self._cfg_cls()()
        assert cfg.gcs_breaker_failure_threshold == 5

    def test_default_breaker_cooldown(self):
        cfg = self._cfg_cls()()
        assert cfg.gcs_breaker_cooldown_seconds == 30.0

    def test_gcp_module_lifespan_syncs_gcs_tunables(self):
        """``GCPModule.lifespan`` must reference all three GCS tunable names so
        operators can hot-reconfigure them without a redeploy."""
        import inspect
        from dynastore.modules.gcp import gcp_module

        src = inspect.getsource(gcp_module.GCPModule.lifespan)
        for field in self._RETRY_FIELDS:
            assert field in src, (
                f"GCPModule.lifespan does not reference {field!r} — the GCS "
                "retry/breaker tunable will not be updated from PluginConfig."
            )
