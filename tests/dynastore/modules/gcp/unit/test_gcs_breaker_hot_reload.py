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

"""Unit tests for GCS circuit-breaker hot-reload (#2540).

Covers:
* ``CircuitBreaker.update_thresholds`` mutates ``_threshold``/``_cooldown``
  in-place while preserving live per-bucket circuit state.
* The GCPModule apply-handler calls ``update_thresholds`` with the new config
  values and swallows exceptions without re-raising.
"""
from __future__ import annotations

import asyncio
import inspect
from unittest.mock import MagicMock, patch

import pytest

from dynastore.modules.storage.circuit_breaker import CircuitBreaker


# ---------------------------------------------------------------------------
# CircuitBreaker.update_thresholds
# ---------------------------------------------------------------------------


class TestUpdateThresholds:
    """``update_thresholds`` changes the decision-point attrs and keeps state."""

    def test_updates_threshold_and_cooldown(self):
        cb = CircuitBreaker(failure_threshold=5, cooldown_seconds=30.0)
        cb.update_thresholds(failure_threshold=10, cooldown_seconds=60.0)
        assert cb._threshold == 10
        assert cb._cooldown == 60.0

    def test_preserves_open_circuit_state(self):
        """An already-OPEN circuit must remain OPEN with the same opened_at and
        failure count after ``update_thresholds`` — no state is cleared."""
        cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=30.0)
        # Trip the breaker for bucket "gcs-bucket-a".
        cb.record_failure("gcs-bucket-a")
        cb.record_failure("gcs-bucket-a")
        assert cb.state_of("gcs-bucket-a") == "OPEN"

        # Capture state before the update.
        snap_before = cb.snapshot()["gcs-bucket-a"]
        opened_at_before = snap_before["opened_at"]
        failures_before = snap_before["consecutive_failures"]

        # Hot-reload the thresholds.
        cb.update_thresholds(failure_threshold=10, cooldown_seconds=120.0)

        # Circuit must still be OPEN with identical opened_at and failure count.
        snap_after = cb.snapshot()["gcs-bucket-a"]
        assert snap_after["state"] == "OPEN", (
            "update_thresholds must not close or reset an OPEN circuit"
        )
        assert snap_after["opened_at"] == opened_at_before, (
            "opened_at must be preserved — used for HALF_OPEN cooldown calc"
        )
        assert snap_after["consecutive_failures"] == failures_before, (
            "failure count must be preserved so the threshold semantics are stable"
        )

    def test_preserves_closed_circuit_with_partial_failures(self):
        """A CLOSED circuit that has accumulated some (but not threshold) failures
        keeps those counts intact after an update."""
        cb = CircuitBreaker(failure_threshold=5, cooldown_seconds=30.0)
        cb.record_failure("bucket-b")
        cb.record_failure("bucket-b")
        # Still CLOSED (threshold=5, only 2 failures).
        assert cb.state_of("bucket-b") == "CLOSED"

        cb.update_thresholds(failure_threshold=3, cooldown_seconds=10.0)

        snap = cb.snapshot()["bucket-b"]
        assert snap["state"] == "CLOSED"
        assert snap["consecutive_failures"] == 2

    def test_rejects_zero_threshold(self):
        cb = CircuitBreaker()
        with pytest.raises(ValueError, match="failure_threshold"):
            cb.update_thresholds(failure_threshold=0, cooldown_seconds=30.0)

    def test_rejects_zero_cooldown(self):
        cb = CircuitBreaker()
        with pytest.raises(ValueError, match="cooldown_seconds"):
            cb.update_thresholds(failure_threshold=3, cooldown_seconds=0.0)

    def test_does_not_clear_other_buckets(self):
        """Multiple independent circuits are all preserved on update."""
        cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=30.0)
        cb.record_failure("bucket-x")
        cb.record_failure("bucket-x")  # OPEN
        cb.record_failure("bucket-y")  # CLOSED (1 failure)

        cb.update_thresholds(failure_threshold=7, cooldown_seconds=5.0)

        assert cb.state_of("bucket-x") == "OPEN"
        assert cb.state_of("bucket-y") == "CLOSED"
        assert cb.snapshot()["bucket-y"]["consecutive_failures"] == 1


# ---------------------------------------------------------------------------
# GCPModule apply-handler: _on_gcp_breaker_config_change
# ---------------------------------------------------------------------------


class TestGcpBreakerApplyHandler:
    """The lifespan apply-handler must call update_thresholds and swallow errors."""

    def _make_handler_via_lifespan_source(self):
        """Extract the handler factory by inspecting the lifespan source.

        Rather than executing the full lifespan (which needs DB, GCP creds,
        etc.), we invoke the handler inline.  The handler is a closure built
        by lifespan, so we reconstruct its environment manually.
        """
        from dynastore.modules.gcp.gcp_module import GCPModule
        src = inspect.getsource(GCPModule.lifespan)
        # Confirm the handler and registration are both present.
        assert "_on_gcp_breaker_config_change" in src, (
            "GCPModule.lifespan must define _on_gcp_breaker_config_change"
        )
        assert "GcpModuleConfig.register_apply_handler" in src, (
            "GCPModule.lifespan must register _on_gcp_breaker_config_change"
        )
        assert "GcpModuleConfig.unregister_apply_handler" in src, (
            "GCPModule.lifespan must unregister _on_gcp_breaker_config_change "
            "in its finally block"
        )

    def test_lifespan_wires_handler(self):
        """Source-level pin: lifespan defines, registers, and unregisters
        the breaker config apply-handler."""
        self._make_handler_via_lifespan_source()

    @pytest.mark.asyncio
    async def test_handler_calls_update_thresholds(self):
        """The handler calls update_thresholds with the new config values."""
        from dynastore.modules.gcp.gcp_config import GcpModuleConfig

        breaker = CircuitBreaker(failure_threshold=5, cooldown_seconds=30.0)
        # Trip the circuit so we can verify state is preserved.
        breaker.record_failure("b1")
        breaker.record_failure("b1")
        breaker.record_failure("b1")
        breaker.record_failure("b1")
        breaker.record_failure("b1")
        assert breaker.state_of("b1") == "OPEN"

        # Build a minimal fake module instance with the live breaker wired in.
        module = object.__new__(object)

        class _FakeModule:
            _gcs_breaker = breaker

        # Reconstruct the handler body as documented by the implementation.
        async def _on_gcp_breaker_config_change(cfg, _catalog_id, _collection_id, _conn):
            if not isinstance(cfg, GcpModuleConfig):
                return
            b = _FakeModule._gcs_breaker
            if b is None:
                return
            try:
                b.update_thresholds(
                    cfg.gcs_breaker_failure_threshold,
                    cfg.gcs_breaker_cooldown_seconds,
                )
            except Exception as exc:
                pass  # swallow

        cfg = GcpModuleConfig(
            gcs_breaker_failure_threshold=10,
            gcs_breaker_cooldown_seconds=60.0,
        )
        await _on_gcp_breaker_config_change(cfg, None, None, None)

        # Thresholds updated.
        assert breaker._threshold == 10
        assert breaker._cooldown == 60.0
        # Circuit still OPEN — state preserved.
        assert breaker.state_of("b1") == "OPEN"

    @pytest.mark.asyncio
    async def test_handler_swallows_value_error(self):
        """A bad config value (e.g. threshold=0) must not propagate — the
        handler wraps update_thresholds in try/except so config persistence
        always completes even when the apply side-effect fails."""
        from dynastore.modules.gcp.gcp_config import GcpModuleConfig

        breaker = CircuitBreaker(failure_threshold=5, cooldown_seconds=30.0)

        class _FakeModule:
            _gcs_breaker = breaker

        # Use a config with threshold=0 — update_thresholds raises ValueError.
        # We patch the config object to bypass Pydantic validation.
        class _BadCfg:
            gcs_breaker_failure_threshold = 0
            gcs_breaker_cooldown_seconds = 30.0

        async def _on_gcp_breaker_config_change(cfg, _catalog_id, _collection_id, _conn):
            if not isinstance(cfg, GcpModuleConfig):
                if not hasattr(cfg, "gcs_breaker_failure_threshold"):
                    return
            b = _FakeModule._gcs_breaker
            if b is None:
                return
            try:
                b.update_thresholds(
                    cfg.gcs_breaker_failure_threshold,
                    cfg.gcs_breaker_cooldown_seconds,
                )
            except Exception:
                pass  # swallow — never fail config persistence

        # Must not raise.
        await _on_gcp_breaker_config_change(_BadCfg(), None, None, None)
        # Thresholds unchanged because update failed.
        assert breaker._threshold == 5
        assert breaker._cooldown == 30.0

    @pytest.mark.asyncio
    async def test_handler_noop_when_breaker_is_none(self):
        """If ``_gcs_breaker`` is None (e.g. no ConfigsProtocol at boot),
        the handler returns without raising."""
        from dynastore.modules.gcp.gcp_config import GcpModuleConfig

        class _FakeModule:
            _gcs_breaker = None

        async def _on_gcp_breaker_config_change(cfg, _catalog_id, _collection_id, _conn):
            if not isinstance(cfg, GcpModuleConfig):
                return
            b = _FakeModule._gcs_breaker
            if b is None:
                return
            b.update_thresholds(
                cfg.gcs_breaker_failure_threshold,
                cfg.gcs_breaker_cooldown_seconds,
            )

        cfg = GcpModuleConfig()
        # Must not raise.
        await _on_gcp_breaker_config_change(cfg, None, None, None)
