#    Copyright 2025 FAO
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

"""Per-indexer circuit breaker for :class:`IndexDispatcher`.

When an indexer (e.g. ES, vector DB) is down, every write op would otherwise
hammer it and pile up in the dispatcher's hot path.  This breaker observes
consecutive failures and short-circuits to OPEN when a threshold is crossed,
letting the dispatcher route subsequent ops through ``FailurePolicy.OUTBOX``
(or skip with WARN) without wasting RTT on the doomed call.

State machine — per ``indexer_id``:

* **CLOSED** — normal operation; ``record_failure`` increments the counter,
  ``record_success`` resets it.
* **OPEN** — last sync attempt failed N times consecutively; ``is_open``
  returns True until the cooldown elapses.
* **HALF_OPEN** — cooldown elapsed; the next call is allowed through as a
  probe.  ``record_success`` closes the breaker; ``record_failure`` re-opens
  it for another cooldown.

In-process L1 only.  Cross-pod consensus via :class:`TieredAsyncBackend`
(Valkey-backed L2) is a follow-up — on-prem deployments without Valkey
already get correct per-pod behaviour from L1.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Dict, Literal, Optional

logger = logging.getLogger(__name__)

CircuitState = Literal["CLOSED", "OPEN", "HALF_OPEN"]


@dataclass
class _IndexerCircuit:
    """Per-``indexer_id`` breaker state."""

    state: CircuitState = "CLOSED"
    consecutive_failures: int = 0
    opened_at: Optional[float] = None  # epoch seconds when state moved to OPEN


class CircuitBreaker:
    """Thread-safe per-indexer circuit breaker.

    Synchronous API (``is_open`` / ``record_success`` / ``record_failure``)
    so the dispatcher's hot path doesn't pay an ``await`` per check.  State
    is in-process; reset on process restart.

    Parameters
    ----------
    failure_threshold
        Number of consecutive failures before the breaker opens.  Default 5.
    cooldown_seconds
        Time the breaker stays OPEN before allowing a HALF_OPEN probe.
        Default 30 s.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        cooldown_seconds: float = 30.0,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if cooldown_seconds <= 0:
            raise ValueError("cooldown_seconds must be > 0")
        self._threshold = failure_threshold
        self._cooldown = cooldown_seconds
        self._circuits: Dict[str, _IndexerCircuit] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public sync API consumed by IndexDispatcher
    # ------------------------------------------------------------------

    def is_open(self, indexer_id: str) -> bool:
        """Return True if the breaker is OPEN and the cooldown has not
        elapsed.  When the cooldown has elapsed the state transitions to
        HALF_OPEN and this returns False (so the next call is allowed
        through as a probe).
        """
        now = time.monotonic()
        with self._lock:
            circuit = self._circuits.get(indexer_id)
            if circuit is None or circuit.state == "CLOSED":
                return False
            if circuit.state == "HALF_OPEN":
                # A probe is already in flight or just attempted; let
                # subsequent calls through too — record_failure will
                # re-open if needed.
                return False
            # OPEN — check cooldown.
            if (
                circuit.opened_at is not None
                and (now - circuit.opened_at) >= self._cooldown
            ):
                logger.info(
                    "CircuitBreaker: '%s' cooldown elapsed — moving to HALF_OPEN.",
                    indexer_id,
                )
                circuit.state = "HALF_OPEN"
                return False
            return True

    def record_success(self, indexer_id: str) -> None:
        """Reset the breaker for ``indexer_id`` to CLOSED."""
        with self._lock:
            circuit = self._circuits.get(indexer_id)
            if circuit is None:
                return
            if circuit.state != "CLOSED" or circuit.consecutive_failures > 0:
                logger.info(
                    "CircuitBreaker: '%s' recovered — closing breaker (was %s).",
                    indexer_id, circuit.state,
                )
            circuit.state = "CLOSED"
            circuit.consecutive_failures = 0
            circuit.opened_at = None

    def record_failure(self, indexer_id: str) -> None:
        """Register a failure.  Opens the breaker if the threshold is
        crossed; re-opens it from HALF_OPEN on a failed probe.
        """
        now = time.monotonic()
        with self._lock:
            circuit = self._circuits.setdefault(indexer_id, _IndexerCircuit())
            if circuit.state == "HALF_OPEN":
                # Probe failed — straight back to OPEN with a fresh cooldown.
                logger.warning(
                    "CircuitBreaker: '%s' probe failed — re-opening for %.1fs.",
                    indexer_id, self._cooldown,
                )
                circuit.state = "OPEN"
                circuit.opened_at = now
                circuit.consecutive_failures += 1
                return
            circuit.consecutive_failures += 1
            if (
                circuit.state == "CLOSED"
                and circuit.consecutive_failures >= self._threshold
            ):
                logger.warning(
                    "CircuitBreaker: '%s' opened after %d consecutive failures "
                    "— skipping sync attempts for %.1fs.",
                    indexer_id, circuit.consecutive_failures, self._cooldown,
                )
                circuit.state = "OPEN"
                circuit.opened_at = now

    # ------------------------------------------------------------------
    # Operator surface — read-only inspection
    # ------------------------------------------------------------------

    def state_of(self, indexer_id: str) -> CircuitState:
        """Return the current circuit state without mutating it.  Used by
        admin / observability endpoints; the dispatcher uses ``is_open``
        which has the side effect of advancing OPEN → HALF_OPEN.
        """
        with self._lock:
            circuit = self._circuits.get(indexer_id)
            if circuit is None:
                return "CLOSED"
            return circuit.state

    def snapshot(self) -> Dict[str, Dict[str, object]]:
        """Return a copy of the current per-indexer state for observability.
        Keys are ``indexer_id``; values are ``{"state", "consecutive_failures",
        "opened_at"}``.
        """
        with self._lock:
            return {
                k: {
                    "state": v.state,
                    "consecutive_failures": v.consecutive_failures,
                    "opened_at": v.opened_at,
                }
                for k, v in self._circuits.items()
            }

    def reset(self, indexer_id: Optional[str] = None) -> None:
        """Operator action — force the breaker back to CLOSED.

        Pass ``indexer_id`` to reset a single circuit, or omit to clear all.
        Used by admin endpoints and tests.
        """
        with self._lock:
            if indexer_id is None:
                self._circuits.clear()
                return
            self._circuits.pop(indexer_id, None)
