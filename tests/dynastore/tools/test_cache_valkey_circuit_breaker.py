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

"""Circuit-breaker trip callback wiring on ``ValkeyCacheBackend`` (#2741).

``_record_failure`` unregisters the backend from ``CacheManager`` after
``circuit_breaker_threshold`` consecutive failures — that part is unchanged.
These tests pin the *new* ``on_trip`` hook: it must fire exactly once the
trip happens (not on every failure below threshold, not more than once per
trip) and must never let a callback exception escape ``_record_failure``.
"""

from __future__ import annotations

from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.tools.cache_valkey import ValkeyCacheBackend


def _backend(*, threshold: int = 3, on_trip=None) -> ValkeyCacheBackend:
    client = MagicMock()
    return ValkeyCacheBackend(
        client=client,
        owns_client=False,
        circuit_breaker_threshold=threshold,
        on_trip=on_trip,
    )


def _patched_manager():
    manager = MagicMock()
    manager.unregister_backend = MagicMock()
    return patch("dynastore.tools.cache.get_cache_manager", return_value=manager)


def test_on_trip_not_called_below_threshold() -> None:
    calls: List[Any] = []
    backend = _backend(threshold=3, on_trip=calls.append)

    with _patched_manager():
        backend._record_failure()
        backend._record_failure()

    assert calls == []


def test_on_trip_called_once_at_threshold() -> None:
    calls: List[Any] = []
    backend = _backend(threshold=3, on_trip=calls.append)

    with _patched_manager():
        backend._record_failure()
        backend._record_failure()
        backend._record_failure()

    assert calls == [backend]


def test_on_trip_called_again_on_further_failures_past_threshold() -> None:
    """Every failure once tripped still re-invokes on_trip — the recovery
    scheduler in cache_module.py is idempotent on its own (guards against a
    duplicate loop), so the backend does not need to track "already fired"."""
    calls: List[Any] = []
    backend = _backend(threshold=1, on_trip=calls.append)

    with _patched_manager():
        backend._record_failure()
        backend._record_failure()

    assert calls == [backend, backend]


def test_on_trip_exception_does_not_propagate() -> None:
    def _boom(_backend: Any) -> None:
        raise RuntimeError("recovery scheduler blew up")

    backend = _backend(threshold=1, on_trip=_boom)

    with _patched_manager():
        backend._record_failure()  # must not raise


def test_no_on_trip_callback_is_fine() -> None:
    backend = _backend(threshold=1, on_trip=None)

    with _patched_manager():
        backend._record_failure()  # must not raise


@pytest.mark.asyncio
async def test_get_failure_triggers_on_trip_at_threshold() -> None:
    """End-to-end through a real backend method, not just _record_failure directly."""
    calls: List[Any] = []
    client = MagicMock()
    client.get = AsyncMock(side_effect=ConnectionError("boom"))
    backend = ValkeyCacheBackend(
        client=client,
        owns_client=False,
        circuit_breaker_threshold=2,
        on_trip=calls.append,
    )

    with _patched_manager():
        await backend.get("k1")
        assert calls == []
        await backend.get("k2")
        assert calls == [backend]
