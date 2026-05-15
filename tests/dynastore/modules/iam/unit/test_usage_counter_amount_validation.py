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

"""Validation tests for ``UsageCounterProtocol.incr*`` ``amount`` argument.

``amount=0`` would burn a round trip to no effect; ``amount<0`` would
silently corrupt the counter (or in Valkey's case, produce a negative
counter that breaks subsequent cap checks). All three drivers reject
non-positive amounts before issuing any backend call.
"""

from __future__ import annotations

from typing import Optional, Tuple

import pytest

from dynastore.modules.iam.usage_counter_layered import LayeredUsageCounter


class _NeverCalledDriver:
    """Stand-in that fails the test if any backend method is invoked."""

    name = "never-called"
    priority = 999

    async def incr(self, *a, **kw):  # pragma: no cover - asserted absent
        raise AssertionError("backend incr must not be reached on invalid amount")

    async def incr_if_below(self, *a, **kw):  # pragma: no cover
        raise AssertionError("backend incr_if_below must not be reached on invalid amount")

    async def get(self, *a, **kw):
        return 0

    async def reset(self, *a, **kw):
        return None

    async def reap_expired(self, *a, **kw):
        return 0


@pytest.fixture
async def layered():
    drv = LayeredUsageCounter(
        valkey=_NeverCalledDriver(),
        postgres=_NeverCalledDriver(),
        flush_threshold=10,
        flush_interval=10.0,
    )
    await drv.start()
    try:
        yield drv
    finally:
        await drv.stop()


@pytest.mark.parametrize("bad_amount", [0, -1, -100])
@pytest.mark.asyncio
async def test_layered_incr_rejects_non_positive(layered, bad_amount):
    with pytest.raises(ValueError, match="amount must be > 0"):
        await layered.incr("p", "u", window_seconds=60, amount=bad_amount)


@pytest.mark.parametrize("bad_amount", [0, -1, -100])
@pytest.mark.asyncio
async def test_layered_incr_if_below_rejects_non_positive(layered, bad_amount):
    with pytest.raises(ValueError, match="amount must be > 0"):
        await layered.incr_if_below(
            "p", "u", limit=10, window_seconds=60, amount=bad_amount
        )


# --- Valkey driver -------------------------------------------------------


@pytest.mark.parametrize("bad_amount", [0, -1])
@pytest.mark.asyncio
async def test_valkey_incr_rejects_non_positive(bad_amount):
    from dynastore.modules.iam.usage_counter_valkey import ValkeyUsageCounter

    drv = ValkeyUsageCounter.__new__(ValkeyUsageCounter)
    # Backend resolver must not be invoked — validation happens first.
    drv._get_backend = lambda: (_ for _ in ()).throw(  # type: ignore[attr-defined]
        AssertionError("_get_backend must not run on invalid amount")
    )
    with pytest.raises(ValueError, match="amount must be > 0"):
        await drv.incr("p", "u", window_seconds=60, amount=bad_amount)


@pytest.mark.parametrize("bad_amount", [0, -1])
@pytest.mark.asyncio
async def test_valkey_incr_if_below_rejects_non_positive(bad_amount):
    from dynastore.modules.iam.usage_counter_valkey import ValkeyUsageCounter

    drv = ValkeyUsageCounter.__new__(ValkeyUsageCounter)
    drv._get_backend = lambda: (_ for _ in ()).throw(  # type: ignore[attr-defined]
        AssertionError("_get_backend must not run on invalid amount")
    )
    with pytest.raises(ValueError, match="amount must be > 0"):
        await drv.incr_if_below("p", "u", limit=10, window_seconds=60, amount=bad_amount)


# --- Postgres driver -----------------------------------------------------


@pytest.mark.parametrize("bad_amount", [0, -1])
@pytest.mark.asyncio
async def test_postgres_incr_rejects_non_positive(bad_amount):
    from dynastore.modules.iam.usage_counter_pg import PostgresUsageCounter

    drv = PostgresUsageCounter.__new__(PostgresUsageCounter)
    # No engine assigned — if validation falls through the call would
    # blow up on ``self._engine`` AttributeError, which the test still
    # catches via the ValueError match. The point is the explicit raise.
    with pytest.raises(ValueError, match="amount must be > 0"):
        await drv.incr("p", "u", window_seconds=60, amount=bad_amount)


@pytest.mark.parametrize("bad_amount", [0, -1])
@pytest.mark.asyncio
async def test_postgres_incr_if_below_rejects_non_positive(bad_amount):
    from dynastore.modules.iam.usage_counter_pg import PostgresUsageCounter

    drv = PostgresUsageCounter.__new__(PostgresUsageCounter)
    with pytest.raises(ValueError, match="amount must be > 0"):
        await drv.incr_if_below("p", "u", limit=10, window_seconds=60, amount=bad_amount)
