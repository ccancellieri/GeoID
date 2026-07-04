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

"""Integration tests for the #2933 bounded pool-acquire design, against a
real Postgres backend.

Background: PR #2930 removed the ``connection_poison_guard`` on the
argument that SQLAlchemy already treats a cancelled *query* as a disconnect
and invalidates the connection natively. That argument does not cover
cancelling ``engine.connect()`` itself -- the pool *checkout*, not a query
on an already-checked-out connection. Two sub-cases matter here, and they
behave very differently under cancellation:

1. **Pool fully saturated** (every slot checked out, no room to create a
   new connection): a checkout is just waiting on the pool's internal FIFO
   queue (``AsyncAdaptedQueue.get()``, itself an
   ``asyncio.wait_for(asyncio.Queue.get(), pool_timeout)``). Cancelling
   that wait is clean -- ``asyncio.Queue.get()`` removes its own waiter
   from the FIFO on cancellation, so an abandoned attempt stops competing
   for a connection immediately. This is the scenario the fail-fast guard
   actually exists for.

2. **A brand new physical connection is being created** (pool below its
   ``pool_size + max_overflow`` ceiling): the asyncpg handshake and the
   dialect's post-connect codec setup (``on_connect``) can already have a
   real backend session open before SQLAlchemy finishes registering the
   connection. A cancellation landing in that narrow window abandons the
   already-open backend session rather than closing it.

An earlier version of ``acquire_engine_connection_bounded`` wrapped the
whole checkout in ``asyncio.shield`` to close case 2. That traded it for a
worse failure mode in case 1 -- the common case under real saturation:
every timeout would leave a zombie checkout registered in the FIFO queue
for up to the full engine ``pool_timeout`` (tens of seconds), accumulating
ahead of genuinely new requests and stealing connections from them as they
free up. The tests below assert the corrected, unshielded design does NOT
do that (the required regression coverage), and separately document case 2
as a known, accepted, self-bounded tradeoff rather than silently reproducing
it.
"""
from __future__ import annotations

import asyncio
import time

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from dynastore.modules.db_config.exceptions import PoolSaturationError
from dynastore.modules.db_config.query_executor import (
    acquire_engine_connection_bounded,
)


# ---------------------------------------------------------------------------
# Case 1: sustained full saturation -- the required pileup regression
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def saturated_engine(db_url):
    """A real engine with a single connection slot, held open for the
    duration of the test so every subsequent checkout is forced onto the
    pool's FIFO wait queue (never a new-connection handshake)."""
    engine = create_async_engine(
        db_url.replace("postgresql://", "postgresql+asyncpg://"),
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
    )
    holder = await engine.connect()
    yield engine, holder
    if not holder.closed:
        await holder.close()
    await engine.dispose()


@pytest.mark.asyncio
async def test_repeated_timeouts_under_sustained_saturation_do_not_pile_up(
    saturated_engine,
):
    """The required #2933 regression: firing many bounded acquires in a row
    against a fully saturated pool must not accumulate zombie waiters in the
    pool's FIFO queue, and must not slow down over the run (each attempt
    bounded by ``timeout_s`` regardless of how many came before it)."""
    engine, _holder = saturated_engine
    timeout_s = 0.05
    attempts = 30

    durations: list[float] = []
    for _ in range(attempts):
        t0 = time.monotonic()
        with pytest.raises(PoolSaturationError):
            await acquire_engine_connection_bounded(engine, timeout_s=timeout_s)
        durations.append(time.monotonic() - t0)

    # No attempt should take meaningfully longer than the bound itself --
    # a pileup would show up as durations growing across the run (later
    # attempts queued behind earlier zombies).
    assert max(durations) < timeout_s * 3, (
        f"a bounded acquire took {max(durations):.3f}s against a "
        f"{timeout_s}s bound -- looks like queued zombies are delaying "
        f"later attempts (durations={durations})"
    )

    # The clearest possible proof: nothing left registered as a waiter on
    # the pool's internal FIFO queue after every one of these timed out.
    waiters = engine.pool._pool._queue._getters
    assert len(waiters) == 0, (
        f"{len(waiters)} abandoned checkout(s) still registered in the "
        "pool's wait queue after their callers timed out -- this is "
        "exactly the zombie pileup the shielded design introduced"
    )


@pytest.mark.asyncio
async def test_concurrent_burst_of_timeouts_does_not_pile_up(saturated_engine):
    """Same property under a concurrent burst rather than a sequential
    loop -- closer to real traffic, where many requests hit a saturated
    pool at once rather than one after another."""
    engine, _holder = saturated_engine
    timeout_s = 0.1
    concurrency = 25

    async def _attempt() -> float:
        t0 = time.monotonic()
        with pytest.raises(PoolSaturationError):
            await acquire_engine_connection_bounded(engine, timeout_s=timeout_s)
        return time.monotonic() - t0

    durations = await asyncio.gather(*(_attempt() for _ in range(concurrency)))

    assert max(durations) < timeout_s * 3, (
        f"a concurrent bounded acquire took {max(durations):.3f}s against "
        f"a {timeout_s}s bound (durations={durations})"
    )
    waiters = engine.pool._pool._queue._getters
    assert len(waiters) == 0


@pytest.mark.asyncio
async def test_freed_connection_after_saturation_goes_to_a_new_waiter_immediately(
    saturated_engine,
):
    """The priority-inversion check: after a burst of abandoned checkouts
    against a saturated pool, releasing the held connection must let a
    brand-new acquire succeed essentially immediately -- not delayed behind
    zombies that would have been ahead of it in the FIFO under the shielded
    design."""
    engine, holder = saturated_engine
    timeout_s = 0.05

    for _ in range(15):
        with pytest.raises(PoolSaturationError):
            await acquire_engine_connection_bounded(engine, timeout_s=timeout_s)

    await holder.close()

    t0 = time.monotonic()
    conn = await asyncio.wait_for(
        acquire_engine_connection_bounded(engine, timeout_s=5), timeout=5
    )
    dt = time.monotonic() - t0
    try:
        result = await conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
    finally:
        await conn.close()

    assert dt < 0.5, (
        f"a fresh acquire took {dt:.3f}s to get the just-freed connection "
        "after a saturation burst -- expected near-instant, since no "
        "zombie should have been ahead of it in the pool's wait queue"
    )


# ---------------------------------------------------------------------------
# Case 2: below-ceiling handshake window -- accepted, documented tradeoff
# ---------------------------------------------------------------------------


def _make_slow_first_checkout_creator(db_url: str, state: dict):
    """Async-creator: the FIRST call opens a genuine backend connection and
    stashes its server PID before stalling briefly -- reproducing the exact
    post-handshake, pre-pool-registration window a cancellation can land in
    when a *new* physical connection is being created (as opposed to a
    checkout that is only waiting in the pool's FIFO queue).
    """
    import asyncpg

    async def creator():
        state["calls"] += 1
        conn = await asyncpg.connect(dsn=db_url)
        if state["calls"] == 1:
            state["first_pid"] = conn.get_server_pid()
            await asyncio.sleep(0.4)
        return conn

    return creator


async def _backend_pid_alive(db_url: str, pid: int) -> bool:
    import asyncpg

    conn = await asyncpg.connect(dsn=db_url)
    try:
        count = await conn.fetchval(
            "select count(*) from pg_stat_activity where pid = $1", pid
        )
        return bool(count)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_handshake_window_cancellation_is_an_accepted_documented_tradeoff(
    db_url,
):
    """Case 2 from the module docstring: when a checkout is creating a
    *new* physical connection (not waiting in the FIFO queue) and the bound
    fires mid-handshake, the checkout IS cancelled for real and the
    already-open backend session IS abandoned rather than closed. This is
    the accepted tradeoff documented on ``acquire_engine_connection_bounded``
    -- kept here as a live regression so a future change to that tradeoff
    (e.g. asyncpg/SQLAlchemy starting to clean this up on their own, or a
    reintroduced shield) is a deliberate, visible decision rather than a
    silent behavior change.

    Also confirms cancellation is immediate and synchronous: unlike the
    shielded design, no ``gc.collect()`` or extra wait is needed for the
    pool's own bookkeeping (``overflow``/``checkedout``) to unwind -- that
    happens by the time the bounded call returns.
    """
    state: dict = {"calls": 0}
    engine = create_async_engine(
        "postgresql+asyncpg://unused-real-creator-supplies-the-dsn",
        async_creator=_make_slow_first_checkout_creator(db_url, state),
        pool_size=1,
        max_overflow=0,
        pool_timeout=5,
    )
    try:
        with pytest.raises(PoolSaturationError):
            await acquire_engine_connection_bounded(engine, timeout_s=0.05)

        assert "first_pid" in state, "the checkout must have opened a real backend"
        assert engine.pool.checkedout() == 0, (
            "pool bookkeeping must unwind synchronously on cancellation, "
            "with no gc.collect() or extra wait required"
        )

        # The cancellation fired mid-handshake; give asyncpg's own transport
        # a moment in case it self-terminates on cancellation (documented
        # here that it does not, once the connection is already past the
        # handshake -- that is the accepted leak).
        await asyncio.sleep(0.3)
        assert await _backend_pid_alive(db_url, state["first_pid"]), (
            "expected the orphaned backend session to still be alive -- if "
            "this now fails, asyncpg/SQLAlchemy started cleaning up a "
            "cancelled checkout on its own and the tradeoff documented on "
            "acquire_engine_connection_bounded should be revisited"
        )

        # And a subsequent acquire is unaffected -- a fresh connection, not
        # a poisoned/reused one.
        conn = await asyncio.wait_for(
            acquire_engine_connection_bounded(engine, timeout_s=5), timeout=5
        )
        try:
            result = await conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
        finally:
            await conn.close()
    finally:
        await engine.dispose()
