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

"""lease_leadership — unit tests for the lease-table leader election CM.

Load-bearing contract: the generator yields exactly once on every path.
A second yield makes contextlib raise ``RuntimeError: generator didn't stop``
and the leader loop resigns every cycle — the regression these tests pin down.

DB layer is mocked via monkeypatching ``managed_transaction`` in the
``locking_tools`` module, mirroring the mock-DQLQuery approach used in
``test_pg_advisory_leadership.py``.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

import dynastore.modules.db_config.connection_health_config as connection_health_config
import dynastore.modules.db_config.locking_tools as locking_tools
from dynastore.modules.db_config.connection_health_config import ConnectionRetryConfig
from dynastore.modules.db_config.locking_tools import (
    _get_stable_lock_id,
    _held_advisory_locks,
    _LEASE_BREAKER_KEY,
    _LEASE_OWNER,
    lease_leadership,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_lease_breaker():
    """Reset the shared lease circuit breaker around every test.

    The breaker is module-global and would otherwise carry failure counts /
    threshold overrides across tests in the same xdist worker.
    """
    b = locking_tools._get_lease_breaker()
    orig_threshold, orig_cooldown = b._threshold, b._cooldown
    b.reset()
    yield
    b.update_thresholds(orig_threshold, orig_cooldown)
    b.reset()


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _make_engine() -> AsyncEngine:
    return MagicMock(spec=AsyncEngine)


def _make_cursor(owner: str | None, epoch: int = 1):
    """CursorResult stand-in.  fetchone() returns a row or None."""
    cursor = MagicMock()
    if owner is None:
        cursor.fetchone.return_value = None
    else:
        cursor.fetchone.return_value = (owner, epoch)
    return cursor


def _patch_managed_transaction(monkeypatch, responses: list):
    """Replace managed_transaction with a sequence of (cursor_or_exc) responses.

    Each ``async with managed_transaction(engine) as conn`` call consumes the
    next entry from *responses*.  If the entry is an ``Exception`` it is
    raised; otherwise ``conn.execute`` returns it as a CursorResult.

    Calls beyond *responses* succeed silently (release UPDATE path).
    """
    call_idx = [0]
    execute_calls: list[tuple[str, dict]] = []

    @asynccontextmanager
    async def _mock(engine):
        idx = call_idx[0]
        call_idx[0] += 1

        if idx < len(responses):
            resp = responses[idx]
            if isinstance(resp, Exception):
                raise resp
            mock_conn = MagicMock()
            mock_conn.execute = AsyncMock(return_value=resp)
        else:
            # Subsequent call (best-effort release UPDATE) — always succeed.
            mock_conn = MagicMock()
            mock_conn.execute = AsyncMock(return_value=MagicMock())

        # Capture execute args for assertions.
        original_execute = mock_conn.execute

        async def _capturing_execute(sql, params=None, **kw):
            sql_str = str(sql) if not isinstance(sql, str) else sql
            execute_calls.append((sql_str, params or {}))
            return await original_execute(sql, params, **kw)

        mock_conn.execute = _capturing_execute
        yield mock_conn

    monkeypatch.setattr(locking_tools, "managed_transaction", _mock)
    return execute_calls, call_idx


# ---------------------------------------------------------------------------
# Tests: acquire-win
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_win_yields_true_none(monkeypatch):
    """CAS returns our owner row → yields (True, None)."""
    cursor = _make_cursor(_LEASE_OWNER)
    _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, lock_conn):
        assert is_leader is True
        assert lock_conn is None


@pytest.mark.asyncio
async def test_win_registers_held_lock(monkeypatch):
    """On win, lock_id is added to _held_advisory_locks during tenure."""
    cursor = _make_cursor(_LEASE_OWNER)
    lock_id = _get_stable_lock_id("test_key")
    _patch_managed_transaction(monkeypatch, [cursor])

    assert lock_id not in _held_advisory_locks
    async with lease_leadership(_make_engine(), "test_key", name="test") as (is_leader, _):
        assert is_leader is True
        assert lock_id in _held_advisory_locks
        assert _held_advisory_locks[lock_id][0] == "test"
    # Entry removed after exit.
    assert lock_id not in _held_advisory_locks


# ---------------------------------------------------------------------------
# Tests: acquire-lose
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lose_yields_false_none(monkeypatch):
    """CAS returns zero rows (foreign owner holds live lease) → (False, None)."""
    cursor = _make_cursor(None)
    _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, lock_conn):
        assert is_leader is False
        assert lock_conn is None


@pytest.mark.asyncio
async def test_lose_does_not_register_held_lock(monkeypatch):
    """On lose, _held_advisory_locks is NOT populated."""
    cursor = _make_cursor(None)
    lock_id = 42
    _patch_managed_transaction(monkeypatch, [cursor])
    before = set(_held_advisory_locks.keys())

    async with lease_leadership(_make_engine(), lock_id, name="test") as (is_leader, _):
        assert is_leader is False

    assert set(_held_advisory_locks.keys()) == before


# ---------------------------------------------------------------------------
# Tests: pre-yield failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_db_failure_before_yield_yields_false(monkeypatch):
    """A non-transient DB error during CAS → (False, None); no RuntimeError.

    Uses RuntimeError (not in the transient set) so the retry decorator does
    not retry — the single attempt fails fast and degrades to non-leader.
    """
    _patch_managed_transaction(monkeypatch, [RuntimeError("db down")])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, lock_conn):
        assert is_leader is False
        assert lock_conn is None


@pytest.mark.asyncio
async def test_yields_exactly_once_on_win(monkeypatch):
    """Generator doesn't stop: win path yields exactly once."""
    cursor = _make_cursor(_LEASE_OWNER)
    _patch_managed_transaction(monkeypatch, [cursor])

    yield_count = 0
    async with lease_leadership(_make_engine(), 42, name="test") as _result:
        yield_count += 1
    assert yield_count == 1


@pytest.mark.asyncio
async def test_yields_exactly_once_on_lose(monkeypatch):
    """Generator doesn't stop: lose path yields exactly once."""
    cursor = _make_cursor(None)
    _patch_managed_transaction(monkeypatch, [cursor])

    yield_count = 0
    async with lease_leadership(_make_engine(), 42, name="test") as _result:
        yield_count += 1
    assert yield_count == 1


@pytest.mark.asyncio
async def test_yields_exactly_once_on_failure(monkeypatch):
    """Generator doesn't stop: failure path yields exactly once."""
    _patch_managed_transaction(monkeypatch, [RuntimeError("kaboom")])

    yield_count = 0
    async with lease_leadership(_make_engine(), 42, name="test") as _result:
        yield_count += 1
    assert yield_count == 1


# ---------------------------------------------------------------------------
# Tests: release on exit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_release_on_exit_issues_expire_update(monkeypatch):
    """On win exit, the finally block runs an UPDATE to expire the lease."""
    cursor = _make_cursor(_LEASE_OWNER)
    execute_calls, _ = _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is True

    # Second managed_transaction execute should be a standalone UPDATE (not INSERT…DO UPDATE).
    standalone_update_calls = [
        sql for sql, _ in execute_calls
        if sql.lstrip().upper().startswith("UPDATE")
    ]
    assert len(standalone_update_calls) == 1
    assert "leader_lease" in standalone_update_calls[0]
    assert "expires_at" in standalone_update_calls[0]


@pytest.mark.asyncio
async def test_release_failure_swallowed(monkeypatch):
    """A failure in the release UPDATE is swallowed; the CM exits cleanly."""
    acquire_cursor = _make_cursor(_LEASE_OWNER)
    # release call raises
    _patch_managed_transaction(monkeypatch, [acquire_cursor, ConnectionError("release failed")])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is True
    # No exception escaped — test passes by reaching here.


@pytest.mark.asyncio
async def test_lose_no_release_update(monkeypatch):
    """On lose, the release UPDATE is NOT issued."""
    cursor = _make_cursor(None)
    execute_calls, call_idx = _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is False

    # Only one managed_transaction call (the CAS); no release call.
    assert call_idx[0] == 1


# ---------------------------------------------------------------------------
# Tests: body exception
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_body_exception_propagates_and_releases(monkeypatch):
    """A failure during the tenure propagates; the release UPDATE still runs."""
    cursor = _make_cursor(_LEASE_OWNER)
    execute_calls, _ = _patch_managed_transaction(monkeypatch, [cursor])

    with pytest.raises(RuntimeError, match="tick failed"):
        async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
            assert is_leader is True
            raise RuntimeError("tick failed")

    standalone_update_calls = [
        sql for sql, _ in execute_calls
        if sql.lstrip().upper().startswith("UPDATE")
    ]
    assert len(standalone_update_calls) == 1


# ---------------------------------------------------------------------------
# Tests: key folding
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_int_key_used_as_is(monkeypatch):
    """Integer key is passed directly as lock_key."""
    cursor = _make_cursor(_LEASE_OWNER)
    execute_calls, _ = _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 0x4D41, name="test") as _:
        pass

    insert_calls = [(sql, p) for sql, p in execute_calls if "INSERT" in sql.upper()]
    assert len(insert_calls) == 1
    assert insert_calls[0][1].get("lock_key") == 0x4D41


@pytest.mark.asyncio
async def test_str_key_folded_to_int(monkeypatch):
    """String key is hashed to a stable int via _get_stable_lock_id."""
    cursor = _make_cursor(_LEASE_OWNER)
    execute_calls, _ = _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), "events_consumer", name="test") as _:
        pass

    insert_calls = [(sql, p) for sql, p in execute_calls if "INSERT" in sql.upper()]
    assert len(insert_calls) == 1
    assert insert_calls[0][1].get("lock_key") == _get_stable_lock_id("events_consumer")


# ---------------------------------------------------------------------------
# Tests: non-AsyncEngine
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("engine", [None, object()])
async def test_non_async_engine_yields_false(engine):
    """None and non-AsyncEngine → (False, None) without DB contact."""
    async with lease_leadership(engine, 42, name="test") as (is_leader, lock_conn):
        assert is_leader is False
        assert lock_conn is None


# ---------------------------------------------------------------------------
# Tests: renew / takeover semantics via CAS return value
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_renew_same_owner_epoch_unchanged(monkeypatch):
    """Second win with same epoch models a successful renew."""
    cursor = _make_cursor(_LEASE_OWNER, epoch=3)
    _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is True  # won → renew acknowledged


@pytest.mark.asyncio
async def test_takeover_after_expiry(monkeypatch):
    """A cursor returning our owner with epoch=2 models a post-expiry takeover."""
    cursor = _make_cursor(_LEASE_OWNER, epoch=2)
    _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is True


@pytest.mark.asyncio
async def test_win_decision_is_row_presence(monkeypatch):
    """Win iff RETURNING yields a row.

    The CAS sets ``owner = EXCLUDED.owner`` (always ours) and RETURNs the row
    only when its WHERE matched, so a non-None row unconditionally means we own
    the lease — the win decision is pure row-presence. A foreign owner can only
    manifest as ZERO rows (the WHERE filtered the update out), which is the lose
    path covered by ``test_lose_yields_false_none``.
    """
    cursor = _make_cursor(_LEASE_OWNER)
    _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is True


# ---------------------------------------------------------------------------
# Concurrency proof: exactly-one-leader via the real CAS WHERE logic
# ---------------------------------------------------------------------------


class _FakeLease:
    """In-memory model of ``configs.leader_lease`` that transcribes the real
    ``INSERT … ON CONFLICT … WHERE expires_at < now() OR owner = EXCLUDED.owner
    RETURNING`` semantics.

    ``now`` is a logical clock the test advances; it mirrors the server-side
    ``now()`` used throughout the CAS (no Python wall-clock is involved). Each
    ``cas`` call is synchronous (no await between read and write), modelling the
    row-lock atomicity PostgreSQL gives the upsert — so N "racing" contenders
    resolve to a serialized order with exactly one winner.
    """

    def __init__(self, now: float = 1000.0) -> None:
        self.row: dict | None = None
        self.now = now

    def cas(self, lock_key: int, owner: str, ttl: float):
        n = self.now
        expires = n + ttl
        if self.row is None:
            # No conflict → INSERT → RETURNING (owner, 1).
            self.row = {"owner": owner, "epoch": 1, "expires_at": expires, "acquired_at": n}
            return (owner, 1)
        ex = self.row
        # ON CONFLICT DO UPDATE … WHERE expires_at < now() OR owner = EXCLUDED.owner
        if ex["expires_at"] < n or ex["owner"] == owner:
            same = ex["owner"] == owner
            epoch = ex["epoch"] if same else ex["epoch"] + 1
            acquired = ex["acquired_at"] if same else n
            self.row = {"owner": owner, "epoch": epoch, "expires_at": expires, "acquired_at": acquired}
            return (owner, epoch)
        # WHERE filtered the update out → ON CONFLICT updated nothing → 0 rows.
        return None


def test_cas_exactly_one_winner_per_round():
    """N contenders racing the same empty lease → exactly one winner; the rest
    get zero rows.  Models PostgreSQL serializing the conflicting upserts."""
    fake = _FakeLease()
    ttl = 30.0
    owners = [f"pod-{i}:owner" for i in range(7)]

    results = [fake.cas(42, o, ttl) for o in owners]
    winners = [o for o, r in zip(owners, results) if r is not None and r[0] == o]

    assert len(winners) == 1, f"expected exactly one winner, got {winners}"
    assert winners[0] == owners[0], "the first contender to execute wins"
    # Every other contender saw a live foreign lease → zero rows.
    assert all(r is None for r in results[1:])


def test_cas_live_lease_blocks_foreign_owner_and_allows_renew():
    """While the lease is live: a foreign owner loses; the holder renews
    (epoch unchanged)."""
    fake = _FakeLease()
    ttl = 30.0
    assert fake.cas(42, "holder:x", ttl) == ("holder:x", 1)

    # Foreign owner, lease still live → zero rows.
    assert fake.cas(42, "intruder:y", ttl) is None
    # Holder renews → same epoch (not a takeover bump).
    assert fake.cas(42, "holder:x", ttl) == ("holder:x", 1)


def test_cas_takeover_after_expiry_bumps_epoch():
    """After the holder's lease expires, a different owner wins and the epoch
    increments — proving takeover (not a silent renew)."""
    fake = _FakeLease()
    ttl = 30.0
    assert fake.cas(42, "holder:x", ttl) == ("holder:x", 1)

    # Advance server-clock past expiry (ttl=30 → expires at now+30).
    fake.now += ttl + 1.0
    result = fake.cas(42, "newpod:z", ttl)
    assert result is not None
    assert result[0] == "newpod:z"
    assert result[1] == 2, "epoch must bump on takeover by a different owner"


# ---------------------------------------------------------------------------
# Transient retry of the CAS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transient_cas_retried_then_declines(monkeypatch):
    """A transient connection error during the CAS is retried on a fresh
    connection (max_retries=3) and, when the budget is exhausted, declines."""
    # Zero the backoff so the three retries are instant.
    monkeypatch.setattr(
        connection_health_config,
        "_retry_config",
        ConnectionRetryConfig(
            max_retries=5, base_delay_seconds=0.0, max_delay_seconds=0.0, jitter=0.0
        ),
    )
    attempts = {"n": 0}

    @asynccontextmanager
    async def _mt_transient(engine):
        attempts["n"] += 1
        raise OSError("connection reset by peer")  # transient → retried
        yield None  # pragma: no cover — required for asynccontextmanager

    monkeypatch.setattr(locking_tools, "managed_transaction", _mt_transient)

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is False
    # Exactly the hardcoded max_retries=3 attempts (not the config's 5).
    assert attempts["n"] == 3


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_breaker_opens_after_threshold_then_short_circuits(monkeypatch):
    """After `threshold` consecutive CAS failures the breaker opens and the CM
    short-circuits to (False, None) WITHOUT touching the DB."""
    calls = {"n": 0}

    @asynccontextmanager
    async def _mt_fail(engine):
        calls["n"] += 1
        raise RuntimeError("db down")  # non-transient → 1 attempt, fast
        yield None  # pragma: no cover

    monkeypatch.setattr(locking_tools, "managed_transaction", _mt_fail)

    # Drive exactly `threshold` (5) consecutive failures → breaker opens.
    for _ in range(5):
        async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
            assert is_leader is False

    assert calls["n"] == 5
    assert locking_tools._get_lease_breaker().state_of(_LEASE_BREAKER_KEY) == "OPEN"

    # Next entry: breaker is OPEN → short-circuit, DB is NOT contacted.
    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is False
    assert calls["n"] == 5, "OPEN breaker must not invoke managed_transaction"


@pytest.mark.asyncio
async def test_breaker_recovers_half_open_to_closed_after_cooldown(monkeypatch):
    """Once the cooldown elapses the breaker half-opens; a successful probe CAS
    closes it again."""
    import asyncio

    breaker = locking_tools._get_lease_breaker()
    breaker.update_thresholds(failure_threshold=5, cooldown_seconds=0.05)

    @asynccontextmanager
    async def _mt_fail(engine):
        raise RuntimeError("db down")
        yield None  # pragma: no cover

    monkeypatch.setattr(locking_tools, "managed_transaction", _mt_fail)
    for _ in range(5):
        async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
            assert is_leader is False
    assert breaker.state_of(_LEASE_BREAKER_KEY) == "OPEN"

    # Let the cooldown elapse so the next entry probes (OPEN → HALF_OPEN).
    await asyncio.sleep(0.07)

    # The probe CAS now succeeds → record_success → breaker closes.
    cursor = _make_cursor(_LEASE_OWNER)

    @asynccontextmanager
    async def _mt_ok(engine):
        conn = MagicMock()

        async def _ex(sql, params=None, **kw):
            return cursor

        conn.execute = _ex
        yield conn

    monkeypatch.setattr(locking_tools, "managed_transaction", _mt_ok)
    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is True
    assert breaker.state_of(_LEASE_BREAKER_KEY) == "CLOSED"
