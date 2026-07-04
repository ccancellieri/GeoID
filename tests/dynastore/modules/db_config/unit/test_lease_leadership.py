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
``locking_tools`` module.
"""
from __future__ import annotations

import asyncio
import logging
import time
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
    lease_leadership_with_heartbeat,
    renew_lease,
    run_lease_leadership_heartbeat_loop,
    start_lease_heartbeat,
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


@pytest.mark.asyncio
async def test_set_local_lock_timeout_precedes_cas_insert(monkeypatch):
    """The CAS transaction scopes its own row-lock wait via ``SET LOCAL
    lock_timeout`` (mirrors ``safe_drop_relation``), issued before the
    INSERT..ON CONFLICT CAS, so a losing contender fails fast instead of
    inheriting the session-wide ``DB_LOCK_TIMEOUT``."""
    cursor = _make_cursor(_LEASE_OWNER)
    execute_calls, _ = _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is True

    sqls = [sql for sql, _ in execute_calls]
    set_local_idx = next(
        i for i, sql in enumerate(sqls) if sql.lstrip().upper().startswith("SET LOCAL")
    )
    insert_idx = next(
        i for i, sql in enumerate(sqls) if sql.lstrip().upper().startswith("INSERT")
    )
    assert "lock_timeout = '500ms'" in sqls[set_local_idx]
    assert set_local_idx < insert_idx


# ---------------------------------------------------------------------------
# Tests: non-locking pre-check (#2959)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_precheck_skips_cas_while_foreign_owner_live(monkeypatch):
    """A live foreign owner's lease is caught by the plain SELECT pre-check,
    so the CAS (SET LOCAL + INSERT..ON CONFLICT) is never issued — the
    follower never joins the row-lock queue for an update it cannot win."""
    cursor = MagicMock()
    cursor.fetchone.return_value = ("intruder:y", True)
    execute_calls, call_idx = _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is False

    sqls = [sql for sql, _ in execute_calls]
    assert any(sql.lstrip().upper().startswith("SELECT") for sql in sqls)
    assert not any(sql.lstrip().upper().startswith("SET LOCAL") for sql in sqls)
    assert not any(sql.lstrip().upper().startswith("INSERT") for sql in sqls)
    # Lost the lease → no release UPDATE either (single managed_transaction call).
    assert call_idx[0] == 1


@pytest.mark.asyncio
async def test_precheck_falls_through_to_cas_after_expiry(monkeypatch):
    """A foreign owner's row exists but is expired: the pre-check does NOT
    skip the CAS — it falls through to the takeover attempt."""
    responses = iter([("intruder:y", False), (_LEASE_OWNER, 2)])
    cursor = MagicMock()
    cursor.fetchone.side_effect = lambda: next(responses)
    execute_calls, _ = _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is True

    sqls = [sql for sql, _ in execute_calls]
    assert any(sql.lstrip().upper().startswith("SELECT") for sql in sqls)
    assert any(sql.lstrip().upper().startswith("INSERT") for sql in sqls)


@pytest.mark.asyncio
async def test_precheck_falls_through_to_cas_when_no_row(monkeypatch):
    """No existing lease row: the pre-check finds nothing and falls through
    to the CAS insert, which wins the (empty) lease."""
    responses = iter([None, (_LEASE_OWNER, 1)])
    cursor = MagicMock()
    cursor.fetchone.side_effect = lambda: next(responses)
    execute_calls, _ = _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is True

    sqls = [sql for sql, _ in execute_calls]
    assert any(sql.lstrip().upper().startswith("SELECT") for sql in sqls)
    assert any(sql.lstrip().upper().startswith("INSERT") for sql in sqls)


@pytest.mark.asyncio
async def test_precheck_falls_through_to_cas_on_own_renewal(monkeypatch):
    """An existing row already owned by this process is a renewal, not a
    foreign lease — the pre-check falls through to the CAS regardless of
    whether the row is still live."""
    responses = iter([(_LEASE_OWNER, True), (_LEASE_OWNER, 1)])
    cursor = MagicMock()
    cursor.fetchone.side_effect = lambda: next(responses)
    execute_calls, _ = _patch_managed_transaction(monkeypatch, [cursor])

    async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
        assert is_leader is True

    sqls = [sql for sql, _ in execute_calls]
    assert any(sql.lstrip().upper().startswith("INSERT") for sql in sqls)


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


@pytest.mark.asyncio
async def test_lock_not_available_does_not_trip_breaker(monkeypatch, caplog):
    """A 55P03 lock-not-available CAS failure is a lost round, not a breaker
    failure: it must not accumulate toward the failure threshold, must log at
    DEBUG (not WARNING), and must decline (False, None) every time — even
    past the ordinary threshold of consecutive losses."""
    caplog.set_level(logging.DEBUG, logger=locking_tools.logger.name)
    record_failure_calls = {"n": 0}
    orig_record_failure = locking_tools._lease_breaker_record_failure

    def _spy_record_failure(lock_name):
        record_failure_calls["n"] += 1
        return orig_record_failure(lock_name)

    monkeypatch.setattr(locking_tools, "_lease_breaker_record_failure", _spy_record_failure)

    @asynccontextmanager
    async def _mt_lock_timeout(engine):
        raise RuntimeError("canceling statement due to lock timeout")
        yield None  # pragma: no cover

    monkeypatch.setattr(locking_tools, "managed_transaction", _mt_lock_timeout)

    # 6 consecutive losses, past the breaker's failure threshold (5).
    for _ in range(6):
        async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, lock_conn):
            assert is_leader is False
            assert lock_conn is None
        assert locking_tools._get_lease_breaker().state_of(_LEASE_BREAKER_KEY) == "CLOSED"

    assert record_failure_calls["n"] == 0
    assert not any(rec.levelno >= logging.WARNING for rec in caplog.records)
    assert any(
        rec.levelno == logging.DEBUG and "row-lock race" in rec.getMessage()
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_non_lock_error_still_trips_breaker(monkeypatch):
    """Negative control: a generic non-lock DB error still counts as a
    breaker failure and, after the threshold, opens the breaker — pinning
    that the 55P03 carve-out is narrow and does not swallow real failures."""

    @asynccontextmanager
    async def _mt_fail(engine):
        raise RuntimeError("db down")  # not a lock-timeout error
        yield None  # pragma: no cover

    monkeypatch.setattr(locking_tools, "managed_transaction", _mt_fail)

    for _ in range(5):
        async with lease_leadership(_make_engine(), 42, name="test") as (is_leader, _):
            assert is_leader is False

    assert locking_tools._get_lease_breaker().state_of(_LEASE_BREAKER_KEY) == "OPEN"


# ---------------------------------------------------------------------------
# renew_lease — CAS-on-own-owner renewal (heartbeat regime, #2597)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_renew_lease_true_when_still_owner(monkeypatch):
    """CAS returns our owner row (still owner) → renewed."""
    cursor = _make_cursor(_LEASE_OWNER)
    _patch_managed_transaction(monkeypatch, [cursor])

    assert await renew_lease(_make_engine(), 42, name="test") is True


@pytest.mark.asyncio
async def test_renew_lease_false_when_ownership_lost(monkeypatch):
    """CAS returns zero rows (a different owner already holds the row) → lost."""
    cursor = _make_cursor(None)
    _patch_managed_transaction(monkeypatch, [cursor])

    assert await renew_lease(_make_engine(), 42, name="test") is False


@pytest.mark.asyncio
async def test_renew_lease_false_on_db_error(monkeypatch):
    """A DB error is treated the same as an explicit loss — fail-safe."""
    _patch_managed_transaction(monkeypatch, [RuntimeError("db down")])

    assert await renew_lease(_make_engine(), 42, name="test") is False


@pytest.mark.asyncio
@pytest.mark.parametrize("engine", [None, object()])
async def test_renew_lease_false_on_non_async_engine(engine):
    """None / non-AsyncEngine → False without DB contact."""
    assert await renew_lease(engine, 42, name="test") is False


def test_fake_lease_renewal_cannot_resurrect_after_takeover():
    """SQL-semantics proof for CAS-on-own-owner: after a takeover by a
    different owner, the original owner's renewal attempt (same CAS, same
    owner string as before) gets zero rows. renew_lease reuses this exact
    CAS, so it structurally cannot resurrect a lease another pod has already
    taken over."""
    fake = _FakeLease()
    ttl = 30.0
    assert fake.cas(42, "pod-A", ttl) == ("pod-A", 1)

    # pod-A's lease expires; pod-B takes over.
    fake.now += ttl + 1.0
    assert fake.cas(42, "pod-B", ttl) == ("pod-B", 2)

    # pod-A's heartbeat fires late and "renews" with its own owner string —
    # zero rows, because pod-B already owns the row.
    assert fake.cas(42, "pod-A", ttl) is None


# ---------------------------------------------------------------------------
# start_lease_heartbeat — background renewal task
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_lease_heartbeat_renews_repeatedly_when_healthy(monkeypatch):
    """A healthy lease is renewed on every interval; lost_event stays unset."""
    calls = {"n": 0}

    async def _ok(engine, key, *, name="leader"):
        calls["n"] += 1
        return True

    monkeypatch.setattr(locking_tools, "renew_lease", _ok)
    lost, task = start_lease_heartbeat(_make_engine(), 42, name="test", interval_seconds=0.02)
    try:
        await asyncio.sleep(0.09)
        assert calls["n"] >= 2, f"expected multiple renewals, got {calls['n']}"
        assert lost.is_set() is False
    finally:
        lost.set()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_start_lease_heartbeat_sets_lost_on_renewal_failure(monkeypatch):
    """A failed renewal sets lost_event immediately and the task stops on its
    own (no further renewal attempts)."""
    calls = {"n": 0}

    async def _fail(engine, key, *, name="leader"):
        calls["n"] += 1
        return False

    monkeypatch.setattr(locking_tools, "renew_lease", _fail)
    lost, task = start_lease_heartbeat(_make_engine(), 42, name="test", interval_seconds=0.02)

    await asyncio.wait_for(task, timeout=2.0)

    assert lost.is_set() is True
    assert calls["n"] == 1, "the loop must not retry after a failed renewal"


@pytest.mark.asyncio
async def test_start_lease_heartbeat_uses_configured_default_interval(monkeypatch):
    """With no explicit interval_seconds, the default TTL/3-style config value
    is read live (not frozen at import time)."""
    monkeypatch.setattr(locking_tools._leadership_config, "lease_renew_interval_seconds", 0.02)
    calls = {"n": 0}

    async def _ok(engine, key, *, name="leader"):
        calls["n"] += 1
        return True

    monkeypatch.setattr(locking_tools, "renew_lease", _ok)
    lost, task = start_lease_heartbeat(_make_engine(), 42, name="test")
    try:
        await asyncio.sleep(0.07)
        assert calls["n"] >= 2
    finally:
        lost.set()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# lease_leadership_with_heartbeat — continuous-tenure acquire CM
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_heartbeat_win_yields_true_and_lost_event(monkeypatch):
    """On win, yields (True, lost_event) where lost_event starts unset."""
    cursor = _make_cursor(_LEASE_OWNER)
    _patch_managed_transaction(monkeypatch, [cursor])

    fake_event = asyncio.Event()
    fake_task = asyncio.get_event_loop().create_future()
    fake_task.set_result(None)

    monkeypatch.setattr(
        locking_tools,
        "start_lease_heartbeat",
        lambda engine, lock_id, *, name="leader", interval_seconds=None: (fake_event, fake_task),
    )

    async with lease_leadership_with_heartbeat(_make_engine(), 42, name="test") as (
        is_leader,
        lost_event,
    ):
        assert is_leader is True
        assert lost_event is fake_event
        assert lost_event.is_set() is False


@pytest.mark.asyncio
async def test_heartbeat_win_registers_held_lock(monkeypatch):
    """On win, lock_id is added to _held_advisory_locks during tenure."""
    cursor = _make_cursor(_LEASE_OWNER)
    lock_id = _get_stable_lock_id("heartbeat_key")
    _patch_managed_transaction(monkeypatch, [cursor])

    fake_event = asyncio.Event()
    fake_task = asyncio.get_event_loop().create_future()
    fake_task.set_result(None)
    monkeypatch.setattr(
        locking_tools,
        "start_lease_heartbeat",
        lambda engine, lid, *, name="leader", interval_seconds=None: (fake_event, fake_task),
    )

    assert lock_id not in _held_advisory_locks
    async with lease_leadership_with_heartbeat(_make_engine(), "heartbeat_key", name="test") as (
        is_leader,
        _,
    ):
        assert is_leader is True
        assert lock_id in _held_advisory_locks
    assert lock_id not in _held_advisory_locks


@pytest.mark.asyncio
async def test_heartbeat_lose_yields_false_none_and_starts_no_heartbeat(monkeypatch):
    """CAS loses → (False, None); no heartbeat task is ever started."""
    cursor = _make_cursor(None)
    _patch_managed_transaction(monkeypatch, [cursor])

    start_spy = MagicMock(side_effect=AssertionError("heartbeat must not start on lose"))
    monkeypatch.setattr(locking_tools, "start_lease_heartbeat", start_spy)

    async with lease_leadership_with_heartbeat(_make_engine(), 42, name="test") as (
        is_leader,
        lost_event,
    ):
        assert is_leader is False
        assert lost_event is None
    start_spy.assert_not_called()


@pytest.mark.asyncio
async def test_heartbeat_non_async_engine_yields_false_no_db_contact():
    """None / non-AsyncEngine → (False, None) without touching the DB or
    starting a heartbeat."""
    async with lease_leadership_with_heartbeat(object(), 42, name="test") as (
        is_leader,
        lost_event,
    ):
        assert is_leader is False
        assert lost_event is None


@pytest.mark.asyncio
async def test_heartbeat_release_cancels_heartbeat_task_and_issues_expire_update(monkeypatch):
    """Exiting the CM stops the real heartbeat task and runs the same
    best-effort expire UPDATE as lease_leadership."""
    cursor = _make_cursor(_LEASE_OWNER)
    execute_calls, _ = _patch_managed_transaction(monkeypatch, [cursor])
    fake_event = asyncio.Event()

    async def _never_ending() -> None:
        await asyncio.Event().wait()

    real_task = asyncio.create_task(_never_ending())
    monkeypatch.setattr(
        locking_tools,
        "start_lease_heartbeat",
        lambda engine, lock_id, *, name="leader", interval_seconds=None: (fake_event, real_task),
    )

    async with lease_leadership_with_heartbeat(_make_engine(), 42, name="test") as (
        is_leader,
        _,
    ):
        assert is_leader is True

    assert real_task.cancelled() or real_task.done()
    assert fake_event.is_set() is True

    standalone_update_calls = [
        sql for sql, _ in execute_calls if sql.lstrip().upper().startswith("UPDATE")
    ]
    assert len(standalone_update_calls) == 1
    assert "leader_lease" in standalone_update_calls[0]


@pytest.mark.asyncio
async def test_heartbeat_body_exception_propagates_and_still_releases(monkeypatch):
    """A failure during the tenure propagates; the heartbeat is still stopped
    and the release UPDATE still runs."""
    cursor = _make_cursor(_LEASE_OWNER)
    execute_calls, _ = _patch_managed_transaction(monkeypatch, [cursor])
    fake_event = asyncio.Event()

    async def _never_ending() -> None:
        await asyncio.Event().wait()

    real_task = asyncio.create_task(_never_ending())
    monkeypatch.setattr(
        locking_tools,
        "start_lease_heartbeat",
        lambda engine, lock_id, *, name="leader", interval_seconds=None: (fake_event, real_task),
    )

    with pytest.raises(RuntimeError, match="tick failed"):
        async with lease_leadership_with_heartbeat(_make_engine(), 42, name="test") as (
            is_leader,
            _,
        ):
            assert is_leader is True
            raise RuntimeError("tick failed")

    assert real_task.cancelled() or real_task.done()
    standalone_update_calls = [
        sql for sql, _ in execute_calls if sql.lstrip().upper().startswith("UPDATE")
    ]
    assert len(standalone_update_calls) == 1


# ---------------------------------------------------------------------------
# Real-time failover proof: heartbeat keeps tenure; a crash fails over
# within lease_ttl_seconds (not before, not unboundedly after).
# ---------------------------------------------------------------------------


class _FakeLeaseTable:
    """In-memory ``configs.leader_lease`` model driven by wall-clock time.

    Mirrors ``_FakeLease`` (used for the synchronous CAS-ordering proofs
    above) but keys expiry off ``time.monotonic()`` so it can back a fake
    ``managed_transaction`` and prove real-time renewal/failover behaviour
    end-to-end through :func:`lease_leadership_with_heartbeat` and
    :func:`lease_leadership` without a live database.
    """

    def __init__(self) -> None:
        self.row: dict | None = None

    def cas(self, owner: str, ttl: float):
        n = time.monotonic()
        if self.row is None or self.row["expires_at"] < n or self.row["owner"] == owner:
            same = self.row is not None and self.row["owner"] == owner
            epoch = (self.row["epoch"] if same else (self.row["epoch"] + 1 if self.row else 1))
            self.row = {"owner": owner, "epoch": epoch, "expires_at": n + ttl}
            return (owner, epoch)
        return None

    def release(self, owner: str) -> None:
        if self.row is not None and self.row["owner"] == owner:
            self.row["expires_at"] = time.monotonic() - 1


@pytest.mark.asyncio
async def test_failover_within_ttl_after_simulated_crash(monkeypatch):
    """Heartbeat-mode leader A crashes (its renewals stop). A competitor
    polling via the default per-tick lease_leadership only wins once
    lease_ttl_seconds has actually elapsed since A's last successful
    renewal -- proving the heartbeat regime fails over within the TTL,
    neither before (no premature takeover of a healthy leader) nor
    unboundedly after (no permanently stuck lease)."""
    table = _FakeLeaseTable()
    ttl = 0.3
    monkeypatch.setattr(locking_tools._leadership_config, "lease_ttl_seconds", ttl)
    # Small renewal interval so pod-A's heartbeat actually fires (and is
    # deliberately failed below) well inside the test window, rather than
    # relying on the 10s production default never firing in time.
    monkeypatch.setattr(locking_tools._leadership_config, "lease_renew_interval_seconds", 0.03)

    @asynccontextmanager
    async def _mt(engine):
        conn = MagicMock()

        async def _ex(sql, params=None, **kw):
            sql_str = str(sql)
            upper = sql_str.lstrip().upper()
            if upper.startswith("SELECT"):
                # Non-locking pre-check: mirrors the real
                # owner/live query against the fake table's current row.
                cur = MagicMock()
                if table.row is None:
                    cur.fetchone.return_value = None
                else:
                    cur.fetchone.return_value = (
                        table.row["owner"], table.row["expires_at"] > time.monotonic()
                    )
                return cur
            if upper.startswith("SET LOCAL"):
                return MagicMock()
            if upper.startswith("INSERT"):
                result = table.cas(params["owner"], params["ttl"])
                cur = MagicMock()
                cur.fetchone.return_value = result
                return cur
            table.release(params["owner"])
            return MagicMock()

        conn.execute = _ex
        yield conn

    monkeypatch.setattr(locking_tools, "managed_transaction", _mt)

    # _LEASE_OWNER is a process-wide singleton (minted once at import time),
    # not derived from the `name` kwarg -- so two contenders in the SAME
    # process/test must explicitly swap it to model different pods. pod-A's
    # identity is fixed here; the poll loop below swaps in pod-B's identity
    # only for the duration of each of its own attempts.
    monkeypatch.setattr(locking_tools, "_LEASE_OWNER", "owner-pod-A")

    async def _try_b_once(engine, lock_id) -> bool:
        locking_tools._LEASE_OWNER = "owner-pod-B"
        try:
            async with lease_leadership(engine, lock_id, name="pod-B") as (b_leader, _):
                return b_leader
        finally:
            locking_tools._LEASE_OWNER = "owner-pod-A"

    engine = _make_engine()
    lock_id = 4242

    async with lease_leadership_with_heartbeat(engine, lock_id, name="pod-A") as (
        is_leader,
        lost_a,
    ):
        assert is_leader is True
        crash_at = time.monotonic()

        # Simulate pod-A crashing: every subsequent renewal attempt fails,
        # standing in for a process that vanished (its heartbeat task never
        # runs `finally`, mirroring a killed pod).
        monkeypatch.setattr(locking_tools, "renew_lease", AsyncMock(return_value=False))

        won_at = None
        deadline = crash_at + ttl * 10
        while time.monotonic() < deadline:
            if await _try_b_once(engine, lock_id):
                won_at = time.monotonic()
                break
            await asyncio.sleep(ttl / 20)

    assert won_at is not None, "competitor never won the lease"
    elapsed = won_at - crash_at
    assert elapsed >= ttl * 0.5, (
        f"competitor won too early ({elapsed:.3f}s), before A's lease could "
        f"have expired (ttl={ttl}s)"
    )
    assert elapsed <= ttl * 4, (
        f"competitor took too long to win ({elapsed:.3f}s) -- failover must "
        f"be bounded by lease_ttl_seconds"
    )


# ---------------------------------------------------------------------------
# run_lease_leadership_heartbeat_loop — control loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_heartbeat_loop_acquires_once_and_ticks_repeatedly(monkeypatch):
    """The lease CM is entered exactly ONCE per tenure; on_tick runs on
    cadence_seconds without releasing leadership between ticks."""
    acquire_count = {"n": 0}
    tick_count = {"n": 0}
    lost = asyncio.Event()

    @asynccontextmanager
    async def _fake_acquire(engine, key, *, name="leader"):
        acquire_count["n"] += 1
        yield (True, lost)

    monkeypatch.setattr(locking_tools, "lease_leadership_with_heartbeat", _fake_acquire)

    shutdown = asyncio.Event()

    async def _on_tick() -> None:
        tick_count["n"] += 1
        if tick_count["n"] >= 3:
            shutdown.set()

    await asyncio.wait_for(
        run_lease_leadership_heartbeat_loop(
            _make_engine(),
            42,
            on_tick=_on_tick,
            name="test",
            cadence_seconds=0.01,
            is_shutdown=shutdown.is_set,
            shutdown_event=shutdown,
        ),
        timeout=2.0,
    )

    assert acquire_count["n"] == 1, "lease must be acquired ONCE, not per tick"
    assert tick_count["n"] == 3


@pytest.mark.asyncio
async def test_heartbeat_loop_no_tick_when_not_leader(monkeypatch):
    """When the CM yields (False, None), on_tick is never called and the
    loop keeps polling on reelect_cadence_seconds."""
    acquire_count = {"n": 0}
    tick_count = {"n": 0}

    @asynccontextmanager
    async def _fake_acquire(engine, key, *, name="leader"):
        acquire_count["n"] += 1
        yield (False, None)

    monkeypatch.setattr(locking_tools, "lease_leadership_with_heartbeat", _fake_acquire)
    shutdown = asyncio.Event()

    async def _on_tick() -> None:
        tick_count["n"] += 1

    task = asyncio.create_task(
        run_lease_leadership_heartbeat_loop(
            _make_engine(),
            42,
            on_tick=_on_tick,
            name="test",
            cadence_seconds=0.02,
            is_shutdown=shutdown.is_set,
            shutdown_event=shutdown,
            reelect_cadence_seconds=0.02,
        )
    )
    await asyncio.sleep(0.1)
    shutdown.set()
    await asyncio.wait_for(task, timeout=2.0)

    assert tick_count["n"] == 0
    assert acquire_count["n"] >= 2


@pytest.mark.asyncio
async def test_heartbeat_loop_stops_immediately_on_lease_loss(monkeypatch):
    """Once lost_event fires, the loop must not start another tick and must
    exit the leadership context before re-electing -- no work continues
    after the lease is known lost (bounds the two-leader overlap to at most
    one in-flight tick, same bound the per-tick TTL clamp gives)."""
    acquire_count = {"n": 0}
    tick_count = {"n": 0}
    lost = asyncio.Event()

    @asynccontextmanager
    async def _fake_acquire(engine, key, *, name="leader"):
        acquire_count["n"] += 1
        yield (True, lost)

    monkeypatch.setattr(locking_tools, "lease_leadership_with_heartbeat", _fake_acquire)
    shutdown = asyncio.Event()

    async def _on_tick() -> None:
        tick_count["n"] += 1
        if tick_count["n"] == 1:
            lost.set()  # simulate a renewal failure detected right after tick 1
        if tick_count["n"] >= 5:
            shutdown.set()  # safety net so a bug here can't hang the test

    task = asyncio.create_task(
        run_lease_leadership_heartbeat_loop(
            _make_engine(),
            42,
            on_tick=_on_tick,
            name="test",
            cadence_seconds=10.0,  # long -- must NOT be waited out
            is_shutdown=shutdown.is_set,
            shutdown_event=shutdown,
            reelect_cadence_seconds=0.02,
        )
    )
    await asyncio.sleep(0.15)
    shutdown.set()
    await asyncio.wait_for(task, timeout=2.0)

    assert tick_count["n"] == 1, "no tick should run after lease loss"
    assert acquire_count["n"] >= 2, "loop must resign and re-attempt acquisition"


@pytest.mark.asyncio
async def test_heartbeat_loop_stops_immediately_on_lease_loss_mid_sleep(monkeypatch):
    """A loss occurring during the inter-tick sleep interrupts that sleep
    immediately instead of waiting out the full cadence."""
    tick_count = {"n": 0}
    lost = asyncio.Event()

    @asynccontextmanager
    async def _fake_acquire(engine, key, *, name="leader"):
        yield (True, lost)

    monkeypatch.setattr(locking_tools, "lease_leadership_with_heartbeat", _fake_acquire)
    shutdown = asyncio.Event()

    async def _on_tick() -> None:
        tick_count["n"] += 1

    async def _lose_shortly_after() -> None:
        await asyncio.sleep(0.03)
        lost.set()

    started = time.monotonic()
    loser = asyncio.create_task(_lose_shortly_after())
    task = asyncio.create_task(
        run_lease_leadership_heartbeat_loop(
            _make_engine(),
            42,
            on_tick=_on_tick,
            name="test",
            cadence_seconds=10.0,  # long -- the loss must cut this sleep short
            is_shutdown=shutdown.is_set,
            shutdown_event=shutdown,
        )
    )
    await asyncio.sleep(0.2)
    shutdown.set()
    await asyncio.wait_for(task, timeout=2.0)
    await loser
    elapsed = time.monotonic() - started

    assert tick_count["n"] == 1, "exactly one tick before the mid-sleep loss"
    assert elapsed < 1.0, (
        f"loop took {elapsed:.2f}s to resign after a mid-sleep loss; "
        f"the 10s cadence sleep should have been interrupted, not waited out"
    )
