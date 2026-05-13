"""Pool-hygiene guard for ``managed_transaction(engine)``.

A pooled connection may be handed back with a pending rollback left over from a
prior caller that exited without cleanly closing its transaction. Before
opening a new transaction the engine branch of ``managed_transaction`` issues a
best-effort ``rollback()`` to reset the checkout, and on
``PendingRollbackError``/``InvalidRequestError`` invalidates the connection
and retries with a fresh one from the pool.

This module also covers the bounded ``retry_on_transient_connect`` wrapper
applied to engine connection acquisition: a brief ``OperationalError`` /
``asyncio.TimeoutError`` / ``OSError`` during pool checkout triggers
exponential-backoff retry instead of crashing the caller. Body errors raised
inside ``async with managed_transaction(...)`` are NOT retried because DML/DQL
idempotency is the caller's responsibility.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, List, Optional, Sequence

import pytest
from sqlalchemy.exc import OperationalError, PendingRollbackError


class _FakeTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self, *, poisoned: bool, log: List[str]):
        self._poisoned = poisoned
        self._log = log
        self.invalidated_count = 0
        self.closed = False
        self.rollback_count = 0
        self.begin_count = 0
        self.sync_connection = SimpleNamespace()

    async def rollback(self):
        self.rollback_count += 1
        self._log.append("rollback")
        if self._poisoned:
            self._poisoned = False
            raise PendingRollbackError(
                "pending rollback on pooled connection", None, None
            )

    async def invalidate(self):
        self.invalidated_count += 1
        self._log.append("invalidate")

    async def close(self):
        self.closed = True
        self._log.append("close")

    def begin(self):
        self.begin_count += 1
        self._log.append("begin")
        return _FakeTx()

    def in_transaction(self) -> bool:
        # Always report no in-flight tx — the executor-workflow tests don't
        # exercise the post-execute paranoid-rollback path.
        return False


class _FakeEngine:
    def __init__(
        self,
        conns: List[_FakeConn],
        *,
        connect_errors: Optional[Sequence[Any]] = None,
    ):
        """connect_errors[i] is raised on the i-th ``connect()`` call instead of
        returning the next conn. ``None`` entries pass through to the conn list."""
        self._conns = list(conns)
        self._connect_errors = list(connect_errors or [])
        self.connect_calls = 0

    async def connect(self):
        idx = self.connect_calls
        self.connect_calls += 1
        if idx < len(self._connect_errors):
            err = self._connect_errors[idx]
            if err is not None:
                if isinstance(err, type):
                    raise err("simulated transient connect failure")
                raise err
        return self._conns.pop(0)


@pytest.mark.asyncio
async def test_pool_hygiene_invalidates_and_retries_on_pending_rollback(
    monkeypatch,
):
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    poisoned = _FakeConn(poisoned=True, log=log)
    clean = _FakeConn(poisoned=False, log=log)
    engine = _FakeEngine([poisoned, clean])

    monkeypatch.setattr(qe, "AsyncEngine", _FakeEngine, raising=True)
    monkeypatch.setattr(qe, "is_async_resource", lambda r: True, raising=True)

    async with qe.managed_transaction(engine) as conn:
        assert conn is clean

    assert engine.connect_calls == 2
    assert poisoned.invalidated_count == 1
    assert poisoned.closed is True
    assert clean.rollback_count == 1
    assert clean.begin_count == 1
    assert clean.closed is True
    assert log == [
        "rollback",
        "invalidate",
        "close",
        "rollback",
        "begin",
        "close",
    ]


@pytest.mark.asyncio
async def test_pool_hygiene_is_cheap_no_op_on_clean_connection(monkeypatch):
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    clean = _FakeConn(poisoned=False, log=log)
    engine = _FakeEngine([clean])

    monkeypatch.setattr(qe, "AsyncEngine", _FakeEngine, raising=True)
    monkeypatch.setattr(qe, "is_async_resource", lambda r: True, raising=True)

    async with qe.managed_transaction(engine) as conn:
        assert conn is clean

    assert engine.connect_calls == 1
    assert clean.rollback_count == 1
    assert clean.invalidated_count == 0
    assert clean.begin_count == 1
    assert clean.closed is True


# ---------------------------------------------------------------------------
# retry_on_transient_connect — applied via _acquire_async_engine_connection
# ---------------------------------------------------------------------------


def _patch_async_engine(monkeypatch):
    """Make qe.AsyncEngine accept _FakeEngine and treat it as async."""
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)
    monkeypatch.setattr(qe, "AsyncEngine", _FakeEngine, raising=True)
    monkeypatch.setattr(qe, "is_async_resource", lambda r: True, raising=True)
    return qe


def _silence_sleep(monkeypatch, qe):
    """Replace asyncio.sleep inside query_executor with a no-op so the
    decorator's exponential backoff doesn't slow tests."""
    async def _fast_sleep(_seconds):
        return None

    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)


def _make_op_error() -> OperationalError:
    return OperationalError("SELECT 1", {}, Exception("connection refused"))


@pytest.mark.xfail(reason="#514 — observation method on the retry path was renamed; assertion stale.", strict=False)
@pytest.mark.asyncio
async def test_connect_retries_on_operational_error_then_succeeds(monkeypatch, caplog):
    qe = _patch_async_engine(monkeypatch)
    _silence_sleep(monkeypatch, qe)

    log: List[str] = []
    clean = _FakeConn(poisoned=False, log=log)
    engine = _FakeEngine([clean], connect_errors=[_make_op_error()])

    with caplog.at_level("WARNING"):
        async with qe.managed_transaction(engine) as conn:
            assert conn is clean

    assert engine.connect_calls == 2  # one fail, one success
    assert clean.rollback_count == 1
    assert clean.begin_count == 1
    assert clean.closed is True
    assert any("retry_on_transient_connect" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_connect_exhausts_all_retries_and_raises(monkeypatch):
    qe = _patch_async_engine(monkeypatch)
    _silence_sleep(monkeypatch, qe)

    # All 5 attempts fail with OperationalError (the default retry budget)
    engine = _FakeEngine([], connect_errors=[_make_op_error()] * 5)

    with pytest.raises(OperationalError):
        async with qe.managed_transaction(engine):
            pass  # body should never run

    assert engine.connect_calls == 5


@pytest.mark.asyncio
async def test_body_error_is_not_retried(monkeypatch):
    """Connect succeeds; the user body raises OperationalError. Retry must NOT
    fire, because retry_on_transient_connect wraps acquisition only — DML/DQL
    inside the body are the caller's responsibility (idempotency, ON CONFLICT)."""
    qe = _patch_async_engine(monkeypatch)
    _silence_sleep(monkeypatch, qe)

    log: List[str] = []
    clean = _FakeConn(poisoned=False, log=log)
    engine = _FakeEngine([clean])

    with pytest.raises(OperationalError):
        async with qe.managed_transaction(engine):
            raise _make_op_error()

    assert engine.connect_calls == 1  # exactly one acquisition, no retry
    assert clean.closed is True


@pytest.mark.asyncio
async def test_non_retryable_from_connect_propagates(monkeypatch):
    """A non-transient exception class (e.g. RuntimeError from a programmer
    bug in the engine) must propagate immediately without retry."""
    qe = _patch_async_engine(monkeypatch)
    _silence_sleep(monkeypatch, qe)

    engine = _FakeEngine([], connect_errors=[RuntimeError])

    with pytest.raises(RuntimeError):
        async with qe.managed_transaction(engine):
            pass

    assert engine.connect_calls == 1


@pytest.mark.asyncio
async def test_cancellation_drains_rollback_before_pool_release(monkeypatch):
    """#628: when the user body is cancelled mid-query, ``managed_transaction``
    must drive the transaction's ``__aexit__`` (which issues ROLLBACK) to
    completion before letting the connection go back to the pool. Without
    this drain the asyncpg wire returns in state 15 (IN_QUERY) and the
    next acquire trips PR #619's invalidate-recover path.

    Asserts the order: yield → CancelledError → rollback emitted via
    ``__aexit__`` → close.
    """
    import asyncio as _asyncio

    qe = _patch_async_engine(monkeypatch)
    _silence_sleep(monkeypatch, qe)

    log: List[str] = []

    class _TrackingTx:
        def __init__(self, sink: List[str]):
            self._sink = sink

        async def __aenter__(self):
            self._sink.append("begin")
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self._sink.append(f"exit:{exc_type.__name__ if exc_type else None}")
            return False

    class _CancelConn(_FakeConn):
        def begin(self):  # type: ignore[override]
            self.begin_count += 1
            return _TrackingTx(self._log)

    conn = _CancelConn(poisoned=False, log=log)
    engine = _FakeEngine([conn])

    async def _body():
        async with qe.managed_transaction(engine):
            raise _asyncio.CancelledError()

    with pytest.raises(_asyncio.CancelledError):
        await _body()

    assert "begin" in log
    exits = [e for e in log if e.startswith("exit:")]
    assert exits and exits[-1] == "exit:CancelledError", log
    assert log.index("exit:CancelledError") < log.index("close"), log
    assert conn.closed is True


@pytest.mark.xfail(reason="#514 — same as above; retry observation point changed.", strict=False)
@pytest.mark.asyncio
async def test_executor_workflow_uses_connect_retry(monkeypatch, caplog):
    """``BaseExecutor._execute_async_workflow`` (the engine-passed entry point
    for DQLQuery / DDLQuery) must also route its connect through
    _acquire_async_engine_connection so platform-wide retry applies uniformly."""
    qe = _patch_async_engine(monkeypatch)
    _silence_sleep(monkeypatch, qe)

    log: List[str] = []
    clean = _FakeConn(poisoned=False, log=log)
    engine = _FakeEngine([clean], connect_errors=[_make_op_error()])

    captured = {}

    async def _fake_build_and_execute_async(self_, conn, raw_params):
        captured["conn"] = conn
        return "result"

    monkeypatch.setattr(
        qe.BaseExecutor,
        "_build_and_execute_async",
        _fake_build_and_execute_async,
        raising=True,
    )

    class _NoopLockScope:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(
        qe, "_connection_lock_scope", lambda c: _NoopLockScope(), raising=True
    )

    # Build a minimal BaseExecutor subclass to exercise the engine-branch
    class _StubExecutor(qe.BaseExecutor):
        def _execute_sync(self, conn, query_obj, params):
            raise NotImplementedError

        async def _execute_async(self, conn, query_obj, params):
            raise NotImplementedError

    # Instantiate without going through from_template (avoids DDL-inference paths)
    executor = _StubExecutor.__new__(_StubExecutor)
    executor.query_builder_strategy = None  # not used by our patched method
    executor.result_handler = None
    executor.post_processors = []

    with caplog.at_level("WARNING"):
        out = await executor._execute_async_workflow(engine, {})

    assert out == "result"
    assert captured["conn"] is clean
    assert engine.connect_calls == 2
    assert any("retry_on_transient_connect" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# asyncpg state-15 / wire-closed on pool-hygiene rollback
# ---------------------------------------------------------------------------
#
# A pooled wire whose asyncpg protocol FSM is non-IDLE (state 15 = mid-op)
# raises ``InternalClientError`` from the hygiene rollback in
# ``_acquire_async_engine_connection``. asyncpg client errors are not
# subclasses of SQLAlchemy ``InterfaceError``, so without explicit listing in
# ``_TRANSIENT_CONNECT_EXCEPTIONS`` the error fell through
# ``retry_on_transient_connect`` and the poisoned slot cascaded to dispatcher
# + warner as ``ConnectionDoesNotExistError`` on the next checkout.
#
# Contract: both asyncpg classes are listed in ``_TRANSIENT_CONNECT_EXCEPTIONS``
# and the outer ``except BaseException`` of ``_acquire_async_engine_connection``
# invalidates before close.


class _StateMachineFakeConn:
    """Minimal AsyncConnection stand-in whose ``rollback()`` can raise any
    pre-seeded exception sequence. Used to drive the state-15 path."""

    def __init__(self, *, rollback_errors: Sequence[Optional[BaseException]], log: List[str], tag: str):
        self._rollback_errors = list(rollback_errors)
        self._log = log
        self.tag = tag
        self.invalidated_count = 0
        self.closed = False
        self.rollback_count = 0
        self.begin_count = 0
        self.sync_connection = SimpleNamespace()

    async def rollback(self):
        self.rollback_count += 1
        self._log.append(f"rollback:{self.tag}")
        if self._rollback_errors:
            exc = self._rollback_errors.pop(0)
            if exc is not None:
                raise exc

    async def invalidate(self):
        self.invalidated_count += 1
        self._log.append(f"invalidate:{self.tag}")

    async def close(self):
        self.closed = True
        self._log.append(f"close:{self.tag}")

    def begin(self):
        self.begin_count += 1
        self._log.append(f"begin:{self.tag}")
        return _FakeTx()

    def in_transaction(self) -> bool:
        return False


@pytest.mark.asyncio
async def test_state15_on_hygiene_rollback_invalidates_and_retries(monkeypatch):
    """asyncpg ``InternalClientError("cannot switch to state 15 …")`` raised
    from the pool-checkout rollback must invalidate the poisoned wire and
    retry with a fresh one — not propagate and poison the pool."""
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)
    monkeypatch.setattr(qe, "AsyncEngine", _FakeEngine, raising=True)
    monkeypatch.setattr(qe, "is_async_resource", lambda r: True, raising=True)
    # Decorator backoff would slow the test; collapse it.
    async def _fast_sleep(_seconds):
        return None
    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)

    log: List[str] = []
    state15 = qe.AsyncpgInternalClientError(
        "cannot switch to state 15; another operation (2) is in progress"
    )
    poisoned = _StateMachineFakeConn(rollback_errors=[state15], log=log, tag="poisoned")
    clean = _StateMachineFakeConn(rollback_errors=[None], log=log, tag="clean")
    engine = _FakeEngine([poisoned, clean])

    async with qe.managed_transaction(engine) as conn:
        assert conn is clean

    # Decorator retried once; the poisoned wire was invalidated AND closed
    # before the second acquire.
    assert engine.connect_calls == 2, log
    assert poisoned.invalidated_count == 1, log
    assert poisoned.closed is True, log
    assert clean.invalidated_count == 0, log
    assert clean.rollback_count == 1, log
    assert clean.begin_count == 1, log
    # The poisoned wire must be invalidated BEFORE close — otherwise the pool
    # bookkeeping returns the wire in non-IDLE state to the next consumer.
    poisoned_invalidate_idx = log.index("invalidate:poisoned")
    poisoned_close_idx = log.index("close:poisoned")
    assert poisoned_invalidate_idx < poisoned_close_idx, log


@pytest.mark.asyncio
async def test_connection_does_not_exist_on_hygiene_rollback_retries(monkeypatch):
    """asyncpg ``ConnectionDoesNotExistError`` (wire closed mid-op) gets the
    same invalidate + retry treatment as state-15."""
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)
    monkeypatch.setattr(qe, "AsyncEngine", _FakeEngine, raising=True)
    monkeypatch.setattr(qe, "is_async_resource", lambda r: True, raising=True)
    async def _fast_sleep(_seconds):
        return None
    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)

    log: List[str] = []
    closed_mid_op = qe.AsyncpgConnectionDoesNotExistError(
        "connection was closed in the middle of operation"
    )
    poisoned = _StateMachineFakeConn(rollback_errors=[closed_mid_op], log=log, tag="poisoned")
    clean = _StateMachineFakeConn(rollback_errors=[None], log=log, tag="clean")
    engine = _FakeEngine([poisoned, clean])

    async with qe.managed_transaction(engine) as conn:
        assert conn is clean

    assert engine.connect_calls == 2, log
    assert poisoned.invalidated_count == 1, log
    assert poisoned.closed is True, log


@pytest.mark.asyncio
async def test_state15_exhausts_retry_budget_and_raises(monkeypatch):
    """If every pool slot is poisoned, the retry budget exhausts and the
    InternalClientError propagates as WARNING (callers handle it via the
    transient back-off in dispatcher/warner)."""
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)
    monkeypatch.setattr(qe, "AsyncEngine", _FakeEngine, raising=True)
    monkeypatch.setattr(qe, "is_async_resource", lambda r: True, raising=True)
    async def _fast_sleep(_seconds):
        return None
    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)

    log: List[str] = []
    poisoned_conns = [
        _StateMachineFakeConn(
            rollback_errors=[qe.AsyncpgInternalClientError("state 15")],
            log=log,
            tag=f"poisoned{i}",
        )
        for i in range(5)
    ]
    engine = _FakeEngine(poisoned_conns)

    with pytest.raises(qe.AsyncpgInternalClientError):
        async with qe.managed_transaction(engine):
            pass

    # All 5 retry slots exercised; each one invalidated.
    assert engine.connect_calls == 5
    for c in poisoned_conns:
        assert c.invalidated_count == 1
        assert c.closed is True
