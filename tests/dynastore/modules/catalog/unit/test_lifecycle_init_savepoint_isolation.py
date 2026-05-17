"""Regression test for ``LifecycleRegistry._run_initializer_isolated``.

Background: ``init_catalog`` and ``init_collection`` used to compute
the initializer's return value BEFORE handing it to
``_run_initializer_isolated``:

    coro = (
        initializer(conn, schema, catalog_id)
        if inspect.iscoroutinefunction(initializer)
        else initializer(conn, schema, catalog_id)
    )
    await self._run_initializer_isolated(conn, label, coro)

Two defects:

1. The if/else conditional was useless — both branches called
   ``initializer(conn, schema, catalog_id)`` identically (ruff RUF034
   flagged this). For async initializers the call returned a coroutine
   that ``_run_initializer_isolated`` awaited; for sync initializers
   the call executed the function immediately and returned its result.

2. Side effect: **sync initializers ran OUTSIDE the savepoint**. The
   purpose of ``_run_initializer_isolated`` is to wrap each hook in
   its own SAVEPOINT so a failing hook only rolls back its own
   changes. Pre-calling sync hooks at the loop site meant their DB
   writes landed in the outer transaction with no per-hook rollback
   boundary. Dormant today because all 9 currently-registered
   initializers are ``async def``, but a future ``def`` initializer
   would silently lose savepoint isolation.

Fix: ``_run_initializer_isolated`` now takes the un-called callable +
its args; it invokes ``func(*args, **kwargs)`` INSIDE the savepoint
block and awaits the result iff it is awaitable. Callers no longer
pre-call. The ``iscoroutinefunction`` discrimination at the call site
goes away because the callee handles both shapes.

These tests pin the corrected behaviour against a fake AsyncConnection
that records the order of operations relative to ``begin_nested`` /
exit, so a regression that re-introduces the pre-call pattern fails
loudly.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, List

import pytest


class _FakeSavepoint:
    """Minimal stand-in for the object returned by ``conn.begin_nested()``."""

    def __init__(self, events: List[str]):
        self._events = events

    async def __aenter__(self) -> "_FakeSavepoint":
        self._events.append("savepoint:enter")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        if exc is None:
            self._events.append("savepoint:exit-commit")
        else:
            self._events.append(f"savepoint:exit-rollback({type(exc).__name__})")
        return False  # propagate


class _FakeAsyncConnection:
    """Just enough surface to make ``isinstance(conn, AsyncConnection)``
    pass and ``begin_nested()`` return a savepoint-shaped context."""

    def __init__(self) -> None:
        self.events: List[str] = []

    def begin_nested(self) -> _FakeSavepoint:
        return _FakeSavepoint(self.events)


@pytest.fixture
def fake_conn(monkeypatch):
    """Make ``isinstance(conn, AsyncConnection)`` return True for the
    fake. We do this by monkey-patching the
    ``sqlalchemy.ext.asyncio.AsyncConnection`` import to point at our
    fake class for the duration of the test — cheaper than spinning
    a real engine for unit-level coverage."""
    import sqlalchemy.ext.asyncio as sa_asyncio

    monkeypatch.setattr(sa_asyncio, "AsyncConnection", _FakeAsyncConnection)
    return _FakeAsyncConnection()


def _build_manager():
    from dynastore.modules.catalog.lifecycle_manager import LifecycleRegistry

    return LifecycleRegistry()


def test_async_initializer_runs_inside_savepoint(fake_conn):
    """The async initializer is invoked AFTER ``begin_nested`` is
    entered and finishes BEFORE the savepoint exits — i.e. the
    initializer's awaitable is awaited *inside* the savepoint block."""
    mgr = _build_manager()

    async def my_init(conn, schema, catalog_id):
        conn.events.append(f"init-async:{schema}/{catalog_id}")

    ok = asyncio.run(
        mgr._run_initializer_isolated(
            fake_conn, "test-label", my_init, fake_conn, "s", "c"
        )
    )

    assert ok is True
    assert fake_conn.events == [
        "savepoint:enter",
        "init-async:s/c",
        "savepoint:exit-commit",
    ], (
        "async initializer must run inside the savepoint — got events "
        f"{fake_conn.events!r}. Regression of the pre-call pattern "
        "would show 'init-async' BEFORE 'savepoint:enter'."
    )


def test_sync_initializer_runs_inside_savepoint(fake_conn):
    """Same invariant for sync (non-coroutine) initializers — they
    must run *inside* the savepoint so their side effects are
    rolled back if a later hook in the same outer tx fails. This is
    the dormant-defect path that pre-calling at the loop site broke."""
    mgr = _build_manager()

    def sync_init(conn, schema, catalog_id):
        conn.events.append(f"init-sync:{schema}/{catalog_id}")
        return "sync-return"  # not awaitable

    ok = asyncio.run(
        mgr._run_initializer_isolated(
            fake_conn, "test-label", sync_init, fake_conn, "s", "c"
        )
    )

    assert ok is True
    assert fake_conn.events == [
        "savepoint:enter",
        "init-sync:s/c",
        "savepoint:exit-commit",
    ], (
        "sync initializer must also run inside the savepoint — got "
        f"events {fake_conn.events!r}. The previous code pre-called "
        "sync initializers at the loop site, so the event ordering "
        "was 'init-sync' BEFORE 'savepoint:enter'."
    )


def test_failing_async_initializer_savepoint_rolled_back(fake_conn):
    """When the initializer raises, the SAVEPOINT context exits with
    the exception so SQLAlchemy issues ROLLBACK TO SAVEPOINT — and
    ``_run_initializer_isolated`` swallows the exception and returns
    False (it is non-fatal per its contract)."""
    mgr = _build_manager()

    async def failing_init(conn, schema, catalog_id):
        conn.events.append(f"init-async:{schema}/{catalog_id}")
        raise ValueError("boom")

    ok = asyncio.run(
        mgr._run_initializer_isolated(
            fake_conn, "test-label", failing_init, fake_conn, "s", "c"
        )
    )

    assert ok is False
    # Savepoint must have been entered and exited via the exception
    # branch — a ROLLBACK TO SAVEPOINT in production.
    assert fake_conn.events == [
        "savepoint:enter",
        "init-async:s/c",
        "savepoint:exit-rollback(ValueError)",
    ]


def test_failing_sync_initializer_savepoint_rolled_back(fake_conn):
    """The sync-initializer failure path: side effects already
    written by the failing sync initializer get rolled back via the
    savepoint, which only works because we invoke it *inside* the
    savepoint block (the dormant defect this PR fixes)."""
    mgr = _build_manager()

    def failing_sync(conn, schema, catalog_id):
        conn.events.append(f"init-sync:{schema}/{catalog_id}")
        raise RuntimeError("sync boom")

    ok = asyncio.run(
        mgr._run_initializer_isolated(
            fake_conn, "test-label", failing_sync, fake_conn, "s", "c"
        )
    )

    assert ok is False
    assert fake_conn.events == [
        "savepoint:enter",
        "init-sync:s/c",
        "savepoint:exit-rollback(RuntimeError)",
    ]


def test_source_does_not_precall_initializer_in_init_catalog():
    """Source-level pin: ``init_catalog`` and ``init_collection`` must
    NOT pre-call the initializer before passing it to
    ``_run_initializer_isolated``. The regression we are pinning is the
    pattern::

        coro = initializer(conn, schema, catalog_id)
        await self._run_initializer_isolated(conn, label, coro)

    where the initializer is invoked OUTSIDE the savepoint block.
    """
    import inspect

    from dynastore.modules.catalog import lifecycle_manager

    src_init_catalog = inspect.getsource(lifecycle_manager.LifecycleRegistry.init_catalog)
    src_init_collection = inspect.getsource(
        lifecycle_manager.LifecycleRegistry.init_collection
    )
    for src, name in [
        (src_init_catalog, "init_catalog"),
        (src_init_collection, "init_collection"),
    ]:
        assert "initializer(conn" not in src, (
            f"{name} pre-calls the initializer before passing it to "
            "_run_initializer_isolated — that bypasses savepoint "
            "isolation for sync initializers. Pass the un-called "
            "callable + its args instead."
        )
        assert "self._run_initializer_isolated(" in src, (
            f"{name} must use _run_initializer_isolated; passing the "
            "initializer directly bypasses the savepoint wrapper"
        )
