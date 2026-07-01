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

"""Regression test for #2616: a lock-timeout on the TasksModule per-schema
startup-init advisory lock must not abort process startup.

Previously ``acquire_startup_lock`` raising ``LockNotAvailableError`` (PG
55P03, "canceling statement due to lock timeout") propagated straight out of
``TasksModule.lifespan``. Since ``TasksModule`` is a foundational module, that
exception was fatal (``CRITICAL: Foundational module 'TasksModule' failed
during startup. Aborting.``), which crash-looped the worker under sustained
lock contention (e.g. a peer pod stalled on a starved connection pool, #2333).

``_run_startup_ddl_tolerating_lock_timeout`` now catches a lock-timeout
specifically and falls back to running the same idempotent DDL on a fresh,
unlocked transaction instead of raising. Any other exception must still
propagate unchanged.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import pytest

from dynastore.modules.tasks import tasks_module


class LockNotAvailableError(Exception):
    """Stand-in for asyncpg's real exception class of the same name.

    ``_is_lock_not_available_error`` (used by
    ``is_lock_not_available_error``) matches on this exact class name, so
    reusing it here exercises the same detection path production hits.
    """

    pgcode = "55P03"


def _make_raising_acquire_startup_lock(exc: BaseException):
    """Build a fake ``acquire_startup_lock`` that raises *exc* before yielding
    -- matching real behaviour when the advisory-lock wait fails.
    """

    @asynccontextmanager
    async def _fake(conn: Any, lock_key: str, timeout: str | None = None):
        raise exc
        yield None  # pragma: no cover - unreachable; keeps this a generator

    return _fake


@pytest.mark.asyncio
async def test_lock_timeout_falls_back_to_unlocked_ddl_instead_of_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lock-timeout must not propagate: the DDL runs once, unlocked."""
    monkeypatch.setattr(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _make_raising_acquire_startup_lock(
            LockNotAvailableError("canceling statement due to lock timeout")
        ),
    )

    fake_conn = object()

    @asynccontextmanager
    async def _fake_managed_transaction(engine: Any):
        yield fake_conn

    monkeypatch.setattr(
        tasks_module, "managed_transaction", _fake_managed_transaction
    )

    seen_conns = []

    async def _ddl_body(conn: Any) -> None:
        seen_conns.append(conn)

    # Must complete without raising despite the simulated lock timeout.
    await tasks_module._run_startup_ddl_tolerating_lock_timeout(
        engine=object(),
        lock_key="tasks_storage_init.tasks",
        ddl_body=_ddl_body,
    )

    # The idempotent DDL still ran exactly once, on the unlocked fallback
    # connection.
    assert seen_conns == [fake_conn]


@pytest.mark.asyncio
async def test_non_lock_timeout_error_still_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuine (non-lock-timeout) failure must still abort -- this helper
    only tolerates the specific, known-safe lock-contention race.
    """
    monkeypatch.setattr(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _make_raising_acquire_startup_lock(ConnectionError("db is unreachable")),
    )

    async def _ddl_body(conn: Any) -> None:
        raise AssertionError("ddl_body must not run when the error isn't a lock timeout")

    with pytest.raises(ConnectionError, match="db is unreachable"):
        await tasks_module._run_startup_ddl_tolerating_lock_timeout(
            engine=object(),
            lock_key="tasks_storage_init.tasks",
            ddl_body=_ddl_body,
        )


@pytest.mark.asyncio
async def test_happy_path_runs_ddl_under_the_lock_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the lock is acquired normally, the DDL runs once on the locked
    connection and ``managed_transaction`` (the unlocked fallback) is never
    touched.
    """
    locked_conn = object()

    @asynccontextmanager
    async def _fake_acquire(conn: Any, lock_key: str, timeout: str | None = None):
        yield locked_conn

    monkeypatch.setattr(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock",
        _fake_acquire,
    )

    async def _unexpected_managed_transaction(engine: Any):
        raise AssertionError("unlocked fallback must not run on the happy path")

    monkeypatch.setattr(
        tasks_module, "managed_transaction", _unexpected_managed_transaction
    )

    seen_conns = []

    async def _ddl_body(conn: Any) -> None:
        seen_conns.append(conn)

    await tasks_module._run_startup_ddl_tolerating_lock_timeout(
        engine=object(),
        lock_key="tasks_storage_init.tasks",
        ddl_body=_ddl_body,
    )

    assert seen_conns == [locked_conn]
