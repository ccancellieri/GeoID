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

"""Unit tests for the shared startup-DDL lock-timeout tolerance helper
(``locking_tools.run_startup_ddl_tolerating_lock_timeout``).

A lock-timeout on a foundational module's startup advisory lock (PG 55P03,
"canceling statement due to lock timeout") must not abort process startup:
the guarded DDL is idempotent, so it's safe to re-run unlocked instead of
crash-looping the worker. Any other exception must still propagate.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import pytest

from dynastore.modules.db_config import locking_tools


class LockNotAvailableError(Exception):
    """Stand-in for asyncpg's real exception class of the same name.

    ``is_lock_not_available_error`` matches on this exact class name, so
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
        locking_tools,
        "acquire_startup_lock",
        _make_raising_acquire_startup_lock(
            LockNotAvailableError("canceling statement due to lock timeout")
        ),
    )

    fake_conn = object()

    @asynccontextmanager
    async def _fake_managed_transaction(engine: Any):
        yield fake_conn

    monkeypatch.setattr(
        locking_tools, "managed_transaction", _fake_managed_transaction
    )

    seen_conns = []

    async def _ddl_body(conn: Any) -> None:
        seen_conns.append(conn)

    # Must complete without raising despite the simulated lock timeout.
    await locking_tools.run_startup_ddl_tolerating_lock_timeout(
        engine=object(),
        lock_key="some_module_storage_init",
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
        locking_tools,
        "acquire_startup_lock",
        _make_raising_acquire_startup_lock(ConnectionError("db is unreachable")),
    )

    async def _ddl_body(conn: Any) -> None:
        raise AssertionError("ddl_body must not run when the error isn't a lock timeout")

    with pytest.raises(ConnectionError, match="db is unreachable"):
        await locking_tools.run_startup_ddl_tolerating_lock_timeout(
            engine=object(),
            lock_key="some_module_storage_init",
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

    monkeypatch.setattr(locking_tools, "acquire_startup_lock", _fake_acquire)

    async def _unexpected_managed_transaction(engine: Any):
        raise AssertionError("unlocked fallback must not run on the happy path")

    monkeypatch.setattr(
        locking_tools, "managed_transaction", _unexpected_managed_transaction
    )

    seen_conns = []

    async def _ddl_body(conn: Any) -> None:
        seen_conns.append(conn)

    await locking_tools.run_startup_ddl_tolerating_lock_timeout(
        engine=object(),
        lock_key="some_module_storage_init",
        ddl_body=_ddl_body,
    )

    assert seen_conns == [locked_conn]
