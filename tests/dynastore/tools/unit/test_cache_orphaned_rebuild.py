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

"""A rebuild that outlives every waiter must not log a torn-down connection
as an actionable failure (#3023).

``_await_shared_rebuild`` runs cache rebuilds as detached tasks shared by
every concurrent miss on a key, and always lets them run to completion
(#2900 -- cancelling in-flight DB work mid-query poisons the pool). If every
waiter gives up before the rebuild finishes and its connection is then torn
down, the resulting ``InterfaceError``/``DatabaseConnectionError`` is
expected teardown noise, not a real failure -- it should log at DEBUG
instead of the WARNING used for genuine rebuild failures.
"""
from __future__ import annotations

import asyncio
import logging

import pytest
from sqlalchemy.exc import InterfaceError

from dynastore.modules.db_config.exceptions import DatabaseConnectionError
from dynastore.tools.cache import (
    _await_shared_rebuild,
    _inflight_rebuilds,
    _is_orphaned_teardown_error,
)


def test_is_orphaned_teardown_error_matches_closed_connection_errors():
    """Both the SQLAlchemy and project connection-closed error shapes match."""
    sa_exc = InterfaceError(
        "stmt", {}, Exception(
            "cannot call Transaction.rollback(): the underlying connection is closed"
        )
    )
    assert _is_orphaned_teardown_error(sa_exc)

    db_exc = DatabaseConnectionError("Cannot start transaction: Connection X is closed.")
    assert _is_orphaned_teardown_error(db_exc)


def test_is_orphaned_teardown_error_rejects_unrelated_errors():
    """A genuine rebuild failure (not a teardown artifact) must not be masked."""
    assert not _is_orphaned_teardown_error(ValueError("bad input"))
    assert not _is_orphaned_teardown_error(
        DatabaseConnectionError("could not connect to server")
    )


@pytest.mark.asyncio
async def test_orphaned_rebuild_logs_debug_not_warning(caplog):
    """A rebuild whose connection tears down after every waiter gave up logs
    at DEBUG (traceable, not actionable) rather than the WARNING used for
    genuine rebuild failures."""
    key = "test-orphaned-rebuild-key"
    _inflight_rebuilds.pop(key, None)

    async def _rebuild_then_die_on_closed_connection():
        raise InterfaceError(
            "stmt", {}, Exception(
                "cannot call Transaction.rollback(): the underlying connection is closed"
            )
        )

    with caplog.at_level(logging.DEBUG, logger="dynastore.tools.cache"):
        with pytest.raises(InterfaceError):
            await _await_shared_rebuild(
                key, _rebuild_then_die_on_closed_connection, timeout=5.0
            )
        # Let the done-callback (scheduled via add_done_callback) run.
        await asyncio.sleep(0)

    messages = [r.getMessage() for r in caplog.records]
    assert any("cache_rebuild_orphaned" in m for m in messages), messages
    assert not any("cache_rebuild_failed" in m for m in messages), messages
    orphaned_record = next(
        r for r in caplog.records if "cache_rebuild_orphaned" in r.getMessage()
    )
    assert orphaned_record.levelno == logging.DEBUG


@pytest.mark.asyncio
async def test_genuine_rebuild_failure_still_logs_warning(caplog):
    """A rebuild failure unrelated to connection teardown keeps its WARNING."""
    key = "test-genuine-rebuild-failure-key"
    _inflight_rebuilds.pop(key, None)

    async def _rebuild_that_fails():
        raise ValueError("something else broke")

    with caplog.at_level(logging.DEBUG, logger="dynastore.tools.cache"):
        with pytest.raises(ValueError):
            await _await_shared_rebuild(key, _rebuild_that_fails, timeout=5.0)
        await asyncio.sleep(0)

    messages = [r.getMessage() for r in caplog.records]
    assert any("cache_rebuild_failed" in m for m in messages), messages
    failed_record = next(
        r for r in caplog.records if "cache_rebuild_failed" in r.getMessage()
    )
    assert failed_record.levelno == logging.WARNING
