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

"""Tests for ``best_effort_savepoint`` (#2701 consolidation).

Five call sites (``events_emit``, ``storage_emit``, ``log_manager``,
``postgres_policy_storage``, ``lifecycle_manager``) hand-rolled the same
"wrap in a SAVEPOINT if the connection supports one, classify one expected
error, otherwise run unguarded" pattern. These tests pin the consolidated
helper's contract directly, independent of any of the five callers.
"""

from __future__ import annotations

from typing import List

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
        return False  # propagate — SAVEPOINT rollback doesn't swallow


class _FakeConnWithSavepoint:
    """Fake connection exposing ``begin_nested()``."""

    def __init__(self) -> None:
        self.events: List[str] = []

    def begin_nested(self) -> _FakeSavepoint:
        return _FakeSavepoint(self.events)


class _FakeConnNoSavepoint:
    """Fake connection with no ``begin_nested`` — the defensive fallback path."""

    def __init__(self) -> None:
        self.events: List[str] = []


@pytest.mark.asyncio
async def test_happy_path_runs_inside_savepoint_and_commits():
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeConnWithSavepoint()

    async with best_effort_savepoint(conn) as outcome:
        conn.events.append("body")

    assert outcome.error is None
    assert conn.events == [
        "savepoint:enter",
        "body",
        "savepoint:exit-commit",
    ]


@pytest.mark.asyncio
async def test_default_tolerate_swallows_any_exception_and_rolls_back():
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeConnWithSavepoint()

    async with best_effort_savepoint(conn) as outcome:
        conn.events.append("body")
        raise ValueError("boom")

    assert isinstance(outcome.error, ValueError)
    assert conn.events == [
        "savepoint:enter",
        "body",
        "savepoint:exit-rollback(ValueError)",
    ]


@pytest.mark.asyncio
async def test_untolerated_exception_reraises_after_savepoint_rollback():
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeConnWithSavepoint()

    with pytest.raises(RuntimeError, match="fatal"):
        async with best_effort_savepoint(conn, tolerate=lambda exc: False):
            conn.events.append("body")
            raise RuntimeError("fatal")

    # SAVEPOINT is still rolled back cleanly before the re-raise.
    assert conn.events == [
        "savepoint:enter",
        "body",
        "savepoint:exit-rollback(RuntimeError)",
    ]


@pytest.mark.asyncio
async def test_tolerate_predicate_classifies_by_exception_type():
    """A ``tolerate`` predicate that only accepts one exception type re-raises
    everything else — the ``postgres_policy_storage`` duplicate-table shape."""
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    def _tolerate_value_error(exc: BaseException) -> bool:
        return isinstance(exc, ValueError)

    conn = _FakeConnWithSavepoint()

    async with best_effort_savepoint(conn, tolerate=_tolerate_value_error) as outcome:
        raise ValueError("tolerated")
    assert isinstance(outcome.error, ValueError)

    conn2 = _FakeConnWithSavepoint()
    with pytest.raises(RuntimeError, match="not tolerated"):
        async with best_effort_savepoint(conn2, tolerate=_tolerate_value_error):
            raise RuntimeError("not tolerated")


@pytest.mark.asyncio
async def test_no_begin_nested_runs_body_unguarded_but_still_classifies():
    """Connections without ``begin_nested`` (defensive fallback for
    engine-level resources) still run the body and still apply ``tolerate`` —
    the only thing lost is SAVEPOINT isolation, not the classification."""
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeConnNoSavepoint()

    async with best_effort_savepoint(conn) as outcome:
        conn.events.append("body")
        raise ValueError("boom")

    assert isinstance(outcome.error, ValueError)
    assert conn.events == ["body"]  # no savepoint:enter/exit — none available


@pytest.mark.asyncio
async def test_no_begin_nested_untolerated_exception_reraises():
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeConnNoSavepoint()

    with pytest.raises(RuntimeError, match="fatal"):
        async with best_effort_savepoint(conn, tolerate=lambda exc: False):
            raise RuntimeError("fatal")


@pytest.mark.asyncio
async def test_parent_transaction_still_usable_after_tolerated_failure():
    """The load-bearing invariant: after a tolerated inner failure, the
    caller's own connection is still usable for further statements — the
    SAVEPOINT rollback isolated the failure from the outer transaction."""
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeConnWithSavepoint()

    async with best_effort_savepoint(conn) as outcome:
        raise ValueError("first attempt fails")
    assert outcome.error is not None

    # Same connection, a second best_effort_savepoint block succeeds cleanly —
    # proof the first failure didn't poison anything.
    async with best_effort_savepoint(conn) as outcome2:
        conn.events.append("second-attempt")
    assert outcome2.error is None
    assert conn.events == [
        "savepoint:enter",
        "savepoint:exit-rollback(ValueError)",
        "savepoint:enter",
        "second-attempt",
        "savepoint:exit-commit",
    ]
