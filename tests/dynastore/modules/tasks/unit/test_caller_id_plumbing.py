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

"""Contract tests for ``caller_id`` plumbing through dispatch + the resolver.

Regression cover for the ``event_drain`` dispatch crash: a task row with a
NULL ``caller_id`` (the ``event_drain`` trigger never stamped one) reached
``RunnerContext(caller_id="")`` and tripped the ``min_length=1`` validator,
failing the drain on every attempt and leaving ``tasks.events`` stuck PENDING.

Locks:

- ``RunnerContext.caller_id`` rejects the empty string (the invariant).
- ``ExecutionEngine.dispatch`` defaults a missing / NULL row ``caller_id`` to
  ``SYSTEM_USER_ID`` instead of ``""`` — so the context always builds.
- A present row ``caller_id`` is carried through unchanged.
- ``current_caller_id()`` reads the request caller snapshot when present and
  falls back to ``SYSTEM_USER_ID`` otherwise.
- The ``event_drain`` enqueue stamps ``caller_id`` on the row (source guard).

These run pure-unit (no DB) by stubbing ``get_runners_for``.
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from sqlalchemy.engine import Engine as _SAEngine

from dynastore.models.auth_models import SYSTEM_USER_ID
from dynastore.modules.tasks.execution import ExecutionEngine
from dynastore.modules.tasks.models import RunnerContext
from dynastore.tools.caller import current_caller_id


def _fake_engine() -> MagicMock:
    return MagicMock(spec=_SAEngine)


def _task_row(**overrides: Any) -> Dict[str, Any]:
    """A minimal claimed-task row as ``dispatch`` receives it."""
    row: Dict[str, Any] = {
        "task_id": "019ecbd7-daa4-7393-a646-b3e43b43ba31",
        "task_type": "event_drain",
        "execution_mode": "ASYNCHRONOUS",
        "inputs": {},
        "schema_name": "platform",
        "timestamp": None,
        "owner_id": None,
    }
    row.update(overrides)
    return row


# --- RunnerContext invariant ------------------------------------------------


def test_runner_context_rejects_empty_caller_id() -> None:
    with pytest.raises(ValidationError):
        RunnerContext(
            engine=_fake_engine(),
            task_type="event_drain",
            caller_id="",
            inputs={},
            extra_context={},
        )


# --- dispatch boundary ------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_defaults_missing_caller_id_to_system() -> None:
    """A row with NO ``caller_id`` must not crash RunnerContext construction.

    Reproduces the ``event_drain`` failure: the trigger inserts the row
    without a ``caller_id`` column, so ``row.get("caller_id")`` is None.
    """
    engine = ExecutionEngine()
    captured: Dict[str, Any] = {}

    class _StubRunner:
        async def run(self, ctx: RunnerContext) -> Any:
            captured["caller_id"] = ctx.caller_id
            return "ok"

    with patch.object(engine, "get_runners_for", return_value=[_StubRunner()]):
        result = await engine.dispatch(_task_row(), engine=_fake_engine())

    assert result == "ok"
    assert captured["caller_id"] == SYSTEM_USER_ID


@pytest.mark.asyncio
async def test_dispatch_preserves_present_caller_id() -> None:
    engine = ExecutionEngine()
    captured: Dict[str, Any] = {}

    class _StubRunner:
        async def run(self, ctx: RunnerContext) -> Any:
            captured["caller_id"] = ctx.caller_id
            return "ok"

    with patch.object(engine, "get_runners_for", return_value=[_StubRunner()]):
        await engine.dispatch(
            _task_row(caller_id="keycloak:alice"), engine=_fake_engine()
        )

    assert captured["caller_id"] == "keycloak:alice"


# --- resolver ---------------------------------------------------------------


def test_current_caller_id_defaults_to_system_without_snapshot() -> None:
    assert current_caller_id() == SYSTEM_USER_ID


def test_current_caller_id_reads_principal_snapshot() -> None:
    from dynastore.models.auth import Principal
    from dynastore.models.protocols.visibility import (
        RequestVisibility,
        reset_request_visibility,
        set_request_visibility,
    )

    principal = Principal(provider="keycloak", subject_id="alice")
    token = set_request_visibility(
        RequestVisibility(principals=("keycloak:alice",), principal=principal)
    )
    try:
        assert current_caller_id() == "keycloak:alice"
    finally:
        reset_request_visibility(token)


# --- source guard: event_drain enqueue stamps caller_id ---------------------


def test_event_drain_enqueue_stamps_caller_id() -> None:
    """The ``event_drain`` INSERT must carry a ``caller_id`` so the row is
    never NULL — otherwise dispatch falls back per-row and the originating
    principal is lost from the task record."""
    import inspect

    from dynastore.modules.events import events_emit

    source = inspect.getsource(events_emit._enqueue_event_drain_trigger)
    assert "caller_id" in source, (
        "event_drain enqueue must stamp caller_id on the tasks row"
    )
    assert "current_caller_id" in source, (
        "event_drain enqueue should resolve caller_id via current_caller_id()"
    )
