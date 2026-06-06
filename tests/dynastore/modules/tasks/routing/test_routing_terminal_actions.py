"""Terminal-outcome routing: on_success / on_failure / on_timeout Actions.

Covers the U3 engine in ``execution.py``:

- ``RunnerTarget.on_timeout`` defaults to DEAD_LETTER and is a distinct field.
- ``resolve_routing_terminal`` returns the selected target's Action triple and
  the sync timeout, failing open to model defaults on empty/raising resolvers.
- ``apply_terminal_action`` enqueues a follow-on ONLY for ROUTE, gates
  failure/timeout ROUTE on ground-truth terminal status, and enforces the
  ``_route_depth`` cycle guard.
- ``hydrate_task_payload`` strips the reserved ``_route_depth`` breadcrumb so
  task input models never see it.
- ``dead_letter_task`` writes status='DEAD_LETTER'.

All DB-free: the resolver, runner registry, status read and ``create_task`` are
mocked.  Run with the isolated recipe (``--noconftest``) — the global conftest
wipes the live gis_dev database.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks import execution
from dynastore.modules.tasks.routing.model import Action, ActionVerb, RunnerTarget


# --- model: on_timeout default ------------------------------------------------

def test_runner_target_on_timeout_defaults_to_dead_letter():
    t = RunnerTarget(runner="background")
    assert t.on_success.action == ActionVerb.REPORT
    assert t.on_failure.action == ActionVerb.DEAD_LETTER
    assert t.on_timeout.action == ActionVerb.DEAD_LETTER


def test_runner_target_on_timeout_accepts_route():
    t = RunnerTarget(
        runner="gcp_cloud_run",
        on_timeout=Action(action=ActionVerb.ROUTE, process="dwh_join_heavy"),
    )
    assert t.on_timeout.action == ActionVerb.ROUTE
    assert t.on_timeout.process == "dwh_join_heavy"


# --- resolve_routing_terminal -------------------------------------------------

def _patch_resolver(monkeypatch, *, targets, selected):
    import dynastore.modules.tasks.routing.resolver as routing_resolver
    import dynastore.modules.tasks.runners as runners_mod
    from dynastore.modules.tasks.models import TaskExecutionMode

    async def _resolved(_key):
        return targets

    monkeypatch.setattr(routing_resolver, "resolved_targets", _resolved)
    monkeypatch.setattr(
        routing_resolver, "select_target",
        lambda *a, **k: selected,
    )
    monkeypatch.setattr(
        runners_mod, "get_runners",
        lambda mode: [] if mode == TaskExecutionMode.ASYNCHRONOUS else [],
    )
    monkeypatch.setattr(
        "dynastore.modules.tasks.dispatcher._SERVICE_NAME", "catalog",
    )
    # Avoid loading TasksPluginConfig in unit context.
    monkeypatch.setattr(
        execution, "_default_task_timeout", AsyncMock(return_value=3600.0),
    )


@pytest.mark.asyncio
async def test_resolve_returns_selected_target_actions(monkeypatch):
    target = RunnerTarget(
        runner="background",
        consumers=["catalog"],
        on_success=Action(action=ActionVerb.ROUTE, process="dwh_join"),
        on_failure=Action(action=ActionVerb.FAIL),
        on_timeout=Action(action=ActionVerb.ROUTE, process="heavy"),
        options={"timeout_seconds": 45},
    )
    _patch_resolver(monkeypatch, targets=[target], selected=target)

    result = await execution.resolve_routing_terminal("dwh_join")
    assert result.on_success.action == ActionVerb.ROUTE
    assert result.on_success.process == "dwh_join"
    assert result.on_failure.action == ActionVerb.FAIL
    assert result.on_timeout.action == ActionVerb.ROUTE
    # per-target options.timeout_seconds wins over the platform default
    assert result.timeout_seconds == 45.0


@pytest.mark.asyncio
async def test_resolve_timeout_falls_back_to_platform_default(monkeypatch):
    target = RunnerTarget(runner="background", options={})  # no per-target timeout
    _patch_resolver(monkeypatch, targets=[target], selected=target)

    result = await execution.resolve_routing_terminal("index_drain")
    assert result.timeout_seconds == 3600.0


@pytest.mark.asyncio
async def test_resolve_fail_open_on_empty_targets(monkeypatch):
    _patch_resolver(monkeypatch, targets=[], selected=None)
    result = await execution.resolve_routing_terminal("unknown_task")
    assert result.on_success.action == ActionVerb.REPORT
    assert result.on_failure.action == ActionVerb.DEAD_LETTER
    assert result.on_timeout.action == ActionVerb.DEAD_LETTER


@pytest.mark.asyncio
async def test_resolve_fail_open_on_resolver_exception(monkeypatch):
    import dynastore.modules.tasks.routing.resolver as routing_resolver

    async def _boom(_key):
        raise RuntimeError("config unreachable")

    monkeypatch.setattr(routing_resolver, "resolved_targets", _boom)
    monkeypatch.setattr(
        execution, "_default_task_timeout", AsyncMock(return_value=None),
    )
    result = await execution.resolve_routing_terminal("any")
    assert result.on_success.action == ActionVerb.REPORT
    assert result.on_failure.action == ActionVerb.DEAD_LETTER


@pytest.mark.asyncio
async def test_resolve_uses_first_target_policy_when_no_runner_matches(monkeypatch):
    """select_target returns None (no local runner), but the first configured
    target still describes the terminal policy."""
    target = RunnerTarget(
        runner="gcp_cloud_run",
        on_success=Action(action=ActionVerb.ROUTE, process="next"),
    )
    _patch_resolver(monkeypatch, targets=[target], selected=None)
    result = await execution.resolve_routing_terminal("gdal")
    assert result.on_success.action == ActionVerb.ROUTE
    assert result.on_success.process == "next"


# --- apply_terminal_action ----------------------------------------------------

_ACTION_FIELDS = dict(
    task_id="11111111-1111-1111-1111-111111111111",
    task_type="ingestion",
    inputs={"source": "s3://x"},
    caller_id="user@example.com",
    collection_id="stations",
    schema="tenant_a",
    scope="CATALOG",
)


@pytest.mark.asyncio
async def test_apply_report_does_not_enqueue(monkeypatch):
    create = AsyncMock()
    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.create_task", create,
    )
    await execution.apply_terminal_action(
        MagicMock(), outcome="success",
        action=Action(action=ActionVerb.REPORT), **_ACTION_FIELDS,
    )
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_dead_letter_and_fail_do_not_enqueue(monkeypatch):
    create = AsyncMock()
    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.create_task", create,
    )
    monkeypatch.setattr(
        execution, "_read_task_status", AsyncMock(return_value="DEAD_LETTER"),
    )
    for verb in (ActionVerb.DEAD_LETTER, ActionVerb.FAIL):
        await execution.apply_terminal_action(
            MagicMock(), outcome="failure",
            action=Action(action=verb), **_ACTION_FIELDS,
        )
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_route_success_enqueues_follow_on(monkeypatch):
    create = AsyncMock(return_value=object())
    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.create_task", create,
    )
    monkeypatch.setattr(execution, "_route_target_kind", lambda _p: "process")

    await execution.apply_terminal_action(
        MagicMock(),
        outcome="success",
        action=Action(
            action=ActionVerb.ROUTE, process="dwh_join", payload={"phase": "join"},
        ),
        **_ACTION_FIELDS,
    )

    create.assert_awaited_once()
    args, kwargs = create.await_args
    task_data = args[1]
    assert task_data.task_type == "dwh_join"
    # original inputs preserved, payload merged, depth stamped to 1
    assert task_data.inputs["source"] == "s3://x"
    assert task_data.inputs["phase"] == "join"
    assert task_data.inputs[execution._ROUTE_DEPTH_KEY] == 1
    assert kwargs["schema"] == "tenant_a"


@pytest.mark.asyncio
async def test_apply_route_failure_gated_on_terminal_status(monkeypatch):
    create = AsyncMock(return_value=object())
    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.create_task", create,
    )
    monkeypatch.setattr(execution, "_route_target_kind", lambda _p: "task")

    # Not terminal yet (transient retry) → no follow-on.
    monkeypatch.setattr(
        execution, "_read_task_status", AsyncMock(return_value="PENDING"),
    )
    await execution.apply_terminal_action(
        MagicMock(), outcome="failure",
        action=Action(action=ActionVerb.ROUTE, process="cleanup"),
        **_ACTION_FIELDS,
    )
    create.assert_not_awaited()

    # Terminal (DEAD_LETTER) → fire the compensation route.
    monkeypatch.setattr(
        execution, "_read_task_status", AsyncMock(return_value="DEAD_LETTER"),
    )
    await execution.apply_terminal_action(
        MagicMock(), outcome="failure",
        action=Action(action=ActionVerb.ROUTE, process="cleanup"),
        **_ACTION_FIELDS,
    )
    create.assert_awaited_once()


@pytest.mark.asyncio
async def test_apply_route_cycle_guard_refuses_past_max_depth(monkeypatch):
    create = AsyncMock(return_value=object())
    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.create_task", create,
    )
    monkeypatch.setattr(execution, "_route_target_kind", lambda _p: "task")

    fields = {**_ACTION_FIELDS, "inputs": {execution._ROUTE_DEPTH_KEY: execution._MAX_ROUTE_DEPTH}}
    await execution.apply_terminal_action(
        MagicMock(), outcome="success",
        action=Action(action=ActionVerb.ROUTE, process="loop"),
        **fields,
    )
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_route_increments_depth(monkeypatch):
    create = AsyncMock(return_value=object())
    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.create_task", create,
    )
    monkeypatch.setattr(execution, "_route_target_kind", lambda _p: "task")

    fields = {**_ACTION_FIELDS, "inputs": {execution._ROUTE_DEPTH_KEY: 3, "k": "v"}}
    await execution.apply_terminal_action(
        MagicMock(), outcome="success",
        action=Action(action=ActionVerb.ROUTE, process="next"),
        **fields,
    )
    task_data = create.await_args.args[1]
    assert task_data.inputs[execution._ROUTE_DEPTH_KEY] == 4
    assert task_data.inputs["k"] == "v"


@pytest.mark.asyncio
async def test_apply_route_continuation_is_fail_soft(monkeypatch):
    """A create_task failure must be swallowed (never re-fail the original row)."""
    create = AsyncMock(side_effect=RuntimeError("PG down"))
    monkeypatch.setattr(
        "dynastore.modules.tasks.tasks_module.create_task", create,
    )
    monkeypatch.setattr(execution, "_route_target_kind", lambda _p: "task")
    # Should not raise.
    await execution.apply_terminal_action(
        MagicMock(), outcome="success",
        action=Action(action=ActionVerb.ROUTE, process="next"),
        **_ACTION_FIELDS,
    )


# --- hydrate_task_payload strips the reserved breadcrumb ----------------------

def test_hydrate_strips_route_depth_from_typed_payload():
    from dynastore.tasks import hydrate_task_payload

    class _FakeTask:
        async def run(self, payload):  # no typed inputs model → inputs stays dict
            return None

    raw = {
        "task_id": "22222222-2222-2222-2222-222222222222",
        "caller_id": "sys",
        "inputs": {"source": "x", "_route_depth": 4},
    }
    payload = hydrate_task_payload(_FakeTask(), raw)
    assert "_route_depth" not in payload.inputs
    assert payload.inputs["source"] == "x"
    # The caller's dict is NOT mutated (terminal-routing read still sees depth).
    assert raw["inputs"]["_route_depth"] == 4


# --- dead_letter_task ---------------------------------------------------------

@pytest.mark.asyncio
async def test_dead_letter_task_writes_dead_letter_status():
    from contextlib import asynccontextmanager
    import uuid as _uuid

    import dynastore.modules.tasks.tasks_module as tm

    captured = {}

    class _FakeQuery:
        def __init__(self, sql, result_handler=None):
            captured["sql"] = sql

        async def execute(self, _conn, **params):
            captured["params"] = params
            return 1  # rowcount

    @asynccontextmanager
    async def _fake_txn(_engine):
        yield object()

    with patch.object(tm, "DQLQuery", _FakeQuery), \
         patch.object(tm, "managed_transaction", _fake_txn):
        ok = await tm.dead_letter_task(
            object(), _uuid.uuid4(), "2026-06-01T00:00:00Z", "timed out",
        )

    assert ok is True
    assert "DEAD_LETTER" in captured["sql"]
    assert captured["params"]["error_message"] == "timed out"
