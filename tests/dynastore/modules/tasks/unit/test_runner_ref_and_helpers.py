"""Unit tests for the generic ``runner_ref`` column and its task-table helpers.

#735: the liveness probe needs the runner's opaque execution handle (for Cloud
Run, the execution resource name). ``owner_id`` only holds the dynastore-internal
hex, and the GCP ``Operation`` carrying the real handle was discarded. A generic
``runner_ref TEXT`` column stores it — runner-agnostic, so any future runner
parks its own handle there.

Source-inspection style (no live DB): the SQL shape is what we pin.
"""

from __future__ import annotations

import inspect


def _tasks_module():
    from dynastore.modules.tasks import tasks_module
    return tasks_module


def test_ddl_has_runner_ref_column():
    """The global tasks table must declare ``runner_ref TEXT`` (after owner_id)."""
    ddl = _tasks_module().GLOBAL_TASKS_TABLE_DDL
    normalised = " ".join(ddl.split()).upper()
    assert "RUNNER_REF TEXT" in normalised


def test_task_model_has_runner_ref_field():
    """The ``Task`` model carries the column so probes can read it off a row."""
    from dynastore.models.tasks import Task

    assert "runner_ref" in Task.model_fields
    t = Task(task_type="x")
    assert t.runner_ref is None


def test_set_runner_ref_exists_and_is_async():
    """A dedicated helper — works identically for REST and dispatcher paths."""
    fn = _tasks_module().set_runner_ref
    assert inspect.iscoroutinefunction(fn)
    sig = inspect.signature(fn)
    for param in ("engine", "task_id", "runner_ref"):
        assert param in sig.parameters, f"missing parameter: {param}"


def test_set_runner_ref_updates_only_runner_ref():
    """The helper writes ``runner_ref`` and nothing else — no status churn."""
    src = inspect.getsource(_tasks_module().set_runner_ref)
    normalised = " ".join(src.split()).upper()
    assert "SET RUNNER_REF = :RUNNER_REF" in normalised
    assert "WHERE TASK_ID = :TASK_ID" in normalised
    assert "STATUS" not in normalised.split("UPDATE", 1)[-1].split("WHERE")[0]


def test_select_lapsed_gcp_tasks_exists_and_is_async():
    fn = _tasks_module().select_lapsed_gcp_tasks
    assert inspect.iscoroutinefunction(fn)


def test_select_lapsed_gcp_tasks_sql_shape():
    """Single scan: ACTIVE + lapsed lease + gcp_cloud_run owner, FOR UPDATE
    SKIP LOCKED so the reconciler and the pg_cron reaper never fight a row.
    Must surface runner_ref / started_at / outputs for the verdict actions."""
    src = inspect.getsource(_tasks_module().select_lapsed_gcp_tasks)
    normalised = " ".join(src.split()).upper()
    assert "STATUS = 'ACTIVE'" in normalised
    assert "LOCKED_UNTIL < NOW()" in normalised
    assert "OWNER_ID LIKE 'GCP_CLOUD_RUN_%'" in normalised
    assert "FOR UPDATE SKIP LOCKED" in normalised
    for col in ("RUNNER_REF", "STARTED_AT", "OUTPUTS"):
        assert col in normalised, f"select must return {col}"


def test_persist_outputs_exists_and_is_async():
    """The #726-followup hardening: a distinct, retryable write that lands
    ``outputs`` on the row *before* the terminal status flip — so a
    reconciler-driven TERMINAL_SUCCEEDED recovery is correct, not empty."""
    fn = _tasks_module().persist_outputs
    assert inspect.iscoroutinefunction(fn)
    sig = inspect.signature(fn)
    for param in ("engine", "task_id", "outputs"):
        assert param in sig.parameters, f"missing parameter: {param}"


def test_persist_outputs_does_not_flip_status():
    """It writes outputs (and progress) only — the status flip stays with
    ``complete_task``. Writing status here would defeat the two-phase safety."""
    src = inspect.getsource(_tasks_module().persist_outputs)
    normalised = " ".join(src.split()).upper()
    assert "SET OUTPUTS = :OUTPUTS" in normalised
    assert "STATUS = 'COMPLETED'" not in normalised


# --- heartbeat_task_if_active (#741: reaper-race detection) ----------------


def test_heartbeat_task_if_active_exists_and_is_async():
    """A single-task conditional heartbeat that reports whether the row was
    still ``ACTIVE`` at UPDATE time — the reconciler uses the return value to
    spot the accepted SELECT→probe→act race window the pg_cron reaper can win."""
    fn = _tasks_module().heartbeat_task_if_active
    assert inspect.iscoroutinefunction(fn)
    sig = inspect.signature(fn)
    for param in ("engine", "task_id", "visibility_timeout"):
        assert param in sig.parameters, f"missing parameter: {param}"


def test_heartbeat_task_if_active_returns_bool_from_rowcount():
    """The function MUST return ``bool`` — the reconciler conditions a
    reaper-race warning on it. Source-pin: ``ResultHandler.ROWCOUNT`` is used
    and the function annotates a ``bool`` return."""
    import typing

    fn = _tasks_module().heartbeat_task_if_active
    hints = typing.get_type_hints(fn)
    assert hints.get("return") is bool, (
        "heartbeat_task_if_active must return bool — the reaper-race signal "
        "depends on the truthiness of UPDATE rowcount."
    )
    src = inspect.getsource(fn)
    assert "ResultHandler.ROWCOUNT" in src


def test_heartbeat_task_if_active_sql_shape():
    """SQL pins: single-row by task_id, conditional on ``status = 'ACTIVE'``.
    Without the status guard the rowcount signal is meaningless (an UPDATE on
    a no-longer-ACTIVE row would still update one row)."""
    src = inspect.getsource(_tasks_module().heartbeat_task_if_active)
    normalised = " ".join(src.split()).upper()
    assert "SET LOCKED_UNTIL = :LOCKED_UNTIL" in normalised
    assert "WHERE TASK_ID = :TASK_ID" in normalised
    assert "STATUS = 'ACTIVE'" in normalised


# --- fail_task / complete_task owner-guard (#750: terminal-path race guard) ---


def test_fail_task_accepts_owner_id_guard_and_returns_bool():
    """#750 — ``fail_task`` gains an optional keyword-only ``owner_id`` race
    guard and returns ``bool`` (whether a row matched), so the reconciler can
    only fail the exact execution attempt it probed."""
    import typing

    fn = _tasks_module().fail_task
    sig = inspect.signature(fn)
    assert "owner_id" in sig.parameters, "fail_task must accept an owner_id guard"
    assert sig.parameters["owner_id"].kind is inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters["owner_id"].default is None  # opt-in: off by default
    assert typing.get_type_hints(fn).get("return") is bool


def test_complete_task_accepts_owner_id_guard_and_returns_bool():
    """#750 — ``complete_task`` gains the same opt-in ``owner_id`` race guard
    and ``bool`` return as ``fail_task``."""
    import typing

    fn = _tasks_module().complete_task
    sig = inspect.signature(fn)
    assert "owner_id" in sig.parameters, "complete_task must accept an owner_id guard"
    assert sig.parameters["owner_id"].kind is inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters["owner_id"].default is None
    assert typing.get_type_hints(fn).get("return") is bool


def test_fail_task_owner_guard_is_conditional_in_sql():
    """The ``owner_id`` predicate is appended to the WHERE clause ONLY when an
    owner_id is supplied — every existing caller (no owner_id) keeps today's
    unconditional ``WHERE task_id = :task_id`` behaviour. ``ROWCOUNT`` handler
    backs the bool return."""
    src = inspect.getsource(_tasks_module().fail_task)
    assert 'owner_guard = " AND owner_id = :owner_id" if owner_id is not None else ""' in src
    assert "ResultHandler.ROWCOUNT" in src
    # The hard-cap retry logic must be untouched by the guard.
    assert "LEAST(max_retries, :hard_cap)" in src


def test_complete_task_owner_guard_is_conditional_in_sql():
    """Same conditional-guard contract for ``complete_task``."""
    src = inspect.getsource(_tasks_module().complete_task)
    assert 'owner_guard = " AND owner_id = :owner_id" if owner_id is not None else ""' in src
    assert "ResultHandler.ROWCOUNT" in src
    assert "SET status = 'COMPLETED'" in src or "status = 'COMPLETED'" in src
