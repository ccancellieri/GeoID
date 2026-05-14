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
