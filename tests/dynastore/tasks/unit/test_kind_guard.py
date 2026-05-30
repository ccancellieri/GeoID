"""Process-vs-Task invocation boundary (formalizes the implicit safety).

Every entry-point task classifies as process|task. A kind=task (system) task is
not an invocable process. Tests are tolerant: if a given task isn't loaded in the
test SCOPE, that sub-assertion is skipped — the universal classification check
always runs."""
from __future__ import annotations

from dynastore.tasks import _DYNASTORE_TASKS, discover_tasks, task_kind
from dynastore.modules.processes.processes_module import _is_invocable_process


def test_every_task_classifies_process_or_task():
    discover_tasks()
    assert _DYNASTORE_TASKS, "no tasks discovered"
    for key, cfg in _DYNASTORE_TASKS.items():
        assert task_kind(cfg) in ("process", "task"), key


def test_system_tasks_are_not_invocable_processes():
    discover_tasks()
    cfg = _DYNASTORE_TASKS.get("cascade_cleanup")
    if cfg is not None:
        assert task_kind(cfg) == "task"
        assert _is_invocable_process("cascade_cleanup") is False


def test_processes_are_invocable():
    discover_tasks()
    # pick any loaded kind=process task and confirm it IS invocable
    proc_key = next(
        (k for k, c in _DYNASTORE_TASKS.items() if task_kind(c) == "process"), None
    )
    if proc_key is not None:
        assert _is_invocable_process(proc_key) is True


def test_unknown_process_id_is_not_invocable():
    assert _is_invocable_process("definitely-not-a-real-process-id") is False
