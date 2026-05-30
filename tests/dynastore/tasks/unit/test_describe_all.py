"""Unit tests for the describe_all() inventory introspection primitive."""
from __future__ import annotations

from typing import ClassVar, Optional

import dynastore.tasks as tasks_pkg
from dynastore.tasks import TaskConfig, describe_all
from dynastore.tasks.protocols import TaskProtocol


class _ProcTask(TaskProtocol):
    """A process-shaped task."""
    affinity_tier: ClassVar[Optional[str]] = "worker"

    async def run(self, payload):  # pragma: no cover
        return None


class _PlainTask(TaskProtocol):
    """A plain background task."""
    async def run(self, payload):  # pragma: no cover
        return None


class _BrokenCls:
    """Stand-in for a definition-only placeholder whose describe() blows up."""
    @classmethod
    def describe(cls):
        raise RuntimeError("placeholder has no real describe")


def _cfg(cls, **kw) -> TaskConfig:
    # module_name/name are required fields on TaskConfig; supply them so the
    # test fixture matches the real dataclass contract (no defaults).
    return TaskConfig(cls=cls, module_name="test", name=cls.__name__, **kw)


def test_describe_all_lists_tasks_and_processes_keyed_by_inventory_key(monkeypatch):
    fake = {
        "proc_x": _cfg(_ProcTask, definition=object()),  # definition set -> process
        "plain_y": _cfg(_PlainTask, definition=None),    # no definition -> task
    }
    monkeypatch.setattr(tasks_pkg, "_DYNASTORE_TASKS", fake)
    out = describe_all()
    # task_key is the inventory key (registry-consistent identity).
    by_key = {d["task_key"]: d for d in out}
    assert by_key["proc_x"]["kind"] == "process"
    assert by_key["proc_x"]["affinity_tier"] == "worker"
    assert by_key["proc_x"]["description"] == "A process-shaped task."
    # name stays the class-declared descriptor name (get_name()).
    assert by_key["proc_x"]["name"] == _ProcTask.get_name()
    assert by_key["plain_y"]["kind"] == "task"
    assert by_key["plain_y"]["payload_schema"] is None


def test_describe_all_is_fail_soft_per_entry(monkeypatch):
    fake = {
        "good": _cfg(_PlainTask, definition=None),
        "broken": _cfg(_BrokenCls, definition=None),
    }
    monkeypatch.setattr(tasks_pkg, "_DYNASTORE_TASKS", fake)
    out = describe_all()
    keys = {d["task_key"] for d in out}
    # The broken entry is skipped; the good one still appears under its key.
    assert keys == {"good"}
