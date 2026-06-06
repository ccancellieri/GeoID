"""``get_task_config`` resolves legacy task-type aliases.

When a task class declares ``legacy_task_types`` (a frozenset of old
``task_type`` values), ``get_task_config`` must return the same config for
both the current ``task_type`` and any legacy alias.  This ensures that
DB rows written with the old ``task_type`` value are still dispatched to
the renamed handler during the one-release transition window.

Phase 0 context: ``OutboxDrainTask.task_type`` was renamed from
``"outbox_drain"`` to ``"index_drain"``; the ``legacy_task_types`` shim
keeps old rows routable without a DDL migration.
"""
from __future__ import annotations

from typing import Optional
from unittest.mock import MagicMock

import pytest

import dynastore.tasks as tasks_mod
from dynastore.tasks import TaskConfig, get_task_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _RenamedTask:
    """Minimal stub that mimics a renamed TaskProtocol implementation."""

    task_type = "index_drain"
    legacy_task_types: frozenset[str] = frozenset({"outbox_drain"})

    def __init__(self) -> None:
        pass


def _make_config() -> TaskConfig:
    return TaskConfig(
        cls=_RenamedTask,  # type: ignore[arg-type]
        module_name=__name__,
        name="index_drain",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_get_task_config_by_current_type(monkeypatch):
    """``get_task_config('index_drain')`` resolves the renamed task."""
    cfg = _make_config()
    monkeypatch.setattr(tasks_mod, "_DYNASTORE_TASKS", {"index_drain": cfg})

    result = get_task_config("index_drain")
    assert result is cfg


def test_get_task_config_by_legacy_alias(monkeypatch):
    """``get_task_config('outbox_drain')`` resolves via legacy_task_types shim."""
    cfg = _make_config()
    monkeypatch.setattr(tasks_mod, "_DYNASTORE_TASKS", {"index_drain": cfg})

    result = get_task_config("outbox_drain")
    assert result is cfg, (
        "get_task_config must resolve 'outbox_drain' to the OutboxDrainTask "
        "config via the legacy_task_types shim so that old DB rows are "
        "still dispatched to the renamed handler."
    )


def test_get_task_config_unknown_returns_none(monkeypatch):
    """``get_task_config('totally_unknown')`` returns None — no shim match."""
    cfg = _make_config()
    monkeypatch.setattr(tasks_mod, "_DYNASTORE_TASKS", {"index_drain": cfg})

    result = get_task_config("totally_unknown")
    assert result is None


def test_legacy_alias_does_not_shadow_primary_when_both_registered(monkeypatch):
    """When two tasks register under different names, the legacy alias of one
    must not shadow the primary registration of the other."""

    class _OtherTask:
        task_type = "other_task"
        legacy_task_types: frozenset[str] = frozenset()

    other_cfg = TaskConfig(
        cls=_OtherTask,  # type: ignore[arg-type]
        module_name=__name__,
        name="other_task",
    )
    renamed_cfg = _make_config()

    monkeypatch.setattr(
        tasks_mod,
        "_DYNASTORE_TASKS",
        {"index_drain": renamed_cfg, "other_task": other_cfg},
    )

    assert get_task_config("index_drain") is renamed_cfg
    assert get_task_config("other_task") is other_cfg
    assert get_task_config("outbox_drain") is renamed_cfg
