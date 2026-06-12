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

"""``get_task_config`` resolves tasks by task_type class attribute.

The legacy_task_types shim (Phase 0 outbox_drain → index_drain) has been
retired.  DB rows with the old task_type were rewritten at bootstrap by the
config_seeder fixup.  Dispatcher now resolves tasks by direct task_type
lookup only.
"""
from __future__ import annotations

import pytest

import dynastore.tasks as tasks_mod
from dynastore.tasks import TaskConfig, get_task_config
from dynastore.tasks.outbox_drain.drain_task import OutboxDrainTask


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_drain_config() -> TaskConfig:
    return TaskConfig(
        cls=OutboxDrainTask,  # type: ignore[arg-type]
        module_name=__name__,
        name="index_drain",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_get_task_config_by_current_type(monkeypatch):
    """``get_task_config('index_drain')`` resolves the drain task."""
    cfg = _make_drain_config()
    monkeypatch.setattr(tasks_mod, "_DYNASTORE_TASKS", {"index_drain": cfg})

    result = get_task_config("index_drain")
    assert result is cfg


def test_get_task_config_unknown_returns_none(monkeypatch):
    """``get_task_config('totally_unknown')`` returns None."""
    cfg = _make_drain_config()
    monkeypatch.setattr(tasks_mod, "_DYNASTORE_TASKS", {"index_drain": cfg})

    result = get_task_config("totally_unknown")
    assert result is None


def test_outbox_drain_is_not_resolvable_by_old_name(monkeypatch):
    """After shim removal, 'outbox_drain' must NOT resolve to OutboxDrainTask.

    The seeder fixup ensures no PENDING rows survive with the old task_type.
    If a stale row somehow reached the dispatcher, it returns None (no handler)
    rather than silently routing to the index_drain task under an old alias.
    """
    cfg = _make_drain_config()
    monkeypatch.setattr(tasks_mod, "_DYNASTORE_TASKS", {"index_drain": cfg})

    result = get_task_config("outbox_drain")
    assert result is None, (
        "get_task_config('outbox_drain') must return None after shim removal; "
        "the seeder fixup rewrites PENDING rows before the dispatcher starts."
    )


def test_legacy_task_types_attribute_removed_from_drain_task():
    """OutboxDrainTask must not declare legacy_task_types after shim removal."""
    assert not hasattr(OutboxDrainTask, "legacy_task_types"), (
        "OutboxDrainTask.legacy_task_types must be removed; "
        "the Phase 0 shim is retired."
    )


def test_get_task_config_does_not_check_legacy_task_types(monkeypatch):
    """get_task_config must not consult legacy_task_types on any class."""
    # Inject a task class that still has legacy_task_types (simulating a
    # not-yet-cleaned external task).  get_task_config must ignore it.
    class _StaleTask:
        task_type = "new_name"
        legacy_task_types: frozenset = frozenset({"old_name"})

    cfg = TaskConfig(
        cls=_StaleTask,  # type: ignore[arg-type]
        module_name=__name__,
        name="new_name",
    )
    monkeypatch.setattr(tasks_mod, "_DYNASTORE_TASKS", {"new_name": cfg})

    assert get_task_config("new_name") is cfg
    assert get_task_config("old_name") is None, (
        "get_task_config must not fall back to legacy_task_types lookup."
    )


def test_two_tasks_independent_resolution(monkeypatch):
    """Two tasks with distinct task_types resolve independently."""

    class _OtherTask:
        task_type = "other_task"

    other_cfg = TaskConfig(
        cls=_OtherTask,  # type: ignore[arg-type]
        module_name=__name__,
        name="other_task",
    )
    drain_cfg = _make_drain_config()

    monkeypatch.setattr(
        tasks_mod,
        "_DYNASTORE_TASKS",
        {"index_drain": drain_cfg, "other_task": other_cfg},
    )

    assert get_task_config("index_drain") is drain_cfg
    assert get_task_config("other_task") is other_cfg
