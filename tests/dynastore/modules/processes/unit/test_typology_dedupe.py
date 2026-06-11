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

"""Duplicate runner registrations must not leak into ``typologies[]``.

The live ``GET /processes?typology=true`` payload carried each in-process
runner twice because ``get_runners()`` re-registered fresh default-runner
instances on every call (the discovery registry dedupes by ``id(obj)``, so
new instances always pass). These tests pin both layers of the fix:
idempotent ``register_default_runners()`` and defensive dedup in
``resolve_typologies()``.
"""

from typing import Any

from dynastore.models.tasks import TaskExecutionMode
from dynastore.modules.processes import inventory
from dynastore.modules.tasks.runners import (
    RunnerProtocol,
    register_default_runners,
)
from dynastore.tools.discovery import get_protocols


class _StubRunner:
    """Minimal structural RunnerProtocol implementation."""

    def __init__(self, runner_type: str, priority: int, mode=TaskExecutionMode.SYNCHRONOUS):
        self.runner_type = runner_type
        self.priority = priority
        self.mode = mode

    async def setup(self, app_state: Any) -> None:  # pragma: no cover - protocol shape
        pass

    @property
    def capabilities(self) -> Any:  # pragma: no cover - protocol shape
        return None

    def can_handle(self, task_type: str) -> bool:
        return True

    def declared_tasks(self) -> list:
        return []

    async def run(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        return None


def test_stub_runner_satisfies_protocol_structurally():
    assert isinstance(_StubRunner("sync", 100), RunnerProtocol)


def test_resolve_typologies_dedupes_duplicate_registrations(monkeypatch):
    """Two instances of the same logical runner yield ONE typology entry."""
    dupes = [
        _StubRunner("sync", 100),
        _StubRunner("sync", 100),
        _StubRunner("background", 90, TaskExecutionMode.ASYNCHRONOUS),
        _StubRunner("background", 90, TaskExecutionMode.ASYNCHRONOUS),
    ]
    monkeypatch.setattr(inventory, "get_protocols", lambda proto: dupes)

    typologies = inventory.resolve_typologies("ingestion")

    keys = [(t.runner_type, t.mode, t.location) for t in typologies]
    assert len(keys) == len(set(keys)) == 2
    # priority-desc order preserved; dispatcher pick (first element) stable
    assert typologies[0].runner_type == "sync"
    assert typologies[0].priority == 100


def test_resolve_typologies_keeps_highest_priority_duplicate(monkeypatch):
    """When duplicates differ in priority, the highest-priority one survives."""
    dupes = [
        _StubRunner("worker_queue", 10, TaskExecutionMode.ASYNCHRONOUS),
        _StubRunner("worker_queue", 80, TaskExecutionMode.ASYNCHRONOUS),
    ]
    monkeypatch.setattr(inventory, "get_protocols", lambda proto: dupes)

    typologies = inventory.resolve_typologies("ingestion")

    assert len(typologies) == 1
    assert typologies[0].priority == 80


def test_register_default_runners_is_idempotent():
    """Repeated calls must not grow the registry (the original bug: every
    ``get_runners()`` call registered fresh default-runner instances)."""
    register_default_runners()
    before = len(get_protocols(RunnerProtocol))
    register_default_runners()
    register_default_runners()
    after = len(get_protocols(RunnerProtocol))
    assert after == before
