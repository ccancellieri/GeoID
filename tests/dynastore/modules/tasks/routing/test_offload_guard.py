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

"""Offload guard: a HEAVY/OFFLOAD-routed process must run on an external
executor (Cloud Run Job / worker queue), never in-process on the serving tier.

Regression context: a claimed ``ingestion`` row was executed by the in-process
``BackgroundRunner`` (priority 100) on the catalog because the queue path picks
runners by priority and never consulted routing — heavy raster/vector work then
saturated the catalog's 2 GiB instance. The guard keys off the ``OFFLOAD`` /
``HEAVY`` exec-hints the cloud routing matrix already assigns, and is fail-open:
no hint / no routing opinion (the ``onprem`` profile, system tasks) leaves
in-process execution available.
"""
from __future__ import annotations

import pytest

from dynastore.modules.tasks import execution


class _Runner:
    def __init__(self, runner_type: str):
        self.runner_type = runner_type


def _target(runner: str, hints):
    from dynastore.modules.tasks.routing.model import RunnerTarget

    return RunnerTarget(runner=runner, consumers=[], hints=set(hints))


# ---------------------------------------------------------------------------
# offload_required
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_offload_required_true_for_heavy_offload_hint(monkeypatch):
    from dynastore.modules.tasks.routing.exec_hints import ExecHint
    import dynastore.modules.tasks.routing.resolver as rr

    async def _targets(_k):
        return [_target("gcp_cloud_run", {ExecHint.OFFLOAD, ExecHint.HEAVY})]

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    assert await execution.offload_required("ingestion") is True


@pytest.mark.asyncio
async def test_offload_required_false_for_background_hint(monkeypatch):
    """onprem profile routes heavy processes to ``background`` — no offload tag."""
    from dynastore.modules.tasks.routing.exec_hints import ExecHint
    import dynastore.modules.tasks.routing.resolver as rr

    async def _targets(_k):
        return [_target("background", {ExecHint.BACKGROUND})]

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    assert await execution.offload_required("ingestion") is False


@pytest.mark.asyncio
async def test_offload_required_false_on_empty_targets(monkeypatch):
    """No routing opinion → fail-open False (in-process stays available)."""
    import dynastore.modules.tasks.routing.resolver as rr

    async def _targets(_k):
        return []

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    assert await execution.offload_required("anything") is False


@pytest.mark.asyncio
async def test_offload_required_false_on_resolver_error(monkeypatch):
    """A resolver failure must never force offload — fail-open False."""
    import dynastore.modules.tasks.routing.resolver as rr

    async def _boom(_k):
        raise RuntimeError("config unreachable")

    monkeypatch.setattr(rr, "resolved_targets", _boom)
    assert await execution.offload_required("anything") is False


# ---------------------------------------------------------------------------
# offload_required — async-write workclass placement (#2732)
#
# event_drain no longer consults the live outbox-backlog signal for
# PLACEMENT (that signal's one legitimate job is ingestion backpressure, see
# tasks/ingestion/main_ingestion.py). Placement is unconditional for any task
# subclassing dynastore.tasks.workclass_drain.AsyncWriteDrainTaskProtocol (or
# otherwise carrying its ``is_async_write_workclass`` marker), resolved
# directly off the task registry — no routing config, no per-task-key
# enumeration in execution.py.
#
# storage_drain (#2732 step 4, in-process-first) deliberately does NOT carry
# the marker: it always starts in-process, bounded by its own byte/wall-clock
# drain budget, and hands off to storage_drain_offload — which DOES carry the
# marker — only once that budget is exhausted with backlog remaining. See
# dynastore/tasks/workclass_drain/storage_drain_task.py.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_offload_required_false_for_storage_drain(monkeypatch):
    """storage_drain (#2732 step 4) is in-process-first — it deliberately
    does NOT carry the async-write workclass marker, so offload_required
    must be False for it even with no static routing hint."""
    import dynastore.modules.tasks.routing.resolver as rr
    # Import triggers TaskProtocol.__init_subclass__ registration; the
    # dispatcher/app normally does this via task discovery at startup.
    import dynastore.tasks.workclass_drain.storage_drain_task  # noqa: F401

    async def _targets(_k):
        return []

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    assert await execution.offload_required("storage_drain") is False


@pytest.mark.asyncio
async def test_offload_required_true_for_storage_drain_offload(monkeypatch):
    """storage_drain_offload is the overflow variant StorageDrainTask hands
    off to once its in-process budget is exhausted — it carries the
    async-write workclass marker, so the marker alone must trigger offload."""
    import dynastore.modules.tasks.routing.resolver as rr
    import dynastore.tasks.workclass_drain.storage_drain_task  # noqa: F401

    async def _targets(_k):
        return []

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    assert await execution.offload_required("storage_drain_offload") is True


@pytest.mark.asyncio
async def test_offload_required_true_for_event_drain(monkeypatch):
    import dynastore.modules.tasks.routing.resolver as rr
    import dynastore.tasks.workclass_drain.event_drain_task  # noqa: F401

    async def _targets(_k):
        return []

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    assert await execution.offload_required("event_drain") is True


@pytest.mark.asyncio
async def test_offload_required_false_for_non_workclass_task(monkeypatch):
    """A task outside the async-write workclass (e.g. gdal) is unaffected —
    only its own OFFLOAD/HEAVY routing hint (absent here) can trigger it."""
    import dynastore.modules.tasks.routing.resolver as rr

    async def _targets(_k):
        return []

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    assert await execution.offload_required("gdal") is False


@pytest.mark.asyncio
async def test_offload_required_false_for_workclass_task_on_registry_lookup_error(monkeypatch):
    """A task-registry failure must never force offload — fail-open False.

    Uses ``event_drain`` (still marker-carrying) rather than storage_drain,
    since storage_drain is False regardless of the registry outcome now —
    this must exercise the actual fail-open path, not a coincidental match.
    """
    import dynastore.modules.tasks.routing.resolver as rr
    import dynastore.tasks as tasks_module

    async def _targets(_k):
        return []

    def _boom(_task_key):
        raise RuntimeError("registry unavailable")

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    monkeypatch.setattr(tasks_module, "get_task_config", _boom)
    assert await execution.offload_required("event_drain") is False


@pytest.mark.asyncio
async def test_offload_required_static_hint_and_workclass_marker_both_sufficient(monkeypatch):
    """A static OFFLOAD/HEAVY routing hint alone is sufficient — the
    workclass registry lookup is never reached in that case."""
    from dynastore.modules.tasks.routing.exec_hints import ExecHint
    import dynastore.modules.tasks.routing.resolver as rr
    import dynastore.tasks as tasks_module

    calls = []

    async def _targets(_k):
        return [_target("gcp_cloud_run", {ExecHint.OFFLOAD})]

    def _record(_task_key):
        calls.append(_task_key)
        return None

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    monkeypatch.setattr(tasks_module, "get_task_config", _record)
    assert await execution.offload_required("storage_drain") is True
    assert calls == []


@pytest.mark.asyncio
async def test_offload_required_true_for_new_drainer_subclass_with_no_execution_py_edit():
    """A brand-new drainer that only subclasses AsyncWriteDrainTaskProtocol
    is picked up by offload_required with zero changes to execution.py —
    the whole point of the workclass marker (#2732)."""
    from typing import ClassVar

    from dynastore.tasks.workclass_drain import AsyncWriteDrainTaskProtocol

    class _FutureDrainer(AsyncWriteDrainTaskProtocol):
        task_type: ClassVar[str] = "test_future_drainer_2732"

        async def run(self, payload):  # pragma: no cover - not exercised
            return None

    assert await execution.offload_required("test_future_drainer_2732") is True


@pytest.mark.asyncio
async def test_offload_required_false_for_plain_taskprotocol_subclass():
    """A task that subclasses TaskProtocol directly (not the workclass base)
    does not inherit placement, even with no routing opinion."""
    from typing import ClassVar

    from dynastore.tasks.protocols import TaskProtocol

    class _PlainTask(TaskProtocol):
        task_type: ClassVar[str] = "test_plain_task_2732"

        async def run(self, payload):  # pragma: no cover - not exercised
            return None

    assert await execution.offload_required("test_plain_task_2732") is False


# ---------------------------------------------------------------------------
# offload_required composed with _restrict_to_offload_runners — the actual
# fail-open enforcement point (#2732)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_workclass_task_stays_in_process_when_no_offload_runner_advertises():
    """offload_required("storage_drain_offload") is True (workclass marker),
    but with no gcp_cloud_run / worker_queue runner in the candidate list,
    _restrict_to_offload_runners leaves the in-process runner untouched —
    the fail-open compose/onprem/tests rely on."""
    import dynastore.tasks.workclass_drain.storage_drain_task  # noqa: F401

    assert await execution.offload_required("storage_drain_offload") is True
    runners = [_Runner("background")]
    kept = execution._restrict_to_offload_runners(runners)
    assert [r.runner_type for r in kept] == ["background"]


@pytest.mark.asyncio
async def test_workclass_task_drops_in_process_when_offload_runner_advertises():
    """Same task, but a gcp_cloud_run runner now advertises it — the
    in-process candidate is dropped."""
    import dynastore.tasks.workclass_drain.storage_drain_task  # noqa: F401

    assert await execution.offload_required("storage_drain_offload") is True
    runners = [_Runner("background"), _Runner("gcp_cloud_run")]
    kept = execution._restrict_to_offload_runners(runners)
    assert [r.runner_type for r in kept] == ["gcp_cloud_run"]


# ---------------------------------------------------------------------------
# _restrict_to_offload_runners
# ---------------------------------------------------------------------------


def test_restrict_drops_in_process_when_offload_present():
    runners = [_Runner("background"), _Runner("gcp_cloud_run"), _Runner("worker_queue")]
    kept = execution._restrict_to_offload_runners(runners)
    assert [r.runner_type for r in kept] == ["gcp_cloud_run", "worker_queue"]


def test_restrict_keeps_in_process_when_no_offload_runner():
    """A tier with no external executor (e.g. maps running gdal in-process)
    keeps its in-process runner rather than ending up with an empty set."""
    runners = [_Runner("background")]
    kept = execution._restrict_to_offload_runners(runners)
    assert [r.runner_type for r in kept] == ["background"]


def test_restrict_preserves_offload_order():
    """Order (already biased toward the routing-preferred runner) is preserved."""
    runners = [_Runner("gcp_cloud_run"), _Runner("background"), _Runner("worker_queue")]
    kept = execution._restrict_to_offload_runners(runners)
    assert [r.runner_type for r in kept] == ["gcp_cloud_run", "worker_queue"]
