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
# offload_required — backlog-adaptive drain tasks (#2622)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_offload_required_true_for_storage_drain_when_backlog_high(monkeypatch):
    """storage_drain has no static OFFLOAD/HEAVY routing hint by default — the
    live backlog signal alone must be able to trigger offload."""
    import dynastore.modules.tasks.routing.resolver as rr
    import dynastore.modules.tasks.async_writer_backlog as backlog

    async def _targets(_k):
        return []

    async def _high(*args, **kwargs):
        return True

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    monkeypatch.setattr(backlog, "backlog_is_high", _high)
    assert await execution.offload_required("storage_drain") is True


@pytest.mark.asyncio
async def test_offload_required_false_for_storage_drain_when_backlog_low(monkeypatch):
    import dynastore.modules.tasks.routing.resolver as rr
    import dynastore.modules.tasks.async_writer_backlog as backlog

    async def _targets(_k):
        return []

    async def _low(*args, **kwargs):
        return False

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    monkeypatch.setattr(backlog, "backlog_is_high", _low)
    assert await execution.offload_required("storage_drain") is False


@pytest.mark.asyncio
async def test_offload_required_true_for_event_drain_when_backlog_high(monkeypatch):
    import dynastore.modules.tasks.routing.resolver as rr
    import dynastore.modules.tasks.async_writer_backlog as backlog

    async def _targets(_k):
        return []

    async def _high(*args, **kwargs):
        return True

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    monkeypatch.setattr(backlog, "backlog_is_high", _high)
    assert await execution.offload_required("event_drain") is True


@pytest.mark.asyncio
async def test_offload_required_ignores_backlog_signal_for_unrelated_task(monkeypatch):
    """Only the backlog-adaptive task keys consult the live signal — a high
    backlog must not spuriously offload an unrelated task."""
    import dynastore.modules.tasks.routing.resolver as rr
    import dynastore.modules.tasks.async_writer_backlog as backlog

    async def _targets(_k):
        return []

    async def _high(*args, **kwargs):
        return True

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    monkeypatch.setattr(backlog, "backlog_is_high", _high)
    assert await execution.offload_required("gdal") is False


@pytest.mark.asyncio
async def test_offload_required_false_for_storage_drain_on_backlog_probe_error(monkeypatch):
    """A backlog-probe failure must never force offload — fail-open False."""
    import dynastore.modules.tasks.routing.resolver as rr
    import dynastore.modules.tasks.async_writer_backlog as backlog

    async def _targets(_k):
        return []

    async def _boom(*args, **kwargs):
        raise RuntimeError("pool exhausted")

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    monkeypatch.setattr(backlog, "backlog_is_high", _boom)
    assert await execution.offload_required("storage_drain") is False


@pytest.mark.asyncio
async def test_offload_required_static_hint_short_circuits_backlog_probe(monkeypatch):
    """A static OFFLOAD/HEAVY routing hint is sufficient on its own — the
    backlog probe must not even be consulted in that case."""
    from dynastore.modules.tasks.routing.exec_hints import ExecHint
    import dynastore.modules.tasks.routing.resolver as rr
    import dynastore.modules.tasks.async_writer_backlog as backlog

    async def _targets(_k):
        return [_target("gcp_cloud_run", {ExecHint.OFFLOAD})]

    async def _boom(*args, **kwargs):
        raise AssertionError("backlog probe must not be called")

    monkeypatch.setattr(rr, "resolved_targets", _targets)
    monkeypatch.setattr(backlog, "backlog_is_high", _boom)
    assert await execution.offload_required("storage_drain") is True


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
