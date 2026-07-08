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

"""CapabilityMap: routing-based service-affinity filtering and fail-open contract."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks import runners
from dynastore.modules.tasks.models import TaskExecutionMode
from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.model import RunnerTarget


class _Runner:
    mode = TaskExecutionMode.ASYNCHRONOUS

    def __init__(self, runner_type: str, task_types: set[str] | None = None):
        self.runner_type = runner_type
        self._task_types = task_types

    def can_handle(self, task_type: str) -> bool:
        return self._task_types is None or task_type in self._task_types


def _patch_runner_registry(monkeypatch, installed: list[_Runner]) -> None:
    def _get_runners(mode):
        return [r for r in installed if r.mode == mode]

    monkeypatch.setattr(runners, "get_runners", _get_runners)


@pytest.mark.asyncio
async def test_unrouted_task_stays_claimable_when_resolver_silent(monkeypatch):
    """When _routed_consumers returns None (no routing opinion), the task stays claimable."""
    import dynastore.modules.tasks.routing.resolver as rr

    async def _none(_task_key):
        return []

    monkeypatch.setattr(rr, "resolved_targets", _none)
    monkeypatch.setattr(runners, "get_loaded_task_types", lambda: ["gdal"])
    _patch_runner_registry(monkeypatch, [_Runner("background", {"gdal"})])

    cmap = runners.CapabilityMap()
    await cmap.refresh()
    assert "gdal" in cmap.async_types


@pytest.mark.asyncio
async def test_wrong_service_filtered_when_resolver_decides(monkeypatch):
    """When routing returns a concrete consumer list that excludes this service, filter it."""
    import dynastore.modules.tasks.routing.resolver as rr

    async def _maps_only(_task_key):
        return [RunnerTarget(consumers=["maps"], runner="background")]

    monkeypatch.setattr(rr, "resolved_targets", _maps_only)
    monkeypatch.setattr(runners, "_SERVICE_NAME", "worker")
    monkeypatch.setattr(runners, "get_loaded_task_types", lambda: ["gdal"])
    _patch_runner_registry(monkeypatch, [_Runner("background", {"gdal"})])

    cmap = runners.CapabilityMap()
    await cmap.refresh()
    assert "gdal" not in cmap.async_types


@pytest.mark.asyncio
async def test_multi_consumer_admits_listed_service(monkeypatch):
    """When this service appears in the consumer list, the task is claimable."""
    import dynastore.modules.tasks.routing.resolver as rr

    async def _multi(_task_key):
        return [RunnerTarget(consumers=["catalog", "worker"], runner="background")]

    monkeypatch.setattr(rr, "resolved_targets", _multi)
    monkeypatch.setattr(runners, "_SERVICE_NAME", "worker")
    monkeypatch.setattr(runners, "get_loaded_task_types", lambda: ["gdal"])
    _patch_runner_registry(monkeypatch, [_Runner("background", {"gdal"})])

    cmap = runners.CapabilityMap()
    await cmap.refresh()
    assert "gdal" in cmap.async_types


@pytest.mark.asyncio
async def test_no_service_name_stays_claimable(monkeypatch):
    """Without a resolved service identity, routing cannot filter — fail-open."""
    import dynastore.modules.tasks.routing.resolver as rr

    async def _maps_only(_task_key):
        return [RunnerTarget(consumers=["maps"], runner="background")]

    monkeypatch.setattr(rr, "resolved_targets", _maps_only)
    monkeypatch.setattr(runners, "_SERVICE_NAME", None)
    monkeypatch.setattr(runners, "get_loaded_task_types", lambda: ["gdal"])
    _patch_runner_registry(monkeypatch, [_Runner("background", {"gdal"})])

    cmap = runners.CapabilityMap()
    await cmap.refresh()
    assert "gdal" in cmap.async_types


@pytest.mark.asyncio
async def test_cloud_offload_route_does_not_add_background_claimability(monkeypatch):
    """A gcp_cloud_run-only route must not be advertised by a background-only pod."""
    import dynastore.modules.tasks.routing.resolver as rr

    async def _cloud_catalog_provision(_task_key):
        return [
            RunnerTarget(
                consumers=["catalog"],
                runner="gcp_cloud_run",
                hints={ExecHint.OFFLOAD, ExecHint.HEAVY},
            )
        ]

    monkeypatch.setattr(rr, "resolved_targets", _cloud_catalog_provision)
    monkeypatch.setattr(runners, "_SERVICE_NAME", "catalog")
    monkeypatch.setattr(runners, "get_loaded_task_types", lambda: ["catalog_provision"])
    _patch_runner_registry(monkeypatch, [_Runner("background", {"catalog_provision"})])

    cmap = runners.CapabilityMap()
    await cmap.refresh()

    assert "catalog_provision" not in cmap.async_types


@pytest.mark.asyncio
async def test_cloud_offload_route_is_claimable_when_gcp_runner_exists(monkeypatch):
    """A catalog pod with a matching gcp_cloud_run runner may claim the routed task."""
    import dynastore.modules.tasks.routing.resolver as rr

    async def _cloud_catalog_provision(_task_key):
        return [
            RunnerTarget(
                consumers=["catalog"],
                runner="gcp_cloud_run",
                hints={ExecHint.OFFLOAD, ExecHint.HEAVY},
            )
        ]

    monkeypatch.setattr(rr, "resolved_targets", _cloud_catalog_provision)
    monkeypatch.setattr(runners, "_SERVICE_NAME", "catalog")
    monkeypatch.setattr(runners, "get_loaded_task_types", lambda: ["catalog_provision"])
    _patch_runner_registry(
        monkeypatch,
        [
            _Runner("background", {"catalog_provision"}),
            _Runner("gcp_cloud_run", {"catalog_provision"}),
        ],
    )

    cmap = runners.CapabilityMap()
    await cmap.refresh()

    assert "catalog_provision" in cmap.async_types


@pytest.mark.asyncio
async def test_onprem_background_route_remains_claimable(monkeypatch):
    """The on-prem route emits background, so in-process claimability remains."""
    import dynastore.modules.tasks.routing.resolver as rr

    async def _onprem_catalog_provision(_task_key):
        return [
            RunnerTarget(
                consumers=["catalog"],
                runner="background",
                hints={ExecHint.BACKGROUND},
            )
        ]

    monkeypatch.setattr(rr, "resolved_targets", _onprem_catalog_provision)
    monkeypatch.setattr(runners, "_SERVICE_NAME", "catalog")
    monkeypatch.setattr(runners, "get_loaded_task_types", lambda: ["catalog_provision"])
    _patch_runner_registry(monkeypatch, [_Runner("background", {"catalog_provision"})])

    cmap = runners.CapabilityMap()
    await cmap.refresh()

    assert "catalog_provision" in cmap.async_types
