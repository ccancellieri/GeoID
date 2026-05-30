"""CapabilityMap keeps today's behavior when placement yields no opinion."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks import runners


@pytest.mark.asyncio
async def test_unrouted_task_stays_claimable_when_resolver_silent(monkeypatch):
    async def _none(_task_key):
        return None
    monkeypatch.setattr(runners, "_placement_consumers", _none)
    monkeypatch.setattr(runners, "get_loaded_task_types", lambda: ["gdal"])
    monkeypatch.setattr(runners, "_service_can_run_async", lambda t: True)
    monkeypatch.setattr(runners, "_service_can_run_sync", lambda t: False)

    cmap = runners.CapabilityMap()
    await cmap.refresh()
    assert "gdal" in cmap.async_types  # not filtered out


@pytest.mark.asyncio
async def test_wrong_service_filtered_when_resolver_decides(monkeypatch):
    async def _maps_only(_task_key):
        return ["maps"]
    monkeypatch.setattr(runners, "_placement_consumers", _maps_only)
    monkeypatch.setattr(runners, "_SERVICE_NAME", "worker")
    monkeypatch.setattr(runners, "get_loaded_task_types", lambda: ["gdal"])
    monkeypatch.setattr(runners, "_service_can_run_async", lambda t: True)
    monkeypatch.setattr(runners, "_service_can_run_sync", lambda t: False)

    cmap = runners.CapabilityMap()
    await cmap.refresh()
    assert "gdal" not in cmap.async_types  # filtered: worker not in [maps]


@pytest.mark.asyncio
async def test_multi_consumer_admits_listed_service(monkeypatch):
    async def _multi(_task_key):
        return ["catalog", "worker"]
    monkeypatch.setattr(runners, "_placement_consumers", _multi)
    monkeypatch.setattr(runners, "_SERVICE_NAME", "worker")
    monkeypatch.setattr(runners, "get_loaded_task_types", lambda: ["gdal"])
    monkeypatch.setattr(runners, "_service_can_run_async", lambda t: True)
    monkeypatch.setattr(runners, "_service_can_run_sync", lambda t: False)

    cmap = runners.CapabilityMap()
    await cmap.refresh()
    assert "gdal" in cmap.async_types  # worker is one of the listed consumers


@pytest.mark.asyncio
async def test_no_service_name_stays_claimable(monkeypatch):
    # Without a resolved service identity the filter cannot decide -> fail-open,
    # even when placement names a concrete (different) consumer list.
    async def _maps_only(_task_key):
        return ["maps"]
    monkeypatch.setattr(runners, "_placement_consumers", _maps_only)
    monkeypatch.setattr(runners, "_SERVICE_NAME", None)
    monkeypatch.setattr(runners, "get_loaded_task_types", lambda: ["gdal"])
    monkeypatch.setattr(runners, "_service_can_run_async", lambda t: True)
    monkeypatch.setattr(runners, "_service_can_run_sync", lambda t: False)

    cmap = runners.CapabilityMap()
    await cmap.refresh()
    assert "gdal" in cmap.async_types  # no service identity -> cannot filter
