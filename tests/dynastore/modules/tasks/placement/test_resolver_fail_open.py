import pytest

from dynastore.modules.tasks.placement import resolver
from dynastore.modules.tasks.placement.model import OFF_LOAD, PlacementEntry, TaskPlacementConfig


def _aval(value):
    async def _f():
        return value
    return _f


@pytest.mark.asyncio
async def test_resolved_consumers_from_config(monkeypatch):
    cfg = TaskPlacementConfig(
        placement_preset="cloud",
        placements={"gdal": PlacementEntry(consumers=["worker"], mode=OFF_LOAD)},
    )
    monkeypatch.setattr(resolver, "_load_config", _aval(cfg))
    assert await resolver.resolved_consumers("gdal") == ["worker"]


@pytest.mark.asyncio
async def test_fail_open_returns_none_on_config_error(monkeypatch):
    async def _boom():
        raise RuntimeError("registry down")
    monkeypatch.setattr(resolver, "_load_config", _boom)
    # None == "no opinion" -> caller falls back to today's capable-set derivation
    assert await resolver.resolved_consumers("gdal") is None
