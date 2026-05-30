import pytest

from dynastore.modules.catalog import catalog_module


@pytest.mark.asyncio
async def test_is_event_consumer_uses_placement(monkeypatch):
    async def _consumers(task_key):
        return ["catalog"] if task_key == catalog_module.EVENT_TASK_KEY else None
    monkeypatch.setattr(catalog_module, "_placement_consumers", _consumers)
    monkeypatch.setattr(catalog_module, "_service_name", lambda: "catalog")
    assert await catalog_module.is_event_consumer() is True

    monkeypatch.setattr(catalog_module, "_service_name", lambda: "worker")
    assert await catalog_module.is_event_consumer() is False
