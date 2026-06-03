import pytest

from dynastore.modules.catalog import catalog_module


@pytest.mark.asyncio
async def test_is_event_consumer_uses_routing(monkeypatch):
    async def _consumers(task_key):
        return ["catalog"] if task_key == catalog_module.EVENT_TASK_KEY else None
    monkeypatch.setattr(catalog_module, "_routed_consumers", _consumers)
    monkeypatch.setattr(catalog_module, "_service_name", lambda: "catalog")
    assert await catalog_module.is_event_consumer() is True

    monkeypatch.setattr(catalog_module, "_service_name", lambda: "worker")
    assert await catalog_module.is_event_consumer() is False


@pytest.mark.asyncio
async def test_silent_config_is_eligible(monkeypatch):
    """No routing entry for outbox_drain (consumers=None) → any listener-bearing
    process is eligible.  The advisory lock (not the routing config) elects
    the single drain leader, so fail-open here is correct.
    """
    async def _no_consumers(*_):
        return None  # routing is silent — no opinion

    monkeypatch.setattr(catalog_module, "_routed_consumers", _no_consumers)
    monkeypatch.setattr(catalog_module, "_service_name", lambda: "catalog")
    assert await catalog_module.is_event_consumer() is True


@pytest.mark.asyncio
async def test_explicit_consumer_list_pins_service(monkeypatch):
    """An explicit non-empty consumer list acts as a positive pin: only listed
    services are eligible; unlisted services are not.
    """
    async def _pinned_consumers(*_):
        return ["worker"]

    monkeypatch.setattr(catalog_module, "_routed_consumers", _pinned_consumers)

    monkeypatch.setattr(catalog_module, "_service_name", lambda: "worker")
    assert await catalog_module.is_event_consumer() is True

    monkeypatch.setattr(catalog_module, "_service_name", lambda: "catalog")
    assert await catalog_module.is_event_consumer() is False

    monkeypatch.setattr(catalog_module, "_service_name", lambda: "geoid")
    assert await catalog_module.is_event_consumer() is False


@pytest.mark.asyncio
async def test_unknown_service_name_with_silent_config_is_eligible(monkeypatch):
    """When the service name is unknown (None) AND routing is silent, the
    process is still eligible.  The advisory lock bounds it to one runner.
    """
    async def _no_consumers(*_):
        return None

    monkeypatch.setattr(catalog_module, "_routed_consumers", _no_consumers)
    monkeypatch.setattr(catalog_module, "_service_name", lambda: None)
    assert await catalog_module.is_event_consumer() is True


@pytest.mark.asyncio
async def test_unknown_service_name_excluded_by_explicit_list(monkeypatch):
    """An unknown service name (None) must NOT be included when the operator
    has pinned draining to a specific set — None cannot be in a string list.
    """
    async def _pinned_consumers(*_):
        return ["worker"]

    monkeypatch.setattr(catalog_module, "_routed_consumers", _pinned_consumers)
    monkeypatch.setattr(catalog_module, "_service_name", lambda: None)
    assert await catalog_module.is_event_consumer() is False
