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
    """No routing entry for event_drain (consumers=None) → any listener-bearing
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


@pytest.mark.asyncio
async def test_event_task_key_is_event_drain():
    """EVENT_TASK_KEY must be 'event_drain' after the Phase 0 rename."""
    assert catalog_module.EVENT_TASK_KEY == "event_drain", (
        "EVENT_TASK_KEY must be 'event_drain'; 'outbox_drain' was the overloaded "
        "pre-rename value that collided with the ES index drain task_type."
    )


@pytest.mark.asyncio
async def test_legacy_event_task_key_constant_exists():
    """_LEGACY_EVENT_TASK_KEY must remain 'outbox_drain' for the one-release shim."""
    assert catalog_module._LEGACY_EVENT_TASK_KEY == "outbox_drain", (
        "_LEGACY_EVENT_TASK_KEY must be 'outbox_drain' to resolve stored routing "
        "configs written before the Phase 0 rename."
    )


@pytest.mark.asyncio
async def test_legacy_routing_key_still_resolves(monkeypatch):
    """A stored routing config that uses the old 'outbox_drain' key must still
    pin the event consumer correctly via the legacy shim in is_event_consumer().
    """
    async def _consumers(task_key):
        # Only the legacy key has an entry (simulating an old stored config).
        return ["worker"] if task_key == catalog_module._LEGACY_EVENT_TASK_KEY else None

    monkeypatch.setattr(catalog_module, "_routed_consumers", _consumers)

    monkeypatch.setattr(catalog_module, "_service_name", lambda: "worker")
    assert await catalog_module.is_event_consumer() is True

    monkeypatch.setattr(catalog_module, "_service_name", lambda: "catalog")
    assert await catalog_module.is_event_consumer() is False
