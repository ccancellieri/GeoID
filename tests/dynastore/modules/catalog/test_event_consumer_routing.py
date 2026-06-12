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
    """EVENT_TASK_KEY must be 'event_drain'."""
    assert catalog_module.EVENT_TASK_KEY == "event_drain", (
        "EVENT_TASK_KEY must be 'event_drain'; 'outbox_drain' was the overloaded "
        "pre-rename value that collided with the ES index drain task_type."
    )


@pytest.mark.asyncio
async def test_legacy_event_task_key_constant_removed():
    """_LEGACY_EVENT_TASK_KEY shim must be gone after the fixup migration."""
    assert not hasattr(catalog_module, "_LEGACY_EVENT_TASK_KEY"), (
        "_LEGACY_EVENT_TASK_KEY must be removed; stored configs are now rewritten "
        "at bootstrap by the config_seeder fixup."
    )
