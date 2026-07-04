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

"""Per-instance liveness heartbeat — DB-free behaviour.

Pins that this fleet-wide, every-pod, every-60s writer never touches the DB
while the zombie-session reaper is disabled (the default) — only the cheap
enabled-live-check itself runs.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import dynastore.modules.db.instance_liveness as mod
from dynastore.modules.db.instance_liveness import (
    InstanceLivenessHeartbeat,
    heartbeat,
)


async def _async_true() -> bool:
    return True


async def _async_false() -> bool:
    return False


def _install_stub(monkeypatch):
    calls: list[dict] = []

    class _DQLStub:
        def __init__(self, sql, result_handler=None, **_kw):
            self.sql = sql

        async def execute(self, _conn, **params):
            calls.append({"sql": self.sql, "params": params})
            return None

    @asynccontextmanager
    async def _txn(_engine):
        yield object()

    monkeypatch.setattr(mod, "DQLQuery", _DQLStub)
    monkeypatch.setattr(mod, "background_managed_transaction", _txn)
    return calls


async def test_heartbeat_upserts_instance_id_and_service(monkeypatch):
    calls = _install_stub(monkeypatch)
    await heartbeat(object(), instance_id="abc123", service="catalog-api")

    assert len(calls) == 1
    assert "ON CONFLICT (instance_id) DO UPDATE" in calls[0]["sql"]
    assert calls[0]["params"] == {"instance_id": "abc123", "service": "catalog-api"}


async def test_tick_calls_heartbeat_with_this_process_identity(monkeypatch):
    calls = _install_stub(monkeypatch)
    monkeypatch.setattr(mod, "get_instance_id", lambda: "fixed-instance")
    monkeypatch.setattr(mod, "get_service_name", lambda: "catalog-api")
    monkeypatch.setattr(mod, "_reaper_enabled", _async_true)

    svc = InstanceLivenessHeartbeat()

    class _Ctx:
        engine = object()

    await svc.tick(_Ctx())

    assert len(calls) == 1
    assert calls[0]["params"] == {
        "instance_id": "fixed-instance",
        "service": "catalog-api",
    }


async def test_tick_is_noop_when_reaper_disabled(monkeypatch):
    calls = _install_stub(monkeypatch)
    monkeypatch.setattr(mod, "_reaper_enabled", _async_false)
    svc = InstanceLivenessHeartbeat()

    class _Ctx:
        engine = object()

    await svc.tick(_Ctx())
    assert calls == []


async def test_tick_is_noop_without_engine(monkeypatch):
    calls = _install_stub(monkeypatch)
    monkeypatch.setattr(mod, "_reaper_enabled", _async_true)
    svc = InstanceLivenessHeartbeat()

    class _Ctx:
        engine = None

    await svc.tick(_Ctx())
    assert calls == []


async def test_tick_swallows_heartbeat_failure(monkeypatch, caplog):
    monkeypatch.setattr(mod, "_reaper_enabled", _async_true)

    async def _boom(_engine, *, instance_id, service):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(mod, "heartbeat", _boom)
    svc = InstanceLivenessHeartbeat()

    class _Ctx:
        engine = object()

    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        await svc.tick(_Ctx())  # must not raise

    assert any("renew failed" in r.getMessage() for r in caplog.records)
