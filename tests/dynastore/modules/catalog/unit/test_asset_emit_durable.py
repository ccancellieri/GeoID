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

"""Unit tests for AssetService._emit_durable (#2256 gap 2).

Covers:
- _emit_durable with a caller-supplied db_resource rides that connection.
- _emit_durable with db_resource=None opens a dedicated managed_transaction
  and forwards a non-None connection to the emitter (the core fix for the
  REST single-asset path that previously skipped the outbox write).
- _emit_durable with db_resource=None and engine=None falls back to
  db_resource=None (best-effort; no crash).
- _emit_durable is a no-op when _event_emitter is None.
- create_asset with db_resource=None calls the emitter with a non-None
  db_resource (end-to-end proof of the fix via a fake engine + driver).
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Dict, List
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog import asset_service as svc_mod
from dynastore.modules.catalog.asset_service import (
    AssetEventType,
    AssetService,
    AssetBase,
)


# ---------------------------------------------------------------------------
# Fake helpers
# ---------------------------------------------------------------------------


class _FakeConn:
    """Sentinel connection object — identity is what we assert on."""


@asynccontextmanager
async def _fake_managed_transaction(_engine: Any):
    yield _FakeConn()


# ---------------------------------------------------------------------------
# _emit_durable unit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_emit_durable_with_db_resource_rides_connection():
    """When db_resource is supplied the emitter receives it directly."""
    calls: List[Dict[str, Any]] = []

    async def emitter(event_type, doc, *, db_resource):
        calls.append({"event_type": event_type, "db_resource": db_resource})

    svc = AssetService(engine=MagicMock(name="engine"), event_emitter=emitter)
    supplied_conn = _FakeConn()
    await svc._emit_durable(AssetEventType.ASSET_CREATED, {"k": "v"}, supplied_conn)

    assert len(calls) == 1
    assert calls[0]["db_resource"] is supplied_conn
    assert calls[0]["event_type"] == AssetEventType.ASSET_CREATED


@pytest.mark.asyncio
async def test_emit_durable_none_db_resource_opens_transaction():
    """When db_resource=None a dedicated transaction is opened and the
    emitter is called with a non-None connection — the fix for #2256."""
    calls: List[Dict[str, Any]] = []

    async def emitter(event_type, doc, *, db_resource):
        calls.append({"event_type": event_type, "db_resource": db_resource})

    fake_engine = MagicMock(name="engine")
    svc = AssetService(engine=fake_engine, event_emitter=emitter)

    with patch.object(svc_mod, "managed_transaction", _fake_managed_transaction):
        await svc._emit_durable(AssetEventType.ASSET_CREATED, {"k": "v"}, None)

    assert len(calls) == 1
    assert calls[0]["db_resource"] is not None
    assert isinstance(calls[0]["db_resource"], _FakeConn)


@pytest.mark.asyncio
async def test_emit_durable_none_db_resource_none_engine_falls_back():
    """No engine and no db_resource: emitter is still called with db_resource=None
    (best-effort path; no exception raised)."""
    calls: List[Dict[str, Any]] = []

    async def emitter(event_type, doc, *, db_resource):
        calls.append({"db_resource": db_resource})

    svc = AssetService(engine=None, event_emitter=emitter)
    await svc._emit_durable(AssetEventType.ASSET_CREATED, {}, None)

    assert len(calls) == 1
    assert calls[0]["db_resource"] is None


@pytest.mark.asyncio
async def test_emit_durable_no_emitter_is_noop():
    """When _event_emitter is None _emit_durable returns immediately."""
    svc = AssetService(engine=MagicMock(name="engine"), event_emitter=None)
    # Should not raise
    with patch.object(svc_mod, "managed_transaction", _fake_managed_transaction):
        await svc._emit_durable(AssetEventType.ASSET_CREATED, {}, None)


# ---------------------------------------------------------------------------
# create_asset integration: db_resource=None still drives emitter with conn
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_asset_none_db_resource_emitter_gets_non_none_conn():
    """create_asset called with db_resource=None (REST path) must drive the
    emitter with a non-None connection via _emit_durable's transaction branch."""
    emitter_calls: List[Dict[str, Any]] = []

    async def emitter(event_type, doc, *, db_resource):
        emitter_calls.append({"event_type": event_type, "db_resource": db_resource})

    fake_engine = MagicMock(name="engine")
    svc = AssetService(engine=fake_engine, event_emitter=emitter)

    _now = datetime.now(timezone.utc)

    # Minimal fake asset doc returned by the write driver
    fake_asset_dict = {
        "asset_id": "asset-1",
        "catalog_id": "cat-1",
        "collection_id": None,
        "asset_type": "ASSET",
        "kind": "physical",
        "status": "active",
        "filename": None,
        "href": None,
        "uri": None,
        "content_hash": None,
        "size_bytes": None,
        "created_at": _now,
        "updated_at": None,
        "metadata": {},
        "owned_by": None,
    }

    mock_driver = MagicMock()
    mock_driver.index_asset = AsyncMock()
    mock_driver.get_asset = AsyncMock(return_value=fake_asset_dict)

    async def fake_get_asset_driver(role, catalog_id, collection_id):
        return mock_driver

    with (
        patch.object(svc_mod, "managed_transaction", _fake_managed_transaction),
        patch(
            "dynastore.modules.storage.router.get_asset_driver",
            side_effect=fake_get_asset_driver,
        ),
        patch.object(svc, "_fan_out_asset_writes", AsyncMock()),
        patch.object(svc_mod, "log_info", AsyncMock()),
    ):
        asset = await svc.create_asset(
            catalog_id="cat-1",
            asset=AssetBase(asset_id="asset-1"),
        )

    assert asset.asset_id == "asset-1"
    assert len(emitter_calls) == 1
    assert emitter_calls[0]["event_type"] == AssetEventType.ASSET_CREATED
    # The key assertion: even though the caller passed db_resource=None,
    # the emitter received a non-None connection.
    assert emitter_calls[0]["db_resource"] is not None
    assert isinstance(emitter_calls[0]["db_resource"], _FakeConn)
