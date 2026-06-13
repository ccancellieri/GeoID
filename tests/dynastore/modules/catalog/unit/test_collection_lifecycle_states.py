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

"""Transitional lifecycle states — PROVISIONING / DELETING (#2066).

Covers the pure logic without a real DB:

1. ``_get_lifecycle`` precedence — a ``lifecycle_status`` overlay outranks
   ``deleted_at`` (a row can be DELETING even while ``deleted_at`` is set).
2. The provisioning finalizer always runs — a collection born PROVISIONING is
   never stranded in that write-gated state even if the async init window has
   no initializers or one of them raises.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest

from dynastore.modules.catalog import collection_service as collection_service_mod
from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.modules.catalog.lifecycle_manager import (
    LifecycleContext,
    LifecycleRegistry,
)
from dynastore.models.protocols.entity_store import CollectionLifecycle
from dynastore.tools.async_utils import signal_bus


# ---------------------------------------------------------------------------
# 1. Resolution precedence (fallback path, no LIFECYCLE driver registered)
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _stub_txn(_engine):
    yield object()


def _svc_with_row(monkeypatch, row):
    """A CollectionService whose fallback registry SELECT returns ``row``."""
    svc = CollectionService(engine=MagicMock())

    async def _phys_schema(_self, catalog_id, db_resource=None):
        return "phys_sch_test"

    class _DQLStub:
        def __init__(self, *_a, **_kw):
            pass

        async def execute(self, *_a, **_kw):
            return row

    monkeypatch.setattr(CollectionService, "_resolve_physical_schema", _phys_schema)
    monkeypatch.setattr(collection_service_mod, "managed_transaction", _stub_txn)
    monkeypatch.setattr(collection_service_mod, "DQLQuery", _DQLStub)
    # Force the degrade-safe fallback (no LIFECYCLE-capable driver). _get_lifecycle
    # imports get_protocols from discovery at call time, so patch it there.
    import dynastore.tools.discovery as _discovery

    monkeypatch.setattr(_discovery, "get_protocols", lambda *_a, **_k: [])
    return svc


@pytest.mark.asyncio
async def test_no_row_is_missing(monkeypatch):
    svc = _svc_with_row(monkeypatch, None)
    lc = await svc._get_lifecycle("cat", "col")
    assert lc == CollectionLifecycle.MISSING


@pytest.mark.asyncio
async def test_plain_row_is_active(monkeypatch):
    svc = _svc_with_row(monkeypatch, {"deleted_at": None, "lifecycle_status": None})
    lc = await svc._get_lifecycle("cat", "col")
    assert lc == CollectionLifecycle.ACTIVE


@pytest.mark.asyncio
async def test_deleted_at_is_tombstoned(monkeypatch):
    svc = _svc_with_row(monkeypatch, {"deleted_at": "2026-01-01", "lifecycle_status": None})
    lc = await svc._get_lifecycle("cat", "col")
    assert lc == CollectionLifecycle.TOMBSTONED


@pytest.mark.asyncio
async def test_provisioning_overlay_outranks_active(monkeypatch):
    svc = _svc_with_row(
        monkeypatch, {"deleted_at": None, "lifecycle_status": "provisioning"}
    )
    lc = await svc._get_lifecycle("cat", "col")
    assert lc == CollectionLifecycle.PROVISIONING


@pytest.mark.asyncio
async def test_deleting_overlay_outranks_deleted_at(monkeypatch):
    # Even with deleted_at set, a 'deleting' overlay wins — the hard-delete
    # purge window must read DELETING, not TOMBSTONED.
    svc = _svc_with_row(
        monkeypatch, {"deleted_at": "2026-01-01", "lifecycle_status": "deleting"}
    )
    lc = await svc._get_lifecycle("cat", "col")
    assert lc == CollectionLifecycle.DELETING


# ---------------------------------------------------------------------------
# 2. Provisioning finalizer always runs (no stranded PROVISIONING)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_has_async_collection_initializers_reflects_registration():
    reg = LifecycleRegistry()
    assert reg.has_async_collection_initializers() is False

    @reg.async_collection_initializer()
    async def _init(catalog_id, collection_id, context):  # noqa: ANN001
        return None

    assert reg.has_async_collection_initializers() is True


@pytest.mark.asyncio
async def test_finalizer_runs_with_no_initializers():
    """on_complete must fire even when no async initializers are registered —
    otherwise a collection marked PROVISIONING (because some *other* pod's
    registry had initializers) would never flip back to ACTIVE on this pod.
    """
    reg = LifecycleRegistry()
    flipped: list[bool] = []

    async def _on_complete():
        flipped.append(True)

    ctx = LifecycleContext(physical_schema="s", physical_table=None, config={})
    # Signal is awaited first inside the task; emit so it returns immediately.
    await signal_bus.emit("AFTER_COLLECTION_CREATION", identifier="col")
    task = reg.init_async_collection("cat", "col", ctx, on_complete=_on_complete)
    assert task is not None
    await task
    assert flipped == [True]


@pytest.mark.asyncio
async def test_finalizer_runs_even_if_initializer_raises():
    """A best-effort initializer failure must not strand PROVISIONING: the
    finalizer is in a ``finally`` and still flips to ACTIVE.
    """
    reg = LifecycleRegistry()
    flipped: list[bool] = []

    @reg.async_collection_initializer()
    async def _boom(catalog_id, collection_id, context):  # noqa: ANN001
        raise RuntimeError("external init exploded")

    async def _on_complete():
        flipped.append(True)

    ctx = LifecycleContext(physical_schema="s", physical_table=None, config={})
    await signal_bus.emit("AFTER_COLLECTION_CREATION", identifier="col2")
    task = reg.init_async_collection("cat", "col2", ctx, on_complete=_on_complete)
    assert task is not None
    await task
    assert flipped == [True]
