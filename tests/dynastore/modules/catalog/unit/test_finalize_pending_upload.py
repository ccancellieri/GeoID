#    Copyright 2025 FAO
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

"""Unit tests for ``AssetService.finalize_pending_upload`` (GAP 3b).

No DB: ``managed_transaction`` and ``DQLQuery`` are stubbed so the row-
selection + activation decision logic is exercised in isolation. Covers:
- ticket_id match wins over the asset_id fallback
- asset_id fallback when no ticket matches
- no born-claimed PENDING row → returns None (caller falls back to create)
- the UPDATE binds uri / owned_by / size_bytes onto the chosen row
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog import asset_service as svc_mod
from dynastore.modules.catalog.asset_service import AssetService


@asynccontextmanager
async def _fake_txn(_engine):
    yield MagicMock(name="conn")


class _FakeDQL:
    """Captures DQLQuery construction + execute calls.

    The first ``DQLQuery(...)`` is the SELECT; subsequent ones are the UPDATE.
    ``select_rows`` is returned by the SELECT execute; UPDATE execute records
    its bind kwargs in ``update_binds``.
    """

    select_rows: list = []
    update_binds: dict = {}
    calls: list = []

    def __init__(self, sql, *, result_handler, post_processor=None):
        self._sql = sql
        type(self).calls.append(sql)
        self._is_select = sql.strip().upper().startswith("SELECT")

    async def execute(self, conn, **kwargs):
        if self._is_select:
            return list(type(self).select_rows)
        type(self).update_binds = dict(kwargs)
        return None


def _make_service() -> AssetService:
    s = AssetService(engine=MagicMock(name="engine"))
    return s


def _patch_common(service, select_rows, activated_asset):
    _FakeDQL.select_rows = select_rows
    _FakeDQL.update_binds = {}
    _FakeDQL.calls = []
    p_schema = patch.object(
        service, "_resolve_schema", AsyncMock(return_value="cat_schema")
    )
    p_txn = patch.object(svc_mod, "managed_transaction", _fake_txn)
    p_dql = patch.object(svc_mod, "DQLQuery", _FakeDQL)
    p_get = patch.object(
        service, "get_asset", AsyncMock(return_value=activated_asset)
    )
    return p_schema, p_txn, p_dql, p_get


@pytest.mark.asyncio
async def test_ticket_id_match_wins():
    service = _make_service()
    rows = [
        {"asset_id": "other", "status": "pending", "metadata": {"_upload": {"ticket_id": "T-OTHER"}}},
        {"asset_id": "target", "status": "pending", "metadata": {"_upload": {"ticket_id": "T-1"}}},
    ]
    activated = MagicMock(asset_id="target")
    p_schema, p_txn, p_dql, p_get = _patch_common(service, rows, activated)
    with p_schema, p_txn, p_dql, p_get:
        result = await service.finalize_pending_upload(
            "cat1", uri="file:///x", owned_by="local",
            ticket_id="T-1", asset_id="ignored_when_ticket_matches",
            size_bytes=42,
        )
    assert result is activated
    assert _FakeDQL.update_binds["asset_id"] == "target"
    assert _FakeDQL.update_binds["uri"] == "file:///x"
    assert _FakeDQL.update_binds["owned_by"] == "local"
    assert _FakeDQL.update_binds["size_bytes"] == 42


@pytest.mark.asyncio
async def test_asset_id_fallback_when_no_ticket_match():
    service = _make_service()
    rows = [
        {"asset_id": "a1", "status": "pending", "metadata": {}},
        {"asset_id": "a2", "status": "pending", "metadata": {}},
    ]
    activated = MagicMock(asset_id="a2")
    p_schema, p_txn, p_dql, p_get = _patch_common(service, rows, activated)
    with p_schema, p_txn, p_dql, p_get:
        result = await service.finalize_pending_upload(
            "cat1", uri="file:///y", owned_by="local",
            ticket_id="NO-SUCH-TICKET", asset_id="a2",
        )
    assert result is activated
    assert _FakeDQL.update_binds["asset_id"] == "a2"


@pytest.mark.asyncio
async def test_no_pending_row_returns_none():
    service = _make_service()
    activated = MagicMock(asset_id="never")
    p_schema, p_txn, p_dql, p_get = _patch_common(service, [], activated)
    with p_schema, p_txn, p_dql, p_get:
        result = await service.finalize_pending_upload(
            "cat1", uri="file:///z", owned_by="local",
            ticket_id="T", asset_id="x",
        )
    assert result is None
    # No UPDATE should have run.
    assert _FakeDQL.update_binds == {}


@pytest.mark.asyncio
async def test_no_correlation_match_returns_none():
    """Rows exist but neither ticket_id nor asset_id matches → None."""
    service = _make_service()
    rows = [{"asset_id": "a1", "status": "pending", "metadata": {}}]
    activated = MagicMock(asset_id="a1")
    p_schema, p_txn, p_dql, p_get = _patch_common(service, rows, activated)
    with p_schema, p_txn, p_dql, p_get:
        result = await service.finalize_pending_upload(
            "cat1", uri="file:///z", owned_by="local",
            ticket_id="NOPE", asset_id="ALSO-NOPE",
        )
    assert result is None
    assert _FakeDQL.update_binds == {}


@pytest.mark.asyncio
async def test_no_engine_returns_none():
    service = AssetService(engine=None)
    result = await service.finalize_pending_upload(
        "cat1", uri="file:///z", owned_by="local", asset_id="a1",
    )
    assert result is None


@pytest.mark.asyncio
async def test_finalize_strips_leaked_upload_blob():
    """The activation UPDATE must clear ``metadata._upload`` so the leaked
    resumable upload URL / ticket_id / expires_at don't persist post-finalize."""
    service = _make_service()
    rows = [
        {"asset_id": "target", "status": "pending", "metadata": {"_upload": {"ticket_id": "T-1"}}},
    ]
    activated = MagicMock(asset_id="target")
    p_schema, p_txn, p_dql, p_get = _patch_common(service, rows, activated)
    with p_schema, p_txn, p_dql, p_get:
        await service.finalize_pending_upload(
            "cat1", uri="file:///x", owned_by="local",
            ticket_id="T-1", size_bytes=42,
        )
    update_sql = next(s for s in _FakeDQL.calls if s.strip().upper().startswith("UPDATE"))
    assert "metadata - '_upload'" in update_sql
