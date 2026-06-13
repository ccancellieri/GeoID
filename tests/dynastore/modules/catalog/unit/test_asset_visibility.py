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

"""Unit tests for the asset listing visibility and direct-get 404 contract.

Covers the five cases required by the issue spec:

(a) Asset listing filtered to visible set — only ids in the allowlist reach SQL.
(b) Empty visible set → no assets returned, no DB call.
(c) None resolver (IAM off) → unfiltered, existing query path unchanged.
(d) Direct asset GET for an unseen asset → driver returns None (404 at HTTP layer).
(e) Direct asset GET for a visible asset → driver fetches from DB normally.

DB interactions are mocked; no PostgreSQL required.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver


# Module paths for monkeypatching.
# The pg_asset_driver uses a local import from dynastore.models.protocols.visibility
# inside each method, so we patch the function at its definition site in the
# visibility module — all callers that do `from dynastore.models.protocols.visibility
# import resolve_asset_listing_ids` inside their function body will resolve the
# already-imported name via the module dict, which is where monkeypatch lands.
_VIS_MODULE = "dynastore.models.protocols.visibility"
_RESOLVE_ASSET = f"{_VIS_MODULE}.resolve_asset_listing_ids"
_MANAGED_TX = "dynastore.modules.catalog.drivers.pg_asset_driver.managed_transaction"


@asynccontextmanager
async def _null_tx(_resource):
    yield MagicMock()


def _make_driver() -> AssetPostgresqlDriver:
    engine = MagicMock()
    return AssetPostgresqlDriver(engine=engine)


def _fake_asset_row(asset_id: str) -> Dict[str, Any]:
    return {
        "asset_id": asset_id,
        "catalog_id": "mycat",
        "collection_id": None,
        "asset_type": "image/tiff",
        "kind": "physical",
        "status": "active",
        "filename": f"{asset_id}.tif",
        "href": None,
        "uri": None,
        "content_hash": None,
        "size_bytes": None,
        "created_at": None,
        "updated_at": None,
        "metadata": {},
        "owned_by": None,
    }


# ---------------------------------------------------------------------------
# (a) Asset listing — filtered to visible set
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_assets_filtered_to_visible_set(monkeypatch):
    """When visible_ids={a1,a2}, the query must include asset_id = ANY(:visible_asset_ids)
    and the SQL must only be executed (not short-circuited)."""
    captured_params: Dict[str, Any] = {}

    async def _fake_execute(conn, **kwargs):
        captured_params.update(kwargs)
        return [_fake_asset_row("a1"), _fake_asset_row("a2")]

    monkeypatch.setattr(
        _RESOLVE_ASSET,
        AsyncMock(return_value=frozenset({"a1", "a2"})),
    )

    driver = _make_driver()
    with patch.object(driver, "_resolve_schema", AsyncMock(return_value="s_mycat")), \
         patch(_MANAGED_TX, side_effect=_null_tx), \
         patch(
             "dynastore.modules.db_config.query_executor.DQLQuery.execute",
             new=AsyncMock(side_effect=_fake_execute),
         ):
        result = await driver.search_assets("mycat", collection_id=None)

    assert len(result) == 2
    assert set(captured_params.get("visible_asset_ids", [])) == {"a1", "a2"}


# ---------------------------------------------------------------------------
# (b) Empty visible set → no assets, no DB call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_assets_empty_visible_set_returns_empty_without_db(monkeypatch):
    """When visible_ids is the empty frozenset, search_assets must return []
    immediately without touching the DB."""
    monkeypatch.setattr(
        _RESOLVE_ASSET,
        AsyncMock(return_value=frozenset()),
    )

    db_touched = False

    @asynccontextmanager
    async def _spy_tx(_resource):
        nonlocal db_touched
        db_touched = True
        yield MagicMock()

    driver = _make_driver()
    with patch.object(driver, "_resolve_schema", AsyncMock(return_value="s_mycat")), \
         patch(_MANAGED_TX, side_effect=_spy_tx):
        result = await driver.search_assets("mycat", collection_id=None)

    assert result == []
    assert not db_touched, "DB must not be touched when visible set is empty"


# ---------------------------------------------------------------------------
# (c) None resolver (IAM off) → unfiltered
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_assets_iam_off_unfiltered(monkeypatch):
    """When resolve_asset_listing_ids returns None (IAM off), the query runs
    without any visible_ids clause."""
    captured_params: Dict[str, Any] = {}

    async def _fake_execute(conn, **kwargs):
        captured_params.update(kwargs)
        return []

    monkeypatch.setattr(
        _RESOLVE_ASSET,
        AsyncMock(return_value=None),
    )

    driver = _make_driver()
    with patch.object(driver, "_resolve_schema", AsyncMock(return_value="s_mycat")), \
         patch(_MANAGED_TX, side_effect=_null_tx), \
         patch(
             "dynastore.modules.db_config.query_executor.DQLQuery.execute",
             new=AsyncMock(side_effect=_fake_execute),
         ):
        result = await driver.search_assets("mycat", collection_id=None)

    assert result == []
    assert "visible_asset_ids" not in captured_params, (
        "No-context path must not inject visible_asset_ids into the query"
    )


# ---------------------------------------------------------------------------
# (d) Direct asset GET — unseen → returns None (404)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_asset_unseen_returns_none_without_db(monkeypatch):
    """When visible_ids does not contain the requested asset_id, get_asset must
    return None immediately without touching the DB."""
    monkeypatch.setattr(
        _RESOLVE_ASSET,
        AsyncMock(return_value=frozenset({"other-asset"})),
    )

    db_touched = False

    @asynccontextmanager
    async def _spy_tx(_resource):
        nonlocal db_touched
        db_touched = True
        yield MagicMock()

    driver = _make_driver()
    with patch.object(driver, "_resolve_schema", AsyncMock(return_value="s_mycat")), \
         patch(_MANAGED_TX, side_effect=_spy_tx):
        result = await driver.get_asset("mycat", "secret-asset", collection_id=None)

    assert result is None
    assert not db_touched, "DB must not be touched when asset is not in visible set"


@pytest.mark.asyncio
async def test_get_asset_empty_visible_set_returns_none(monkeypatch):
    """An empty visible frozenset for assets also yields None (404)."""
    monkeypatch.setattr(
        _RESOLVE_ASSET,
        AsyncMock(return_value=frozenset()),
    )

    driver = _make_driver()
    with patch.object(driver, "_resolve_schema", AsyncMock(return_value="s_mycat")):
        result = await driver.get_asset("mycat", "any-asset", collection_id=None)

    assert result is None


# ---------------------------------------------------------------------------
# (e) Direct asset GET — visible → normal DB path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_asset_visible_fetches_from_db(monkeypatch):
    """When visible_ids contains the requested asset_id, get_asset proceeds to
    query the DB and returns the row."""
    monkeypatch.setattr(
        _RESOLVE_ASSET,
        AsyncMock(return_value=frozenset({"asset-1", "asset-2"})),
    )

    fake_row = _fake_asset_row("asset-1")

    driver = _make_driver()
    with patch.object(driver, "_resolve_schema", AsyncMock(return_value="s_mycat")), \
         patch(_MANAGED_TX, side_effect=_null_tx), \
         patch(
             "dynastore.modules.db_config.query_executor.DQLQuery.execute",
             new=AsyncMock(return_value=fake_row),
         ):
        result = await driver.get_asset("mycat", "asset-1", collection_id=None)

    assert result is fake_row


@pytest.mark.asyncio
async def test_get_asset_iam_off_fetches_from_db(monkeypatch):
    """When resolve_asset_listing_ids returns None (IAM off), get_asset fetches
    from the DB without any visibility gate."""
    monkeypatch.setattr(
        _RESOLVE_ASSET,
        AsyncMock(return_value=None),
    )

    fake_row = _fake_asset_row("asset-x")

    driver = _make_driver()
    with patch.object(driver, "_resolve_schema", AsyncMock(return_value="s_mycat")), \
         patch(_MANAGED_TX, side_effect=_null_tx), \
         patch(
             "dynastore.modules.db_config.query_executor.DQLQuery.execute",
             new=AsyncMock(return_value=fake_row),
         ):
        result = await driver.get_asset("mycat", "asset-x", collection_id=None)

    assert result is fake_row
