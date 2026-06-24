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

"""Unit tests for the direct-GET visibility contract.

Per the #2050 design contract: a catalog or collection the caller has no
visibility grant for must be indistinguishable from a missing one on a
direct GET — returned as 404, never 403 or 200-with-data.

CatalogService.get_catalog raises ValueError (same path as "not found") when
the caller's visible-id set does not contain the requested catalog id.
CatalogService.get_collection returns None (same path as "not found") when
the caller's visible-id set does not contain the requested collection id.

When the resolver returns None (IAM not active), both methods behave
exactly as they did before this change.

DB interactions are mocked; no PostgreSQL required.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from dynastore.modules.catalog.catalog_service import CatalogService
from dynastore.modules.catalog import catalog_service as catalog_service_mod


_VIS_MODULE = "dynastore.models.protocols.visibility"
_RESOLVE_CATALOG = f"{_VIS_MODULE}.resolve_catalog_listing_ids"
_RESOLVE_COLLECTION = f"{_VIS_MODULE}.resolve_collection_listing_ids"
_MANAGED_TX = "dynastore.modules.catalog.catalog_service.managed_transaction"
_GET_ENGINE = "dynastore.modules.catalog.catalog_service.get_catalog_engine"


@pytest.fixture(autouse=True)
def _no_external_id_db(monkeypatch):
    """Phase 2: resolution of external→internal id requires a DB call.

    All tests in this module use raw (already-internal-equivalent) ids and
    do not set up external_id mappings in the database.  Patch the two DB
    lookup helpers to return None so the resolution path falls through to
    passthrough mode (original id used unchanged) rather than opening a real
    connection.
    """
    from dynastore.modules.catalog.catalog_service import (
        _catalog_external_id_cache,
    )
    _catalog_external_id_cache.cache_clear()

    async def _absent(_self, cid: str) -> None:
        return None

    monkeypatch.setattr(
        catalog_service_mod.CatalogService,
        "_get_catalog_id_by_external_id_db",
        _absent,
    )


def _make_service() -> CatalogService:
    return CatalogService(engine=None)


def _fake_catalog(catalog_id: str) -> MagicMock:
    cat = MagicMock()
    cat.id = catalog_id
    return cat


def _fake_collection(collection_id: str) -> MagicMock:
    col = MagicMock()
    col.id = collection_id
    return col


@asynccontextmanager
async def _null_tx(_engine):
    yield MagicMock()


# ---------------------------------------------------------------------------
# get_catalog — unseen catalog → ValueError (renders as 404)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_catalog_unseen_raises_value_error(monkeypatch):
    """When visible_ids does not contain the requested catalog_id, get_catalog
    must raise ValueError — same exception as a genuine miss, ensuring the
    HTTP layer returns 404 not 403.

    Phase 2 note: resolution of external→internal id requires a DB lookup
    before the visibility check so this test no longer asserts no-DB-touch;
    it only asserts the correct ValueError is raised.
    """
    monkeypatch.setattr(_RESOLVE_CATALOG, AsyncMock(return_value=frozenset({"other"})))

    with patch(_MANAGED_TX, side_effect=_null_tx), \
         patch(_GET_ENGINE, return_value=MagicMock()):
        svc = _make_service()
        with pytest.raises(ValueError, match="not found"):
            await svc.get_catalog("secret")


@pytest.mark.asyncio
async def test_get_catalog_empty_visible_set_raises_value_error(monkeypatch):
    """An empty visible frozenset (caller may see nothing) also triggers 404."""
    monkeypatch.setattr(_RESOLVE_CATALOG, AsyncMock(return_value=frozenset()))

    svc = _make_service()
    with pytest.raises(ValueError, match="not found"):
        await svc.get_catalog("any-catalog")


# ---------------------------------------------------------------------------
# get_catalog — visible catalog → 200 (returns model)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_catalog_visible_returns_model(monkeypatch):
    """When visible_ids contains the requested catalog_id, get_catalog returns
    the model normally."""
    monkeypatch.setattr(
        _RESOLVE_CATALOG, AsyncMock(return_value=frozenset({"mycatalog"}))
    )
    fake_cat = _fake_catalog("mycatalog")

    with patch(_MANAGED_TX, side_effect=_null_tx), \
         patch(_GET_ENGINE, return_value=MagicMock()), \
         patch(
             "dynastore.modules.db_config.query_executor.DQLQuery.execute",
             new=AsyncMock(return_value=[fake_cat]),
         ):
        svc = _make_service()
        # get_catalog_model returns None when _unpack_catalog_row returns None;
        # patch _get_catalog_model_db to return the fake directly.
        with patch.object(svc, "get_catalog_model", AsyncMock(return_value=fake_cat)):
            result = await svc.get_catalog("mycatalog")

    assert result is fake_cat


# ---------------------------------------------------------------------------
# get_catalog — IAM off (resolver returns None) → unfiltered pass-through
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_catalog_iam_off_returns_model(monkeypatch):
    """When resolve_catalog_listing_ids returns None (no authorization layer),
    get_catalog must behave as it did before this change — no 404 injection."""
    monkeypatch.setattr(_RESOLVE_CATALOG, AsyncMock(return_value=None))
    fake_cat = _fake_catalog("mycatalog")

    svc = _make_service()
    with patch.object(svc, "get_catalog_model", AsyncMock(return_value=fake_cat)):
        result = await svc.get_catalog("mycatalog")

    assert result is fake_cat


@pytest.mark.asyncio
async def test_get_catalog_iam_off_missing_still_raises(monkeypatch):
    """When IAM is off and the catalog genuinely does not exist, get_catalog
    still raises ValueError as before."""
    monkeypatch.setattr(_RESOLVE_CATALOG, AsyncMock(return_value=None))

    svc = _make_service()
    with patch.object(svc, "get_catalog_model", AsyncMock(return_value=None)):
        with pytest.raises(ValueError, match="not found"):
            await svc.get_catalog("ghost")


# ---------------------------------------------------------------------------
# get_collection — unseen collection → None (renders as 404)
# ---------------------------------------------------------------------------


def _col_svc_patch(col_model_return):
    """Return a context-manager that patches CatalogService._col_svc property
    with a MagicMock whose get_collection_model is an AsyncMock returning
    ``col_model_return``.

    Phase 2: also stubs resolve_collection_id to return None (passthrough —
    no external_id mapping in unit tests) so get_collection's resolution step
    falls through to the original collection_id.
    """
    mock_col_svc = MagicMock()
    mock_col_svc.get_collection_model = AsyncMock(return_value=col_model_return)
    mock_col_svc.resolve_collection_id = AsyncMock(return_value=None)
    return patch.object(
        CatalogService,
        "_col_svc",
        new_callable=PropertyMock,
        return_value=mock_col_svc,
    )


@pytest.mark.asyncio
async def test_get_collection_unseen_returns_none(monkeypatch):
    """When visible_ids does not contain the requested collection_id,
    get_collection returns None so the HTTP layer raises 404."""
    monkeypatch.setattr(
        _RESOLVE_COLLECTION, AsyncMock(return_value=frozenset({"other-col"}))
    )

    fake_col = _fake_collection("secret-col")
    with _col_svc_patch(fake_col) as mock_prop:
        svc = _make_service()
        result = await svc.get_collection("mycatalog", "secret-col")
        mock_col_svc = mock_prop.return_value
        mock_col_svc.get_collection_model.assert_not_awaited()

    assert result is None


@pytest.mark.asyncio
async def test_get_collection_empty_visible_set_returns_none(monkeypatch):
    """An empty visible frozenset for collections also yields None (404)."""
    monkeypatch.setattr(_RESOLVE_COLLECTION, AsyncMock(return_value=frozenset()))

    fake_col = _fake_collection("x")
    with _col_svc_patch(fake_col) as mock_prop:
        svc = _make_service()
        result = await svc.get_collection("cat", "x")
        mock_col_svc = mock_prop.return_value
        mock_col_svc.get_collection_model.assert_not_awaited()

    assert result is None


# ---------------------------------------------------------------------------
# get_collection — visible collection → 200 (returns model)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_collection_visible_returns_model(monkeypatch):
    """When visible_ids contains the requested collection_id, get_collection
    delegates to the underlying service and returns its result."""
    monkeypatch.setattr(
        _RESOLVE_COLLECTION, AsyncMock(return_value=frozenset({"col1", "col2"}))
    )
    fake_col = _fake_collection("col1")

    with _col_svc_patch(fake_col) as mock_prop:
        svc = _make_service()
        result = await svc.get_collection("mycat", "col1")
        mock_col_svc = mock_prop.return_value
        mock_col_svc.get_collection_model.assert_awaited_once_with(
            "mycat", "col1", db_resource=None, hints=frozenset()
        )

    assert result is fake_col


# ---------------------------------------------------------------------------
# get_collection — IAM off (resolver returns None) → unfiltered pass-through
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_collection_iam_off_returns_model(monkeypatch):
    """When resolve_collection_listing_ids returns None (IAM off),
    get_collection passes through to the underlying service unchanged."""
    monkeypatch.setattr(_RESOLVE_COLLECTION, AsyncMock(return_value=None))
    fake_col = _fake_collection("col1")

    with _col_svc_patch(fake_col) as mock_prop:
        svc = _make_service()
        result = await svc.get_collection("mycat", "col1")
        mock_col_svc = mock_prop.return_value
        mock_col_svc.get_collection_model.assert_awaited_once()

    assert result is fake_col


@pytest.mark.asyncio
async def test_get_collection_iam_off_missing_returns_none(monkeypatch):
    """When IAM is off and the collection genuinely does not exist,
    get_collection returns None (not found), same as before."""
    monkeypatch.setattr(_RESOLVE_COLLECTION, AsyncMock(return_value=None))

    with _col_svc_patch(None):
        svc = _make_service()
        result = await svc.get_collection("cat", "ghost")

    assert result is None
