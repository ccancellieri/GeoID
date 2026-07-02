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

"""Unit tests for the virtual-surface collection-visibility gate (#2168).

Each route in StacVirtualMixin that operates on a caller-supplied
``collection_id`` must 404 when the collection is not in the current
request's visibility set — indistinguishable from a genuinely absent
collection (same contract as CatalogService.get_collection).

When the resolver returns None (IAM not active) the route passes through
to the underlying collaborators unchanged.

No database is required; all collaborators are patched.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

import dynastore.extensions.stac.stac_virtual as stac_virtual_mod
from dynastore.extensions.stac.stac_service import STACService

_RESOLVE = "dynastore.models.protocols.visibility.resolve_collection_listing_ids"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _request(path: str = "/virtual") -> SimpleNamespace:
    from starlette.datastructures import URL

    return SimpleNamespace(
        state=SimpleNamespace(
            principal="P",
            principal_id="user:alice",
            principal_role=["reader"],
        ),
        query_params={},
        url=URL(f"http://t{path}"),
    )


@asynccontextmanager
async def _fake_txn(_engine):
    yield None


def _acoro(value):
    async def _coro(*args, **kwargs):
        return value
    return _coro


def _svc() -> STACService:
    return STACService.__new__(STACService)


# ---------------------------------------------------------------------------
# get_virtual_asset_list — hidden collection → 404; IAM-off → passthrough
# ---------------------------------------------------------------------------


async def test_get_virtual_asset_list_hidden_collection_raises_404(monkeypatch):
    """When the caller's visible set does not contain collection_id, 404 is
    raised before any DB or service call."""
    svc = _svc()

    # Patch managed_transaction so any accidental passthrough doesn't hang.
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    # Make _get_catalogs_service explode if reached — proving we gate early.
    monkeypatch.setattr(
        svc,
        "_get_catalogs_service",
        AsyncMock(side_effect=AssertionError("must not reach catalogs service")),
    )

    with patch(_RESOLVE, AsyncMock(return_value=frozenset({"other-col"}))):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_virtual_asset_list(
                catalog_id="mycat",
                collection_id="secret",
                request=_request(),
                engine=object(),
                limit=10,
                offset=0,
            )

    assert exc_info.value.status_code == 404


async def test_get_virtual_asset_list_empty_visible_set_raises_404(monkeypatch):
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc, "_get_catalogs_service", AsyncMock(side_effect=AssertionError))

    with patch(_RESOLVE, AsyncMock(return_value=frozenset())):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_virtual_asset_list(
                catalog_id="mycat",
                collection_id="secret",
                request=_request(),
                engine=object(),
            )

    assert exc_info.value.status_code == 404


async def test_get_virtual_asset_list_iam_off_reaches_service(monkeypatch):
    """When resolver returns None (IAM disabled), the route falls through to
    the underlying asset/stac-config collaborators."""
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)

    stac_config = SimpleNamespace(
        asset_tracking=SimpleNamespace(enabled=False),
        default_limit=10, max_limit=1000,
    )
    monkeypatch.setattr(svc, "_get_stac_config", _acoro(stac_config))

    assets_stub = SimpleNamespace()
    catalogs_stub = SimpleNamespace(assets=assets_stub)
    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(catalogs_stub))

    # asset_tracking.enabled is False → the route raises 404 for that reason.
    # That means the service layer WAS reached, proving the visibility gate
    # passed through (IAM-off).
    with patch(_RESOLVE, AsyncMock(return_value=None)):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_virtual_asset_list(
                catalog_id="mycat",
                collection_id="anycol",
                request=_request(),
                engine=object(),
                limit=10,
            )

    assert exc_info.value.status_code == 404
    assert "tracking" in exc_info.value.detail.lower()


# ---------------------------------------------------------------------------
# get_virtual_asset_collection — hidden → 404; IAM-off → passthrough
# ---------------------------------------------------------------------------


async def test_get_virtual_asset_collection_hidden_raises_404(monkeypatch):
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc, "_get_stac_config", AsyncMock(side_effect=AssertionError("must not reach")))

    with patch(_RESOLVE, AsyncMock(return_value=frozenset({"allowed-col"}))):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_virtual_asset_collection(
                catalog_id="mycat",
                collection_id="secret",
                asset_code="a1",
                request=_request(),
                engine=object(),
            )

    assert exc_info.value.status_code == 404


async def test_get_virtual_asset_collection_iam_off_reaches_service(monkeypatch):
    """IAM off → route reaches asset-service layer (get_asset returns None → 404
    with an asset-not-found message rather than collection-not-found)."""
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc, "_get_stac_config", _acoro(SimpleNamespace()))

    # get_protocol returns an AssetsProtocol stub whose get_asset returns None.
    assets_stub = SimpleNamespace(get_asset=AsyncMock(return_value=None))
    monkeypatch.setattr(stac_virtual_mod, "get_protocol", lambda _p: assets_stub)

    with patch(_RESOLVE, AsyncMock(return_value=None)):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_virtual_asset_collection(
                catalog_id="mycat",
                collection_id="anycol",
                asset_code="no-such",
                request=_request(),
                engine=object(),
            )

    assert exc_info.value.status_code == 404
    # Asset-not-found message proves the gate passed through.
    assert "no-such" in exc_info.value.detail or "not found" in exc_info.value.detail.lower()


# ---------------------------------------------------------------------------
# get_virtual_asset_items — hidden → 404; IAM-off → passthrough
# ---------------------------------------------------------------------------


async def test_get_virtual_asset_items_hidden_raises_404(monkeypatch):
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc, "_get_catalogs_service", AsyncMock(side_effect=AssertionError))

    with patch(_RESOLVE, AsyncMock(return_value=frozenset({"other"}))):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_virtual_asset_items(
                catalog_id="mycat",
                collection_id="secret",
                asset_id="a1",
                request=_request(),
                engine=object(),
                limit=10,
                offset=0,
                language="en",
            )

    assert exc_info.value.status_code == 404


async def test_get_virtual_asset_items_iam_off_reaches_service(monkeypatch):
    """IAM off → route reaches catalogs/items layer.  We stub a short-circuit
    via maybe_dispatch_items_to_search_driver returning an empty result."""
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(
        svc, "_get_stac_config",
        _acoro(SimpleNamespace(default_limit=10, max_limit=1000)),
    )

    catalogs_stub = SimpleNamespace(
        get_collection_config=AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(catalogs_stub))
    monkeypatch.setattr(stac_virtual_mod, "get_protocol", lambda _p: SimpleNamespace())

    async def _empty_aiter():
        return
        yield  # pragma: no cover

    import dynastore.extensions.tools.query as query_mod
    monkeypatch.setattr(
        query_mod,
        "maybe_dispatch_items_to_search_driver",
        AsyncMock(return_value=SimpleNamespace(items=_empty_aiter(), total_count=0)),
    )

    # The route should complete without error (empty item list).
    with patch(_RESOLVE, AsyncMock(return_value=None)):
        result = await svc.get_virtual_asset_items(
            catalog_id="mycat",
            collection_id="anycol",
            asset_id="a1",
            request=_request(path="/virtual/assets/a1/catalogs/mycat/collections/anycol/items"),
            engine=object(),
            limit=10,
            offset=0,
            language="en",
        )

    assert result is not None


# ---------------------------------------------------------------------------
# get_virtual_hierarchy_collection — hidden → 404; IAM-off → passthrough
# ---------------------------------------------------------------------------


async def test_get_virtual_hierarchy_collection_hidden_raises_404(monkeypatch):
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(stac_virtual_mod, "get_protocol", lambda _p: None)

    with patch(_RESOLVE, AsyncMock(return_value=frozenset({"other"}))):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_virtual_hierarchy_collection(
                hierarchy_id="region",
                catalog_id="mycat",
                collection_id="secret",
                request=_request(),
                engine=object(),
            )

    assert exc_info.value.status_code == 404


async def test_get_virtual_hierarchy_collection_iam_off_reaches_service(monkeypatch):
    """IAM off → proceeds past gate; hierarchy-not-enabled check fires next."""
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)

    # get_config returns a config with hierarchy disabled → 404 for that reason.
    cfg_stub = SimpleNamespace(hierarchy=None, default_limit=10, max_limit=1000)
    config_mgr = SimpleNamespace(get_config=AsyncMock(return_value=cfg_stub))
    monkeypatch.setattr(stac_virtual_mod, "get_protocol", lambda _p: config_mgr)

    with patch(_RESOLVE, AsyncMock(return_value=None)):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_virtual_hierarchy_collection(
                hierarchy_id="region",
                catalog_id="mycat",
                collection_id="anycol",
                request=_request(),
                engine=object(),
                limit=100,
            )

    assert exc_info.value.status_code == 404
    assert "hierarchy" in exc_info.value.detail.lower()


# ---------------------------------------------------------------------------
# get_virtual_hierarchy_items — hidden → 404; IAM-off → passthrough
# ---------------------------------------------------------------------------


async def test_get_virtual_hierarchy_items_hidden_raises_404(monkeypatch):
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc, "_get_configs_service", AsyncMock(side_effect=AssertionError))

    with patch(_RESOLVE, AsyncMock(return_value=frozenset())):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_virtual_hierarchy_items(
                hierarchy_id="region",
                catalog_id="mycat",
                collection_id="secret",
                request=_request(),
                engine=object(),
                language="en",
            )

    assert exc_info.value.status_code == 404


async def test_get_virtual_hierarchy_items_iam_off_reaches_service(monkeypatch):
    """IAM off → proceeds; hierarchy-not-enabled fires (hierarchy is None)."""
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)

    cfg_stub = SimpleNamespace(hierarchy=None, default_limit=10, max_limit=1000)
    configs_stub = SimpleNamespace(get_config=AsyncMock(return_value=cfg_stub))
    monkeypatch.setattr(svc, "_get_configs_service", _acoro(configs_stub))

    with patch(_RESOLVE, AsyncMock(return_value=None)):
        with pytest.raises(HTTPException) as exc_info:
            await svc.get_virtual_hierarchy_items(
                hierarchy_id="region",
                catalog_id="mycat",
                collection_id="anycol",
                request=_request(),
                engine=object(),
                language="en",
                limit=10,
            )

    assert exc_info.value.status_code == 404
    assert "hierarchy" in exc_info.value.detail.lower()


# ---------------------------------------------------------------------------
# search_virtual_hierarchy_items — hidden → 404; IAM-off → passthrough
# ---------------------------------------------------------------------------


async def test_search_virtual_hierarchy_items_hidden_raises_404(monkeypatch):
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc, "_get_configs_service", AsyncMock(side_effect=AssertionError))

    search_req = SimpleNamespace(
        catalog_id=None, collections=None, offset=0, limit=10,
    )

    with patch(_RESOLVE, AsyncMock(return_value=frozenset({"other"}))):
        with pytest.raises(HTTPException) as exc_info:
            await svc.search_virtual_hierarchy_items(
                hierarchy_id="region",
                catalog_id="mycat",
                collection_id="secret",
                search_request=search_req,
                request=_request(),
                engine=object(),
                language="en",
            )

    assert exc_info.value.status_code == 404


async def test_search_virtual_hierarchy_items_iam_off_reaches_service(monkeypatch):
    """IAM off → proceeds past gate; hierarchy check fires next."""
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)

    cfg_stub = SimpleNamespace(hierarchy=None)
    configs_stub = SimpleNamespace(get_config=AsyncMock(return_value=cfg_stub))
    monkeypatch.setattr(svc, "_get_configs_service", _acoro(configs_stub))

    search_req = SimpleNamespace(
        catalog_id=None, collections=None, offset=0, limit=10,
    )

    with patch(_RESOLVE, AsyncMock(return_value=None)):
        with pytest.raises(HTTPException) as exc_info:
            await svc.search_virtual_hierarchy_items(
                hierarchy_id="region",
                catalog_id="mycat",
                collection_id="anycol",
                search_request=search_req,
                request=_request(),
                engine=object(),
                language="en",
            )

    assert exc_info.value.status_code == 404
    assert "hierarchy" in exc_info.value.detail.lower()


# ---------------------------------------------------------------------------
# resolve_asset_source — hidden → 404; IAM-off → passthrough
# The /assets/{asset_code}/source route redirects to (or proxies) the asset
# bytes, so a hidden collection must 404 before any asset/config resolution.
# ---------------------------------------------------------------------------


async def test_resolve_asset_source_hidden_raises_404(monkeypatch):
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    # get_protocol must never be reached once the gate fires.
    monkeypatch.setattr(
        stac_virtual_mod,
        "get_protocol",
        lambda _p: (_ for _ in ()).throw(AssertionError("must not reach")),
    )

    with patch(_RESOLVE, AsyncMock(return_value=frozenset({"allowed-col"}))):
        with pytest.raises(HTTPException) as exc_info:
            await svc.resolve_asset_source(
                catalog_id="mycat",
                collection_id="secret",
                asset_code="a1",
                request=_request(),
                engine=object(),
            )

    assert exc_info.value.status_code == 404
    assert "secret" in exc_info.value.detail


async def test_resolve_asset_source_iam_off_reaches_service(monkeypatch):
    """IAM off → gate passes through to the asset-service layer (get_asset
    returns None → asset-not-found 404, not collection-not-found)."""
    svc = _svc()
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    assets_stub = SimpleNamespace(get_asset=AsyncMock(return_value=None))
    monkeypatch.setattr(stac_virtual_mod, "get_protocol", lambda _p: assets_stub)

    with patch(_RESOLVE, AsyncMock(return_value=None)):
        with pytest.raises(HTTPException) as exc_info:
            await svc.resolve_asset_source(
                catalog_id="mycat",
                collection_id="anycol",
                asset_code="no-such",
                request=_request(),
                engine=object(),
            )

    assert exc_info.value.status_code == 404
    # Asset-not-found message proves the gate passed through.
    assert "no-such" in exc_info.value.detail or "not found" in exc_info.value.detail.lower()
