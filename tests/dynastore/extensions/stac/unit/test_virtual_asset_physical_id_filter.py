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

"""Unit tests: stac_virtual.get_virtual_asset_items resolves the logical
asset_id to its physical_id and uses that as the sidecar filter value (#2296).

Scenarios covered:
1. Resolver returns a physical_id → PG sidecar filter uses the physical_id,
   ES dispatch still gets the logical id.
2. Resolver returns None (asset not yet landed) → both filters fall back to
   the logical id so behavior degrades gracefully.
3. After a rename the physical_id stays the same → item is still found under
   the new logical asset_id.
4. Protocol unavailable (get_protocol returns None) → no crash, logical id fallback.

No live DB required; all collaborators are patched.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import dynastore.extensions.stac.stac_virtual as stac_virtual_mod
from dynastore.extensions.stac.stac_service import STACService

_RESOLVE_VIS = "dynastore.models.protocols.visibility.resolve_collection_listing_ids"

_LOGICAL_ID = "my-asset-logical-name"
_PHYSICAL_ID = "01900000-feed-cafe-feed-cafefeedcafe"
_RENAMED_LOGICAL_ID = "my-asset-new-name"


def _request(path: str = "/virtual/assets") -> SimpleNamespace:
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


def _svc() -> STACService:
    return STACService.__new__(STACService)


async def _empty_aiter():
    return
    yield  # pragma: no cover


def _stub_query_res(total_count: int = 0):
    return SimpleNamespace(items=_empty_aiter(), total_count=total_count)


# ---------------------------------------------------------------------------
# Shared patch helper
# ---------------------------------------------------------------------------


def _patch_collaborators(monkeypatch, svc: STACService):
    """Patch the collaborators that are not under test."""
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)
    monkeypatch.setattr(svc, "_get_stac_config", AsyncMock(return_value=SimpleNamespace()))
    catalogs_stub = SimpleNamespace(
        get_collection_config=AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(svc, "_get_catalogs_service", AsyncMock(return_value=catalogs_stub))

    import dynastore.extensions.tools.query as query_mod
    monkeypatch.setattr(
        query_mod,
        "maybe_dispatch_items_to_search_driver",
        AsyncMock(return_value=None),  # force PG path
    )
    import dynastore.modules.storage.access_scope as access_mod
    monkeypatch.setattr(
        access_mod, "collection_uses_pg_access_envelope", AsyncMock(return_value=False)
    )


# ---------------------------------------------------------------------------
# 1. Physical_id used for PG sidecar filter when resolver succeeds
# ---------------------------------------------------------------------------


async def test_pg_filter_uses_physical_id_when_resolved(monkeypatch):
    """get_virtual_asset_items must build the PG sidecar filter with the
    resolved physical_id, not the logical asset_id."""
    svc = _svc()
    _patch_collaborators(monkeypatch, svc)

    # Items protocol stub — records the QueryRequest it receives.
    captured_requests: list = []

    async def _stub_stream_items(_catalog, _coll, *, request, ctx=None, consumer=None):
        captured_requests.append(request)
        return _stub_query_res()

    items_stub = SimpleNamespace(stream_items=_stub_stream_items)

    # get_protocol returns:
    # - AssetsProtocol → resolver stub (returns physical_id)
    # - ItemsProtocol  → items_stub
    def _get_proto(proto):
        from dynastore.models.protocols import AssetsProtocol, ItemsProtocol
        if proto is AssetsProtocol:
            assets = AsyncMock()
            assets.resolve_asset_physical_id = AsyncMock(return_value=_PHYSICAL_ID)
            return assets
        if proto is ItemsProtocol:
            return items_stub
        return None

    monkeypatch.setattr(stac_virtual_mod, "get_protocol", _get_proto)

    with patch(_RESOLVE_VIS, AsyncMock(return_value=None)):
        await svc.get_virtual_asset_items(
            catalog_id="mycat",
            collection_id="mycoll",
            asset_id=_LOGICAL_ID,
            request=_request(),
            engine=object(),
            limit=10,
            offset=0,
            language="en",
        )

    assert len(captured_requests) == 1, "stream_items must be called once"
    req = captured_requests[0]
    assert req.filters, "QueryRequest must have at least one filter"
    pg_filter = req.filters[0]
    assert pg_filter.value == _PHYSICAL_ID, (
        f"PG sidecar filter must use the physical_id '{_PHYSICAL_ID}', "
        f"got '{pg_filter.value}'"
    )


# ---------------------------------------------------------------------------
# 2. Graceful fallback when resolver returns None
# ---------------------------------------------------------------------------


async def test_pg_filter_falls_back_to_logical_id_when_resolver_returns_none(monkeypatch):
    """When the resolver returns None the PG sidecar filter must use the
    logical asset_id so the endpoint degrades gracefully."""
    svc = _svc()
    _patch_collaborators(monkeypatch, svc)

    captured_requests: list = []

    async def _stub_stream_items(_catalog, _coll, *, request, ctx=None, consumer=None):
        captured_requests.append(request)
        return _stub_query_res()

    items_stub = SimpleNamespace(stream_items=_stub_stream_items)

    def _get_proto(proto):
        from dynastore.models.protocols import AssetsProtocol, ItemsProtocol
        if proto is AssetsProtocol:
            assets = AsyncMock()
            assets.resolve_asset_physical_id = AsyncMock(return_value=None)
            return assets
        if proto is ItemsProtocol:
            return items_stub
        return None

    monkeypatch.setattr(stac_virtual_mod, "get_protocol", _get_proto)

    with patch(_RESOLVE_VIS, AsyncMock(return_value=None)):
        await svc.get_virtual_asset_items(
            catalog_id="mycat",
            collection_id="mycoll",
            asset_id=_LOGICAL_ID,
            request=_request(),
            engine=object(),
            limit=10,
            offset=0,
            language="en",
        )

    assert len(captured_requests) == 1
    pg_filter = captured_requests[0].filters[0]
    assert pg_filter.value == _LOGICAL_ID, (
        "When resolver returns None the PG filter must fall back to the logical id"
    )


# ---------------------------------------------------------------------------
# 3. After a rename the physical_id stays the same → item still found
# ---------------------------------------------------------------------------


async def test_physical_id_unchanged_after_rename(monkeypatch):
    """After renaming an asset (logical id changes) the physical_id must be
    identical to what was stamped at write time — the virtual-asset→items
    resolution still finds the item."""
    svc = _svc()

    calls_before: list = []
    calls_after: list = []

    async def _stream_before(_cat, _coll, *, request, ctx=None, consumer=None):
        calls_before.append(request.filters[0].value)
        return _stub_query_res()

    async def _stream_after(_cat, _coll, *, request, ctx=None, consumer=None):
        calls_after.append(request.filters[0].value)
        return _stub_query_res()

    for items_stub, captured in [
        (SimpleNamespace(stream_items=_stream_before), calls_before),
        (SimpleNamespace(stream_items=_stream_after), calls_after),
    ]:
        _patch_collaborators(monkeypatch, svc)

        def _get_proto(proto, _items=items_stub):
            from dynastore.models.protocols import AssetsProtocol, ItemsProtocol
            if proto is AssetsProtocol:
                assets = AsyncMock()
                # Resolver always maps → the same physical_id regardless of
                # which logical name is passed (before or after rename).
                assets.resolve_asset_physical_id = AsyncMock(return_value=_PHYSICAL_ID)
                return assets
            if proto is ItemsProtocol:
                return _items
            return None

        monkeypatch.setattr(stac_virtual_mod, "get_protocol", _get_proto)
        logical_id = _LOGICAL_ID if captured is calls_before else _RENAMED_LOGICAL_ID

        import dynastore.extensions.tools.query as query_mod
        monkeypatch.setattr(
            query_mod,
            "maybe_dispatch_items_to_search_driver",
            AsyncMock(return_value=None),
        )

        with patch(_RESOLVE_VIS, AsyncMock(return_value=None)):
            await svc.get_virtual_asset_items(
                catalog_id="mycat",
                collection_id="mycoll",
                asset_id=logical_id,
                request=_request(),
                engine=object(),
                limit=10,
                offset=0,
                language="en",
            )

    assert len(calls_before) == 1 and len(calls_after) == 1
    assert calls_before[0] == calls_after[0] == _PHYSICAL_ID, (
        "The PG sidecar filter value must be the same physical_id before "
        "and after an asset rename"
    )


# ---------------------------------------------------------------------------
# 4. Protocol unavailable → no crash, logical id fallback
# ---------------------------------------------------------------------------


async def test_no_crash_when_assets_protocol_unavailable(monkeypatch):
    """When get_protocol(AssetsProtocol) returns None the endpoint must not
    crash — it falls back to the logical id filter."""
    svc = _svc()
    _patch_collaborators(monkeypatch, svc)

    captured_requests: list = []

    async def _stub_stream_items(_cat, _coll, *, request, ctx=None, consumer=None):
        captured_requests.append(request)
        return _stub_query_res()

    items_stub = SimpleNamespace(stream_items=_stub_stream_items)

    def _get_proto(proto):
        from dynastore.models.protocols import AssetsProtocol, ItemsProtocol
        if proto is AssetsProtocol:
            return None  # protocol not available
        if proto is ItemsProtocol:
            return items_stub
        return None

    monkeypatch.setattr(stac_virtual_mod, "get_protocol", _get_proto)

    with patch(_RESOLVE_VIS, AsyncMock(return_value=None)):
        result = await svc.get_virtual_asset_items(
            catalog_id="mycat",
            collection_id="mycoll",
            asset_id=_LOGICAL_ID,
            request=_request(),
            engine=object(),
            limit=10,
            offset=0,
            language="en",
        )

    assert result is not None
    assert len(captured_requests) == 1
    pg_filter = captured_requests[0].filters[0]
    assert pg_filter.value == _LOGICAL_ID, (
        "When protocol is unavailable the filter must fall back to the logical id"
    )
