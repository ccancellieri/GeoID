"""Endpoint-level tests pinning the OGC API - Records ``/items`` routing-aware
dispatch (#1047).

``get_records`` resolves the items SEARCH driver via routing and dispatches
through its streaming ``read_entities`` + ``count_entities`` contract
(``maybe_dispatch_items_to_search_driver``) for a
structural-only listing; when the helper declines (CQL ``filter`` / free-text
``q`` / read-primary PG driver) it falls through to the PostgreSQL
``stream_items`` path. Either way the response is a GeoJSON
``FeatureCollection`` with paging links and ``numberMatched``.
"""

from __future__ import annotations

from typing import List

import pytest
from geojson_pydantic import Feature as _GeoJSONFeature

import dynastore.extensions.tools.query as _query_mod
from dynastore.extensions.records.records_service import RecordsService
from dynastore.models.query_builder import QueryResponse


def _make_request(
    path: str = "/records/catalogs/cat/collections/col/items",
    query_string: bytes = b"",
):
    """Build a real Starlette ``Request`` from a minimal ASGI scope so the
    URL helpers (``get_root_url`` / ``build_pagination_links``) work."""
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "GET",
        "scheme": "http",
        "server": ("test", 80),
        "path": path,
        "raw_path": path.encode(),
        "query_string": query_string,
        "headers": [(b"host", b"test")],
        "root_path": "",
    }
    return Request(scope)


def _feature(fid: str) -> _GeoJSONFeature:
    return _GeoJSONFeature(
        type="Feature", id=fid, geometry=None, properties={"title": fid}
    )


class _FakeCatalogs:
    """Fake CatalogsProtocol / ItemsProtocol for the records read path."""

    def __init__(self, stream_features: List[_GeoJSONFeature], total: int):
        self._stream_features = stream_features
        self._total = total
        self.stream_called = False

    async def get_collection(self, catalog_id, collection_id, lang="en"):
        return {"id": collection_id}

    async def get_collection_config(self, catalog_id, collection_id, ctx=None):
        return None

    async def stream_items(self, **kwargs):
        self.stream_called = True

        async def _gen():
            for f in self._stream_features:
                yield f

        return QueryResponse(
            items=_gen(),
            total_count=self._total,
            catalog_id=kwargs["catalog_id"],
            collection_id=kwargs["collection_id"],
        )


def _build_query_response(features: List[_GeoJSONFeature], total: int, cat, col):
    async def _gen():
        for f in features:
            yield f

    return QueryResponse(
        items=_gen(), total_count=total, catalog_id=cat, collection_id=col,
    )


@pytest.mark.asyncio
async def test_get_records_uses_search_dispatch_when_available(monkeypatch):
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1")], total=99)

    async def _get_catalogs():
        return catalogs

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)

    dispatched = [_feature("es-1"), _feature("es-2")]

    async def _fake_dispatch(**kwargs):
        return _build_query_response(
            dispatched, total=2,
            cat=kwargs["catalog_id"], col=kwargs["collection_id"],
        )

    monkeypatch.setattr(
        _query_mod, "maybe_dispatch_items_to_search_driver", _fake_dispatch
    )

    resp = await svc.get_records(
        request=_make_request(),
        catalog_id="cat", collection_id="col",
        conn=None, limit=10, offset=0, filter=None, sortby=None, q=None,
    )

    import json
    body = json.loads(bytes(resp.body))
    assert body["type"] == "FeatureCollection"
    assert body["numberMatched"] == 2
    assert body["numberReturned"] == 2
    assert {f["id"] for f in body["features"]} == {"es-1", "es-2"}
    # The search driver served the listing — the PG stream path was skipped.
    assert catalogs.stream_called is False


@pytest.mark.asyncio
async def test_get_records_falls_back_to_pg_when_dispatch_declines(monkeypatch):
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1")], total=1)

    async def _get_catalogs():
        return catalogs

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)

    async def _decline(**kwargs):
        return None

    monkeypatch.setattr(
        _query_mod, "maybe_dispatch_items_to_search_driver", _decline
    )

    resp = await svc.get_records(
        request=_make_request(),
        catalog_id="cat", collection_id="col",
        conn=None, limit=10, offset=0, filter=None, sortby=None, q=None,
    )

    import json
    body = json.loads(bytes(resp.body))
    assert body["numberMatched"] == 1
    assert [f["id"] for f in body["features"]] == ["pg-1"]
    assert catalogs.stream_called is True


@pytest.mark.asyncio
async def test_get_records_skips_dispatch_for_cql_filter(monkeypatch):
    """A CQL ``filter`` must not consult the search dispatch — PG path only."""
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1")], total=1)

    async def _get_catalogs():
        return catalogs

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)

    called = {"dispatch": False}

    async def _spy(**kwargs):
        called["dispatch"] = True
        return None

    monkeypatch.setattr(
        _query_mod, "maybe_dispatch_items_to_search_driver", _spy
    )

    await svc.get_records(
        request=_make_request(),
        catalog_id="cat", collection_id="col",
        conn=None, limit=10, offset=0,
        filter="title = 'x'", sortby=None, q=None,
    )

    assert called["dispatch"] is False  # short-circuited before dispatch
    assert catalogs.stream_called is True


@pytest.mark.asyncio
async def test_get_records_skips_dispatch_for_free_text_q(monkeypatch):
    """Free-text ``q`` folds into a CQL ILIKE → PG path, never dispatch."""
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1")], total=1)

    async def _get_catalogs():
        return catalogs

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)

    called = {"dispatch": False}

    async def _spy(**kwargs):
        called["dispatch"] = True
        return None

    monkeypatch.setattr(
        _query_mod, "maybe_dispatch_items_to_search_driver", _spy
    )

    await svc.get_records(
        request=_make_request(),
        catalog_id="cat", collection_id="col",
        conn=None, limit=10, offset=0, filter=None, sortby=None, q="rainfall",
    )

    assert called["dispatch"] is False
    assert catalogs.stream_called is True
