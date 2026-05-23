"""Endpoint-level tests pinning the OGC API - Features ``/items`` routing-aware
dispatch (#1047).

``get_items`` resolves the items SEARCH driver via routing and dispatches
through its streaming ``read_entities`` + ``count_entities`` contract
(``maybe_dispatch_items_to_search_driver``) for a
structural-only listing; when the helper declines (CQL ``filter`` / shorthand
attribute filter / non-4326 CRS / read-primary PG driver) it falls through to
the PostgreSQL ``stream_items`` path. Either way the response is a streamed
GeoJSON ``FeatureCollection``.
"""

from __future__ import annotations

import json
from typing import List

import pytest

import dynastore.extensions.tools.query as _query_mod
from dynastore.extensions.features.features_config import FeaturesPluginConfig
from dynastore.extensions.features.features_service import OGCFeaturesService
from dynastore.extensions.tools.formatters import OutputFormatEnum
from dynastore.models.ogc import Feature as _OGCFeature
from dynastore.models.query_builder import QueryResponse


def _make_request(
    path: str = "/features/catalogs/cat/collections/col/items",
    query_string: bytes = b"",
):
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


def _feature(fid: str) -> _OGCFeature:
    return _OGCFeature(
        type="Feature", id=fid, geometry=None, properties={"datetime": "2024-01-01T00:00:00Z"}
    )


class _FakeCatalogs:
    def __init__(self, stream_features: List[_OGCFeature], total: int):
        self._stream_features = stream_features
        self._total = total
        self.stream_called = False

    async def get_collection(self, catalog_id, collection_id, lang="en"):
        return {"id": collection_id}

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


def _wire(monkeypatch, svc, catalogs):
    async def _get_catalogs():
        return catalogs

    async def _get_configs():
        class _Cfg:
            async def get_config(self, cls, catalog_id=None, ctx=None):
                # cache_on_demand defaults False → no storage cache path
                return FeaturesPluginConfig()

        return _Cfg()

    async def _get_storage():
        return None

    async def _resolve_crs(conn, catalog_id, crs):
        return None  # default 4326

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)
    monkeypatch.setattr(svc, "_get_configs_service", _get_configs, raising=False)
    monkeypatch.setattr(svc, "_get_storage_service", _get_storage, raising=False)
    monkeypatch.setattr(svc, "_resolve_crs_srid", _resolve_crs, raising=False)


async def _read_body(resp) -> bytes:
    chunks = []
    async for chunk in resp.body_iterator:
        chunks.append(chunk if isinstance(chunk, bytes) else chunk.encode())
    return b"".join(chunks)


@pytest.mark.asyncio
async def test_get_items_uses_search_dispatch_when_available(monkeypatch):
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1")], total=99)
    _wire(monkeypatch, svc, catalogs)

    dispatched = [_feature("es-1"), _feature("es-2")]

    captured = {}

    async def _fake_dispatch(**kwargs):
        captured.update(kwargs)

        async def _gen():
            for f in dispatched:
                yield f

        return QueryResponse(
            items=_gen(), total_count=2,
            catalog_id=kwargs["catalog_id"], collection_id=kwargs["collection_id"],
        )

    monkeypatch.setattr(
        _query_mod, "maybe_dispatch_items_to_search_driver", _fake_dispatch
    )

    resp = await svc.get_items(
        request=_make_request(), catalog_id="cat", collection_id="col",
        conn=None, limit=10, offset=0, bbox=None, datetime_param=None,
        filter=None, filter_lang="cql2-text", crs=None, bbox_crs=None,
        sortby=None, f=OutputFormatEnum.GEOJSON,
    )
    body = json.loads(await _read_body(resp))
    assert body["type"] == "FeatureCollection"
    assert {f["id"] for f in body["features"]} == {"es-1", "es-2"}
    assert catalogs.stream_called is False
    # collection threaded correctly to the dispatch helper
    assert captured["catalog_id"] == "cat"
    assert captured["collection_id"] == "col"


@pytest.mark.asyncio
async def test_get_items_threads_bbox_and_datetime_to_dispatch(monkeypatch):
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    captured = {}

    async def _fake_dispatch(**kwargs):
        captured.update(kwargs)

        async def _gen():
            if False:
                yield  # pragma: no cover

        return QueryResponse(
            items=_gen(), total_count=0,
            catalog_id=kwargs["catalog_id"], collection_id=kwargs["collection_id"],
        )

    monkeypatch.setattr(
        _query_mod, "maybe_dispatch_items_to_search_driver", _fake_dispatch
    )

    await svc.get_items(
        request=_make_request(), catalog_id="cat", collection_id="col",
        conn=None, limit=25, offset=50, bbox="1,2,3,4",
        datetime_param="2024-01-01/2024-12-31",
        filter=None, filter_lang="cql2-text", crs=None, bbox_crs=None,
        sortby=None, f=OutputFormatEnum.GEOJSON,
    )
    assert captured["bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert captured["datetime"] == "2024-01-01/2024-12-31"
    assert captured["limit"] == 25
    assert captured["offset"] == 50


@pytest.mark.asyncio
async def test_get_items_falls_back_to_pg_when_dispatch_declines(monkeypatch):
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1")], total=1)
    _wire(monkeypatch, svc, catalogs)

    async def _decline(**kwargs):
        return None

    monkeypatch.setattr(
        _query_mod, "maybe_dispatch_items_to_search_driver", _decline
    )

    resp = await svc.get_items(
        request=_make_request(), catalog_id="cat", collection_id="col",
        conn=None, limit=10, offset=0, bbox=None, datetime_param=None,
        filter=None, filter_lang="cql2-text", crs=None, bbox_crs=None,
        sortby=None, f=OutputFormatEnum.GEOJSON,
    )
    body = json.loads(await _read_body(resp))
    assert [f["id"] for f in body["features"]] == ["pg-1"]
    assert catalogs.stream_called is True


@pytest.mark.asyncio
async def test_get_items_skips_dispatch_for_cql_filter(monkeypatch):
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1")], total=1)
    _wire(monkeypatch, svc, catalogs)

    called = {"dispatch": False}

    async def _spy(**kwargs):
        called["dispatch"] = True
        return None

    monkeypatch.setattr(
        _query_mod, "maybe_dispatch_items_to_search_driver", _spy
    )

    await svc.get_items(
        request=_make_request(), catalog_id="cat", collection_id="col",
        conn=None, limit=10, offset=0, bbox=None, datetime_param=None,
        filter="prop = 'x'", filter_lang="cql2-text", crs=None, bbox_crs=None,
        sortby=None, f=OutputFormatEnum.GEOJSON,
    )
    assert called["dispatch"] is False
    assert catalogs.stream_called is True


@pytest.mark.asyncio
async def test_get_items_skips_dispatch_for_non_4326_crs(monkeypatch):
    """A non-4326 output CRS reprojection is a PG-only capability → PG path."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1")], total=1)
    _wire(monkeypatch, svc, catalogs)

    async def _resolve_crs(conn, catalog_id, crs):
        return 3857 if crs else None

    monkeypatch.setattr(svc, "_resolve_crs_srid", _resolve_crs, raising=False)

    called = {"dispatch": False}

    async def _spy(**kwargs):
        called["dispatch"] = True
        return None

    monkeypatch.setattr(
        _query_mod, "maybe_dispatch_items_to_search_driver", _spy
    )

    await svc.get_items(
        request=_make_request(), catalog_id="cat", collection_id="col",
        conn=None, limit=10, offset=0, bbox=None, datetime_param=None,
        filter=None, filter_lang="cql2-text",
        crs="http://www.opengis.net/def/crs/EPSG/0/3857", bbox_crs=None,
        sortby=None, f=OutputFormatEnum.GEOJSON,
    )
    assert called["dispatch"] is False
    assert catalogs.stream_called is True
