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

"""Endpoint-level tests pinning the OGC API - Features ``/items`` listing
contract.

OGC Features ``/items`` prefers exact, full-precision geometry, so ``get_items``
deliberately does NOT take the Elasticsearch search fast-path. It parses the
query parameters into a :class:`QueryRequest`, sets ``search_dispatch=None`` and
routes through ``dispatch_or_stream_items`` with ``EXACT_READ_HINTS``. With no
pre-built search dispatch the helper streams from the items protocol
(``stream_items``); the ``EXACT_READ_HINTS`` hint makes the router pick the
exact-geometry driver (today PostgreSQL) even when a simplified-geometry
Elasticsearch driver is registered first for READ. On an ES-only catalog the
router relaxes the hint and falls back to the available reader rather than
returning empty.

These tests therefore assert what the listing handler hands to ``stream_items``:
the parsed ``QueryRequest`` (limit/offset direct; ``bbox`` and ``datetime``
translated into filter conditions; CQL into ``cql_filter``), the
``OGC_FEATURES`` consumer, and the exact-geometry hint — and that the streamed
items are what the response carries.
"""

from __future__ import annotations

import json
from typing import List

import pytest

from dynastore.extensions.features.features_config import FeaturesPluginConfig
from dynastore.extensions.features.features_service import OGCFeaturesService
from dynastore.extensions.tools.formatters import OutputFormatEnum
from dynastore.models.ogc import Feature as _OGCFeature
from dynastore.models.query_builder import QueryResponse
from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
from dynastore.modules.storage.hints import EXACT_READ_HINTS


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
        self.stream_kwargs: dict = {}

    async def get_collection(self, catalog_id, collection_id, lang="en"):
        return {"id": collection_id}

    async def stream_items(self, **kwargs):
        self.stream_called = True
        self.stream_kwargs = kwargs

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


async def _call_get_items(svc, **overrides):
    """Invoke ``get_items`` with the full default OGC argument set, allowing
    individual parameters to be overridden per test."""
    kwargs = dict(
        request=_make_request(),
        catalog_id="cat",
        collection_id="col",
        conn=None,
        limit=10,
        offset=0,
        bbox=None,
        datetime_param=None,
        filter=None,
        filter_lang="cql2-text",
        crs=None,
        bbox_crs=None,
        sortby=None,
        f=OutputFormatEnum.GEOJSON,
        language="en",
    )
    kwargs.update(overrides)
    return await svc.get_items(**kwargs)


@pytest.mark.asyncio
async def test_get_items_streams_from_items_protocol(monkeypatch):
    """No ES search fast-path: the listing streams from the items protocol and
    the response carries exactly those items."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1"), _feature("pg-2")], total=2)
    _wire(monkeypatch, svc, catalogs)

    resp = await _call_get_items(svc, limit=10, offset=0)
    body = json.loads(await _read_body(resp))

    assert body["type"] == "FeatureCollection"
    assert {f["id"] for f in body["features"]} == {"pg-1", "pg-2"}
    assert catalogs.stream_called is True
    assert catalogs.stream_kwargs["catalog_id"] == "cat"
    assert catalogs.stream_kwargs["collection_id"] == "col"


@pytest.mark.asyncio
async def test_get_items_forces_exact_geometry_and_features_consumer(monkeypatch):
    """The listing pins the exact-geometry hint and the OGC_FEATURES consumer so
    the router selects the full-precision (PG) reader."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1")], total=1)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_items(svc)

    assert EXACT_READ_HINTS.issubset(catalogs.stream_kwargs["hints"])
    assert catalogs.stream_kwargs["consumer"] == ConsumerType.OGC_FEATURES


@pytest.mark.asyncio
async def test_get_items_threads_limit_and_offset_into_query_request(monkeypatch):
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_items(svc, limit=25, offset=50)

    req = catalogs.stream_kwargs["request"]
    assert req.limit == 25
    assert req.offset == 50


@pytest.mark.asyncio
async def test_get_items_translates_bbox_and_datetime_into_filter_conditions(monkeypatch):
    """``bbox`` and a ``datetime`` interval are parsed into the QueryRequest as
    a spatial ``geom`` filter and a ``validity`` range filter respectively."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_items(
        svc, bbox="1,2,3,4", datetime_param="2024-01-01T00:00:00Z/2024-12-31T00:00:00Z"
    )

    req = catalogs.stream_kwargs["request"]
    # bbox → a spatial geometry filter carrying the requested envelope.
    geom_filters = [f for f in req.filters if f.field == "geom" and f.spatial_op]
    assert geom_filters, "bbox must produce a spatial geom filter"
    assert "POLYGON" in str(geom_filters[0].value)
    # datetime interval → a validity range filter (operator "&&").
    range_filters = [
        f for f in req.filters if f.field == "validity" and f.operator == "&&"
    ]
    assert range_filters, "datetime interval must produce a validity range filter"
    assert "2024-01-01" in str(range_filters[0].value)
    assert "2024-12-31" in str(range_filters[0].value)


@pytest.mark.asyncio
async def test_get_items_threads_cql_filter_into_query_request(monkeypatch):
    """An explicit CQL2 ``filter`` is carried on the QueryRequest as
    ``cql_filter`` (the items protocol compiles it downstream)."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1")], total=1)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_items(svc, filter="prop = 'x'", filter_lang="cql2-text")

    req = catalogs.stream_kwargs["request"]
    assert req.cql_filter is not None
    assert "prop" in req.cql_filter
    assert catalogs.stream_called is True


@pytest.mark.asyncio
async def test_get_items_over_max_limit_clamps_instead_of_erroring(monkeypatch):
    """OGC API - Features Part 1 Core /req/core/fc-limit-response-1: a
    ``limit`` above the configured maximum (1000 by default) is clamped, not
    rejected. The handler itself never sees a value above 1000 (FastAPI's
    ``le=`` gate — removed — used to 422 here instead)."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    resp = await _call_get_items(svc, limit=5000, offset=0)
    assert resp.status_code == 200

    req = catalogs.stream_kwargs["request"]
    assert req.limit == 1000


@pytest.mark.asyncio
async def test_get_items_omitted_limit_uses_configured_default(monkeypatch):
    """``limit=None`` (query param omitted) falls back to the configured
    default (10), not an unbounded scan."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_items(svc, limit=None, offset=0)

    req = catalogs.stream_kwargs["request"]
    assert req.limit == 10


@pytest.mark.asyncio
async def test_get_items_in_range_limit_is_unchanged(monkeypatch):
    """Existing in-range behaviour is preserved."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_items(svc, limit=250, offset=0)

    req = catalogs.stream_kwargs["request"]
    assert req.limit == 250


@pytest.mark.asyncio
async def test_get_items_serves_non_4326_crs_via_items_protocol(monkeypatch):
    """A non-4326 output CRS reprojection is a PG-capable path; the listing
    still streams through the items protocol (the router/driver handles the
    reprojection), it does not error or take a separate code path."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    catalogs = _FakeCatalogs(stream_features=[_feature("pg-1")], total=1)
    _wire(monkeypatch, svc, catalogs)

    async def _resolve_crs(conn, catalog_id, crs):
        return 3857 if crs else None

    monkeypatch.setattr(svc, "_resolve_crs_srid", _resolve_crs, raising=False)

    resp = await _call_get_items(
        svc, crs="http://www.opengis.net/def/crs/EPSG/0/3857"
    )
    body = json.loads(await _read_body(resp))

    assert [f["id"] for f in body["features"]] == ["pg-1"]
    assert catalogs.stream_called is True
