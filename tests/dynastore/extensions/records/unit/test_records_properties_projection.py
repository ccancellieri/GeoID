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

"""Proof-of-concept tests pinning the Phase-1 wiring (#1385) on the Records
service. Records shares the OGC items grammar with Features, so the same three
sub-features apply:

* ``properties`` query parameter — service-layer post-fetch projection.
* ``filter-crs`` query parameter — threaded into ``QueryRequest.filter_crs_srid``.
* ``filter-lang=cql2-json`` — accepted; ``cql2-text`` unchanged.

These tests focus on the wiring (params accepted, threaded to QueryRequest,
post-fetch projection applied). Full CQL→SQL compile coverage lives in the
Features test file under ``tests/dynastore/extensions/features/unit/``.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pytest
from geojson_pydantic import Feature as _GeoJSONFeature

import dynastore.extensions.tools.query as _query_mod
from dynastore.extensions.records.records_service import RecordsService
from dynastore.models.query_builder import QueryResponse


def _make_request(
    path: str = "/records/catalogs/cat/collections/col/items",
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


def _feature(fid: str, props: Optional[Dict[str, Any]] = None) -> _GeoJSONFeature:
    return _GeoJSONFeature(
        type="Feature",
        id=fid,
        geometry=None,
        properties=props if props is not None else {
            "title": fid,
            "country": "IT",
            "rainfall_mm": 1.0,
        },
    )


class _FakeCatalogs:
    def __init__(self, stream_features: List[_GeoJSONFeature], total: int):
        self._stream_features = stream_features
        self._total = total
        self.stream_called = False
        self.last_request = None

    async def get_collection(self, catalog_id, collection_id, lang="en"):
        return {"id": collection_id}

    async def get_collection_config(self, catalog_id, collection_id, ctx=None):
        return None

    async def resolve_catalog_id(self, catalog_id, allow_missing=False):
        return None

    async def resolve_catalog_alias(self, catalog_id):
        return None

    async def resolve_collection_alias(self, catalog_internal_id, collection_id):
        return None

    async def get_catalog_model(self, catalog_internal_id):
        return None

    async def stream_items(self, **kwargs):
        self.stream_called = True
        self.last_request = kwargs.get("request")

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

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)

    # The Records read path resolves the items-read policy via
    # ``resolve_items_read_policy`` (module-level in
    # ``dynastore.extensions.tools.query``). For unit tests we don't need an
    # actual policy — stub it to None so the policy-aware ``db_row_to_record``
    # branch falls back to the default identity projection.
    import dynastore.extensions.records.records_service as _rsvc

    async def _no_read_policy(*args, **kwargs):
        return None

    monkeypatch.setattr(_rsvc, "resolve_items_read_policy", _no_read_policy)

    # Default: search-dispatch declines → PG stream path.
    async def _decline(**kwargs):
        return None

    monkeypatch.setattr(
        _query_mod, "maybe_dispatch_items_to_search_driver", _decline
    )


def _call_get_records(svc, **overrides):
    kwargs = dict(
        request=_make_request(),
        catalog_id="cat",
        collection_id="col",
        conn=None,
        limit=10,
        offset=0,
        filter=None,
        filter_lang="cql2-text",
        filter_crs=None,
        properties=None,
        skip_geometry=None,
        return_geometry=None,
        sortby=None,
        bbox=None,
        q=None,
        request_hints=frozenset(),
    )
    kwargs.update(overrides)
    return svc.get_records(**kwargs)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_records_properties_subset(monkeypatch):
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[_feature("r-1")], total=1)
    _wire(monkeypatch, svc, catalogs)

    resp = await _call_get_records(svc, properties="title,country")
    body = json.loads(bytes(resp.body))
    feat = body["features"][0]
    # Property set narrowed by the post-fetch projection.
    assert set(feat["properties"].keys()) == {"title", "country"}


@pytest.mark.asyncio
async def test_records_filter_crs_threaded(monkeypatch):
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_records(
        svc,
        filter="title = 'x'",
        filter_crs="http://www.opengis.net/def/crs/EPSG/0/3857",
    )
    assert catalogs.last_request is not None
    assert catalogs.last_request.filter_crs_srid == 3857


@pytest.mark.asyncio
async def test_records_filter_lang_cql2_json_accepted(monkeypatch):
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    cql_json = json.dumps({"op": "=", "args": [{"property": "title"}, "x"]})
    await _call_get_records(svc, filter=cql_json, filter_lang="cql2-json")
    assert catalogs.last_request is not None
    assert catalogs.last_request.filter_lang == "cql2-json"
    assert catalogs.last_request.cql_filter == cql_json


@pytest.mark.asyncio
async def test_records_filter_lang_invalid_400(monkeypatch):
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as excinfo:
        await _call_get_records(svc, filter="x=1", filter_lang="ecql")
    assert excinfo.value.status_code == 400


# ---------------------------------------------------------------------------
# ``skipGeometry`` — service threads it onto QueryRequest
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_records_skip_geometry_threaded(monkeypatch):
    """``skipGeometry=true`` lands on ``QueryRequest.skip_geometry`` so the
    driver layer can push the projection down."""
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_records(svc, skip_geometry=True)
    assert catalogs.last_request is not None
    assert catalogs.last_request.skip_geometry is True


@pytest.mark.asyncio
async def test_records_skip_geometry_default_false(monkeypatch):
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_records(svc)
    assert catalogs.last_request is not None
    assert catalogs.last_request.skip_geometry is False


# ---------------------------------------------------------------------------
# ``returnGeometry`` alias + conflict handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_records_return_geometry_false_skips(monkeypatch):
    """``returnGeometry=false`` is equivalent to ``skipGeometry=true``."""
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_records(svc, return_geometry=False)
    assert catalogs.last_request is not None
    assert catalogs.last_request.skip_geometry is True


@pytest.mark.asyncio
async def test_records_return_geometry_true_keeps(monkeypatch):
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_records(svc, return_geometry=True)
    assert catalogs.last_request is not None
    assert catalogs.last_request.skip_geometry is False


@pytest.mark.asyncio
async def test_records_skip_and_return_conflict_400(monkeypatch):
    """Conflicting skipGeometry + returnGeometry → HTTP 400."""
    from fastapi import HTTPException
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    with pytest.raises(HTTPException) as exc:
        await _call_get_records(svc, skip_geometry=True, return_geometry=True)
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_records_skip_and_return_consistent_ok(monkeypatch):
    svc = RecordsService.__new__(RecordsService)
    catalogs = _FakeCatalogs(stream_features=[], total=0)
    _wire(monkeypatch, svc, catalogs)

    await _call_get_records(svc, skip_geometry=False, return_geometry=True)
    assert catalogs.last_request is not None
    assert catalogs.last_request.skip_geometry is False
