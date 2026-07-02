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

"""Unit tests for the OGC Features ``/items`` response byte budget (#2681).

On a collection with large geometries (e.g. GAUL admin-boundary
multipolygons), a page-size ``limit`` alone does not bound response memory —
a handful of large features can already exceed process memory well under
``max_limit``. The configured ``max_response_bytes`` budget stops the
streamed GeoJSON page once the serialized ``features`` bytes cross it:
fewer items are served than ``limit``/the SQL result set, ``numberReturned``
reflects what was actually served, ``numberMatched`` is unaffected, and the
``next`` link resumes exactly where the page stopped (``offset +
numberReturned``) rather than skipping the unserved items.
"""

from __future__ import annotations

import json
from typing import List
from urllib.parse import parse_qs, urlparse

import pytest

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


def _padded_feature(fid: str, padding_bytes: int = 2000) -> _OGCFeature:
    return _OGCFeature(
        type="Feature",
        id=fid,
        geometry=None,
        properties={"datetime": "2024-01-01T00:00:00Z", "pad": "x" * padding_bytes},
    )


class _FakeCatalogs:
    """Fake ``ItemsProtocol.stream_items`` that tracks whether the source
    async generator was closed early (the byte-budget cutoff must release
    the underlying DB stream/transaction promptly, not wait for GC)."""

    def __init__(self, stream_features: List[_OGCFeature], total: int):
        self._stream_features = stream_features
        self._total = total
        self.aclosed = False

    async def get_collection(self, catalog_id, collection_id, lang="en"):
        return {"id": collection_id}

    async def stream_items(self, **kwargs):
        outer = self

        async def _gen():
            try:
                for f in outer._stream_features:
                    yield f
            except GeneratorExit:
                outer.aclosed = True
                raise

        return QueryResponse(
            items=_gen(),
            total_count=self._total,
            catalog_id=kwargs["catalog_id"],
            collection_id=kwargs["collection_id"],
        )


def _wire(monkeypatch, svc, catalogs, plugin_config: FeaturesPluginConfig):
    async def _get_catalogs():
        return catalogs

    async def _get_configs():
        class _Cfg:
            async def get_config(
                self, cls, catalog_id=None, collection_id=None, ctx=None
            ):
                return plugin_config

        return _Cfg()

    async def _get_storage():
        return None

    async def _resolve_crs(conn, catalog_id, crs):
        return None

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)
    monkeypatch.setattr(svc, "_get_configs_service", _get_configs, raising=False)
    monkeypatch.setattr(svc, "_get_storage_service", _get_storage, raising=False)
    monkeypatch.setattr(svc, "_resolve_crs_srid", _resolve_crs, raising=False)


async def _read_body(resp) -> bytes:
    chunks = []
    async for chunk in resp.body_iterator:
        chunks.append(chunk if isinstance(chunk, bytes) else chunk.encode())
    return b"".join(chunks)


def _next_offset(links: list) -> int | None:
    for link in links:
        if link.get("rel") == "next":
            qs = parse_qs(urlparse(link["href"]).query)
            return int(qs["offset"][0])
    return None


@pytest.mark.asyncio
async def test_byte_budget_cuts_page_short_with_correct_next_link(monkeypatch):
    """5 large features match; the SQL page (limit=5) would naturally be the
    last page (offset + limit == total, no ``next``). A small byte budget
    forces the page to stop after 2 features — ``numberReturned`` reflects
    that, ``numberMatched`` is unaffected, and a ``next`` link is produced
    (one that would NOT exist without the byte-budget cutoff) resuming at
    ``offset + numberReturned``, not skipping the 3 unserved features."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    features = [_padded_feature(f"pg-{i}") for i in range(5)]
    catalogs = _FakeCatalogs(stream_features=features, total=5)

    # Each feature serializes to ~2050 bytes; budget fits ~2 of them.
    plugin_config = FeaturesPluginConfig(max_response_bytes=3000)
    _wire(monkeypatch, svc, catalogs, plugin_config)

    resp = await svc.get_items(
        request=_make_request(),
        catalog_id="cat",
        collection_id="col",
        conn=None,
        limit=5,
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
    body = json.loads(await _read_body(resp))

    assert body["type"] == "FeatureCollection"
    assert body["numberMatched"] == 5
    assert body["numberReturned"] == 2
    assert [f["id"] for f in body["features"]] == ["pg-0", "pg-1"]

    next_offset = _next_offset(body["links"])
    assert next_offset == 2, (
        "next link must resume at offset+numberReturned (2), not skip the "
        "3 unserved features by pointing at offset+limit (5)"
    )
    assert catalogs.aclosed is True, (
        "byte-budget cutoff must close the source iterator promptly"
    )


@pytest.mark.asyncio
async def test_byte_budget_disabled_serves_full_page(monkeypatch):
    """``max_response_bytes=None`` disables the budget — unbounded legacy
    behaviour, unchanged."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    features = [_padded_feature(f"pg-{i}") for i in range(5)]
    catalogs = _FakeCatalogs(stream_features=features, total=5)

    plugin_config = FeaturesPluginConfig(max_response_bytes=None)
    _wire(monkeypatch, svc, catalogs, plugin_config)

    resp = await svc.get_items(
        request=_make_request(),
        catalog_id="cat",
        collection_id="col",
        conn=None,
        limit=5,
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
    body = json.loads(await _read_body(resp))

    assert body["numberReturned"] == 5
    assert body["numberMatched"] == 5
    assert _next_offset(body["links"]) is None
    assert catalogs.aclosed is False


@pytest.mark.asyncio
async def test_byte_budget_always_returns_at_least_one_feature(monkeypatch):
    """Even a single feature exceeding the budget must still be served —
    the page must never be empty."""
    svc = OGCFeaturesService.__new__(OGCFeaturesService)
    features = [_padded_feature(f"pg-{i}") for i in range(3)]
    catalogs = _FakeCatalogs(stream_features=features, total=3)

    plugin_config = FeaturesPluginConfig(max_response_bytes=1)
    _wire(monkeypatch, svc, catalogs, plugin_config)

    resp = await svc.get_items(
        request=_make_request(),
        catalog_id="cat",
        collection_id="col",
        conn=None,
        limit=3,
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
    body = json.loads(await _read_body(resp))

    assert body["numberReturned"] == 1
    assert body["numberMatched"] == 3
    assert _next_offset(body["links"]) == 1
