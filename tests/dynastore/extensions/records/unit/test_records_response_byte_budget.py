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

"""Unit tests for the OGC Records ``/items`` response byte budget (#2681).

``get_records`` streams its page through the same
``stream_ogc_features``/``_stream_ogc_json`` machinery as OGC Features (#2781)
— on a collection with large records a page-size ``limit`` alone does not
bound response memory, so the configured ``max_response_bytes`` budget stops
the streamed page once the serialized ``features`` bytes cross it: fewer
records are served than the SQL result set, ``numberReturned`` reflects what
was actually served, ``numberMatched`` is unaffected, and the ``next`` link
resumes exactly where the page stopped (``offset + numberReturned``) rather
than skipping the unserved records.
"""

from __future__ import annotations

import json
from typing import List
from urllib.parse import parse_qs, urlparse

import pytest
from geojson_pydantic import Feature as _GeoJSONFeature

from dynastore.extensions.records.config import RecordsPluginConfig
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


def _padded_feature(fid: str, padding_bytes: int = 2000) -> _GeoJSONFeature:
    return _GeoJSONFeature(
        type="Feature", id=fid, geometry=None, properties={"pad": "x" * padding_bytes}
    )


class _FakeCatalogs:
    """Fake ``ItemsProtocol.stream_items`` that tracks whether the source
    async generator was closed early (the byte-budget cutoff must release
    the underlying DB stream/transaction promptly, not wait for GC)."""

    def __init__(self, stream_features: List[_GeoJSONFeature], total: int):
        self._stream_features = stream_features
        self._total = total
        self.aclosed = False

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


def _get_records_defaults(**overrides) -> dict:
    kwargs = dict(
        request=_make_request(),
        catalog_id="cat", collection_id="col",
        conn=None, limit=10, offset=0,
        filter=None, filter_lang="cql2-text", filter_crs=None,
        properties=None, skip_geometry=None, return_geometry=None,
        sortby=None, bbox=None, q=None,
        request_hints=frozenset(),
    )
    kwargs.update(overrides)
    return kwargs


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


def _wire(monkeypatch, svc, catalogs, plugin_config: RecordsPluginConfig):
    async def _get_catalogs():
        return catalogs

    async def _get_plugin_config(cls, catalog_id=None, collection_id=None):
        return plugin_config

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)
    monkeypatch.setattr(svc, "_get_plugin_config", _get_plugin_config, raising=False)


@pytest.mark.asyncio
async def test_byte_budget_cuts_page_short_with_correct_next_link(monkeypatch):
    """5 large records match; the SQL page (limit=5) would naturally be the
    last page (offset + limit == total, no ``next``). A small byte budget
    forces the page to stop after 2 records — ``numberReturned`` reflects
    that, ``numberMatched`` is unaffected, and a ``next`` link is produced
    resuming at ``offset + numberReturned`` rather than skipping the 3
    unserved records."""
    svc = RecordsService.__new__(RecordsService)
    features = [_padded_feature(f"pg-{i}") for i in range(5)]
    catalogs = _FakeCatalogs(stream_features=features, total=5)

    # Each record serializes to ~2050 bytes; budget fits ~2 of them.
    plugin_config = RecordsPluginConfig(max_response_bytes=3000)
    _wire(monkeypatch, svc, catalogs, plugin_config)

    resp = await svc.get_records(**_get_records_defaults(limit=5))
    body = json.loads(await _read_body(resp))

    assert body["type"] == "FeatureCollection"
    assert body["numberMatched"] == 5
    assert body["numberReturned"] == 2
    assert [f["id"] for f in body["features"]] == ["pg-0", "pg-1"]

    next_offset = _next_offset(body["links"])
    assert next_offset == 2, (
        "next link must resume at offset+numberReturned (2), not skip the "
        "3 unserved records by pointing at offset+limit (5)"
    )
    assert catalogs.aclosed is True, (
        "byte-budget cutoff must close the source iterator promptly"
    )


@pytest.mark.asyncio
async def test_byte_budget_disabled_serves_full_page(monkeypatch):
    """``max_response_bytes=None`` disables the budget — unbounded legacy
    behaviour, unchanged."""
    svc = RecordsService.__new__(RecordsService)
    features = [_padded_feature(f"pg-{i}") for i in range(5)]
    catalogs = _FakeCatalogs(stream_features=features, total=5)

    plugin_config = RecordsPluginConfig(max_response_bytes=None)
    _wire(monkeypatch, svc, catalogs, plugin_config)

    resp = await svc.get_records(**_get_records_defaults(limit=5))
    body = json.loads(await _read_body(resp))

    assert body["numberReturned"] == 5
    assert body["numberMatched"] == 5
    assert _next_offset(body["links"]) is None
    assert catalogs.aclosed is False


@pytest.mark.asyncio
async def test_byte_budget_always_returns_at_least_one_record(monkeypatch):
    """Even a single record exceeding the budget must still be served — the
    page must never be empty."""
    svc = RecordsService.__new__(RecordsService)
    features = [_padded_feature(f"pg-{i}") for i in range(3)]
    catalogs = _FakeCatalogs(stream_features=features, total=3)

    plugin_config = RecordsPluginConfig(max_response_bytes=1)
    _wire(monkeypatch, svc, catalogs, plugin_config)

    resp = await svc.get_records(**_get_records_defaults(limit=3))
    body = json.loads(await _read_body(resp))

    assert body["numberReturned"] == 1
    assert body["numberMatched"] == 3
    assert _next_offset(body["links"]) == 1
