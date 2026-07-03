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

"""The Collection Search branch of ``list_stac_collections`` (any request
carrying limit/offset/bbox/datetime/q/sortby) hand-builds its JSON response,
and used to emit no ``links`` at all — so ``?limit=1`` against a catalog with
1971 collections came back with ``links: []`` and no way to page. It must now
emit ``self`` plus a ``next`` link while more collections remain, and a
``prev`` link once past the first page.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager

import pytest
from fastapi import Request
from unittest.mock import AsyncMock

import dynastore.extensions.stac.stac_service as stac_service
from dynastore.extensions.stac.stac_service import STACService


class _FakeCatalogsService:
    async def get_catalog(self, catalog_id, lang=None, hints=None):
        return {"id": catalog_id}


def _make_request(query_string: bytes) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "https",
            "server": ("testserver", 443),
            "path": "/stac/catalogs/cat-1/collections",
            "headers": [],
            "query_string": query_string,
        }
    )


def _mk_service(monkeypatch, page, matched, effective_limit):
    svc = STACService()
    svc._get_catalogs_service = AsyncMock(return_value=_FakeCatalogsService())
    svc._get_stac_config = AsyncMock(
        return_value=type("Cfg", (), {"default_limit": 10, "max_limit": 1000})()
    )

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield None

    monkeypatch.setattr(stac_service, "managed_transaction", _fake_managed_transaction)

    async def _fake_search_collections(_conn, search_req, **_kw):
        # Real search_collections resolves/clamps the page size in place; the
        # link math downstream depends on that concrete int.
        search_req.limit = effective_limit
        return page, matched

    monkeypatch.setattr(stac_service, "search_collections", _fake_search_collections)
    monkeypatch.setattr(
        stac_service, "_pg_collections_to_stac_dicts",
        lambda collections_list, language: [{"id": f"col-{i}"} for i in range(len(page))],
    )
    return svc


def _links_by_rel(body):
    return {link["rel"]: link["href"] for link in body["links"]}


@pytest.mark.asyncio
async def test_first_page_emits_self_and_next_no_prev(monkeypatch):
    """limit=1 over 1971 collections: self + next (offset=1), no prev."""
    svc = _mk_service(monkeypatch, page=[object()], matched=1971, effective_limit=1)

    response = await svc.list_stac_collections(
        catalog_id="cat-1",
        request=_make_request(b"limit=1"),
        engine="fake-engine",
        language="en",
        request_hints=frozenset(),
        bbox=None, datetime=None, q=None, limit=1, offset=0, sortby=None,
    )
    body = json.loads(response.body)
    links = _links_by_rel(body)

    assert set(links) == {"self", "next"}
    assert links["self"].endswith("/stac/catalogs/cat-1/collections?limit=1")
    assert "offset=1" in links["next"]
    assert "limit=1" in links["next"]  # other query params are preserved
    assert body["context"]["matched"] == 1971


@pytest.mark.asyncio
async def test_last_page_emits_prev_no_next(monkeypatch):
    """offset on the final page: prev present, no next (offset+limit == matched)."""
    svc = _mk_service(monkeypatch, page=[object()], matched=1971, effective_limit=1)

    response = await svc.list_stac_collections(
        catalog_id="cat-1",
        request=_make_request(b"limit=1&offset=1970"),
        engine="fake-engine",
        language="en",
        request_hints=frozenset(),
        bbox=None, datetime=None, q=None, limit=1, offset=1970, sortby=None,
    )
    body = json.loads(response.body)
    links = _links_by_rel(body)

    assert set(links) == {"self", "prev"}
    assert "offset=1969" in links["prev"]
