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

"""#2829: a ``QueryRequest.group_by`` must route to a GROUP_BY-capable driver.

``_derive_hints_from_request`` is the single helper both ``search_items`` and
``stream_items`` use to fold ``Hint.GROUP_BY`` into the hints forwarded to
``_try_driver_dispatch`` — these tests pin the helper itself and its wiring
into both dispatch entry points, mirroring ``test_dwh_join_pg_hint.py``.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dynastore.models.query_builder import FieldSelection, QueryRequest, QueryResponse
from dynastore.modules.catalog import item_query as iq
from dynastore.modules.storage.hints import Hint


class _StubItemService(iq.ItemQueryMixin):
    """Minimal host for the mixin methods under test — no real DB engine."""

    engine = None


async def _empty_stream():
    if False:  # pragma: no cover - never executes, keeps this an async gen
        yield


def _group_by_request() -> QueryRequest:
    return QueryRequest(
        select=[FieldSelection(field="region")],
        group_by=["region"],
        limit=10,
    )


# ---------------------------------------------------------------------------
# _derive_hints_from_request — pure function
# ---------------------------------------------------------------------------

def test_derive_hints_adds_group_by_when_present():
    hints = iq._derive_hints_from_request(_group_by_request())
    assert hints == frozenset({Hint.GROUP_BY})


def test_derive_hints_unions_with_caller_supplied_hints():
    hints = iq._derive_hints_from_request(_group_by_request(), frozenset({Hint.JOIN}))
    assert hints == frozenset({Hint.JOIN, Hint.GROUP_BY})


def test_derive_hints_unchanged_when_group_by_absent():
    request = QueryRequest(select=[FieldSelection(field="region")])
    assert iq._derive_hints_from_request(request) == frozenset()
    assert iq._derive_hints_from_request(request, frozenset({Hint.JOIN})) == frozenset(
        {Hint.JOIN}
    )


def test_derive_hints_handles_none_request():
    assert iq._derive_hints_from_request(None) == frozenset()


# ---------------------------------------------------------------------------
# search_items / stream_items — forward the derived hint to dispatch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_items_forwards_group_by_hint_to_dispatch():
    captured: dict = {}

    async def fake_dispatch(
        catalog_id, collection_id, operation, request, limit, offset,
        entity_ids=None, hints=frozenset(),
    ):
        captured["hints"] = hints
        return QueryResponse(
            items=_empty_stream(), total_count=None,
            catalog_id=catalog_id, collection_id=collection_id,
        )

    svc = _StubItemService()
    with patch.object(iq, "_try_driver_dispatch", new=fake_dispatch):
        result = await svc.search_items("cat", "col", _group_by_request())

    assert result == []
    assert captured["hints"] == frozenset({Hint.GROUP_BY})


@pytest.mark.asyncio
async def test_search_items_no_group_by_hints_stay_empty():
    captured: dict = {}

    async def fake_dispatch(
        catalog_id, collection_id, operation, request, limit, offset,
        entity_ids=None, hints=frozenset(),
    ):
        captured["hints"] = hints
        return QueryResponse(
            items=_empty_stream(), total_count=None,
            catalog_id=catalog_id, collection_id=collection_id,
        )

    svc = _StubItemService()
    request = QueryRequest(select=[FieldSelection(field="region")])
    with patch.object(iq, "_try_driver_dispatch", new=fake_dispatch):
        await svc.search_items("cat", "col", request)

    assert captured["hints"] == frozenset()


@pytest.mark.asyncio
async def test_stream_items_forwards_group_by_hint_to_dispatch():
    captured: dict = {}

    async def fake_dispatch(
        catalog_id, collection_id, operation, request, limit, offset,
        entity_ids=None, hints=frozenset(),
    ):
        captured["hints"] = hints
        return QueryResponse(
            items=_empty_stream(), total_count=None,
            catalog_id=catalog_id, collection_id=collection_id,
        )

    svc = _StubItemService()
    with patch.object(iq, "_try_driver_dispatch", new=fake_dispatch):
        await svc.stream_items("cat", "col", _group_by_request())

    assert captured["hints"] == frozenset({Hint.GROUP_BY})


@pytest.mark.asyncio
async def test_stream_items_unions_caller_hints_with_group_by():
    captured: dict = {}

    async def fake_dispatch(
        catalog_id, collection_id, operation, request, limit, offset,
        entity_ids=None, hints=frozenset(),
    ):
        captured["hints"] = hints
        return QueryResponse(
            items=_empty_stream(), total_count=None,
            catalog_id=catalog_id, collection_id=collection_id,
        )

    svc = _StubItemService()
    with patch.object(iq, "_try_driver_dispatch", new=fake_dispatch):
        await svc.stream_items(
            "cat", "col", _group_by_request(), hints=frozenset({Hint.JOIN}),
        )

    assert captured["hints"] == frozenset({Hint.JOIN, Hint.GROUP_BY})


@pytest.mark.asyncio
async def test_stream_items_default_hints_empty_without_group_by():
    captured: dict = {}

    async def fake_dispatch(
        catalog_id, collection_id, operation, request, limit, offset,
        entity_ids=None, hints=frozenset(),
    ):
        captured["hints"] = hints
        return QueryResponse(
            items=_empty_stream(), total_count=None,
            catalog_id=catalog_id, collection_id=collection_id,
        )

    svc = _StubItemService()
    request = QueryRequest(select=[FieldSelection(field="region")])
    with patch.object(iq, "_try_driver_dispatch", new=fake_dispatch):
        await svc.stream_items("cat", "col", request)

    assert captured["hints"] == frozenset()
