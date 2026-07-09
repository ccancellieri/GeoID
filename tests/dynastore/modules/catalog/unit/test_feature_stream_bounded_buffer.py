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

"""Bounded page buffer for feature_stream (#3142).

feature_stream used to hold its pooled DB connection for the whole HTTP
response; a handful of slow readers could pin every slot of a small serving
pool. A page that fits the row/byte buffer must now release the connection
BEFORE the first feature is yielded; a page that overruns the buffer must
keep the pre-#3142 behaviour (stream the remainder inside the still-open
transaction).
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dynastore.modules.catalog.item_query as item_query
from dynastore.modules.catalog.item_query import (
    ItemQueryMixin,
    _approx_feature_bytes,
    _buffer_feature_page,
)
from dynastore.models.query_builder import QueryRequest


# ---------------------------------------------------------------------------
# _buffer_feature_page / _approx_feature_bytes unit coverage
# ---------------------------------------------------------------------------


async def _aiter(items: List[Any]) -> AsyncIterator[Any]:
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_buffer_exhausts_small_stream_within_budget() -> None:
    rows = _aiter([{"id": 1}, {"id": 2}, {"id": 3}])
    buffered, tripped = await _buffer_feature_page(rows, lambda r: r)
    assert tripped is False
    assert buffered == [{"id": 1}, {"id": 2}, {"id": 3}]


@pytest.mark.asyncio
async def test_buffer_trips_on_row_cap_leaving_remainder_consumable() -> None:
    rows = _aiter([{"id": n} for n in range(5)])
    buffered, tripped = await _buffer_feature_page(rows, lambda r: r, row_cap=2)
    assert tripped is True
    assert [f["id"] for f in buffered] == [0, 1]
    # The remainder is still pending on the same iterator — the caller keeps
    # streaming it inside the open transaction.
    remainder = [r["id"] async for r in rows]
    assert remainder == [2, 3, 4]


@pytest.mark.asyncio
async def test_buffer_trips_on_byte_budget() -> None:
    rows = _aiter([{"blob": "x" * 100}, {"blob": "y" * 100}])
    buffered, tripped = await _buffer_feature_page(
        rows, lambda r: r, byte_budget=50
    )
    assert tripped is True
    assert len(buffered) == 1


def test_approx_feature_bytes_prefers_model_dump_json() -> None:
    class _PydanticLike:
        def model_dump_json(self) -> str:
            return '{"id": 1}'

    assert _approx_feature_bytes(_PydanticLike()) == len('{"id": 1}')


def test_approx_feature_bytes_unestimable_forces_budget_pressure() -> None:
    class _Unserializable:
        def model_dump_json(self) -> str:
            raise RuntimeError("boom")

    assert (
        _approx_feature_bytes(_Unserializable())
        == item_query._UNESTIMATED_FEATURE_BYTES
    )


# ---------------------------------------------------------------------------
# Connection-release ordering through the real stream_items path
# ---------------------------------------------------------------------------


class _FakeRow:
    def __init__(self, n: int) -> None:
        self._mapping = {"id": n}


class _RecordingTransaction:
    """managed_transaction stand-in that logs enter/exit into ``events``."""

    def __init__(self, events: List[str], rows: List[_FakeRow]) -> None:
        self._events = events
        self._rows = rows

    async def __aenter__(self) -> Any:
        self._events.append("txn_enter")
        conn = MagicMock()
        conn.stream = AsyncMock(return_value=_aiter(self._rows))
        return conn

    async def __aexit__(self, *exc: Any) -> bool:
        self._events.append("txn_exit")
        return False


class _FakeAsyncEngine:
    # is_async_resource duck-checks for the AsyncEngine surface.
    sync_engine = object()


class _StubItemService(ItemQueryMixin):
    def __init__(self, engine: Any) -> None:
        self.engine = engine

    async def _resolve_read_policy(self, *a: Any, **kw: Any) -> None:
        return None

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        col_config: Any,
        *,
        context: Any,
        read_policy: Any,
    ) -> Dict[str, Any]:
        return row

    async def _apply_query_transformations(self, *a: Any, **kw: Any):
        return "SELECT 1", {}


async def _run_stream(events: List[str], n_rows: int) -> List[Dict[str, Any]]:
    svc = _StubItemService(_FakeAsyncEngine())
    request = QueryRequest(select=[], limit=100, offset=0, include_total_count=False)
    rows = [_FakeRow(n) for n in range(n_rows)]

    with (
        patch(
            "dynastore.modules.catalog.item_query._try_driver_dispatch",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            "dynastore.modules.catalog.item_query.managed_transaction",
            side_effect=lambda *_a, **_kw: _RecordingTransaction(events, rows),
        ),
        patch(
            "dynastore.modules.catalog.item_query.is_async_resource",
            return_value=True,
        ),
    ):
        svc._get_collection_config = AsyncMock(return_value=MagicMock())  # type: ignore[attr-defined]
        svc._resolve_physical_schema = AsyncMock(return_value="s")  # type: ignore[attr-defined]
        svc._resolve_physical_table = AsyncMock(return_value="t")  # type: ignore[attr-defined]

        response = await svc.stream_items(
            catalog_id="c", collection_id="col", request=request
        )
        features: List[Dict[str, Any]] = []
        async for feature in response.items:
            events.append("item")
            features.append(feature)
    return features


def _last_index(events: List[str], name: str) -> int:
    return len(events) - 1 - events[::-1].index(name)


@pytest.mark.asyncio
async def test_small_page_releases_connection_before_first_yield() -> None:
    """The #3142 fix: a page that fits the buffer exits the STREAM
    transaction (returning the pooled connection) before the first feature
    reaches the consumer, so a slow HTTP reader cannot pin the pool slot.

    stream_items opens two managed transactions: query prep, then the
    feature stream — the assertion targets the LAST txn_exit (the stream's).
    """
    events: List[str] = []
    features = await _run_stream(events, n_rows=3)

    assert len(features) == 3
    assert events.count("txn_exit") == 2  # prep + stream
    assert _last_index(events, "txn_exit") < events.index("item")


@pytest.mark.asyncio
async def test_overrun_page_streams_remainder_inside_open_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A page that overruns the buffer keeps the pre-#3142 shape: features
    flow while the stream transaction is still open, and it closes only
    after the stream is exhausted."""
    monkeypatch.setattr(item_query, "_STREAM_BUFFER_ROW_CAP", 2)
    events: List[str] = []
    features = await _run_stream(events, n_rows=5)

    assert len(features) == 5
    assert events.index("item") < _last_index(events, "txn_exit")
    assert events[-1] == "txn_exit"
