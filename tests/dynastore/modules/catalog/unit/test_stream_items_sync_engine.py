"""Unit tests for the sync-engine streaming path in ``ItemQueryMixin.stream_items``.

Bug A fix (#1416): when ``self.engine`` is a sync ``Engine``, calling
``stream_conn.stream(...)`` raises ``AttributeError`` because sync
connections have no ``.stream()`` method.  The fix branches on
``is_async_resource(self.engine)`` and, for the sync path, uses a
server-side cursor (``execution_options(stream_results=True)``) fed via
``asyncio.to_thread`` so the outer ``async for`` stays async-compatible.

These tests exercise the sync branch in isolation — no real DB required.
"""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.item_query import ItemQueryMixin
from dynastore.models.query_builder import QueryRequest


# ---------------------------------------------------------------------------
# Fake sync engine + cursor
# ---------------------------------------------------------------------------


class _FakeRow:
    """Minimal row with a ``._mapping`` dict."""

    def __init__(self, data: Dict[str, Any]) -> None:
        self._mapping = data


class _FakeSyncResult:
    """Mimics a SQLAlchemy sync ``Result`` with server-side cursor semantics."""

    def __init__(self, rows: List[Dict[str, Any]], batch: int = 10) -> None:
        self._rows = [_FakeRow(r) for r in rows]
        self._batch = batch
        self._pos = 0
        self.closed = False

    def fetchmany(self, size: int) -> List[_FakeRow]:
        chunk = self._rows[self._pos : self._pos + size]
        self._pos += size
        return chunk

    def close(self) -> None:
        self.closed = True


class _FakeSyncConn:
    """Minimal sync connection returned by ``_FakeSyncEngine.connect()``."""

    def __init__(self, result: _FakeSyncResult) -> None:
        self._result = result
        self.closed = False
        self._options: Dict[str, Any] = {}

    def execution_options(self, **kw: Any) -> "_FakeSyncConn":
        self._options.update(kw)
        return self

    def execute(self, stmt: Any, params: Any) -> _FakeSyncResult:
        return self._result

    def close(self) -> None:
        self.closed = True


class _FakeSyncEngine:
    """A non-async engine that satisfies ``is_async_resource(...) == False``."""

    def __init__(self, result: _FakeSyncResult) -> None:
        self._result = result

    def connect(self) -> _FakeSyncConn:
        return _FakeSyncConn(self._result)


# ---------------------------------------------------------------------------
# Minimal concrete ItemQueryMixin subclass
# ---------------------------------------------------------------------------


class _StubItemService(ItemQueryMixin):
    """Concrete stub with just enough to exercise feature_stream."""

    def __init__(self, engine: Any, rows: List[Dict[str, Any]]) -> None:
        self.engine = engine
        self._rows = rows

    async def _resolve_physical_schema(self, *a: Any, **kw: Any) -> str:
        return "test_schema"

    async def _resolve_physical_table(self, *a: Any, **kw: Any) -> str:
        return "test_table"

    async def _get_collection_config(self, *a: Any, **kw: Any) -> MagicMock:
        return MagicMock()

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

    async def _apply_query_transformations(
        self,
        request: Any,
        context: Any,
        catalog_id: str,
        collection_id: str,
        col_config: Any,
        *,
        db_resource: Any = None,
        consumer: Any = None,
    ):
        sql = "SELECT * FROM test_schema.test_table"
        params: Dict[str, Any] = {}
        return sql, params


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_engine_yields_all_rows() -> None:
    """Sync engine path delivers every row from the server-side cursor."""
    row_data = [{"id": i, "name": f"feat_{i}"} for i in range(5)]
    fake_result = _FakeSyncResult(row_data)
    engine = _FakeSyncEngine(fake_result)
    svc = _StubItemService(engine, row_data)

    request = QueryRequest(
        select=[],
        limit=100,
        offset=0,
        include_total_count=False,
    )

    # Patch the driver dispatch + managed_transaction (metadata fetch only uses
    # managed_transaction on the sync engine — we need to let that pass)
    with (
        patch(
            "dynastore.modules.catalog.item_query._try_driver_dispatch",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            "dynastore.modules.catalog.item_query.managed_transaction"
        ) as mock_mt,
    ):
        # managed_transaction used for metadata fetch (sync engine -> sync conn path)
        fake_conn = MagicMock()
        fake_conn.__aenter__ = AsyncMock(return_value=fake_conn)
        fake_conn.__aexit__ = AsyncMock(return_value=False)
        # col_config calls use AsyncMock
        svc._get_collection_config = AsyncMock(return_value=MagicMock())  # type: ignore[assignment]
        svc._resolve_physical_schema = AsyncMock(return_value="test_schema")  # type: ignore[assignment]
        svc._resolve_physical_table = AsyncMock(return_value="test_table")  # type: ignore[assignment]
        mock_mt.return_value = fake_conn

        response = await svc.stream_items(
            catalog_id="test_catalog",
            collection_id="test_collection",
            request=request,
        )

        collected: List[Dict[str, Any]] = []
        async for item in response.items:
            collected.append(item)

    assert len(collected) == len(row_data)
    for i, row in enumerate(row_data):
        assert collected[i]["id"] == row["id"]


@pytest.mark.asyncio
async def test_sync_engine_closes_cursor_on_full_consumption() -> None:
    """Cursor and connection are closed after all rows are consumed.

    When the generator runs to completion, the ``finally`` block in
    ``feature_stream`` fires synchronously (no early-exit timing edge cases)
    and must close the server-side cursor and connection.
    """
    row_data = [{"id": i} for i in range(7)]
    fake_result = _FakeSyncResult(row_data, batch=3)
    fake_conn_obj = _FakeSyncConn(fake_result)

    class _EngineWithCapture(_FakeSyncEngine):
        def connect(self) -> _FakeSyncConn:
            return fake_conn_obj

    engine = _EngineWithCapture(fake_result)
    svc = _StubItemService(engine, row_data)

    request = QueryRequest(
        select=[],
        limit=100,
        offset=0,
        include_total_count=False,
    )

    with (
        patch(
            "dynastore.modules.catalog.item_query._try_driver_dispatch",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            "dynastore.modules.catalog.item_query.managed_transaction"
        ) as mock_mt,
    ):
        fake_conn = MagicMock()
        fake_conn.__aenter__ = AsyncMock(return_value=fake_conn)
        fake_conn.__aexit__ = AsyncMock(return_value=False)
        svc._get_collection_config = AsyncMock(return_value=MagicMock())  # type: ignore[assignment]
        svc._resolve_physical_schema = AsyncMock(return_value="test_schema")  # type: ignore[assignment]
        svc._resolve_physical_table = AsyncMock(return_value="test_table")  # type: ignore[assignment]
        mock_mt.return_value = fake_conn

        response = await svc.stream_items(
            catalog_id="test_catalog",
            collection_id="test_collection",
            request=request,
        )

        # Consume ALL rows — generator runs to completion, triggering the finally block
        collected: List[Dict[str, Any]] = []
        async for item in response.items:
            collected.append(item)

    assert len(collected) == len(row_data)
    # After full consumption the finally block runs synchronously
    assert fake_result.closed, "Result cursor should be closed after full consumption"
    assert fake_conn_obj.closed, "Connection should be closed after full consumption"
