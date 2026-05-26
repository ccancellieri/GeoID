"""Tests confirming feature_stream requires an async engine.

CPU-bound workers omit module_db and therefore have only a sync psycopg2
engine available.  Calling stream_items against such a worker must raise
RuntimeError immediately rather than silently degrading or blocking the
event loop (see #1420, which retired the asyncio.to_thread workaround
from #1418).
"""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.item_query import ItemQueryMixin
from dynastore.models.query_builder import QueryRequest


# ---------------------------------------------------------------------------
# Minimal stubs
# ---------------------------------------------------------------------------


class _FakeSyncEngine:
    """A non-async engine: is_async_resource returns False for this type."""

    # Deliberately NOT an AsyncEngine — no .sync_engine attr used by
    # is_async_resource, so the check falls through to the False branch.
    pass


class _StubItemService(ItemQueryMixin):
    """Concrete stub with just enough to exercise feature_stream."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

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
        return "SELECT 1", {}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_feature_stream_raises_for_sync_engine() -> None:
    """RuntimeError is raised when the engine is not async.

    Sync-only worker SCOPEs (CPU-bound) do not install asyncpg and therefore
    end up with a sync Engine.  Attempting to stream features must fail fast
    with a clear message pointing to the fix (add module_db to SCOPE).
    """
    engine = _FakeSyncEngine()
    svc = _StubItemService(engine)

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

        with pytest.raises(RuntimeError, match="feature_stream requires async engine"):
            async for _ in response.items:
                pass


@pytest.mark.asyncio
async def test_feature_stream_raises_for_none_engine() -> None:
    """RuntimeError is raised when engine is None (module_db not loaded)."""
    svc = _StubItemService(None)

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

        with pytest.raises(RuntimeError, match="feature_stream requires async engine"):
            async for _ in response.items:
                pass
