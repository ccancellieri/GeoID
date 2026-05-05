"""Index-failures route — paginated JSON list of failure rows for a
catalog plus auth gating.

Covers two layers:

* **Unit** — handler called directly with a stub ``IndexFailureLog`` to
  pin the response shape (``items`` + ``_links.self`` + ``_links.next``)
  and the auth-required behaviour without touching FastAPI's full app
  stack (no ``api_client`` fixture in this repo, see Phase 14
  follow-ups).

* **Integration** — handler driven against a real per-tenant PG schema
  via ``async_conn`` + ``async_schema`` from the local conftest.
  Exercises the actual :class:`PgIndexFailureLog` adapter so the wire
  shape is verified against rows produced by the production code path
  the drain task uses.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional, Sequence
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from dynastore.extensions.index_failures.service import IndexFailureService
from dynastore.models.protocols.indexing import IndexFailureRecord


def _make_record(idx: int) -> IndexFailureRecord:
    return IndexFailureRecord(
        failure_id=uuid4(),
        occurred_at=datetime(2026, 1, 1, 12, 0, idx, tzinfo=timezone.utc),
        collection_id="cc",
        driver_id="items_elasticsearch_driver",
        driver_instance_id="x",
        op_id=uuid4(),
        item_id=f"i{idx}",
        op="upsert",
        attempts=4,
        error_class="X",
        error_message=f"msg{idx}",
        status="failed",
        correlation_id=None,
    )


class _StubLog:
    """In-memory ``IndexFailureLog`` stub for handler-level tests."""

    def __init__(self, records: Sequence[IndexFailureRecord]) -> None:
        self._records = list(records)
        self.last_call: dict[str, Any] = {}

    async def record(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401
        raise NotImplementedError

    async def list_failures(
        self,
        *,
        catalog_id: str,
        collection_id: Optional[str] = None,
        driver_id: Optional[str] = None,
        since: Optional[datetime] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[IndexFailureRecord]:
        self.last_call = dict(
            catalog_id=catalog_id, collection_id=collection_id,
            driver_id=driver_id, since=since, status=status,
            limit=limit, offset=offset,
        )
        return list(self._records[offset : offset + limit])


def _fake_request(url: str, principal: Any = object()) -> Any:
    """Build a minimal ``Request``-shaped object for the handler.

    Only the attributes the handler reads are populated:
    ``state.principal`` (auth gate) and ``url`` (used for
    ``_links.self`` / ``_links.next``). Starlette's
    :class:`URL.include_query_params` does the heavy lifting on URL
    rewriting, so we hand it a real ``URL`` instance.
    """
    from starlette.datastructures import URL

    req = MagicMock()
    req.state.principal = principal
    req.url = URL(url)
    return req


@pytest.mark.asyncio
async def test_handler_returns_items_and_links_with_next() -> None:
    """3 records + limit=2 → items=2, next non-null pointing at offset=2."""
    svc = IndexFailureService()
    svc.log_factory = lambda: _StubLog([_make_record(i) for i in range(3)])

    body = await svc.list_failures(
        request=_fake_request(
            "https://x.test/v2/api/index_failures/catalogs/cat1/index-failures",
        ),
        catalog_id="cat1",
        limit=2,
    )

    assert "items" in body and len(body["items"]) == 2
    assert body["_links"]["self"].endswith("/index-failures")
    nxt = body["_links"]["next"]
    assert nxt is not None and "offset=2" in nxt


@pytest.mark.asyncio
async def test_handler_omits_next_when_page_not_full() -> None:
    """Less-than-limit rows returned → next is None (last page)."""
    svc = IndexFailureService()
    svc.log_factory = lambda: _StubLog([_make_record(0)])

    body = await svc.list_failures(
        request=_fake_request(
            "https://x.test/v2/api/index_failures/catalogs/cat1/index-failures",
        ),
        catalog_id="cat1",
        limit=10,
    )

    assert len(body["items"]) == 1
    assert body["_links"]["next"] is None


@pytest.mark.asyncio
async def test_handler_passes_filters_through_to_log() -> None:
    """Query params land on :meth:`IndexFailureLog.list_failures` verbatim."""
    stub = _StubLog([])
    svc = IndexFailureService()
    svc.log_factory = lambda: stub

    since = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await svc.list_failures(
        request=_fake_request(
            "https://x.test/v2/api/index_failures/catalogs/cat1/index-failures",
        ),
        catalog_id="cat1",
        collection="C",
        driver="items_elasticsearch_driver",
        since=since,
        status="retrying",
        limit=50,
        offset=10,
    )

    assert stub.last_call["catalog_id"] == "cat1"
    assert stub.last_call["collection_id"] == "C"
    assert stub.last_call["driver_id"] == "items_elasticsearch_driver"
    assert stub.last_call["since"] == since
    assert stub.last_call["status"] == "retrying"
    assert stub.last_call["limit"] == 50
    assert stub.last_call["offset"] == 10


@pytest.mark.asyncio
async def test_handler_rejects_anonymous_with_401() -> None:
    """No ``request.state.principal`` → 401 (defense in depth behind IAM)."""
    from fastapi import HTTPException

    svc = IndexFailureService()
    svc.log_factory = lambda: _StubLog([])

    with pytest.raises(HTTPException) as exc_info:
        await svc.list_failures(
            request=_fake_request(
                "https://x.test/v2/api/index_failures/catalogs/cat1/index-failures",
                principal=None,
            ),
            catalog_id="cat1",
        )
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_handler_response_shape_serialises_to_json(
    async_conn, async_schema,
) -> None:
    """End-to-end against real PG: seed 3 rows, GET via handler, verify
    the wire shape (``items`` + ``_links.next``) and that the records
    flowed through :class:`PgIndexFailureLog`."""
    from dynastore.modules.storage.outbox_ddl import (
        ensure_index_failure_log_asyncpg,
    )
    from dynastore.modules.storage.pg_index_failure_log import (
        PgIndexFailureLog,
    )

    await ensure_index_failure_log_asyncpg(async_conn, async_schema)
    log = PgIndexFailureLog(single_conn=async_conn)
    for i in range(3):
        await log.record(
            async_conn,
            catalog_id=async_schema, collection_id="cc",
            driver_instance_id="x",
            driver_id="items_elasticsearch_driver",
            op_id=uuid4(), item_id=f"i{i}", op="upsert",
            attempts=4, error_class="X", error_message=f"msg{i}",
            status="failed",
        )

    svc = IndexFailureService()
    svc.log_factory = lambda: log  # reuse the test-mode log

    body = await svc.list_failures(
        request=_fake_request(
            f"https://x.test/v2/api/index_failures/catalogs/{async_schema}/index-failures",
        ),
        catalog_id=async_schema,
        limit=2,
    )

    assert len(body["items"]) == 2
    nxt = body["_links"]["next"]
    assert nxt is not None and "offset=2" in nxt
    # Sanity: items carry the wire-shape fields (str-form UUIDs).
    assert all(isinstance(it["failure_id"], str) for it in body["items"])
    assert all(it["status"] == "failed" for it in body["items"])
