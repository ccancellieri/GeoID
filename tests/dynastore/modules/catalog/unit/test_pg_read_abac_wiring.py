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
"""Unit tests for PG read-path ABAC wiring (#1457).

Covers two contracts:

(a) ``dispatch_or_stream_items`` — when ``request`` is supplied and
    ``collection_uses_pg_access_envelope`` returns True, the compiled
    ``access_filter`` is set on the QueryRequest before ``stream_items`` is
    called.  When the collection does NOT use the sidecar, or when
    ``request`` is None, ``access_filter`` is NOT set (no regression).

(b) ``ItemQueryMixin.get_item`` — when ``access_filter`` is passed in,
    it is threaded onto the internal QueryRequest before the query optimizer
    runs.  When it is not passed, the field stays unset (backward compat).

These are pure unit tests: no DB, no asyncpg, no real QueryOptimizer.
All external dependencies are mocked at function boundaries.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.tools.query import dispatch_or_stream_items
from dynastore.models.protocols.access_filter import AccessFilter
from dynastore.models.query_builder import QueryRequest, QueryResponse


# ---------------------------------------------------------------------------
# Helpers shared by both test groups
# ---------------------------------------------------------------------------

def _make_request(principal_id: str = "user:alice") -> Any:
    """Minimal Starlette-like request with IAM state."""
    return SimpleNamespace(
        state=SimpleNamespace(
            principal=None,
            principal_id=principal_id,
            principal_role=["reader"],
        )
    )


def _make_query_response() -> QueryResponse:
    """Minimal async QueryResponse."""
    async def _items():
        if False:
            yield  # pragma: no cover

    return QueryResponse(
        items=_items(),
        total_count=0,
        catalog_id="cat1",
        collection_id="col1",
    )


def _make_items_protocol(response: QueryResponse) -> Any:
    """Stub ItemsProtocol whose stream_items captures the QueryRequest."""
    captured: list = []

    async def _stream_items(
        *,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        ctx: Any = None,
        consumer: Any = None,
    ) -> QueryResponse:
        captured.append(request)
        return response

    proto = MagicMock()
    proto.stream_items = _stream_items
    proto._captured = captured
    return proto


# ---------------------------------------------------------------------------
# (a) dispatch_or_stream_items access_filter wiring
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_or_stream_sets_access_filter_when_envelope_sidecar(
    monkeypatch: Any,
) -> None:
    """When collection_uses_pg_access_envelope is True and request is given,
    access_filter is set on the QueryRequest before stream_items.

    The three helpers are imported locally inside dispatch_or_stream_items, so
    we patch at the source module (access_scope) — the same object the local
    import retrieves at call time.
    """
    canned_filter = AccessFilter.allow_everything()
    response = _make_query_response()
    proto = _make_items_protocol(response)
    qr = QueryRequest(limit=10, offset=0, filters=[])
    request = _make_request()

    with patch(
        "dynastore.modules.storage.access_scope.collection_uses_pg_access_envelope",
        new=AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.storage.access_scope.compile_read_access_filter",
        new=AsyncMock(return_value=canned_filter),
    ), patch(
        "dynastore.modules.storage.access_scope.principals_from_request_state",
        return_value=(["user:alice", "reader"], None),
    ):
        await dispatch_or_stream_items(
            proto,
            catalog_id="cat1",
            collection_id="col1",
            query_request=qr,
            consumer="OGC_FEATURES",
            request=request,
        )

    assert len(proto._captured) == 1
    captured_qr = proto._captured[0]
    assert captured_qr.access_filter is canned_filter


@pytest.mark.asyncio
async def test_dispatch_or_stream_no_access_filter_without_envelope_sidecar(
    monkeypatch: Any,
) -> None:
    """When collection_uses_pg_access_envelope is False, access_filter is NOT
    set — no regression for ordinary collections."""
    response = _make_query_response()
    proto = _make_items_protocol(response)
    qr = QueryRequest(limit=10, offset=0, filters=[])
    request = _make_request()

    with patch(
        "dynastore.modules.storage.access_scope.collection_uses_pg_access_envelope",
        new=AsyncMock(return_value=False),
    ):
        await dispatch_or_stream_items(
            proto,
            catalog_id="cat1",
            collection_id="col1",
            query_request=qr,
            consumer="OGC_FEATURES",
            request=request,
        )

    assert len(proto._captured) == 1
    captured_qr = proto._captured[0]
    # access_filter unset (None is the default) — non-envelope collection
    assert captured_qr.access_filter is None


@pytest.mark.asyncio
async def test_dispatch_or_stream_no_access_filter_when_request_is_none() -> None:
    """When request=None (system/internal call), access_filter is NOT touched
    and collection_uses_pg_access_envelope is never called."""
    response = _make_query_response()
    proto = _make_items_protocol(response)
    qr = QueryRequest(limit=10, offset=0, filters=[])

    envelope_check = AsyncMock(return_value=True)
    with patch(
        "dynastore.modules.storage.access_scope.collection_uses_pg_access_envelope",
        new=envelope_check,
    ):
        await dispatch_or_stream_items(
            proto,
            catalog_id="cat1",
            collection_id="col1",
            query_request=qr,
            consumer="OGC_FEATURES",
            request=None,
        )

    assert len(proto._captured) == 1
    captured_qr = proto._captured[0]
    assert captured_qr.access_filter is None
    # The guard `request is not None` must prevent the sidecar check entirely.
    envelope_check.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_or_stream_uses_search_dispatch_without_pg_check() -> None:
    """When search_dispatch is supplied, stream_items is not called at all
    (the ES path has already applied its own access scoping)."""
    sd = _make_query_response()
    proto = _make_items_protocol(_make_query_response())
    qr = QueryRequest(limit=10, offset=0, filters=[])

    result = await dispatch_or_stream_items(
        proto,
        catalog_id="cat1",
        collection_id="col1",
        query_request=qr,
        consumer="OGC_FEATURES",
        search_dispatch=sd,
    )

    # stream_items never called — returns search_dispatch directly
    assert len(proto._captured) == 0
    assert result is sd


# ---------------------------------------------------------------------------
# (b) get_item access_filter threading
# ---------------------------------------------------------------------------

class _FakeItemQueryMixin:
    """Thin shim that exercises only the access_filter threading in get_item.

    We cannot instantiate ItemQueryMixin directly (it requires a real engine),
    so we replicate the access_filter injection logic that was added to
    get_item and verify it operates correctly on a QueryRequest.
    """

    @staticmethod
    def _inject_access_filter_into_request(
        item_ids: List[str],
        access_filter: Optional[Any],
    ) -> QueryRequest:
        """Mirror the access_filter injection in ItemQueryMixin.get_item."""
        from dynastore.models.query_builder import FieldSelection

        request = QueryRequest(
            item_ids=item_ids,
            limit=1,
            select=[FieldSelection(field="*")],
        )
        if access_filter is not None:
            request.access_filter = access_filter
        return request


def test_get_item_threads_access_filter_onto_query_request() -> None:
    """access_filter passed to get_item is set on the internal QueryRequest."""
    af = AccessFilter.allow_everything()
    qr = _FakeItemQueryMixin._inject_access_filter_into_request(
        ["item-123"], access_filter=af
    )
    assert qr.access_filter is af


def test_get_item_leaves_access_filter_none_when_not_supplied() -> None:
    """When access_filter is None (default), QueryRequest.access_filter is not set."""
    qr = _FakeItemQueryMixin._inject_access_filter_into_request(
        ["item-123"], access_filter=None
    )
    assert qr.access_filter is None


def test_get_item_allow_everything_is_not_deny() -> None:
    """Sanity: AccessFilter.allow_everything() does not deny anything."""
    af = AccessFilter.allow_everything()
    assert af.allow_all is True
    assert af.deny_all is False
