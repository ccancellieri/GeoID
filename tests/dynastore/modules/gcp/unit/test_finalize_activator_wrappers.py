#    Copyright 2025 FAO
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

"""Contract tests: gcp_finalize_activator routes SQL through DQLQuery wrappers.

Verifies that the SELECT and UPDATE paths in :func:`activate` use
DQLQuery rather than raw ``conn.execute(text(...))``, so they benefit
from the standardized executor infrastructure (connection-lock scope,
retry, error mapping, result handling).

Two complementary layers:

1. **Source-level grep guards** — ``inspect.getsource`` confirms the
   banned pattern ``conn.execute(text(`` is absent and the expected
   wrappers appear. These catch regressions at import time with no DB.

2. **Behaviour tests with a fake conn** — exercise the live code path
   through a minimal ``AsyncConnection``-shaped mock that captures every
   ``execute`` call. Assert on SQL text and bind-parameter names.
"""

from __future__ import annotations

import inspect
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.modules.gcp import gcp_finalize_activator as _mod
from dynastore.modules.gcp.gcp_finalize_activator import (
    FinalizeEvent,
    OrphanFinalizeEvent,
    activate,
)


# ---------------------------------------------------------------------------
# Source-level grep guards
# ---------------------------------------------------------------------------


def test_select_path_uses_dqlquery_not_raw_execute():
    """The SELECT FOR UPDATE must go through DQLQuery, not raw conn.execute."""
    src = inspect.getsource(_mod)
    # Raw pattern must be absent from the module.
    assert "conn.execute(text(" not in src
    # DQLQuery must be referenced in the activate function body.
    activate_src = inspect.getsource(_mod.activate)
    assert "DQLQuery" in activate_src


def test_update_path_uses_dqlquery_not_raw_execute():
    """The UPDATE path must go through DQLQuery, not raw conn.execute."""
    activate_src = inspect.getsource(_mod.activate)
    # Both SELECT and UPDATE route through DQLQuery.
    assert activate_src.count("DQLQuery") >= 2
    # No inline text() call on conn.
    assert "conn.execute(" not in activate_src


# ---------------------------------------------------------------------------
# Fake-conn behaviour tests
# ---------------------------------------------------------------------------


class _FakeResult:
    """Minimal Result duck-type supporting ``.all()`` and ``.rowcount``."""

    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> List[Dict[str, Any]]:
        return list(self._rows)

    @property
    def rowcount(self) -> int:
        return len(self._rows)


def _make_fake_async_conn(select_rows: List[Dict[str, Any]]) -> Any:
    """Build an AsyncConnection-shaped mock that captures SQL + bind params.

    Speccing against ``AsyncConnection`` makes ``is_async_resource`` return
    True so DQLQuery dispatches the async code path.  ``execute`` is an
    ``AsyncMock`` that records every call and returns staged results.
    """
    calls: List[Dict[str, Any]] = []

    async def _execute(sql_clause: Any, params: Any = None, **_kw: Any) -> Any:
        bound = dict(params) if isinstance(params, dict) else {}
        calls.append({"sql": str(sql_clause), "params": bound})
        if len(calls) == 1:
            return _FakeResult(select_rows)
        return _FakeResult([])

    conn = MagicMock(spec=AsyncConnection)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.in_transaction = MagicMock(return_value=False)
    conn.calls = calls
    return conn


@pytest.fixture(autouse=True)
def _patch_is_async_for_mock():
    """Extend is_async_resource to treat MagicMock(spec=AsyncConnection) as async."""
    import dynastore.modules.db_config.query_executor as _qe

    _orig = _qe.is_async_resource

    def _patched(c):
        # MagicMock(spec=AsyncConnection) has __class__ pointing to AsyncConnection.
        if isinstance(c, MagicMock):
            return True
        return _orig(c)

    with patch.object(_qe, "is_async_resource", side_effect=_patched):
        yield


def _pending_row(asset_id: str = "asset_1") -> Dict[str, Any]:
    return {"asset_id": asset_id, "status": "pending", "metadata": {}}


def _make_event(
    catalog_id: str = "cat",
    collection_id: Optional[str] = "col",
    filename: str = "file.tif",
    md5_hash: Optional[str] = "abc==",
) -> FinalizeEvent:
    return FinalizeEvent(
        bucket="bkt",
        object_name=f"path/{filename}",
        filename=filename,
        uri=f"gs://bkt/path/{filename}",
        catalog_id=catalog_id,
        collection_id=collection_id,
        md5_hash=md5_hash,
        size_bytes=100,
    )


@pytest.mark.asyncio
async def test_select_uses_dqlquery_named_params():
    """SELECT is issued via DQLQuery with named bind parameters."""
    conn = _make_fake_async_conn([_pending_row()])
    event = _make_event()

    with patch("dynastore.modules.catalog.log_manager.log_event", AsyncMock()):
        await activate(conn, "test_schema", event)

    assert len(conn.calls) >= 1
    first_call = conn.calls[0]
    # The SQL must contain the FOR UPDATE lock and reference the schema.
    assert "FOR UPDATE" in first_call["sql"]
    assert "test_schema" in first_call["sql"]
    # Params must use named keys, never positional lists.
    assert "catalog_id" in first_call["params"]
    assert "collection_id" in first_call["params"]
    assert "filename" in first_call["params"]
    assert first_call["params"]["catalog_id"] == "cat"
    assert first_call["params"]["filename"] == "file.tif"


@pytest.mark.asyncio
async def test_update_uses_dqlquery_named_params():
    """UPDATE is issued via DQLQuery with named bind parameters including asset_id."""
    conn = _make_fake_async_conn([_pending_row("asset_99")])
    event = _make_event(md5_hash="myhash")

    with patch("dynastore.modules.catalog.log_manager.log_event", AsyncMock()):
        await activate(conn, "test_schema", event)

    # Second call is the UPDATE.
    assert len(conn.calls) == 2
    update_call = conn.calls[1]
    assert "UPDATE" in update_call["sql"].upper()
    assert "asset_id" in update_call["params"]
    assert update_call["params"]["asset_id"] == "asset_99"
    assert update_call["params"]["content_hash"] == "md5:myhash"


@pytest.mark.asyncio
async def test_orphan_skips_update_call():
    """No rows → orphan path → exactly one execute call (SELECT only)."""
    conn = _make_fake_async_conn([])
    event = _make_event()

    with patch("dynastore.modules.catalog.log_manager.log_event", AsyncMock()):
        with pytest.raises(OrphanFinalizeEvent):
            await activate(conn, "s", event)

    assert len(conn.calls) == 1
