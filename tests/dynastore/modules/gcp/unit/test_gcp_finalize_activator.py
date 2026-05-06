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

"""Stage 4.2 inline-activator unit tests.

Pure unit coverage of :func:`gcp_finalize_activator.activate` and
:func:`finalize_event_from_pubsub` — no real DB, no real Pub/Sub.

We mock the connection's ``execute`` coroutine and assert on the SQL
strings + bind parameters. The activator is intentionally written so
that the SELECT/UPDATE SQL strings are deterministic, schema-prefixed
strings — easy to inspect without parsing.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.gcp import gcp_finalize_activator as activator_mod
from dynastore.modules.gcp.gcp_finalize_activator import (
    ActivationOutcome,
    FinalizeEvent,
    OrphanFinalizeEvent,
    activate,
    finalize_event_from_pubsub,
)


# ---------------------------------------------------------------------------
# Test scaffolding — _FakeConn captures every SQL string + bind dict.
# ---------------------------------------------------------------------------


class _FakeResult:
    """Mimics SQLAlchemy ``Result.fetchall()``."""

    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows

    def fetchall(self) -> List[Dict[str, Any]]:
        return list(self._rows)


class _FakeConn:
    """Captures executed SQL + binds.

    The activator runs ``conn.execute(text(SQL), {binds})`` — the first
    call is the SELECT FOR UPDATE, the second is the UPDATE. We let
    callers stage the SELECT result via ``select_rows``.
    """

    def __init__(self, select_rows: List[Dict[str, Any]]) -> None:
        self._select_rows = select_rows
        self.calls: List[Dict[str, Any]] = []

    async def execute(self, sql_clause: Any, binds: Dict[str, Any]) -> Any:
        # ``text()`` clauses str() back to the rendered SQL string.
        self.calls.append({"sql": str(sql_clause), "binds": dict(binds)})
        if len(self.calls) == 1:
            return _FakeResult(self._select_rows)
        return _FakeResult([])


def _make_event(
    *,
    catalog_id: str = "cat_a",
    collection_id: Optional[str] = "col_a",
    filename: str = "image.tif",
    md5_hash: Optional[str] = "abc==",
    size_bytes: Optional[int] = 12345,
    custom_metadata: Optional[Dict[str, Any]] = None,
) -> FinalizeEvent:
    return FinalizeEvent(
        bucket="bkt",
        object_name=f"collections/{collection_id}/{filename}"
        if collection_id
        else f"catalog/{filename}",
        filename=filename,
        uri=(
            f"gs://bkt/collections/{collection_id}/{filename}"
            if collection_id
            else f"gs://bkt/catalog/{filename}"
        ),
        catalog_id=catalog_id,
        collection_id=collection_id,
        md5_hash=md5_hash,
        size_bytes=size_bytes,
        content_type="image/tiff",
        generation="42",
        custom_metadata=custom_metadata or {},
    )


# ---------------------------------------------------------------------------
# Activator behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_activates_pending_row_to_active() -> None:
    """A single PENDING row → SELECT then UPDATE → action='activated'."""
    rows = [{"asset_id": "asset_42", "status": "pending", "metadata": {}}]
    conn = _FakeConn(select_rows=rows)
    event = _make_event()

    outcome = await activate(conn, "schema_x", event)

    assert isinstance(outcome, ActivationOutcome)
    assert outcome.action == "activated"
    assert outcome.asset_id == "asset_42"
    # SELECT then UPDATE — exactly two execute calls.
    assert len(conn.calls) == 2
    assert "FOR UPDATE" in conn.calls[0]["sql"]
    assert "UPDATE" in conn.calls[1]["sql"].upper()


@pytest.mark.asyncio
async def test_idempotent_redelivery_no_op() -> None:
    """Row already at status='active' → no UPDATE issued."""
    rows = [{"asset_id": "asset_99", "status": "active", "metadata": {}}]
    conn = _FakeConn(select_rows=rows)
    event = _make_event()

    outcome = await activate(conn, "schema_x", event)

    assert outcome.action == "already_active"
    assert outcome.asset_id == "asset_99"
    # Only the SELECT ran — no UPDATE.
    assert len(conn.calls) == 1
    assert "FOR UPDATE" in conn.calls[0]["sql"]


@pytest.mark.asyncio
async def test_orphan_event_logs_and_raises() -> None:
    """No rows match → orphan_finalize logged, OrphanFinalizeEvent raised."""
    conn = _FakeConn(select_rows=[])
    event = _make_event()

    fake_log = AsyncMock()
    with patch(
        "dynastore.modules.catalog.log_manager.log_event", fake_log
    ):
        with pytest.raises(OrphanFinalizeEvent):
            await activate(conn, "schema_x", event)

    # log_event called exactly once with event_type='orphan_finalize'.
    fake_log.assert_awaited_once()
    call = fake_log.call_args
    assert call.kwargs["event_type"] == "orphan_finalize"
    assert call.kwargs["catalog_id"] == "cat_a"
    assert call.kwargs["level"] == "WARNING"


@pytest.mark.asyncio
async def test_for_update_lock_present_in_sql() -> None:
    """The SELECT must lock — no SKIP LOCKED. Concurrent redeliveries
    serialise so the second sees the committed ACTIVE row."""
    conn = _FakeConn(
        select_rows=[{"asset_id": "x", "status": "pending", "metadata": {}}]
    )
    await activate(conn, "schema_x", _make_event())
    select_sql = conn.calls[0]["sql"]
    assert "FOR UPDATE" in select_sql
    assert "SKIP LOCKED" not in select_sql


@pytest.mark.asyncio
async def test_ticket_id_disambiguation_when_present() -> None:
    """Multiple PENDING rows → activator picks the one whose
    metadata._upload.ticket_id matches event.custom_metadata['ticket_id']."""
    rows = [
        {
            "asset_id": "wrong_one",
            "status": "pending",
            "metadata": {"_upload": {"ticket_id": "T-OTHER"}},
        },
        {
            "asset_id": "right_one",
            "status": "pending",
            "metadata": {"_upload": {"ticket_id": "T-MINE"}},
        },
    ]
    conn = _FakeConn(select_rows=rows)
    event = _make_event(custom_metadata={"ticket_id": "T-MINE"})

    outcome = await activate(conn, "schema_x", event)

    assert outcome.action == "activated"
    assert outcome.asset_id == "right_one"
    # The UPDATE bind set must address the chosen asset_id.
    update_call = conn.calls[1]
    assert update_call["binds"]["asset_id"] == "right_one"


@pytest.mark.asyncio
async def test_md5_and_size_propagated_to_update() -> None:
    """The UPDATE bind dict must carry content_hash + size_bytes from the event."""
    rows = [{"asset_id": "asset_M", "status": "pending", "metadata": {}}]
    conn = _FakeConn(select_rows=rows)
    event = _make_event(md5_hash="md5BASE64", size_bytes=98765)

    await activate(conn, "schema_x", event)

    update_call = conn.calls[1]
    assert update_call["binds"]["content_hash"] == "md5BASE64"
    assert update_call["binds"]["size_bytes"] == 98765
    assert update_call["binds"]["uri"] == event.uri


@pytest.mark.asyncio
async def test_ticket_id_disambiguation_handles_jsonstring_metadata() -> None:
    """Driver may surface metadata as a JSON string (asyncpg cursor) — the
    activator's row-getter must tolerate that without crashing."""
    rows = [
        {
            "asset_id": "right_one",
            "status": "pending",
            "metadata": json.dumps({"_upload": {"ticket_id": "T-MINE"}}),
        }
    ]
    conn = _FakeConn(select_rows=rows)
    event = _make_event(custom_metadata={"ticket_id": "T-MINE"})

    outcome = await activate(conn, "schema_x", event)
    assert outcome.action == "activated"
    assert outcome.asset_id == "right_one"


# ---------------------------------------------------------------------------
# finalize_event_from_pubsub
# ---------------------------------------------------------------------------


def test_finalize_event_from_pubsub_basic_shape() -> None:
    """Synthetic Pub/Sub payload reduces to the expected FinalizeEvent."""
    body = {
        "bucket": "bkt",
        "name": "collections/col1/image.tif",
        "size": "987",
        "md5Hash": "MD5==",
        "metadata": {"ticket_id": "T-1", "asset_type": "RASTER"},
        "generation": 42,
        "contentType": "image/tiff",
    }
    ev = finalize_event_from_pubsub(
        body, catalog_id="cat_a", collection_id="col1"
    )
    assert ev.bucket == "bkt"
    assert ev.object_name == "collections/col1/image.tif"
    assert ev.filename == "image.tif"
    assert ev.uri == "gs://bkt/collections/col1/image.tif"
    assert ev.catalog_id == "cat_a"
    assert ev.collection_id == "col1"
    assert ev.md5_hash == "MD5=="
    assert ev.size_bytes == 987
    assert ev.generation == "42"
    assert ev.custom_metadata == {"ticket_id": "T-1", "asset_type": "RASTER"}


def test_finalize_event_from_pubsub_tolerates_missing_size() -> None:
    """Some events omit ``size`` — must not crash, size_bytes stays None."""
    body = {"bucket": "bkt", "name": "catalog/f.geojson"}
    ev = finalize_event_from_pubsub(
        body, catalog_id="cat_b", collection_id=None
    )
    assert ev.size_bytes is None
    assert ev.collection_id is None
    assert ev.filename == "f.geojson"
    assert ev.uri == "gs://bkt/catalog/f.geojson"
