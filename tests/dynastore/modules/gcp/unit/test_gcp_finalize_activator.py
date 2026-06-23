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
    """Mimics a SQLAlchemy ``Result`` — supports ``.all()``, ``.fetchall()``, and ``.rowcount``.

    DQLQuery uses ``result.all()`` (via ``ResultHandler.ALL`` /
    ``ResultHandler.ROWCOUNT``); the original code used ``result.fetchall()``.
    Both methods are provided so the scaffold works with either call path.
    """

    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows

    def fetchall(self) -> List[Dict[str, Any]]:
        return list(self._rows)

    def all(self) -> List[Dict[str, Any]]:
        return list(self._rows)

    @property
    def rowcount(self) -> int:
        return len(self._rows)


class _FakeConn:
    """Captures executed SQL + binds.

    DQLQuery calls ``conn.execute(text(SQL), {binds})`` under the hood;
    this fake intercepts that call and records it in ``self.calls``.

    ``is_async_resource`` is patched in each test to return True for
    ``_FakeConn`` instances so DQLQuery dispatches via the async path.
    """

    def __init__(self, select_rows: List[Dict[str, Any]]) -> None:
        self._select_rows = select_rows
        self.calls: List[Dict[str, Any]] = []

    async def execute(self, sql_clause: Any, binds: Any = None, **_kw: Any) -> Any:
        bound = dict(binds) if isinstance(binds, dict) else {}
        self.calls.append({"sql": str(sql_clause), "binds": bound})
        if len(self.calls) == 1:
            return _FakeResult(self._select_rows)
        return _FakeResult([])


@pytest.fixture(autouse=True)
def _patch_is_async_resource():
    """Make DQLQuery treat ``_FakeConn`` as an async resource.

    DQLQuery dispatches via ``is_async_resource(conn)``; without this patch the
    sync path is chosen and the async ``_FakeConn.execute`` coroutine is never
    awaited.  The patch intercepts the check for ``_FakeConn`` while leaving all
    real SQLAlchemy types unaffected.
    """
    import dynastore.modules.db_config.query_executor as _qe

    _orig = _qe.is_async_resource

    def _patched(conn):
        if isinstance(conn, _FakeConn):
            return True
        return _orig(conn)

    with patch.object(_qe, "is_async_resource", side_effect=_patched):
        yield


def _make_event(
    *,
    catalog_id: str = "cat_a",
    collection_physical_id: Optional[str] = "c_phys_a",
    filename: str = "image.tif",
    md5_hash: Optional[str] = "abc==",
    size_bytes: Optional[int] = 12345,
    custom_metadata: Optional[Dict[str, Any]] = None,
) -> FinalizeEvent:
    # GCS stores objects under the collection PHYSICAL id, so the object-prefix
    # tail (and thus FinalizeEvent.collection_physical_id) is the physical id.
    return FinalizeEvent(
        bucket="bkt",
        object_name=f"collections/{collection_physical_id}/{filename}"
        if collection_physical_id
        else f"catalog/{filename}",
        filename=filename,
        uri=(
            f"gs://bkt/collections/{collection_physical_id}/{filename}"
            if collection_physical_id
            else f"gs://bkt/catalog/{filename}"
        ),
        catalog_id=catalog_id,
        collection_physical_id=collection_physical_id,
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
async def test_matches_pending_row_on_collection_physical_id() -> None:
    """Regression: the finalize event carries the collection PHYSICAL id (the
    object-prefix tail), and the activator must match the PENDING row on
    ``collection_physical_id`` — NOT the mutable logical ``collection_id``.

    Previously the matcher keyed on ``collection_id``, so the physical id from
    the event never matched the logical id on the row → 0 rows → the asset
    wedged in PENDING (orphan_finalize). This asserts both the SELECT and the
    UPDATE key on ``collection_physical_id`` and bind the event's physical id.
    """
    rows = [{"asset_id": "asset_phys", "status": "pending", "metadata": {}}]
    conn = _FakeConn(select_rows=rows)
    # The event's collection identifier IS the physical id (c_…), the value
    # stored in assets.collection_physical_id — never the logical 'gaul'.
    event = _make_event(collection_physical_id="c_3qwwjsp")

    outcome = await activate(conn, "schema_x", event)

    assert outcome.action == "activated"
    select_call, update_call = conn.calls[0], conn.calls[1]
    # Both statements key on the physical column, not the logical one.
    assert "collection_physical_id IS NOT DISTINCT FROM :collection_physical_id" in select_call["sql"]
    assert "collection_physical_id IS NOT DISTINCT FROM :collection_physical_id" in update_call["sql"]
    assert "AND collection_id " not in select_call["sql"]
    # The bound value is the physical id from the event.
    assert select_call["binds"]["collection_physical_id"] == "c_3qwwjsp"
    assert update_call["binds"]["collection_physical_id"] == "c_3qwwjsp"
    assert "collection_id" not in select_call["binds"]


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
    """UPDATE binds carry size_bytes verbatim and content_hash with the
    ``md5:`` algorithm prefix so cross-driver comparison via the
    CONTENT_HASH matcher can disambiguate (Stage F2)."""
    rows = [{"asset_id": "asset_M", "status": "pending", "metadata": {}}]
    conn = _FakeConn(select_rows=rows)
    event = _make_event(md5_hash="md5BASE64", size_bytes=98765)

    await activate(conn, "schema_x", event)

    update_call = conn.calls[1]
    assert update_call["binds"]["content_hash"] == "md5:md5BASE64"
    assert update_call["binds"]["size_bytes"] == 98765
    assert update_call["binds"]["uri"] == event.uri


@pytest.mark.asyncio
async def test_missing_md5_writes_null_content_hash() -> None:
    """When the finalize event has no md5 (rare — e.g. composite uploads),
    UPDATE binds the content_hash as NULL rather than the literal ``md5:``."""
    rows = [{"asset_id": "asset_X", "status": "pending", "metadata": {}}]
    conn = _FakeConn(select_rows=rows)
    event = _make_event(md5_hash=None, size_bytes=42)

    await activate(conn, "schema_x", event)

    update_call = conn.calls[1]
    assert update_call["binds"]["content_hash"] is None


def test_update_sql_sets_owned_by_and_clears_upload_metadata() -> None:
    """The activate UPDATE must claim the row for the GCS backend and scrub
    the leaked resumable-upload credential.

    * ``owned_by = 'gcs'`` so ``GcsAssetDownload.applies_to`` resolves a
      download backend (otherwise the endpoint 404s) and GCS metadata
      mirroring / hard-delete bucket cleanup stay enabled.
    * ``metadata = metadata - '_upload'`` removes the resumable upload URL +
      ticket_id + expires_at (an expired signed credential) from the row.
    """
    sql = activator_mod._UPDATE_ACTIVATE_SQL
    assert "owned_by = 'gcs'" in sql
    assert "metadata = metadata - '_upload'" in sql


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
        body, catalog_id="cat_a", collection_physical_id="c_phys1"
    )
    assert ev.bucket == "bkt"
    assert ev.object_name == "collections/col1/image.tif"
    assert ev.filename == "image.tif"
    assert ev.uri == "gs://bkt/collections/col1/image.tif"
    assert ev.catalog_id == "cat_a"
    assert ev.collection_physical_id == "c_phys1"
    assert ev.md5_hash == "MD5=="
    assert ev.size_bytes == 987
    assert ev.generation == "42"
    assert ev.custom_metadata == {"ticket_id": "T-1", "asset_type": "RASTER"}


def test_finalize_event_from_pubsub_tolerates_missing_size() -> None:
    """Some events omit ``size`` — must not crash, size_bytes stays None."""
    body = {"bucket": "bkt", "name": "catalog/f.geojson"}
    ev = finalize_event_from_pubsub(
        body, catalog_id="cat_b", collection_physical_id=None
    )
    assert ev.size_bytes is None
    assert ev.collection_physical_id is None
    assert ev.filename == "f.geojson"
    assert ev.uri == "gs://bkt/catalog/f.geojson"
