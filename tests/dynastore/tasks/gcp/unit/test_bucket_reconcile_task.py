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

"""Unit tests for the Stage 6 bucket↔DB reconciliation task.

The reconcile algorithm has two collaborator boundaries we mock:

1. ``list_bucket_blobs`` — replaced with an in-memory list of
   ``(blob_path, filename)`` tuples. No real GCS client, no Pub/Sub —
   per ``feedback_local_pubsub_events_disable.md``.

2. ``managed_transaction`` — patched to yield a recording connection
   that captures every ``execute(text, params)`` so we can assert on
   INSERT / UPDATE shape without spinning up PG.

That pair lets us drive the diff-and-apply loop end-to-end with pure
Python doubles and verify each transition (orphan blob, ghost row,
stuck PENDING, no-drift) in isolation.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock

import pytest

from dynastore.tasks.gcp import bucket_reconcile_task as brt_module
from dynastore.tasks.gcp.bucket_reconcile_task import (
    BucketReconcileInputs,
    DriftKind,
    reconcile_bucket,
)


# ---------------------------------------------------------------------------
# Recording fakes
# ---------------------------------------------------------------------------


class _RecordingConn:
    """Records every ``execute(stmt, params)`` for later inspection.

    Mirrors the SQLAlchemy ``AsyncConnection.execute`` shape close enough
    that ``DQLQuery.execute`` (which calls ``conn.execute(text, params)``
    and reads ``.rowcount`` from the result) doesn't blow up.
    """

    def __init__(self) -> None:
        self.calls: List[Tuple[str, Dict[str, Any]]] = []

    async def execute(self, stmt: Any, params: Optional[Dict[str, Any]] = None) -> Any:
        # ``stmt`` is a SQLAlchemy ``TextClause``; for assertion purposes
        # we keep the rendered string (it round-trips via ``.text``).
        sql = str(getattr(stmt, "text", stmt))
        self.calls.append((sql, dict(params or {})))

        class _Result:
            rowcount = 1

            def fetchone(self):  # pragma: no cover - unused
                return None

            def fetchall(self):  # used by SELECT path
                return []

            def all(self):
                return []

        return _Result()


@pytest.fixture
def recording_conn(monkeypatch: pytest.MonkeyPatch) -> _RecordingConn:
    """Patch ``managed_transaction`` to yield a single ``_RecordingConn``."""
    conn = _RecordingConn()

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield conn

    monkeypatch.setattr(
        brt_module, "managed_transaction", _fake_managed_transaction
    )
    return conn


def _patch_bucket(
    monkeypatch: pytest.MonkeyPatch,
    blobs: List[Tuple[str, str]],
) -> None:
    """Replace ``list_bucket_blobs`` with a coroutine returning ``blobs``."""

    async def _fake_list(_client, _bucket, _prefix):
        return list(blobs)

    monkeypatch.setattr(brt_module, "list_bucket_blobs", _fake_list)


def _patch_db_rows(
    monkeypatch: pytest.MonkeyPatch,
    rows: List[Dict[str, Any]],
    capture: Optional[List[Tuple[str, Dict[str, Any]]]] = None,
) -> None:
    """Stub ``DQLQuery.execute`` so the reconcile loop runs without a real PG.

    First call (the SELECT) returns ``rows``; subsequent INSERT/UPDATE
    calls record ``(sql, params)`` into ``capture`` (when supplied) and
    return ``1`` (mimicking ``rowcount`` for the ROWCOUNT handler).
    """
    call_state = {"select_consumed": False}

    async def _fake_execute(self, conn, **params):
        if not call_state["select_consumed"]:
            call_state["select_consumed"] = True
            return rows
        sql = self.template if hasattr(self, "template") else ""
        if capture is not None:
            capture.append((sql, dict(params)))
        return 1  # ROWCOUNT-shaped return

    monkeypatch.setattr(brt_module.DQLQuery, "execute", _fake_execute)


def _make_inputs(
    *,
    catalog_id: str = "cat-1",
    collection_id: Optional[str] = None,
    apply: bool = False,
    pending_ttl_minutes: int = 60,
) -> BucketReconcileInputs:
    return BucketReconcileInputs(
        catalog_id=catalog_id,
        collection_id=collection_id,
        apply=apply,
        pending_ttl_minutes=pending_ttl_minutes,
    )


async def _run(
    inputs: BucketReconcileInputs,
    *,
    schema: str = "ds_test",
    bucket_name: str = "bkt",
):
    return await reconcile_bucket(
        inputs,
        schema=schema,
        bucket_name=bucket_name,
        storage_client=AsyncMock(),
        engine=AsyncMock(),
    )


# ---------------------------------------------------------------------------
# 1. No drift → empty report
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_drift_returns_empty_report(
    monkeypatch: pytest.MonkeyPatch, recording_conn: _RecordingConn
) -> None:
    # Bucket has data.tif under collections/col1/, DB has the matching row.
    _patch_bucket(
        monkeypatch,
        [("collections/col1/data.tif", "data.tif")],
    )
    captured: List[Tuple[str, Dict[str, Any]]] = []
    _patch_db_rows(
        monkeypatch,
        [
            {
                "asset_id": "a1",
                "collection_id": "col1",
                "filename": "data.tif",
                "status": "active",
                "uri": "gs://bkt/collections/col1/data.tif",
                "owned_by": "gcs",
                "kind": "physical",
                "created_at": datetime.now(timezone.utc),
                "metadata": {},
            }
        ],
        capture=captured,
    )

    report = await _run(_make_inputs(collection_id="col1", apply=True))

    assert report.orphans_imported == 0
    assert report.ghosts_marked_failed == 0
    assert report.stuck_pending_failed == 0
    assert report.drift_details == []
    assert report.total_bucket_blobs == 1
    assert report.total_db_rows == 1
    # No mutations issued — apply branch never fired because no drift.
    assert captured == []


# ---------------------------------------------------------------------------
# 2. Orphan blob — apply=True imports as ACTIVE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_orphan_blob_imported_when_apply_true(
    monkeypatch: pytest.MonkeyPatch, recording_conn: _RecordingConn
) -> None:
    _patch_bucket(
        monkeypatch,
        [("collections/col1/data.tif", "data.tif")],
    )
    captured: List[Tuple[str, Dict[str, Any]]] = []
    _patch_db_rows(monkeypatch, [], capture=captured)

    report = await _run(_make_inputs(collection_id="col1", apply=True))

    assert report.orphans_imported == 1
    assert report.dry_run is False
    assert len(report.drift_details) == 1
    entry = report.drift_details[0]
    assert entry.kind == DriftKind.ORPHAN_BLOB
    assert entry.filename == "data.tif"
    assert entry.uri == "gs://bkt/collections/col1/data.tif"

    # An INSERT was recorded with status='active', owned_by='gcs:reconcile_orphan'.
    inserts = [c for c in captured if c[0].lstrip().startswith("INSERT")]
    assert len(inserts) == 1
    sql, params = inserts[0]
    assert params["status"] == "active"
    assert params["owned_by"] == "gcs:reconcile_orphan"
    assert params["kind"] == "physical"
    assert params["filename"] == "data.tif"
    assert params["uri"] == "gs://bkt/collections/col1/data.tif"
    assert params["collection_id"] == "col1"


# ---------------------------------------------------------------------------
# 3. Orphan blob — apply=False reports but does NOT mutate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_orphan_blob_dry_run_does_not_insert(
    monkeypatch: pytest.MonkeyPatch, recording_conn: _RecordingConn
) -> None:
    _patch_bucket(
        monkeypatch,
        [("collections/col1/data.tif", "data.tif")],
    )
    captured: List[Tuple[str, Dict[str, Any]]] = []
    _patch_db_rows(monkeypatch, [], capture=captured)

    report = await _run(_make_inputs(collection_id="col1", apply=False))

    assert report.orphans_imported == 1  # WOULD-import count
    assert report.dry_run is True
    inserts = [c for c in captured if c[0].lstrip().startswith("INSERT")]
    assert inserts == []


# ---------------------------------------------------------------------------
# 4. Ghost row — apply marks status=failed with reason=missing_blob
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ghost_row_marked_failed(
    monkeypatch: pytest.MonkeyPatch, recording_conn: _RecordingConn
) -> None:
    _patch_bucket(monkeypatch, [])  # bucket empty
    captured: List[Tuple[str, Dict[str, Any]]] = []
    _patch_db_rows(
        monkeypatch,
        [
            {
                "asset_id": "phantom-1",
                "collection_id": "col1",
                "filename": "phantom.tif",
                "status": "active",
                "uri": "gs://bkt/collections/col1/phantom.tif",
                "owned_by": "gcs",
                "kind": "physical",
                "created_at": datetime.now(timezone.utc),
                "metadata": {},
            }
        ],
        capture=captured,
    )

    report = await _run(_make_inputs(collection_id="col1", apply=True))

    assert report.ghosts_marked_failed == 1
    assert report.orphans_imported == 0
    entry = report.drift_details[0]
    assert entry.kind == DriftKind.GHOST_ROW
    assert entry.asset_id == "phantom-1"

    updates = [c for c in captured if c[0].lstrip().startswith("UPDATE")]
    assert len(updates) == 1
    sql, params = updates[0]
    assert params["asset_id"] == "phantom-1"
    assert params["collection_id"] == "col1"
    # The patch JSON carries reason=missing_blob so the audit trail is
    # discoverable in the row's metadata.
    assert "missing_blob" in params["patch"]


# ---------------------------------------------------------------------------
# 5. Stuck PENDING — apply flips to status=failed with upload_abandoned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stuck_pending_failed(
    monkeypatch: pytest.MonkeyPatch, recording_conn: _RecordingConn
) -> None:
    _patch_bucket(monkeypatch, [])

    two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
    captured: List[Tuple[str, Dict[str, Any]]] = []
    _patch_db_rows(
        monkeypatch,
        [
            {
                "asset_id": "stuck-1",
                "collection_id": "col1",
                "filename": "stuck.tif",
                "status": "pending",
                "uri": None,
                "owned_by": None,
                "kind": "physical",
                "created_at": two_hours_ago,
                "metadata": {},
            }
        ],
        capture=captured,
    )

    report = await _run(
        _make_inputs(collection_id="col1", apply=True, pending_ttl_minutes=60)
    )

    assert report.stuck_pending_failed == 1
    assert report.ghosts_marked_failed == 0
    entry = report.drift_details[0]
    assert entry.kind == DriftKind.STUCK_PENDING
    assert entry.asset_id == "stuck-1"

    updates = [c for c in captured if c[0].lstrip().startswith("UPDATE")]
    assert len(updates) == 1
    sql, params = updates[0]
    assert params["asset_id"] == "stuck-1"
    assert "upload_abandoned" in params["patch"]


# ---------------------------------------------------------------------------
# 6. Collection scope filters: catalog-tier blobs/rows are ignored
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_scope_filters_blobs(
    monkeypatch: pytest.MonkeyPatch, recording_conn: _RecordingConn
) -> None:
    # When scope=col1, the listing prefix is collections/col1/. Only
    # blobs under that prefix should surface; the bucket-list helper
    # is responsible for the prefix filter and we mock it returning the
    # already-filtered list.
    _patch_bucket(
        monkeypatch,
        [("collections/col1/scoped.tif", "scoped.tif")],
    )
    # The DB SELECT was issued with WHERE collection_id = :collection_id
    # so the result rows are already scoped — return only the col1 row.
    captured: List[Tuple[str, Dict[str, Any]]] = []
    _patch_db_rows(
        monkeypatch,
        [
            {
                "asset_id": "scoped-1",
                "collection_id": "col1",
                "filename": "scoped.tif",
                "status": "active",
                "uri": "gs://bkt/collections/col1/scoped.tif",
                "owned_by": "gcs",
                "kind": "physical",
                "created_at": datetime.now(timezone.utc),
                "metadata": {},
            }
        ],
        capture=captured,
    )

    report = await _run(_make_inputs(collection_id="col1", apply=True))

    assert report.collection_id == "col1"
    assert report.total_bucket_blobs == 1
    assert report.total_db_rows == 1
    assert report.orphans_imported == 0
    assert report.ghosts_marked_failed == 0
