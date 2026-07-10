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

"""#504 — structured log lines for index dispatch observability."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from dynastore.models.protocols.indexer import IndexContext, IndexOp
from dynastore.modules.storage.index_dispatcher import StoragePlaneOutboxWriter


@pytest.mark.asyncio
async def test_outbox_enqueue_emits_chunk_emitted_log(caplog, monkeypatch):
    import dynastore.modules.storage.storage_emit as storage_emit_mod

    async def _fake_enqueue_id_only(conn, *, catalog_id, rows):
        return None

    monkeypatch.setattr(
        storage_emit_mod, "enqueue_storage_op_id_only", _fake_enqueue_id_only,
    )

    writer = StoragePlaneOutboxWriter()
    conn = MagicMock()
    ctx = IndexContext(
        catalog="catA", collection="colB", correlation_id="cid",
        pg_conn=conn,
    )
    op = IndexOp(
        op_type="upsert", entity_type="item", entity_id="e1", payload={},
    )

    with caplog.at_level(logging.INFO, logger="dynastore.modules.storage.index_dispatcher"):
        await writer.enqueue(indexer_id="ix1", ctx=ctx, ops=[op])

    matches = [r for r in caplog.records if r.getMessage().startswith("index_chunk_emitted ")]
    assert len(matches) == 1
    msg = matches[0].getMessage()
    assert "indexer=ix1" in msg
    assert "source=storage_plane_outbox" in msg
    assert "catalog=catA" in msg
    assert "chunk_size=1" in msg


def test_task_drained_success_emission(caplog):
    from dynastore.modules.tasks.dispatcher import _log_task_terminal

    ts = datetime.now(timezone.utc) - timedelta(seconds=0.5)
    with caplog.at_level(logging.INFO, logger="dynastore.modules.tasks.dispatcher"):
        _log_task_terminal(
            "storage_drain", "t-1", ts, outcome="success", error=None,
        )
    msgs = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("task_drained ") for m in msgs)
    m = next(m for m in msgs if m.startswith("task_drained "))
    assert "task_type=storage_drain" in m
    assert "outcome=success" in m
    assert re.search(r"enqueue_to_drain_seconds=\d+\.\d+", m)


def test_task_failed_emission_per_outcome(caplog):
    """All three failure paths emit `task_failed` with distinct outcomes."""
    from dynastore.modules.tasks.dispatcher import _log_task_terminal

    ts = datetime.now(timezone.utc) - timedelta(seconds=0.1)
    outcomes = ("cancelled", "permanent_failure", "transient_failure")
    with caplog.at_level(logging.INFO, logger="dynastore.modules.tasks.dispatcher"):
        for outcome in outcomes:
            _log_task_terminal(
                "storage_drain", "t-X", ts, outcome=outcome, error="boom",
            )

    failed_msgs = [
        r.getMessage() for r in caplog.records
        if r.getMessage().startswith("task_failed ")
    ]
    assert len(failed_msgs) == 3
    for outcome, msg in zip(outcomes, failed_msgs):
        assert f"outcome={outcome}" in msg
        assert "error='boom'" in msg
        assert re.search(r"enqueue_to_drain_seconds=\d+\.\d+", msg)


def test_task_claim_rejected_log_shape():
    line = "task_claim_rejected task_type=storage_drain capability=ix1 task_id=t-1 — can_claim returned False on this worker"
    m = re.match(
        r"^task_claim_rejected task_type=(?P<task_type>\S+) "
        r"capability=(?P<capability>\S+) task_id=(?P<task_id>\S+) — ",
        line,
    )
    assert m is not None
    assert m["task_type"] == "storage_drain"
    assert m["capability"] == "ix1"
