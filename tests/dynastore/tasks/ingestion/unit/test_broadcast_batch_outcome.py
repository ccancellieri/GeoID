"""Regression coverage for dynastore#263: detailed-report empty array.

Pre-fix, ``run_ingestion_task`` called every ``ReportingInterface`` hook
except ``process_batch_outcome``, so ``GcsDetailedReporter``'s buffer
stayed empty and the uploaded report was the literal ``[]``. The fix
introduces ``_broadcast_batch_outcome`` and wires it after each successful
upsert flush. These tests pin the helper's contract directly — full
``run_ingestion_task`` is exercised by the integration suite.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pytest

import dynastore.models.protocols  # noqa: F401  (pre-warm registration chain)

from dynastore.tasks.ingestion.main_ingestion import _broadcast_batch_outcome


class _RecordingReporter:
    """Captures every ``process_batch_outcome`` call for assertion."""

    def __init__(self) -> None:
        self.calls: List[List[Dict[str, Any]]] = []

    async def process_batch_outcome(self, batch_results: List[Dict[str, Any]]) -> None:
        self.calls.append(batch_results)


@pytest.mark.asyncio
async def test_broadcast_synthesizes_success_outcome_per_input_row():
    """One outcome per input row, all SUCCESS, ``record`` field populated."""
    batch = [{"id": "a", "geometry": None}, {"id": "b", "geometry": None}]
    reporter = _RecordingReporter()

    await _broadcast_batch_outcome([reporter], batch, upsert_result=None)

    assert len(reporter.calls) == 1
    outcomes = reporter.calls[0]
    assert len(outcomes) == len(batch)
    for outcome in outcomes:
        assert outcome["status"] == "SUCCESS"
        assert outcome["message"] is None
        assert "record" in outcome


@pytest.mark.asyncio
async def test_broadcast_prefers_upsert_result_as_record_when_aligned():
    """When upsert returns a list aligned with the batch, those items become
    the canonical ``record`` (carries server-assigned fields like item_id)."""
    batch = [{"id": "a"}, {"id": "b"}]
    upsert_result = [
        {"id": "a", "item_id": "uuid-a"},
        {"id": "b", "item_id": "uuid-b"},
    ]
    reporter = _RecordingReporter()

    await _broadcast_batch_outcome([reporter], batch, upsert_result)

    outcomes = reporter.calls[0]
    assert outcomes[0]["record"] == {"id": "a", "item_id": "uuid-a"}
    assert outcomes[1]["record"] == {"id": "b", "item_id": "uuid-b"}


@pytest.mark.asyncio
async def test_broadcast_falls_back_to_input_when_upsert_result_shape_mismatch():
    """If upsert returns a single dict, a different-length list, or anything
    non-aligned, drop back to the input feature so ``record`` is still
    populated rather than mis-zipped."""
    batch = [{"id": "a"}, {"id": "b"}]

    for bad_result in (None, {"id": "a"}, [{"id": "only-one"}], "garbage"):
        reporter = _RecordingReporter()
        await _broadcast_batch_outcome([reporter], batch, bad_result)
        outcomes = reporter.calls[0]
        assert [o["record"] for o in outcomes] == batch, (
            f"fallback failed for upsert_result={bad_result!r}"
        )


@pytest.mark.asyncio
async def test_broadcast_fans_out_to_every_reporter():
    """All reporters in the list receive the same payload — this was the
    root gap in #263: ``GcsDetailedReporter`` was registered but never
    invoked, so the buffered file stayed empty."""
    batch = [{"id": "a"}]
    r1 = _RecordingReporter()
    r2 = _RecordingReporter()
    r3 = _RecordingReporter()

    await _broadcast_batch_outcome([r1, r2, r3], batch, upsert_result=batch)

    for reporter in (r1, r2, r3):
        assert len(reporter.calls) == 1
        assert reporter.calls[0][0]["status"] == "SUCCESS"


@pytest.mark.asyncio
async def test_broadcast_with_empty_reporter_list_is_a_noop():
    """Reporter list may be empty when the task request omits ``reporting``."""
    await _broadcast_batch_outcome([], [{"id": "a"}], upsert_result=None)
