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

from dynastore.tasks.ingestion.main_ingestion import (
    _broadcast_batch_outcome,
    _enrich_report_record,
)


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
    # Server-assigned fields (item_id) ride at the GeoJSON top level alongside
    # the envelope siblings (always-present empty ``properties``/``stats``/
    # ``system`` when no ``generated`` is supplied).
    rec0 = outcomes[0]["record"]
    assert rec0["id"] == "a"
    assert rec0["item_id"] == "uuid-a"
    assert rec0["properties"] == {}
    assert rec0["stats"] == {}
    assert rec0["system"] == {}
    rec1 = outcomes[1]["record"]
    assert rec1["id"] == "b"
    assert rec1["item_id"] == "uuid-b"


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
        # Fallback to the input batch: the input ``id`` survives at the top,
        # and the envelope siblings come through empty (no ``generated``).
        assert [o["record"]["id"] for o in outcomes] == [r["id"] for r in batch], (
            f"fallback failed for upsert_result={bad_result!r}"
        )
        for outcome in outcomes:
            assert outcome["record"]["system"] == {}
            assert outcome["record"]["stats"] == {}


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


# --- generated-info enrichment (geoid / external_id / asset_id / statistics) --


def test_enrich_splits_envelope_into_properties_stats_system():
    """Reporter shape D1: ``properties`` stays user-only; platform statistics
    land under ``stats``; identity lives under ``system``."""
    record = {"type": "Feature", "id": "geoid-a", "properties": {"NAME": "Friuli"}}
    generated = {
        "geoid": "geoid-a",
        "external_id": "1621",
        "asset_id": "ITAL1_01",
        "stats": {"area": 7845.0, "perimeter": 412.3},
    }
    out = _enrich_report_record(record, generated)
    # user attributes unmixed
    assert out["properties"] == {"NAME": "Friuli"}
    # platform-derived siblings
    assert out["stats"] == {"area": 7845.0, "perimeter": 412.3}
    assert out["system"] == {
        "geoid": "geoid-a",
        "external_id": "1621",
        "asset_id": "ITAL1_01",
    }
    # identity no longer at the record top level
    for key in ("geoid", "external_id", "asset_id"):
        assert key not in out, f"{key!r} must live under record['system'] only"
    # GeoJSON envelope keys preserved
    assert out["type"] == "Feature"
    assert out["id"] == "geoid-a"


def test_enrich_omits_absent_system_keys():
    """A None-valued identity field is dropped from ``system`` entirely."""
    out = _enrich_report_record(
        {"properties": {}},
        {"geoid": "g", "external_id": None, "asset_id": None, "stats": {}},
    )
    assert out["system"] == {"geoid": "g"}
    assert out["stats"] == {}


def test_enrich_keeps_user_property_clashing_with_stat_name():
    """A user attribute with the same name as a derived stat is no longer
    clobbered — the two now live in separate sibling bags."""
    out = _enrich_report_record(
        {"properties": {"area": "surveyed"}},
        {"geoid": "g", "stats": {"area": 7845.0}},
    )
    assert out["properties"] == {"area": "surveyed"}
    assert out["stats"] == {"area": 7845.0}


def test_enrich_lifts_optional_system_fields_when_generated_carries_them():
    out = _enrich_report_record(
        {"properties": {}},
        {
            "geoid": "g",
            "external_id": "ext",
            "asset_id": "a",
            "geometry_hash": "ghash",
            "attributes_hash": "ahash",
            "validity": "[2024-01-01,)",
            "transaction_time": "2026-02-26T18:09:04.131762+00:00",
            "deleted_at": None,
            "stats": {},
        },
    )
    assert out["system"] == {
        "geoid": "g",
        "external_id": "ext",
        "asset_id": "a",
        "geometry_hash": "ghash",
        "attributes_hash": "ahash",
        "validity": "[2024-01-01,)",
        "transaction_time": "2026-02-26T18:09:04.131762+00:00",
    }


@pytest.mark.asyncio
async def test_broadcast_applies_generated_when_aligned_with_result():
    batch = [{"id": "1621"}, {"id": "1622"}]
    upsert_result = [
        {"type": "Feature", "id": "geo-1", "properties": {"NAME": "A"}},
        {"type": "Feature", "id": "geo-2", "properties": {"NAME": "B"}},
    ]
    generated = [
        {"geoid": "geo-1", "external_id": "1621", "asset_id": "X", "stats": {"area": 1.0}},
        {"geoid": "geo-2", "external_id": "1622", "asset_id": "X", "stats": {"area": 2.0}},
    ]
    reporter = _RecordingReporter()

    await _broadcast_batch_outcome([reporter], batch, upsert_result, generated)

    recs = [o["record"] for o in reporter.calls[0]]
    assert recs[0]["system"]["geoid"] == "geo-1"
    assert recs[0]["system"]["external_id"] == "1621"
    assert recs[0]["properties"] == {"NAME": "A"}
    assert recs[0]["stats"] == {"area": 1.0}
    assert recs[1]["properties"] == {"NAME": "B"}
    assert recs[1]["stats"] == {"area": 2.0}


@pytest.mark.asyncio
async def test_broadcast_drops_generated_on_input_batch_fallback():
    """Generated stats align with the upsert result, not the input batch, so a
    shape mismatch (which falls back to the input batch) must not stamp
    identity into ``system``."""
    batch = [{"id": "1621"}, {"id": "1622"}]
    generated = [{"geoid": "geo-1", "stats": {"area": 1.0}}]  # wrong length anyway
    reporter = _RecordingReporter()

    # single-dict upsert_result → fallback to input batch
    await _broadcast_batch_outcome([reporter], batch, {"id": "1621"}, generated)

    recs = [o["record"] for o in reporter.calls[0]]
    for rec, original in zip(recs, batch):
        # Record still echoes the input row's user fields, but the envelope
        # split applies: empty ``stats`` and ``system`` (nothing to stamp).
        assert rec["id"] == original["id"]
        assert rec["system"] == {}
        assert rec["stats"] == {}


# --- per-row rejections → FAILED outcomes (partial-batch reporting) ----------
# A single feature that fails value/sidecar validation is surfaced by the
# upsert via ``ctx.extensions["_rejections"]``. The job must report it as a row
# FAILURE (so the detailed report names it) while the accepted siblings still
# report SUCCESS — instead of the whole job aborting.


@pytest.mark.asyncio
async def test_broadcast_reports_rejections_as_failed_alongside_accepted():
    batch = [{"id": "a"}, {"id": "b"}]
    upsert_result = [{"id": "a", "item_id": "uuid-a"}]  # only 'a' persisted
    rejections = [
        {
            "external_id": "b",
            "reason": "validation_error",
            "message": (
                "Feature properties violate the items schema: "
                "END_DATE: None is not of type 'string'"
            ),
            "record": {"id": "b", "properties": {"END_DATE": None}},
        }
    ]
    reporter = _RecordingReporter()

    await _broadcast_batch_outcome(
        [reporter], batch, upsert_result, rejections=rejections
    )

    outcomes = reporter.calls[0]
    statuses = sorted(o["status"] for o in outcomes)
    assert statuses == ["FAILED", "SUCCESS"]
    failed = next(o for o in outcomes if o["status"] == "FAILED")
    assert "violate the items schema" in failed["message"]
    # Failure path uses the same envelope: ``properties`` echoes the offending
    # input, ``system`` carries derived identity (here just external_id from
    # the rejection metadata), ``stats`` is empty.
    assert failed["record"]["id"] == "b"
    assert failed["record"]["properties"] == {"END_DATE": None}
    assert failed["record"]["system"] == {"external_id": "b"}
    assert failed["record"]["stats"] == {}
    success = next(o for o in outcomes if o["status"] == "SUCCESS")
    assert success["record"]["item_id"] == "uuid-a"


@pytest.mark.asyncio
async def test_broadcast_rejection_without_record_falls_back_to_identity():
    batch = [{"id": "a"}]
    rejections = [
        {
            "geoid": "g2",
            "external_id": "ext-2",
            "reason": "validation_error",
            "message": "bad",
        }
    ]
    reporter = _RecordingReporter()

    await _broadcast_batch_outcome(
        [reporter], batch, upsert_result=[], rejections=rejections
    )

    failed = [o for o in reporter.calls[0] if o["status"] == "FAILED"]
    assert len(failed) == 1
    # No ``record`` on the rejection → envelope synthesised from identity only.
    assert failed[0]["record"]["system"] == {"geoid": "g2", "external_id": "ext-2"}
    assert failed[0]["record"]["properties"] == {}
    assert failed[0]["record"]["stats"] == {}


@pytest.mark.asyncio
async def test_broadcast_all_rejected_reports_only_failures():
    batch = [{"id": "a"}, {"id": "b"}]
    rejections = [
        {"reason": "validation_error", "message": "bad a", "record": {"id": "a"}},
        {"reason": "validation_error", "message": "bad b", "record": {"id": "b"}},
    ]
    reporter = _RecordingReporter()

    await _broadcast_batch_outcome(
        [reporter], batch, upsert_result=[], rejections=rejections
    )

    outcomes = reporter.calls[0]
    assert len(outcomes) == 2
    assert all(o["status"] == "FAILED" for o in outcomes)
