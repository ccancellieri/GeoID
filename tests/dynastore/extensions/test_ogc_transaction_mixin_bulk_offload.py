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

"""In-process byte/wall-clock budget gate on the bulk sync-write path (#3253).

``OGCTransactionMixin._ingest_items`` hands the un-flushed remainder of a
sub-batched bulk POST off to :meth:`OGCTransactionMixin._offload_bulk_remainder`
once the collection's ``sync_ingest_inprocess_max_bytes`` /
``sync_ingest_inprocess_max_seconds`` budget is crossed. These tests fake
``_offload_bulk_remainder`` directly (the round-trip classification and
spill/enqueue mechanics it delegates to are covered by
``test_ogc_bulk_offload.py``) and assert the gate's own contract: the
no-overflow path is byte-for-byte unchanged, an overflowing request returns
the 202 split, a failed offload lands its remainder in rejections rather
than accepted, and the 0-sentinel disables the gate.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from dynastore.extensions.ogc_base import OGCServiceMixin, OGCTransactionMixin
from dynastore.extensions.ogc_bulk_offload import BulkOffloadOutcome
from dynastore.extensions.ogc_models_shared import SidecarRejection
from dynastore.models.driver_context import DriverContext


class _Svc(OGCServiceMixin, OGCTransactionMixin):
    pass


class _FakeCatalogsSvc:
    def __init__(self) -> None:
        self.calls: List[List[Any]] = []

    async def upsert(self, catalog_id, collection_id, items, ctx):
        batch = items if isinstance(items, list) else [items]
        self.calls.append(list(batch))
        accepted = [
            type("Row", (), {"id": it["id"], "properties": {"external_id": it["id"]}})()
            for it in batch
        ]
        ctx.extensions["_rejections"] = []
        return accepted


class _FakeConfigsSvc:
    """Stands in for the platform configs service. The byte/seconds
    defaults below simulate a collection that has opted into the budget
    (``CollectionPluginConfig``'s real defaults are 0/0 — disabled — since
    the gate is opt-in per collection; see ``test_defaults_disable_...``
    below for a test against the real, unopted-in default)."""

    def __init__(
        self,
        row_cap: int,
        mem_mb: int,
        inprocess_max_bytes: int = 64 * 1024 * 1024,
        inprocess_max_seconds: float = 5.0,
    ) -> None:
        self._row_cap = row_cap
        self._mem_mb = mem_mb
        self._inprocess_max_bytes = inprocess_max_bytes
        self._inprocess_max_seconds = inprocess_max_seconds

    async def get_config(
        self,
        config_cls,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
        config_snapshot: Optional[Dict[str, Any]] = None,
    ):
        return config_cls(
            sync_ingest_batch_rows=self._row_cap,
            sync_ingest_batch_memory_mb=self._mem_mb,
            sync_ingest_inprocess_max_bytes=self._inprocess_max_bytes,
            sync_ingest_inprocess_max_seconds=self._inprocess_max_seconds,
        )


def _make_items(n: int) -> List[Dict[str, Any]]:
    return [
        {"type": "Feature", "id": f"f{i}", "properties": {"external_id": f"f{i}"}}
        for i in range(n)
    ]


def _svc(
    row_cap: int, mem_mb: int, inprocess_max_bytes: int, inprocess_max_seconds: float,
) -> "tuple[_Svc, _FakeCatalogsSvc]":
    svc = _Svc()
    catalogs = _FakeCatalogsSvc()
    svc._ogc_catalogs_protocol = catalogs
    svc._ogc_configs_protocol = _FakeConfigsSvc(
        row_cap, mem_mb, inprocess_max_bytes, inprocess_max_seconds,
    )
    return svc, catalogs


@pytest.mark.asyncio
async def test_no_overflow_path_is_byte_for_byte_unchanged():
    """A payload that never crosses the (generous default) budget must
    produce output identical to the pre-#3253 split path."""
    svc, catalogs = _svc(
        row_cap=3, mem_mb=32,
        inprocess_max_bytes=64 * 1024 * 1024, inprocess_max_seconds=5.0,
    )
    svc._offload_bulk_remainder = _unreachable_offload

    ctx = DriverContext()
    payload = _make_items(10)
    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, ctx, "/configs/...",
    )

    assert batch_size == 10
    assert [len(c) for c in catalogs.calls] == [3, 3, 3, 1]
    assert rejections == []
    assert sorted(accepted_rows) == sorted(f"f{i}" for i in range(10))
    assert "_async_offload" not in ctx.extensions

    response = svc._build_bulk_creation_response(accepted_rows, ctx)
    assert response.status_code == 201


async def _unreachable_offload(*args, **kwargs):
    raise AssertionError("offload must not be attempted when the budget is never crossed")


@pytest.mark.asyncio
async def test_budget_crossed_offloads_remainder_and_returns_202_split():
    # 3 sub-batches of 2 rows each cross a 1-row-flush "byte" budget after
    # the first flush (row_cap forces a flush every 2 items; the inprocess
    # byte budget of 1 byte is crossed on the very first flush).
    svc, catalogs = _svc(
        row_cap=2, mem_mb=32, inprocess_max_bytes=1, inprocess_max_seconds=5.0,
    )
    captured: Dict[str, Any] = {}

    async def _fake_offload(catalog_id, collection_id, remainder, *, ctx, policy_source):
        captured["catalog_id"] = catalog_id
        captured["collection_id"] = collection_id
        captured["remainder_ids"] = [it["id"] for it in remainder]
        captured["policy_source"] = policy_source
        return BulkOffloadOutcome(
            job_id="job-1",
            monitor_url="/processes/catalogs/cat/collections/col/jobs/job-1",
            count=len(remainder),
            rejections=[],
        )

    svc._offload_bulk_remainder = _fake_offload

    ctx = DriverContext()
    payload = _make_items(10)
    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, ctx, "/configs/...",
    )

    assert batch_size == 10
    # Only the first sub-batch (2 rows) flushed inline before the byte
    # budget fired; the remaining 8 items were handed to the offload.
    assert [len(c) for c in catalogs.calls] == [2]
    assert sorted(accepted_rows) == ["f0", "f1"]
    assert rejections == []
    assert captured["remainder_ids"] == [f"f{i}" for i in range(2, 10)]
    assert captured["catalog_id"] == "cat"
    assert captured["collection_id"] == "col"

    assert ctx.extensions["_async_offload"] == {
        "job_id": "job-1",
        "monitor_url": "/processes/catalogs/cat/collections/col/jobs/job-1",
        "count": 8,
    }

    response = svc._build_bulk_creation_response(accepted_rows, ctx)
    assert response.status_code == 202
    assert response.headers["location"] == "/processes/catalogs/cat/collections/col/jobs/job-1"
    import json as _json
    body = _json.loads(response.body)
    assert sorted(body["accepted"]) == ["f0", "f1"]
    assert body["accepted_async"] == {
        "job_id": "job-1",
        "monitor_url": "/processes/catalogs/cat/collections/col/jobs/job-1",
        "count": 8,
    }
    assert body["rejections"] == []
    assert body["total"] == 10


@pytest.mark.asyncio
async def test_failed_offload_lands_remainder_in_rejections_not_accepted():
    """A remainder that cannot be durably spilled/enqueued must never be
    reported accepted (#2825 acknowledged-set discipline, applied here to
    the deferred lane) — it lands in ``rejections`` instead."""
    svc, catalogs = _svc(
        row_cap=2, mem_mb=32, inprocess_max_bytes=1, inprocess_max_seconds=5.0,
    )

    async def _failing_offload(catalog_id, collection_id, remainder, *, ctx, policy_source):
        return BulkOffloadOutcome(
            job_id=None,
            monitor_url=None,
            count=0,
            rejections=[
                SidecarRejection(
                    external_id=it["id"],
                    reason="async_offload_failed",
                    message="spill failed",
                    policy_source=policy_source,
                )
                for it in remainder
            ],
        )

    svc._offload_bulk_remainder = _failing_offload

    ctx = DriverContext()
    payload = _make_items(10)
    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, ctx, "/configs/...",
    )

    assert [len(c) for c in catalogs.calls] == [2]
    assert sorted(accepted_rows) == ["f0", "f1"]
    assert "_async_offload" not in ctx.extensions
    assert sorted(r.external_id for r in rejections) == [f"f{i}" for i in range(2, 10)]
    assert all(r.reason == "async_offload_failed" for r in rejections)

    response = svc._build_rejection_response(accepted_rows, rejections, batch_size, ctx)
    assert response.status_code == 207


@pytest.mark.asyncio
async def test_zero_sentinel_disables_the_byte_budget():
    svc, catalogs = _svc(
        row_cap=2, mem_mb=32, inprocess_max_bytes=0, inprocess_max_seconds=5.0,
    )
    svc._offload_bulk_remainder = _unreachable_offload

    ctx = DriverContext()
    payload = _make_items(10)
    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, ctx, "/configs/...",
    )

    assert [len(c) for c in catalogs.calls] == [2, 2, 2, 2, 2]
    assert sorted(accepted_rows) == sorted(f"f{i}" for i in range(10))
    assert rejections == []
    assert "_async_offload" not in ctx.extensions


@pytest.mark.asyncio
async def test_zero_sentinel_disables_the_wall_clock_budget():
    svc, catalogs = _svc(
        row_cap=2, mem_mb=32, inprocess_max_bytes=64 * 1024 * 1024, inprocess_max_seconds=0,
    )
    svc._offload_bulk_remainder = _unreachable_offload

    ctx = DriverContext()
    payload = _make_items(10)
    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, ctx, "/configs/...",
    )

    assert [len(c) for c in catalogs.calls] == [2, 2, 2, 2, 2]
    assert sorted(accepted_rows) == sorted(f"f{i}" for i in range(10))
    assert rejections == []
    assert "_async_offload" not in ctx.extensions


class _RealDefaultConfigsSvc:
    """Returns ``CollectionPluginConfig`` with only the row/byte
    sub-batching knobs overridden — ``sync_ingest_inprocess_max_bytes``/
    ``sync_ingest_inprocess_max_seconds`` keep the class's real defaults
    (0/0), proving the offload gate stays off unless a collection opts in."""

    def __init__(self, row_cap: int, mem_mb: int) -> None:
        self._row_cap = row_cap
        self._mem_mb = mem_mb

    async def get_config(
        self,
        config_cls,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
        config_snapshot: Optional[Dict[str, Any]] = None,
    ):
        return config_cls(
            sync_ingest_batch_rows=self._row_cap,
            sync_ingest_batch_memory_mb=self._mem_mb,
        )


@pytest.mark.asyncio
async def test_defaults_disable_the_gate_entirely_even_for_a_huge_payload():
    """The offload budget is opt-in per collection — ``CollectionPluginConfig``'s
    real defaults for both knobs are 0 (disabled). A payload whose
    cumulative flushed bytes would have crossed the pre-#3253 64 MiB
    default many times over must still take the legacy, fully-inline path
    unchanged when a collection has not opted in."""
    svc = _Svc()
    catalogs = _FakeCatalogsSvc()
    svc._ogc_catalogs_protocol = catalogs
    svc._ogc_configs_protocol = _RealDefaultConfigsSvc(row_cap=1000, mem_mb=1)
    svc._offload_bulk_remainder = _unreachable_offload

    # ~586 KiB/item (512 + 25000 ordinates * 24 bytes) x 120 items ~= 68.7
    # MiB cumulative — comfortably over the old 64 MiB default.
    big_geometry = {
        "type": "LineString",
        "coordinates": [[float(i), float(i)] for i in range(12500)],
    }
    payload = [
        {
            "type": "Feature", "id": f"f{i}",
            "properties": {"external_id": f"f{i}"},
            "geometry": big_geometry,
        }
        for i in range(120)
    ]

    ctx = DriverContext()
    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, ctx, "/configs/...",
    )

    assert batch_size == 120
    assert sorted(accepted_rows) == sorted(f"f{i}" for i in range(120))
    assert rejections == []
    assert "_async_offload" not in ctx.extensions

    response = svc._build_bulk_creation_response(accepted_rows, ctx)
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_shape_unsupported_remainder_falls_back_to_inline_continuation():
    """A remainder whose shape cannot round-trip through the offload path
    (STAC-shaped items, etc.) is not a failure — the caller must finish
    writing it inline instead of rejecting it or fabricating an async job,
    exactly as if the budget had never been crossed. Legacy 201, all items
    written, zero rejections."""
    svc, catalogs = _svc(
        row_cap=2, mem_mb=32, inprocess_max_bytes=1, inprocess_max_seconds=5.0,
    )

    async def _shape_unsupported_offload(catalog_id, collection_id, remainder, *, ctx, policy_source):
        return BulkOffloadOutcome(
            job_id=None, monitor_url=None, count=0, rejections=[],
            shape_unsupported=True,
        )

    svc._offload_bulk_remainder = _shape_unsupported_offload

    ctx = DriverContext()
    payload = _make_items(10)
    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, ctx, "/configs/...",
    )

    assert batch_size == 10
    assert sorted(accepted_rows) == sorted(f"f{i}" for i in range(10))
    assert rejections == []
    assert "_async_offload" not in ctx.extensions
    assert sum(len(c) for c in catalogs.calls) == 10

    response = svc._build_bulk_creation_response(accepted_rows, ctx)
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_dedup_hit_remainder_falls_back_to_inline_continuation():
    """A dedup hit (an equivalent job for this exact remainder is already
    in flight) is handled the same way as an unsupported shape: no second
    job is fabricated, the remainder is written inline instead — safe
    because the write path's upserts are idempotent."""
    svc, catalogs = _svc(
        row_cap=2, mem_mb=32, inprocess_max_bytes=1, inprocess_max_seconds=5.0,
    )

    async def _dedup_hit_offload(catalog_id, collection_id, remainder, *, ctx, policy_source):
        return BulkOffloadOutcome(
            job_id=None, monitor_url=None, count=0, rejections=[],
            dedup_hit=True,
        )

    svc._offload_bulk_remainder = _dedup_hit_offload

    ctx = DriverContext()
    payload = _make_items(10)
    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, ctx, "/configs/...",
    )

    assert batch_size == 10
    assert sorted(accepted_rows) == sorted(f"f{i}" for i in range(10))
    assert rejections == []
    assert "_async_offload" not in ctx.extensions
    assert sum(len(c) for c in catalogs.calls) == 10

    response = svc._build_bulk_creation_response(accepted_rows, ctx)
    assert response.status_code == 201
