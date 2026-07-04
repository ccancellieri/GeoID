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

"""Sub-batching of the direct synchronous bulk POST ingest path (#2657).

``OGCTransactionMixin._ingest_items`` bounds the one remaining unbounded
O(dataset) memory path — a single ``POST /collections/{id}/items`` call
with a large FeatureCollection. A single item, or a multi-item payload that
fits under the collection's ``sync_ingest_batch_rows`` /
``sync_ingest_batch_memory_mb`` (``CollectionPluginConfig``), is still
written with exactly one ``CatalogsProtocol.upsert()`` call, unchanged.
A larger payload is split into sub-batches bounded by both limits, each
reduced to accepted ID strings before the next sub-batch is prepared.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from dynastore.extensions.ogc_base import OGCTransactionMixin, OGCServiceMixin
from dynastore.models.driver_context import DriverContext


class _Svc(OGCServiceMixin, OGCTransactionMixin):
    pass


class _FakeCatalogsSvc:
    """Records every ``upsert()`` call and deterministically accepts/rejects
    items based on a ``_reject`` marker in their properties, independent of
    how the caller chunked the payload."""

    def __init__(self) -> None:
        self.calls: List[List[Any]] = []

    async def upsert(self, catalog_id, collection_id, items, ctx):
        batch = items if isinstance(items, list) else [items]
        self.calls.append(list(batch))

        accepted: List[Any] = []
        rej_out: List[dict] = []
        for it in batch:
            props = it.get("properties", {}) if isinstance(it, dict) else {}
            ext_id = it.get("id") if isinstance(it, dict) else getattr(it, "id", None)
            if props.get("_reject"):
                rej_out.append({
                    "geoid": None,
                    "external_id": ext_id,
                    "sidecar_id": "dim",
                    "matcher": "external_id",
                    "reason": "sidecar_not_acceptable",
                    "message": f"refused {ext_id}",
                })
            else:
                accepted.append(
                    SimpleNamespace(id=ext_id, properties={"external_id": ext_id})
                )
        ctx.extensions["_rejections"] = rej_out
        return accepted


class _FakeConfigsSvc:
    """Returns a ``CollectionPluginConfig`` with a fixed row cap / byte
    budget, standing in for the platform configs service waterfall."""

    def __init__(self, row_cap: int, mem_mb: int, max_bulk_features: Optional[int] = None) -> None:
        self._row_cap = row_cap
        self._mem_mb = mem_mb
        self._max_bulk_features = max_bulk_features

    async def get_config(
        self,
        config_cls,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
        config_snapshot: Optional[Dict[str, Any]] = None,
    ):
        kwargs: Dict[str, Any] = {
            "sync_ingest_batch_rows": self._row_cap,
            "sync_ingest_batch_memory_mb": self._mem_mb,
        }
        if self._max_bulk_features is not None:
            kwargs["max_bulk_features"] = self._max_bulk_features
        return config_cls(**kwargs)


def _make_items(n: int, geometry: Optional[dict] = None, reject_ids=frozenset()):
    items = []
    for i in range(n):
        fid = f"f{i}"
        props: Dict[str, Any] = {"external_id": fid}
        if fid in reject_ids:
            props["_reject"] = True
        item: Dict[str, Any] = {"type": "Feature", "id": fid, "properties": props}
        if geometry is not None:
            item["geometry"] = geometry
        items.append(item)
    return items


def _svc(row_cap: int, mem_mb: int) -> "tuple[_Svc, _FakeCatalogsSvc]":
    svc = _Svc()
    catalogs = _FakeCatalogsSvc()
    svc._ogc_catalogs_protocol = catalogs
    svc._ogc_configs_protocol = _FakeConfigsSvc(row_cap, mem_mb)
    return svc, catalogs


@pytest.mark.asyncio
async def test_row_cap_splits_bulk_payload_into_multiple_upsert_calls():
    svc, catalogs = _svc(row_cap=3, mem_mb=32)
    payload = _make_items(10)

    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, DriverContext(), "/configs/...",
    )

    assert batch_size == 10
    assert was_single is False
    assert [len(c) for c in catalogs.calls] == [3, 3, 3, 1]
    assert all(len(c) <= 3 for c in catalogs.calls)
    assert rejections == []
    assert all(isinstance(r, str) for r in accepted_rows)
    assert sorted(accepted_rows) == sorted(f"f{i}" for i in range(10))


@pytest.mark.asyncio
async def test_byte_budget_splits_bulk_payload_of_large_individual_geometries():
    # Each item's estimated size is ~512 + 25000 * 24 bytes ~= 600 KB, so two
    # of them cross a 1 MiB budget well before the (deliberately large)
    # row cap of 100 would ever trigger a flush.
    big_geometry = {
        "type": "LineString",
        "coordinates": [[float(i), float(i)] for i in range(12500)],
    }
    svc, catalogs = _svc(row_cap=100, mem_mb=1)
    payload = _make_items(5, geometry=big_geometry)

    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, DriverContext(), "/configs/...",
    )

    assert batch_size == 5
    assert [len(c) for c in catalogs.calls] == [2, 2, 1]
    assert all(len(c) < 100 for c in catalogs.calls)  # row cap never the trigger
    assert rejections == []
    assert sorted(accepted_rows) == sorted(f"f{i}" for i in range(5))


@pytest.mark.asyncio
async def test_split_path_preserves_accepted_ids_and_rejections_vs_baseline():
    payload = _make_items(6, reject_ids={"f2", "f4"})

    # Baseline: payload fits comfortably under the caps, single upsert call.
    baseline_svc, baseline_catalogs = _svc(row_cap=100, mem_mb=32)
    baseline_rows, baseline_rejections, _, _ = await baseline_svc._ingest_items(
        "cat", "col", payload, DriverContext(), "/configs/...",
    )
    baseline_ids = baseline_svc._resolve_accepted_ids(baseline_rows)
    assert len(baseline_catalogs.calls) == 1

    # Split path: force sub-batching well below the payload size.
    split_svc, split_catalogs = _svc(row_cap=2, mem_mb=32)
    split_rows, split_rejections, _, _ = await split_svc._ingest_items(
        "cat", "col", payload, DriverContext(), "/configs/...",
    )
    assert len(split_catalogs.calls) > 1

    assert sorted(split_rows) == sorted(baseline_ids)
    assert sorted((r.external_id, r.reason, r.message) for r in split_rejections) == sorted(
        (r.external_id, r.reason, r.message) for r in baseline_rejections
    )


@pytest.mark.asyncio
async def test_single_item_makes_one_upsert_call_and_returns_full_row():
    svc, catalogs = _svc(row_cap=500, mem_mb=32)
    single_payload = {"type": "Feature", "id": "solo", "properties": {"external_id": "solo"}}

    rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", single_payload, DriverContext(), "/configs/...",
    )

    assert was_single is True
    assert batch_size == 1
    assert len(catalogs.calls) == 1
    assert not isinstance(rows[0], str)
    assert rows[0].id == "solo"


@pytest.mark.asyncio
async def test_small_bulk_makes_one_upsert_call_and_returns_full_rows():
    svc, catalogs = _svc(row_cap=500, mem_mb=32)
    payload = _make_items(5)

    rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, DriverContext(), "/configs/...",
    )

    assert was_single is False
    assert batch_size == 5
    assert len(catalogs.calls) == 1
    assert len(catalogs.calls[0]) == 5
    assert all(not isinstance(r, str) for r in rows)
    assert sorted(r.id for r in rows) == sorted(f"f{i}" for i in range(5))


@pytest.mark.asyncio
async def test_max_bulk_features_rejects_full_list_even_when_sub_batched():
    """Sub-batching (#2875) caps every ``upsert()`` call at
    ``sync_ingest_batch_rows``, so the ``max_bulk_features`` guard that used
    to live inside ``item_service.upsert()`` never sees the full payload
    once a request is split — a request of arbitrary size would otherwise
    sail through as a sequence of individually-compliant sub-batches.
    ``_ingest_items`` must re-check the full list against
    ``max_bulk_features`` before any splitting happens (#2657 commit 4).
    """
    svc = _Svc()
    catalogs = _FakeCatalogsSvc()
    svc._ogc_catalogs_protocol = catalogs
    svc._ogc_configs_protocol = _FakeConfigsSvc(row_cap=2, mem_mb=32, max_bulk_features=5)
    payload = _make_items(10)

    with pytest.raises(ValueError, match="exceeding the maximum of 5"):
        await svc._ingest_items("cat", "col", payload, DriverContext(), "/configs/...")

    # Rejected before any sub-batch flush reached the catalogs service.
    assert catalogs.calls == []


@pytest.mark.asyncio
async def test_max_bulk_features_within_limit_still_sub_batches_normally():
    """A payload under ``max_bulk_features`` but over the row cap must
    still take the normal sub-batching path, unaffected by the guard."""
    svc, catalogs = _svc(row_cap=3, mem_mb=32)
    payload = _make_items(10)

    accepted_rows, rejections, was_single, batch_size = await svc._ingest_items(
        "cat", "col", payload, DriverContext(), "/configs/...",
    )

    assert batch_size == 10
    assert len(catalogs.calls) > 1
    assert rejections == []


@pytest.mark.asyncio
async def test_bulk_creation_response_byte_identical_split_vs_baseline():
    payload = _make_items(6)

    baseline_svc, _ = _svc(row_cap=100, mem_mb=32)
    baseline_rows, _, _, _ = await baseline_svc._ingest_items(
        "cat", "col", payload, DriverContext(), "/configs/...",
    )
    baseline_response = baseline_svc._build_bulk_creation_response(baseline_rows)

    split_svc, split_catalogs = _svc(row_cap=2, mem_mb=32)
    split_rows, _, _, _ = await split_svc._ingest_items(
        "cat", "col", payload, DriverContext(), "/configs/...",
    )
    assert len(split_catalogs.calls) > 1
    split_response = split_svc._build_bulk_creation_response(split_rows)

    assert baseline_response.status_code == split_response.status_code == 201
    assert baseline_response.body == split_response.body


@pytest.mark.asyncio
async def test_ingestion_report_byte_identical_split_vs_baseline():
    payload = _make_items(6, reject_ids={"f2", "f4"})

    baseline_svc, _ = _svc(row_cap=100, mem_mb=32)
    baseline_rows, baseline_rejections, _, baseline_size = await baseline_svc._ingest_items(
        "cat", "col", payload, DriverContext(), "/configs/...",
    )
    baseline_response = baseline_svc._build_rejection_response(
        baseline_rows, baseline_rejections, baseline_size,
    )

    split_svc, split_catalogs = _svc(row_cap=2, mem_mb=32)
    split_rows, split_rejections, _, split_size = await split_svc._ingest_items(
        "cat", "col", payload, DriverContext(), "/configs/...",
    )
    assert len(split_catalogs.calls) > 1
    split_response = split_svc._build_rejection_response(
        split_rows, split_rejections, split_size,
    )

    assert baseline_response.status_code == split_response.status_code == 207
    assert baseline_response.body == split_response.body
