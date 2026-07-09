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

"""Unit tests for Phase 4's byte-adaptive write-chunk sizing and post-burst
malloc trim in ``ItemService.upsert()`` (#3154).

Reuses the Branch B (PG primary) unit-test scaffolding from
``test_item_service_pg_sidecar_abac.py`` (no DB, no asyncpg, no real
``managed_transaction``). ``estimate_doc_bytes`` is monkeypatched to a
controlled constant per test so these assertions exercise the *wiring*
inside ``item_service.py`` rather than re-deriving Phase 2's exact payload
shape — the underlying byte/row math is already covered by
``tests/dynastore/tools/test_adaptive_chunk_sizing.py``.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from dynastore.modules.catalog.item_service import ItemService

from tests.dynastore.modules.catalog.test_item_service_pg_sidecar_abac import (
    _patch_branch_b,
)


class _NoopSidecar:
    """Minimal Phase-2-shaped sidecar: no payload, no partition keys."""

    sidecar_id = "attributes"

    def is_mandatory(self) -> bool:
        return False

    def validate_insert(self, feature: Any, context: Any) -> Any:
        class _OK:
            valid = True
            error = None

        return _OK()

    def prepare_upsert_payload(
        self, feature: Any, context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        return None

    def get_partition_keys(self) -> List[str]:
        return []


def _items(n: int) -> List[Dict[str, Any]]:
    return [
        {"type": "Feature", "id": f"item{i}", "geometry": None, "properties": {}}
        for i in range(n)
    ]


class _ChunkTracker:
    """Records Phase 4 write-chunk boundaries via ``managed_transaction``
    entries, and which ``insert_or_update_distributed`` calls land in each
    one (the per-row path — Branch B's stub config resolves no
    ``ItemsWritePolicy``, so ``enable_batch_insert`` never fires)."""

    def __init__(self) -> None:
        self.groups: List[List[str]] = []

    @asynccontextmanager
    async def transaction(self, engine: Any):
        self.groups.append([])
        yield None

    async def record_insert(
        self,
        conn: Any,
        cat: str,
        col: str,
        hub_payload: Dict[str, Any],
        sidecar_payloads: Any,
        **_kw: Any,
    ) -> Dict[str, Any]:
        geoid = hub_payload.get("geoid", "stub-geoid")
        self.groups[-1].append(geoid)
        return {"geoid": geoid}

    @property
    def chunk_sizes(self) -> List[int]:
        # Phase 1's config-resolution transaction and Phase 5's read-back
        # transaction open/close with no insert_or_update_distributed calls
        # in between — drop those empty groups to leave only Phase 4 chunks.
        return [len(g) for g in self.groups if g]


def _wire_tracker(monkeypatch: Any, svc: ItemService) -> _ChunkTracker:
    tracker = _ChunkTracker()
    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.managed_transaction",
        tracker.transaction,
    )
    monkeypatch.setattr(svc, "insert_or_update_distributed", tracker.record_insert)
    return tracker


async def test_light_items_probe_then_jump_to_ceiling(monkeypatch: Any) -> None:
    """A negligible measured cost lets the sizer jump straight from the
    1-row probe to the collection's ingest_chunk_size ceiling (50, per the
    Branch B config stub)."""
    svc = ItemService()
    _patch_branch_b(monkeypatch, svc, [_NoopSidecar()])
    tracker = _wire_tracker(monkeypatch, svc)
    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.estimate_doc_bytes",
        lambda doc, **_kw: 1,
    )

    await svc.upsert("cat1", "col1", _items(6))

    assert tracker.chunk_sizes == [1, 5]


async def test_heavy_items_shrink_chunk_below_ceiling(monkeypatch: Any) -> None:
    """A 2 MiB-per-document measured cost keeps chunk_rows at 8 (16 MiB
    budget // 2 MiB) instead of applying the fixed 50-row ceiling to
    multi-MB items."""
    svc = ItemService()
    _patch_branch_b(monkeypatch, svc, [_NoopSidecar()])
    tracker = _wire_tracker(monkeypatch, svc)
    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.estimate_doc_bytes",
        lambda doc, **_kw: 2 * 1024 * 1024,
    )

    await svc.upsert("cat1", "col1", _items(12))

    assert tracker.chunk_sizes == [1, 8, 3]


async def test_trim_called_once_after_bulk_upsert(monkeypatch: Any) -> None:
    """trim_malloc_arenas fires exactly once per bulk upsert() call — a
    batch boundary after the burst, mirroring StorageDrainTask."""
    svc = ItemService()
    _patch_branch_b(monkeypatch, svc, [_NoopSidecar()])
    calls: List[None] = []
    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.trim_malloc_arenas",
        lambda: calls.append(None),
    )

    await svc.upsert("cat1", "col1", _items(3))

    assert len(calls) == 1


async def test_trim_not_called_for_single_item_upsert(monkeypatch: Any) -> None:
    """A single-item write is not a burst — no trim on the ordinary
    create/update request path."""
    svc = ItemService()
    _patch_branch_b(monkeypatch, svc, [_NoopSidecar()])
    calls: List[None] = []
    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.trim_malloc_arenas",
        lambda: calls.append(None),
    )

    await svc.upsert("cat1", "col1", _items(1)[0])

    assert len(calls) == 0
