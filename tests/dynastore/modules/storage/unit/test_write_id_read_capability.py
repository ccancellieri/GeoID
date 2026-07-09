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

"""Regression tests for the write-id read-capability contract.

See ``modules/storage/README.md`` ("Write-ID Read-Capability Contract") for
the full contract this guards: a driver may only receive a grouped
write-id outbox row when it implements ``read_indexable_write_batch`` or
the ``read_active_rows_by_write_id`` / ``read_tombstoned_ids_by_write_id``
pair; otherwise every producer must degrade to id-only rows without ever
skipping the enqueue.

These are pure unit tests — no DB, no app graph.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, List
from unittest import mock


# ---------------------------------------------------------------------------
# driver_supports_write_id_reads — capability matrix
# ---------------------------------------------------------------------------


def test_true_for_indexable_write_batch_reader():
    from dynastore.modules.storage.storage_emit import driver_supports_write_id_reads

    class _BatchReader:
        async def read_indexable_write_batch(self, **kwargs):
            return [], [], None

    assert driver_supports_write_id_reads(_BatchReader()) is True


def test_true_for_paired_by_write_id_readers():
    from dynastore.modules.storage.storage_emit import driver_supports_write_id_reads

    class _PairedReader:
        async def read_active_rows_by_write_id(self, **kwargs):
            return [], None

        async def read_tombstoned_ids_by_write_id(self, **kwargs):
            return [], None

    assert driver_supports_write_id_reads(_PairedReader()) is True


def test_false_when_only_active_rows_reader_present():
    """Half the reader pair must not satisfy the gate — a write-id row
    grouped against this driver could hydrate active rows but never learn
    which ids were tombstoned, silently losing deletes."""
    from dynastore.modules.storage.storage_emit import driver_supports_write_id_reads

    class _ActiveOnlyReader:
        async def read_active_rows_by_write_id(self, **kwargs):
            return [], None

    assert driver_supports_write_id_reads(_ActiveOnlyReader()) is False


def test_false_when_only_tombstoned_reader_present():
    from dynastore.modules.storage.storage_emit import driver_supports_write_id_reads

    class _TombstoneOnlyReader:
        async def read_tombstoned_ids_by_write_id(self, **kwargs):
            return [], None

    assert driver_supports_write_id_reads(_TombstoneOnlyReader()) is False


def test_false_for_driver_without_capability():
    from dynastore.modules.storage.storage_emit import driver_supports_write_id_reads

    class _BareDriver:
        async def write_entities(self, *a, **kw):
            return []

    assert driver_supports_write_id_reads(_BareDriver()) is False


def test_false_for_none_driver():
    from dynastore.modules.storage.storage_emit import driver_supports_write_id_reads

    assert driver_supports_write_id_reads(None) is False


# ---------------------------------------------------------------------------
# ItemsPostgresqlDriver — the one production driver the contract requires
# to pass the gate (routing pins it as the sole WRITE primary)
# ---------------------------------------------------------------------------


def test_postgresql_driver_passes_the_capability_gate():
    """Guards against a future edit silently renaming/removing
    ``read_active_rows_by_write_id`` / ``read_tombstoned_ids_by_write_id``
    off ``ItemsPostgresqlDriver`` — the only driver the shipped routing
    defaults ever resolve as WRITE primary, so a regression here would
    force every collection onto the id-only fallback with no producer-side
    signal beyond a warning log."""
    from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver
    from dynastore.modules.storage.storage_emit import driver_supports_write_id_reads

    assert driver_supports_write_id_reads(ItemsPostgresqlDriver()) is True


# ---------------------------------------------------------------------------
# Producer degrade path — a partially-capable primary must still degrade
# to id-only rows, never skip the enqueue outright.
# ---------------------------------------------------------------------------


class _PartiallyCapablePrimary:
    """A primary WRITE driver implementing only one half of the by-write-id
    reader pair. Must fail the capability gate exactly like a driver with
    neither reader — a producer that grouped a write-id row against it
    would leave the drain unable to hydrate tombstoned ids and retry
    forever."""

    async def read_active_rows_by_write_id(self, **kwargs):
        return [], None


def _delete_only_async_outbox_routing() -> Any:
    from dynastore.modules.storage.routing_config import (
        FailurePolicy,
        Operation,
        OperationDriverEntry,
        WriteMode,
    )

    entry = OperationDriverEntry(
        driver_ref="items_elasticsearch_driver",
        on_failure=FailurePolicy.OUTBOX,
        write_mode=WriteMode.ASYNC,
        secondary_index=True,
    )
    return {Operation.WRITE: [entry]}


def test_enqueue_index_deletes_degrades_partially_capable_primary_to_id_only():
    """End-to-end: ``ItemQueryMixin._enqueue_index_deletes`` fed a primary
    driver that implements exactly one of the two by-write-id readers must
    (a) never enqueue a grouped write-id row, and (b) still enqueue an
    id-only row per target instead of skipping the obligation entirely."""
    from dynastore.modules.catalog.item_service import ItemService
    from dynastore.modules.storage.storage_emit import driver_supports_write_id_reads

    partial = _PartiallyCapablePrimary()
    assert driver_supports_write_id_reads(partial) is False, (
        "a driver exposing only one of the paired by-write-id readers must "
        "not satisfy the capability gate"
    )

    svc = ItemService.__new__(ItemService)

    async def _resolver(_catalog_id: str, _collection_id: str):
        return SimpleNamespace(operations=_delete_only_async_outbox_routing())

    svc._test_routing_resolver = _resolver  # type: ignore[attr-defined]

    async def _registry(_driver_ref: str):
        return partial

    svc._test_driver_registry = _registry  # type: ignore[attr-defined]

    write_id_enqueued: List[Any] = []
    id_only_enqueued: List[Any] = []

    async def _fake_write_id_enqueue(conn, *, catalog_id: str, rows) -> None:
        write_id_enqueued.append(list(rows))

    async def _fake_id_only_enqueue(conn, *, catalog_id: str, rows) -> None:
        id_only_enqueued.append(list(rows))

    with mock.patch(
        "dynastore.modules.storage.storage_emit.enqueue_storage_op_write_id",
        _fake_write_id_enqueue,
    ), mock.patch(
        "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
        _fake_id_only_enqueue,
    ):
        asyncio.run(
            svc._enqueue_index_deletes(
                object(), "cat-x", "col-y", ["g1", "g2"], write_id="w-partial-1",
            ),
        )

    assert write_id_enqueued == [], (
        "a partially-capable primary must never receive a grouped write-id "
        "row — the drain could not hydrate it"
    )
    assert len(id_only_enqueued) == 1, (
        "the degrade path must still enqueue — an incapable primary must "
        "never cause the async secondary-index obligation to be skipped"
    )
    id_only_rows = id_only_enqueued[0]
    assert {row.item_id for row in id_only_rows} == {"g1", "g2"}
    assert all(row.op == "delete" for row in id_only_rows)


def test_enqueue_index_deletes_groups_write_id_when_primary_capable():
    """Control case: the same routing/target shape, but the primary
    implements both readers — the producer must group into a single
    write-id row instead of falling back."""
    from dynastore.modules.catalog.item_service import ItemService

    class _FullyCapablePrimary:
        async def read_active_rows_by_write_id(self, **kwargs):
            return [], None

        async def read_tombstoned_ids_by_write_id(self, **kwargs):
            return [], None

    svc = ItemService.__new__(ItemService)

    async def _resolver(_catalog_id: str, _collection_id: str):
        return SimpleNamespace(operations=_delete_only_async_outbox_routing())

    svc._test_routing_resolver = _resolver  # type: ignore[attr-defined]

    async def _registry(_driver_ref: str):
        return _FullyCapablePrimary()

    svc._test_driver_registry = _registry  # type: ignore[attr-defined]

    write_id_enqueued: List[Any] = []
    id_only_enqueued: List[Any] = []

    async def _fake_write_id_enqueue(conn, *, catalog_id: str, rows) -> None:
        write_id_enqueued.append(list(rows))

    async def _fake_id_only_enqueue(conn, *, catalog_id: str, rows) -> None:
        id_only_enqueued.append(list(rows))

    with mock.patch(
        "dynastore.modules.storage.storage_emit.enqueue_storage_op_write_id",
        _fake_write_id_enqueue,
    ), mock.patch(
        "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
        _fake_id_only_enqueue,
    ):
        asyncio.run(
            svc._enqueue_index_deletes(
                object(), "cat-x", "col-y", ["g1"], write_id="w-full-1",
            ),
        )

    assert id_only_enqueued == [], (
        "a fully-capable primary must not receive an id-only fallback row"
    )
    assert len(write_id_enqueued) == 1
    assert write_id_enqueued[0][0].write_id == "w-full-1"
