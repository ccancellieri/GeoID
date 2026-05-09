"""Closes followup #4 from PR #420.

Pins the symmetry between :func:`_adapter_collection_hard_deletion` and
:func:`_adapter_catalog_hard_deletion`: both must pre-resolve the GCS
bucket name from ``StorageProtocol`` while the catalog state is still
intact, and both must thread it via ``TaskCreate.inputs["bucket_name"]``
so the downstream ``GcpCatalogCleanupTask`` works even if the parent
catalog config or schema changes between event emission and task run.

Asserts:

* Happy path — ``StorageProtocol.get_storage_identifier(catalog_id)``
  is called once and its return value lands in inputs.
* Defensive fallback — if ``StorageProtocol`` raises, the adapter still
  enqueues with ``bucket_name=None`` (matches catalog adapter behavior;
  task does a live lookup as a last resort).
* No StorageProtocol available — adapter still enqueues with
  ``bucket_name=None`` (no crash).

Hermetic: stubs ``get_protocol`` and ``create_task_for_catalog`` —
no DB, no module bring-up, no real GCS client.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.extensions.gcp import gcp_events


@pytest.fixture
def captured_task_inputs() -> List[Dict[str, Any]]:
    return []


@pytest.fixture
def patched_create_task(monkeypatch, captured_task_inputs):
    async def _fake_create_task(engine, task_data, catalog_id):  # noqa: ARG001
        captured_task_inputs.append(task_data.inputs)
        return MagicMock(task_id="t-fake", dedup_key=task_data.dedup_key)

    import dynastore.modules.tasks.tasks_module as tasks_module

    monkeypatch.setattr(tasks_module, "create_task_for_catalog", _fake_create_task)
    return _fake_create_task


def _patch_protocols(monkeypatch, *, storage: Optional[MagicMock]) -> None:
    """Stub ``get_protocol`` to return a fake DatabaseProtocol always, and
    optionally a StorageProtocol."""
    db_stub = MagicMock(engine=MagicMock(name="fake-engine"))

    def _fake_get_protocol(proto):
        name = getattr(proto, "__name__", "")
        if name == "DatabaseProtocol":
            return db_stub
        if name == "StorageProtocol":
            return storage
        return None

    monkeypatch.setattr(gcp_events, "get_protocol", _fake_get_protocol)


@pytest.mark.asyncio
async def test_collection_adapter_preresolves_bucket_name(
    monkeypatch, patched_create_task, captured_task_inputs
):
    storage = MagicMock()
    storage.get_storage_identifier = AsyncMock(return_value="resolved-bucket-xyz")
    _patch_protocols(monkeypatch, storage=storage)

    await gcp_events._adapter_collection_hard_deletion(
        catalog_id="cat_a", collection_id="col_a"
    )

    storage.get_storage_identifier.assert_awaited_once_with("cat_a")
    assert len(captured_task_inputs) == 1
    inputs = captured_task_inputs[0]
    assert inputs["scope"] == "collection"
    assert inputs["catalog_id"] == "cat_a"
    assert inputs["collection_id"] == "col_a"
    assert inputs["bucket_name"] == "resolved-bucket-xyz", (
        "Pre-resolved bucket_name must be threaded into TaskCreate.inputs "
        "so cleanup is robust to parent catalog mutation/teardown. "
        "Asymmetry vs _adapter_catalog_hard_deletion regressed."
    )


@pytest.mark.asyncio
async def test_collection_adapter_falls_through_when_storage_raises(
    monkeypatch, patched_create_task, captured_task_inputs
):
    storage = MagicMock()
    storage.get_storage_identifier = AsyncMock(
        side_effect=RuntimeError("transient backend hiccup")
    )
    _patch_protocols(monkeypatch, storage=storage)

    await gcp_events._adapter_collection_hard_deletion(
        catalog_id="cat_b", collection_id="col_b"
    )

    storage.get_storage_identifier.assert_awaited_once_with("cat_b")
    assert len(captured_task_inputs) == 1
    inputs = captured_task_inputs[0]
    assert inputs["bucket_name"] is None, (
        "If pre-resolve raises, adapter must still enqueue with "
        "bucket_name=None so the task's live-lookup fallback kicks in."
    )
    assert inputs["catalog_id"] == "cat_b"
    assert inputs["collection_id"] == "col_b"


@pytest.mark.asyncio
async def test_collection_adapter_works_without_storage_protocol(
    monkeypatch, patched_create_task, captured_task_inputs
):
    _patch_protocols(monkeypatch, storage=None)

    await gcp_events._adapter_collection_hard_deletion(
        catalog_id="cat_c", collection_id="col_c"
    )

    assert len(captured_task_inputs) == 1
    inputs = captured_task_inputs[0]
    assert inputs["bucket_name"] is None
    assert inputs["catalog_id"] == "cat_c"
    assert inputs["collection_id"] == "col_c"
