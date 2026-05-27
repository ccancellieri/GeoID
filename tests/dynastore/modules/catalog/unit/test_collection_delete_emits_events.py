"""Regression coverage for #1438 — collection delete must emit the
``BEFORE_/COLLECTION_/AFTER_COLLECTION_*_DELETION`` events.

The cascade handler ``CatalogModule._on_collection_hard_deletion``
(``catalog_module.py:316``) and several siblings (tile cache cleanup,
GCP per-collection blob-prefix cleanup, webhook fan-out) are wired to
these events. Without the emits in ``CollectionService.delete_collection``
none of them fire — assets table rows and storage blobs are left
orphaned on hard-delete, exactly the user-reported symptom in #1438.

Two layers of coverage:

1. Source-shape — pins the literal emit references so a future refactor
   that drops one of them fails loudly (mirrors
   ``test_catalog_delete_emits_metadata_changed.py``).

2. Runtime — patches the DB plumbing and asserts the patched
   ``emit_event`` symbol receives the expected event types and kwargs
   under both the ``force=True`` and ``force=False`` branches, including
   the no-op branch (already-tombstoned row) which must NOT emit.
"""

from __future__ import annotations

import inspect
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.catalog import collection_service as collection_service_mod
from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.modules.catalog.event_service import CatalogEventType


# ---------------------------------------------------------------------------
# Source-shape: cheap, deterministic regression guard
# ---------------------------------------------------------------------------


def _delete_collection_source() -> str:
    return inspect.getsource(CollectionService.delete_collection)


def test_hard_delete_branch_emits_before_collection_hard_deletion() -> None:
    src = _delete_collection_source()
    assert "CatalogEventType.BEFORE_COLLECTION_HARD_DELETION" in src, (
        "delete_collection(force=True) no longer emits "
        "BEFORE_COLLECTION_HARD_DELETION — re-add the inline emit_event so "
        "gcp_events._adapter_collection_hard_deletion can pre-resolve the "
        "bucket name before the catalog state is torn down (#1438)."
    )


def test_hard_delete_branch_emits_collection_hard_deletion() -> None:
    src = _delete_collection_source()
    assert "CatalogEventType.COLLECTION_HARD_DELETION" in src, (
        "delete_collection(force=True) no longer emits "
        "COLLECTION_HARD_DELETION — re-add the inline emit_event so the "
        "tile cache cleanup and webhook fan-out subscribers fire (#1438)."
    )


def test_hard_delete_branch_emits_after_collection_hard_deletion() -> None:
    src = _delete_collection_source()
    assert "CatalogEventType.AFTER_COLLECTION_HARD_DELETION" in src, (
        "delete_collection(force=True) no longer emits "
        "AFTER_COLLECTION_HARD_DELETION — re-add the inline emit_event so "
        "catalog_module._on_collection_hard_deletion cascades to "
        "AssetsProtocol.delete_assets(hard=True). Without it the assets "
        "table rows and storage blobs are left orphaned on hard-delete "
        "(#1438)."
    )


def test_soft_delete_branch_emits_collection_deletion() -> None:
    src = _delete_collection_source()
    assert "CatalogEventType.COLLECTION_DELETION" in src, (
        "delete_collection(force=False) no longer emits COLLECTION_DELETION "
        "— catalog_module._on_collection_deletion soft-cascades to "
        "AssetsProtocol.delete_assets(hard=False), so removing this emit "
        "leaves assets live under a tombstoned collection (#1438)."
    )


# ---------------------------------------------------------------------------
# Runtime: assert emit_event is actually invoked with the right shape
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _stub_txn(_engine):
    """Yield a sentinel "conn" without opening a real transaction."""
    yield object()


@pytest.fixture
def record_emit(monkeypatch):
    """Replace ``emit_event`` (imported by name in collection_service) with a
    recorder that captures ``(event_type, kwargs)`` per call.
    """
    calls: list[tuple] = []

    async def _recorder(event_type, *args, **kwargs):
        calls.append((event_type, kwargs))

    monkeypatch.setattr(
        collection_service_mod, "emit_event", AsyncMock(side_effect=_recorder)
    )
    monkeypatch.setattr(
        collection_service_mod, "managed_transaction", _stub_txn
    )
    return calls


@pytest.fixture
def svc(monkeypatch):
    """A CollectionService with the catalog-id → physical-schema resolver
    and the storage-purge helper stubbed so the test can exercise
    ``delete_collection`` without spinning up the SQL stack.
    """
    s = CollectionService(engine=MagicMock())

    async def _phys_schema(_self, catalog_id, db_resource=None):
        return "phys_sch_test"

    async def _purge(_self, conn, phys_schema, catalog_id, collection_id):
        return "phys_table_test"

    monkeypatch.setattr(CollectionService, "_resolve_physical_schema", _phys_schema)
    monkeypatch.setattr(CollectionService, "_purge_collection_storage", _purge)
    # Neutralise the post-commit async-destroyer fan-out — the hooks aren't
    # the unit under test and may pull in unrelated module state.
    monkeypatch.setattr(
        collection_service_mod.lifecycle_registry,
        "destroy_async_collection",
        lambda *a, **kw: None,
    )
    # ConfigsProtocol may be unavailable in the unit context; the snapshot
    # try/except already tolerates that, but keep the resolver silent.
    monkeypatch.setattr(
        collection_service_mod, "get_protocol", lambda *_a, **_kw: None
    )
    return s


@pytest.mark.asyncio
async def test_hard_delete_runtime_emits_three_events_in_order(svc, record_emit):
    ok = await svc.delete_collection("cat_a", "col_a", force=True)
    assert ok is True

    event_types = [c[0] for c in record_emit]
    assert event_types == [
        CatalogEventType.BEFORE_COLLECTION_HARD_DELETION,
        CatalogEventType.COLLECTION_HARD_DELETION,
        CatalogEventType.AFTER_COLLECTION_HARD_DELETION,
    ], (
        "Hard-delete must emit BEFORE -> HARD_DELETION -> AFTER in that "
        "order so gcp_events can pre-resolve bucket state in BEFORE and "
        "catalog_module._on_collection_hard_deletion can cascade asset "
        "cleanup on AFTER (#1438)."
    )

    # Every emit must carry both the catalog and collection ids — the
    # cascade subscribers key off these to find the right scope.
    for _evt, kwargs in record_emit:
        assert kwargs["catalog_id"] == "cat_a"
        assert kwargs["collection_id"] == "col_a"
        assert kwargs["physical_schema"] == "phys_sch_test"

    # HARD_DELETION and AFTER additionally pass the resolved physical_table
    # for downstream consumers (e.g. tile cache that wants to dedupe by
    # table name).
    hd = next(k for e, k in record_emit if e == CatalogEventType.COLLECTION_HARD_DELETION)
    after = next(k for e, k in record_emit if e == CatalogEventType.AFTER_COLLECTION_HARD_DELETION)
    assert hd["physical_table"] == "phys_table_test"
    assert after["physical_table"] == "phys_table_test"


@pytest.mark.asyncio
async def test_soft_delete_emits_collection_deletion_when_row_transitions(
    svc, record_emit, monkeypatch
):
    """A real soft-delete (rows>0) must emit COLLECTION_DELETION exactly
    once so the soft asset cascade in catalog_module fires.
    """

    class _DQLStub:
        def __init__(self, *_a, **_kw):
            pass

        async def execute(self, *_a, **_kw):
            return 1  # one row tombstoned

    monkeypatch.setattr(collection_service_mod, "DQLQuery", _DQLStub)

    ok = await svc.delete_collection("cat_b", "col_b", force=False)
    assert ok is True

    event_types = [c[0] for c in record_emit]
    assert event_types == [CatalogEventType.COLLECTION_DELETION]
    kwargs = record_emit[0][1]
    assert kwargs["catalog_id"] == "cat_b"
    assert kwargs["collection_id"] == "col_b"


@pytest.mark.asyncio
async def test_soft_delete_no_emit_when_row_already_tombstoned(
    svc, record_emit, monkeypatch
):
    """An idempotent soft-delete (no rows actually transitioned) must
    NOT re-fire COLLECTION_DELETION — otherwise the cascade subscribers
    would re-run the asset soft-delete on every redundant call.
    """

    class _DQLStub:
        def __init__(self, *_a, **_kw):
            pass

        async def execute(self, *_a, **_kw):
            return 0  # nothing to tombstone

    monkeypatch.setattr(collection_service_mod, "DQLQuery", _DQLStub)

    ok = await svc.delete_collection("cat_c", "col_c", force=False)
    assert ok is True  # preserves prior contract of returning True
    assert record_emit == []
