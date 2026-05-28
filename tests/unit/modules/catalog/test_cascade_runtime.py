"""Unit tests for cascade_runtime.py — Chunk 2 of Resource Cascade Cleanup.

Covers:
- Orchestrator collects refs from multiple registered owners.
- Empty registry returns None (no task enqueued).
- Owners returning [] are filtered out; task not enqueued when all return [].
- Enqueue is called with the right payload shape (scope_ref, mode, refs).
- Owner.describe_scope exception is swallowed and logged; other owners proceed.
- snapshot_and_enqueue is called BEFORE _drop_schema_query.execute in
  _purge_catalog_storage (ordering invariant for cascade replay safety).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Iterable
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry
from dynastore.modules.catalog.resource_owner import (
    CleanupMode,
    CleanupOutcome,
    CleanupRef,
    ResourceScope,
    ScopeRef,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_owner(
    owner_id: str,
    refs: list[CleanupRef],
    scopes: tuple[ResourceScope, ...] = (ResourceScope.CATALOG,),
) -> Any:
    """Build a minimal compliant owner that returns *refs* from describe_scope."""

    class _FakeOwner:
        pass

    _FakeOwner.owner_id = owner_id  # type: ignore[attr-defined]

    def supported_scopes(self: Any) -> Iterable[ResourceScope]:
        return scopes

    async def describe_scope(self: Any, scope_ref: ScopeRef, conn: Any) -> list[CleanupRef]:
        return refs

    async def cleanup_one(
        self: Any, ref: CleanupRef, mode: CleanupMode, *, dry_run: bool = False
    ) -> CleanupOutcome:
        return CleanupOutcome.DONE

    _FakeOwner.supported_scopes = supported_scopes  # type: ignore[attr-defined]
    _FakeOwner.describe_scope = describe_scope  # type: ignore[attr-defined]
    _FakeOwner.cleanup_one = cleanup_one  # type: ignore[attr-defined]

    return _FakeOwner()  # type: ignore[return-value]


def _make_raising_owner(owner_id: str) -> Any:
    """Owner whose describe_scope raises an exception."""

    class _RaisingOwner:
        pass

    _RaisingOwner.owner_id = owner_id  # type: ignore[attr-defined]

    def supported_scopes(self: Any) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG,)

    async def describe_scope(self: Any, scope_ref: ScopeRef, conn: Any) -> list[CleanupRef]:
        raise RuntimeError("simulated owner failure")

    async def cleanup_one(
        self: Any, ref: CleanupRef, mode: CleanupMode, *, dry_run: bool = False
    ) -> CleanupOutcome:
        return CleanupOutcome.DONE

    _RaisingOwner.supported_scopes = supported_scopes  # type: ignore[attr-defined]
    _RaisingOwner.describe_scope = describe_scope  # type: ignore[attr-defined]
    _RaisingOwner.cleanup_one = cleanup_one  # type: ignore[attr-defined]

    return _RaisingOwner()  # type: ignore[return-value]


def _make_orchestrator(registry: CascadeCleanupRegistry):
    from dynastore.modules.catalog.cascade_runtime import CascadeOrchestrator
    return CascadeOrchestrator(registry=registry)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestOrchestratorSnapshotAndEnqueue:
    @pytest.mark.asyncio
    async def test_empty_registry_returns_none(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.freeze()
        orch = _make_orchestrator(reg)
        conn = MagicMock()
        scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-1")

        result = await orch.snapshot_and_enqueue(conn, scope_ref, CleanupMode.HARD)
        assert result is None

    @pytest.mark.asyncio
    async def test_all_owners_return_empty_list_no_enqueue(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("owner-a", []))
        reg.register(_make_owner("owner-b", []))
        reg.freeze()
        orch = _make_orchestrator(reg)

        result = await orch.snapshot_and_enqueue(
            MagicMock(), ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-1"),
            CleanupMode.HARD,
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_collects_refs_from_multiple_owners(self) -> None:
        ref_a = CleanupRef(kind="es_index", locator="idx-a", owner_id="owner-a")
        ref_b = CleanupRef(kind="gcs_prefix", locator="gs://bucket/b", owner_id="owner-b")

        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("owner-a", [ref_a]))
        reg.register(_make_owner("owner-b", [ref_b]))
        reg.freeze()
        orch = _make_orchestrator(reg)

        fake_task = MagicMock()
        fake_task.task_id = "task-uuid-123"
        with patch.object(orch, "_enqueue", new=AsyncMock(return_value="task-uuid-123")) as mock_enqueue:
            scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-1")
            result = await orch.snapshot_and_enqueue(MagicMock(), scope_ref, CleanupMode.HARD)

        assert result == "task-uuid-123"
        mock_enqueue.assert_awaited_once()
        _, _, refs = mock_enqueue.call_args.args
        assert len(refs) == 2
        locators = {r["locator"] for r in refs}
        assert locators == {"idx-a", "gs://bucket/b"}

    @pytest.mark.asyncio
    async def test_enqueue_payload_contains_scope_and_mode(self) -> None:
        ref_a = CleanupRef(kind="es_index", locator="idx-a", owner_id="owner-a")
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("owner-a", [ref_a]))
        reg.freeze()
        orch = _make_orchestrator(reg)

        captured: dict = {}

        async def _capture(scope_ref: ScopeRef, mode: CleanupMode, refs: list) -> str:
            captured["scope_ref"] = scope_ref
            captured["mode"] = mode
            captured["refs"] = refs
            return "task-id-42"

        with patch.object(orch, "_enqueue", new=_capture):
            scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-42")
            await orch.snapshot_and_enqueue(MagicMock(), scope_ref, CleanupMode.HARD)

        assert captured["scope_ref"].catalog_id == "cat-42"
        assert captured["mode"] == CleanupMode.HARD
        assert len(captured["refs"]) == 1
        assert captured["refs"][0]["owner_id"] == "owner-a"

    @pytest.mark.asyncio
    async def test_owner_exception_propagates_fail_closed(self) -> None:
        """Fail-closed: any owner exception must propagate so the caller rolls back."""
        ref_b = CleanupRef(kind="gcs_prefix", locator="gs://b", owner_id="owner-b")
        reg = CascadeCleanupRegistry()
        reg.register(_make_raising_owner("owner-a"))
        reg.register(_make_owner("owner-b", [ref_b]))
        reg.freeze()
        orch = _make_orchestrator(reg)

        with patch.object(orch, "_enqueue", new=AsyncMock(return_value="task-x")):
            with pytest.raises(RuntimeError, match="simulated owner failure"):
                await orch.snapshot_and_enqueue(
                    MagicMock(),
                    ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-x"),
                    CleanupMode.HARD,
                )

    @pytest.mark.asyncio
    async def test_raising_owner_alone_propagates(self) -> None:
        """Fail-closed: single raising owner propagates; caller's transaction rolls back."""
        reg = CascadeCleanupRegistry()
        reg.register(_make_raising_owner("owner-a"))
        reg.freeze()
        orch = _make_orchestrator(reg)

        with pytest.raises(RuntimeError, match="simulated owner failure"):
            await orch.snapshot_and_enqueue(
                MagicMock(),
                ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-x"),
                CleanupMode.HARD,
            )


class TestPurgeCatalogStorageOrdering:
    """Regression guard for #1470: snapshot_and_enqueue precedes schema drop."""

    @pytest.mark.asyncio
    async def test_snapshot_called_before_schema_drop(self) -> None:
        """snapshot_and_enqueue must be awaited before _drop_schema_query.execute."""
        import dynastore.modules.catalog.catalog_service as cs_mod
        from dynastore.modules.catalog.catalog_service import CatalogService

        conn = MagicMock()

        mock_orchestrator = MagicMock()
        snapshot_mock = AsyncMock(return_value="task-id-test")
        mock_orchestrator.snapshot_and_enqueue = snapshot_mock

        parent = Mock()
        parent.attach_mock(snapshot_mock, "snapshot_and_enqueue")

        drop_execute_mock = AsyncMock(return_value=None)
        parent.attach_mock(drop_execute_mock, "drop_execute")

        fake_dql_instance = MagicMock()
        fake_dql_instance.execute = AsyncMock(return_value="s_test_schema")

        hard_delete_execute_mock = AsyncMock(return_value=1)

        @asynccontextmanager
        async def _fake_nested_tx(conn_arg: Any):  # noqa: ANN001
            yield MagicMock()

        service = CatalogService(cascade_orchestrator=mock_orchestrator)

        with (
            patch.object(cs_mod._drop_schema_query, "execute", drop_execute_mock),
            patch.object(cs_mod._hard_delete_catalog_query, "execute", hard_delete_execute_mock),
            patch(
                "dynastore.modules.catalog.catalog_service.DQLQuery",
                return_value=fake_dql_instance,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.managed_nested_transaction",
                _fake_nested_tx,
            ),
        ):
            await service._purge_catalog_storage(conn, "cat-ordering-test")

        snapshot_mock.assert_awaited_once()
        drop_execute_mock.assert_awaited_once()

        call_names = [c[0] for c in parent.mock_calls]
        assert "snapshot_and_enqueue" in call_names
        assert "drop_execute" in call_names
        snapshot_pos = call_names.index("snapshot_and_enqueue")
        drop_pos = call_names.index("drop_execute")
        assert snapshot_pos < drop_pos, (
            f"_drop_schema_query.execute (pos {drop_pos}) ran BEFORE "
            f"snapshot_and_enqueue (pos {snapshot_pos}). "
            "The cascade snapshot would capture an empty schema — "
            "revert the reordering in _purge_catalog_storage."
        )
