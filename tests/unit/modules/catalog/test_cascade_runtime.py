"""Unit tests for cascade_runtime.py — Chunk 2 of Resource Cascade Cleanup.

Covers:
- Orchestrator collects refs from multiple registered owners.
- Empty registry returns None (no task enqueued).
- Owners returning [] are filtered out; task not enqueued when all return [].
- Enqueue is called with the right payload shape (scope_ref, mode, refs).
- Owner.describe_scope exception is swallowed and logged; other owners proceed.
"""

from __future__ import annotations

from typing import Any, Iterable
from unittest.mock import AsyncMock, MagicMock, patch

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
    async def test_owner_exception_swallowed_others_proceed(self) -> None:
        ref_b = CleanupRef(kind="gcs_prefix", locator="gs://b", owner_id="owner-b")
        reg = CascadeCleanupRegistry()
        reg.register(_make_raising_owner("owner-a"))
        reg.register(_make_owner("owner-b", [ref_b]))
        reg.freeze()
        orch = _make_orchestrator(reg)

        with patch.object(orch, "_enqueue", new=AsyncMock(return_value="task-x")):
            result = await orch.snapshot_and_enqueue(
                MagicMock(),
                ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-x"),
                CleanupMode.HARD,
            )
        assert result == "task-x"

    @pytest.mark.asyncio
    async def test_raising_owner_alone_returns_none(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_raising_owner("owner-a"))
        reg.freeze()
        orch = _make_orchestrator(reg)

        result = await orch.snapshot_and_enqueue(
            MagicMock(),
            ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-x"),
            CleanupMode.HARD,
        )
        assert result is None
