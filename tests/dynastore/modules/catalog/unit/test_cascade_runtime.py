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


class TestObservabilityOnSilentLossPaths:
    """Regression guards for #1469: ERROR-level structured logs at the
    silent-loss points so operators can attribute fail-closed rollbacks
    to a specific owner."""

    @pytest.mark.asyncio
    async def test_describe_scope_failure_emits_error_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_raising_owner("owner-broken"))
        reg.freeze()
        orch = _make_orchestrator(reg)

        with caplog.at_level("ERROR", logger="dynastore.modules.catalog.cascade_runtime"):
            with pytest.raises(RuntimeError, match="simulated owner failure"):
                await orch.snapshot_and_enqueue(
                    MagicMock(),
                    ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-9"),
                    CleanupMode.HARD,
                )

        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert error_records, "expected ERROR log on describe_scope failure"
        msg = error_records[0].getMessage()
        assert "cascade_describe_scope_failed" in msg
        assert "owner-broken" in msg
        assert "cat-9" in msg
        assert "fail-closed" in msg.lower() or "rolls back" in msg.lower() or "roll back" in msg.lower()

    @pytest.mark.asyncio
    async def test_enqueue_failure_emits_error_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ref = CleanupRef(kind="es_index", locator="idx-x", owner_id="owner-ok")
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("owner-ok", [ref]))
        reg.freeze()
        orch = _make_orchestrator(reg)

        async def _boom(*_args: Any, **_kwargs: Any) -> str:
            raise RuntimeError("task storage unavailable")

        with caplog.at_level("ERROR", logger="dynastore.modules.catalog.cascade_runtime"):
            with patch.object(orch, "_enqueue", new=_boom):
                with pytest.raises(RuntimeError, match="task storage unavailable"):
                    await orch.snapshot_and_enqueue(
                        MagicMock(),
                        ScopeRef(scope=ResourceScope.CATALOG, catalog_id="cat-enq"),
                        CleanupMode.HARD,
                    )

        error_records = [r for r in caplog.records if r.levelname == "ERROR"]
        assert error_records, "expected ERROR log on enqueue failure"
        msg = error_records[0].getMessage()
        assert "cascade_enqueue_failed" in msg
        assert "cat-enq" in msg
        assert "ref_count=1" in msg


class TestPurgeCatalogStorageOrdering:
    """Regression guard for #1470: snapshot_and_enqueue precedes schema drop."""

    @pytest.mark.asyncio
    async def test_snapshot_called_before_schema_drop(self) -> None:
        """snapshot_and_enqueue must be awaited before the schema is dropped.

        The drop now runs through ``safe_drop_relation`` (bounded lock_timeout +
        retry on 55P03 under concurrent catalog deletes), so the ordering guard
        patches that helper rather than the raw ``_drop_schema_query.execute``.
        """
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
            patch(
                "dynastore.modules.db_config.locking_tools.safe_drop_relation",
                drop_execute_mock,
            ),
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
            f"safe_drop_relation (pos {drop_pos}) ran BEFORE "
            f"snapshot_and_enqueue (pos {snapshot_pos}). "
            "The cascade snapshot would capture an empty schema — "
            "revert the reordering in _purge_catalog_storage."
        )
