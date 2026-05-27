"""Unit tests for CascadeCleanupTask — Chunk 2 of Resource Cascade Cleanup.

Covers:
- Task deserializes refs via CleanupRef.from_json and routes to the right owner.
- RETRY outcome re-raises RuntimeError so the task framework retries.
- DEAD outcome is logged but processing continues; task raises at end.
- Unknown owner_id is logged as error and treated as DEAD.
- Malformed ref dict is treated as DEAD.
- All DONE → success return dict.
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
from dynastore.tasks.cascade_cleanup.task import CascadeCleanupTask


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_owner(
    owner_id: str,
    outcome: CleanupOutcome = CleanupOutcome.DONE,
) -> Any:
    class _FakeOwner:
        pass

    _FakeOwner.owner_id = owner_id  # type: ignore[attr-defined]

    def supported_scopes(self: Any) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG,)

    async def describe_scope(self: Any, scope_ref: ScopeRef, conn: Any) -> list[CleanupRef]:
        return []

    async def cleanup_one(
        self: Any,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome:
        return outcome

    _FakeOwner.supported_scopes = supported_scopes  # type: ignore[attr-defined]
    _FakeOwner.describe_scope = describe_scope  # type: ignore[attr-defined]
    _FakeOwner.cleanup_one = cleanup_one  # type: ignore[attr-defined]

    return _FakeOwner()  # type: ignore[return-value]


def _make_payload(refs: list[dict], mode: str = "hard") -> Any:
    scope_ref_dict = {"scope": "catalog", "catalog_id": "cat-1"}
    payload = MagicMock()
    payload.inputs = {
        "scope_ref": scope_ref_dict,
        "mode": mode,
        "refs": refs,
    }
    return payload


def _run_with_registry(task: CascadeCleanupTask, payload: Any, registry: CascadeCleanupRegistry):
    """Patch the global registry with a test instance and run the task."""
    with patch(
        "dynastore.modules.catalog.cascade_registry.cascade_cleanup_registry",
        new=registry,
    ):
        import asyncio
        return asyncio.get_event_loop().run_until_complete(task.run(payload))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCascadeCleanupTaskAllDone:
    def test_all_done_returns_success_dict(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("owner-a", CleanupOutcome.DONE))
        reg.freeze()

        ref = CleanupRef(kind="es_index", locator="idx-a", owner_id="owner-a")
        payload = _make_payload([ref.to_json()])

        task = CascadeCleanupTask()
        result = _run_with_registry(task, payload, reg)

        assert result["status"] == "ok"
        assert result["done"] == 1
        assert result["dead"] == 0
        assert result["retry"] == 0

    def test_empty_refs_returns_ok(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.freeze()

        payload = _make_payload([])
        task = CascadeCleanupTask()
        result = _run_with_registry(task, payload, reg)

        assert result["status"] == "ok"
        assert result["done"] == 0


class TestCascadeCleanupTaskRetry:
    def test_retry_outcome_raises_runtime_error(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("owner-a", CleanupOutcome.RETRY))
        reg.freeze()

        ref = CleanupRef(kind="es_index", locator="idx-a", owner_id="owner-a")
        payload = _make_payload([ref.to_json()])

        task = CascadeCleanupTask()
        with pytest.raises(RuntimeError, match="retry"):
            _run_with_registry(task, payload, reg)

    def test_retry_after_done_still_raises(self) -> None:
        """If any ref is RETRY the whole task raises (even if others are DONE)."""
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("owner-a", CleanupOutcome.DONE))
        reg.register(_make_owner("owner-b", CleanupOutcome.RETRY))
        reg.freeze()

        refs = [
            CleanupRef(kind="es_index", locator="idx-a", owner_id="owner-a").to_json(),
            CleanupRef(kind="es_index", locator="idx-b", owner_id="owner-b").to_json(),
        ]
        payload = _make_payload(refs)

        task = CascadeCleanupTask()
        with pytest.raises(RuntimeError, match="retry"):
            _run_with_registry(task, payload, reg)


class TestCascadeCleanupTaskDead:
    def test_dead_outcome_raises_after_all_refs_processed(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("owner-a", CleanupOutcome.DEAD))
        reg.register(_make_owner("owner-b", CleanupOutcome.DONE))
        reg.freeze()

        refs = [
            CleanupRef(kind="es_index", locator="idx-a", owner_id="owner-a").to_json(),
            CleanupRef(kind="es_index", locator="idx-b", owner_id="owner-b").to_json(),
        ]
        payload = _make_payload(refs)

        task = CascadeCleanupTask()
        with pytest.raises(RuntimeError, match="permanently failed"):
            _run_with_registry(task, payload, reg)

    def test_dead_and_retry_both_raise(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.register(_make_owner("owner-a", CleanupOutcome.DEAD))
        reg.register(_make_owner("owner-b", CleanupOutcome.RETRY))
        reg.freeze()

        refs = [
            CleanupRef(kind="es_index", locator="idx-a", owner_id="owner-a").to_json(),
            CleanupRef(kind="es_index", locator="idx-b", owner_id="owner-b").to_json(),
        ]
        payload = _make_payload(refs)

        task = CascadeCleanupTask()
        # RETRY takes priority in the final raise — retry_refs is checked first.
        with pytest.raises(RuntimeError):
            _run_with_registry(task, payload, reg)


class TestCascadeCleanupTaskUnknownOwner:
    def test_unknown_owner_id_treated_as_dead(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.freeze()

        ref = CleanupRef(kind="es_index", locator="idx-x", owner_id="ghost.owner")
        payload = _make_payload([ref.to_json()])

        task = CascadeCleanupTask()
        with pytest.raises(RuntimeError, match="permanently failed"):
            _run_with_registry(task, payload, reg)


class TestCascadeCleanupTaskMalformedRef:
    def test_malformed_ref_dict_treated_as_dead(self) -> None:
        reg = CascadeCleanupRegistry()
        reg.freeze()

        bad_ref: dict = {"broken": "no required keys"}
        payload = _make_payload([bad_ref])

        task = CascadeCleanupTask()
        with pytest.raises(RuntimeError, match="permanently failed"):
            _run_with_registry(task, payload, reg)
