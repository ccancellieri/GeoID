"""Unit tests for the role-hierarchy cycle guard and list-edges service method.

These tests exercise IamService.add_role_hierarchy / list_role_hierarchy
without a database: storage is stubbed via AsyncMock so the suite runs
fully in-process.

Coverage:
- Self-loop is rejected before storage is called.
- Back-edge (parent already reachable from child) is rejected.
- Valid edge (no existing hierarchy) succeeds.
- Diamond shape (A->B, A->C, B->D, C->D) is not a cycle — D->A would be
  one but C->D is fine — verified here.
- list_role_hierarchy delegates to storage and returns the raw tuple list.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.iam.iam_service import IamService


def _make_service(hierarchy: dict[str, list[str]]) -> IamService:
    """Build a minimal IamService whose storage is stubbed.

    `hierarchy` maps parent_role -> list of direct children.
    get_role_hierarchy on the stub returns a flat descendant list
    (mirrors the real recursive-CTE behaviour: input role + all
    reachable descendants).
    """

    def _descendants(start: str) -> list[str]:
        visited: set[str] = set()
        queue = [start]
        while queue:
            cur = queue.pop()
            if cur in visited:
                continue
            visited.add(cur)
            for child in hierarchy.get(cur, []):
                queue.append(child)
        return list(visited)

    storage = MagicMock()
    storage.add_role_hierarchy = AsyncMock()
    storage.list_role_hierarchy_edges = AsyncMock(
        return_value=[(p, c) for p, children in hierarchy.items() for c in children]
    )

    async def _get_role_hierarchy(
        role_names: list[str],
        conn: Any = None,
        schema: str = "iam",
    ) -> list[str]:
        result: set[str] = set()
        for rn in role_names:
            result.update(_descendants(rn))
        return list(result)

    storage.get_role_hierarchy = AsyncMock(side_effect=_get_role_hierarchy)

    svc = IamService.__new__(IamService)
    svc.storage = storage  # type: ignore[attr-defined]
    svc.policy_service = None  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]

    async def _resolve_schema(_catalog_id: Any = None) -> str:
        return "iam"

    svc._resolve_schema = _resolve_schema  # type: ignore[attr-defined]
    return svc


@pytest.mark.asyncio
async def test_self_loop_is_rejected():
    svc = _make_service(hierarchy={})
    with pytest.raises(ValueError, match="self-loop"):
        await svc.add_role_hierarchy("editor", "editor")
    svc.storage.add_role_hierarchy.assert_not_called()


@pytest.mark.asyncio
async def test_back_edge_is_rejected():
    # A -> B -> C; adding C -> A would close a cycle.
    svc = _make_service(hierarchy={"A": ["B"], "B": ["C"]})
    with pytest.raises(ValueError, match="cycle"):
        await svc.add_role_hierarchy("C", "A")
    svc.storage.add_role_hierarchy.assert_not_called()


@pytest.mark.asyncio
async def test_valid_edge_succeeds():
    svc = _make_service(hierarchy={"A": ["B"]})
    await svc.add_role_hierarchy("B", "C")
    svc.storage.add_role_hierarchy.assert_awaited_once_with("B", "C", schema="iam")


@pytest.mark.asyncio
async def test_diamond_non_cycle_is_allowed():
    # A->B, A->C, B->D, C->D — valid diamond; only D->A would be a cycle.
    svc = _make_service(hierarchy={"A": ["B", "C"], "B": ["D"], "C": ["D"]})
    # Adding C->D again (already exists) is idempotent at the storage level;
    # the guard only blocks *cycle-forming* edges. C->D is not a back-edge
    # for any of the existing parents, so the guard passes.
    await svc.add_role_hierarchy("C", "D")
    svc.storage.add_role_hierarchy.assert_awaited_once()


@pytest.mark.asyncio
async def test_diamond_cycle_addition_is_rejected():
    # After diamond A->B, A->C, B->D, C->D: adding D->A closes a cycle.
    svc = _make_service(hierarchy={"A": ["B", "C"], "B": ["D"], "C": ["D"]})
    with pytest.raises(ValueError, match="cycle"):
        await svc.add_role_hierarchy("D", "A")
    svc.storage.add_role_hierarchy.assert_not_called()


@pytest.mark.asyncio
async def test_list_role_hierarchy_returns_edges():
    svc = _make_service(hierarchy={"A": ["B"], "B": ["C"]})
    edges = await svc.list_role_hierarchy()
    assert ("A", "B") in edges
    assert ("B", "C") in edges
    svc.storage.list_role_hierarchy_edges.assert_awaited_once_with(schema="iam")
