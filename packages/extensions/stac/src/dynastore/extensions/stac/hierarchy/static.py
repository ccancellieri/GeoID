"""In-memory static-tree generator.

Reads an embedded tree from `config.params.tree` — a list of
`{code, label, labels?, parent_code, level?, extra?}` dicts — and walks it
without any SQL or HTTP. Useful for tiny config-only hierarchies.
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional

from .base import ChildrenPage, HierarchyExtent, HierarchyNode
from .config import HierarchyProviderConfig
from .registry import register_provider


class StaticTreeProvider:
    kind: ClassVar[str] = "static"

    def __init__(self, config: HierarchyProviderConfig, _ctx: Any) -> None:
        nodes: List[Dict[str, Any]] = list(config.params.get("tree") or [])
        self._by_code: Dict[str, Dict[str, Any]] = {n["code"]: n for n in nodes}
        self._children: Dict[Optional[str], List[Dict[str, Any]]] = {}
        for n in nodes:
            self._children.setdefault(n.get("parent_code"), []).append(n)

    @staticmethod
    def _to_node(raw: Dict[str, Any], has_children: bool) -> HierarchyNode:
        return HierarchyNode(
            code=raw["code"],
            label=raw.get("label") or raw["code"],
            parent_code=raw.get("parent_code"),
            level=raw.get("level"),
            has_children=has_children,
            labels=dict(raw.get("labels") or {}),
            extra={k: v for k, v in raw.items() if k not in {"code", "label", "labels", "parent_code", "level"}},
        )

    def _page(self, rows: List[Dict[str, Any]], limit: int, offset: int) -> ChildrenPage:
        total = len(rows)
        window = rows[offset : offset + limit]
        members = [self._to_node(r, bool(self._children.get(r["code"]))) for r in window]
        return ChildrenPage(
            members=members,
            number_matched=total,
            number_returned=len(members),
            limit=limit,
            offset=offset,
        )

    async def roots(self, ctx: Any, *, limit: int = 100, offset: int = 0, language: Optional[str] = None) -> ChildrenPage:
        return self._page(self._children.get(None, []), limit, offset)

    async def children(self, ctx: Any, parent_code: str, *, limit: int = 100, offset: int = 0, language: Optional[str] = None) -> ChildrenPage:
        return self._page(self._children.get(parent_code, []), limit, offset)

    async def ancestors(self, ctx: Any, member_code: str, *, language: Optional[str] = None) -> List[HierarchyNode]:
        chain: List[HierarchyNode] = []
        seen: set[str] = set()
        cur = self._by_code.get(member_code)
        while cur is not None and cur["code"] not in seen:
            seen.add(cur["code"])
            chain.append(self._to_node(cur, bool(self._children.get(cur["code"]))))
            parent = cur.get("parent_code")
            cur = self._by_code.get(parent) if parent else None
        return list(reversed(chain))

    async def extent(self, ctx: Any, parent_code: Optional[str]) -> HierarchyExtent:
        return HierarchyExtent()

    async def has_children(self, ctx: Any, member_code: str) -> bool:
        return bool(self._children.get(member_code))


@register_provider("static")
def _build_static(config: HierarchyProviderConfig, ctx: Any) -> StaticTreeProvider:
    return StaticTreeProvider(config, ctx)
