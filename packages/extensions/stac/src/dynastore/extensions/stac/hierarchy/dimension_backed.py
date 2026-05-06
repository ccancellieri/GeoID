"""Dimension-backed generator — wraps an ogc-dimensions hierarchical provider.

Delegates every call to the provider. No SQL, no materialisation.
For non-hierarchical providers, construction raises so the route can 404.

`ctx` must expose `get_provider(dimension_id) -> DimensionProvider` OR
carry a pre-resolved `provider` attribute.
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar, List, Optional

from .base import ChildrenPage, HierarchyExtent, HierarchyNode
from .config import HierarchyProviderConfig
from .registry import register_provider


def _member_to_node(m: Any, parent_code: Optional[str] = None) -> HierarchyNode:
    """Adapt an ogc-dimensions `GeneratedMember` to `HierarchyNode`."""
    extra = dict(getattr(m, "extra", {}) or {})
    labels = dict(extra.get("labels") or {})
    return HierarchyNode(
        code=str(getattr(m, "code")),
        label=str(getattr(m, "label", None) or getattr(m, "code")),
        parent_code=extra.get("parent_code", parent_code),
        level=extra.get("level"),
        has_children=bool(getattr(m, "has_children", False)),
        labels=labels,
        extra=extra,
    )


class DimensionBackedProvider:
    kind: ClassVar[str] = "dimension-backed"

    def __init__(self, config: HierarchyProviderConfig, ctx: Any) -> None:
        assert config.dimension_id  # enforced by model_validator
        provider = getattr(ctx, "provider", None)
        if provider is None:
            resolver = getattr(ctx, "get_provider", None)
            if resolver is None:
                raise LookupError("ctx must expose `provider` or `get_provider(dimension_id)`")
            provider = resolver(config.dimension_id)
        if not getattr(provider, "hierarchical", False):
            raise LookupError(
                f"dimension {config.dimension_id!r} provider is not hierarchical"
            )
        self._provider = provider

    @staticmethod
    async def _to_thread(fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    def _page(self, result: Any, limit: int, offset: int, parent_code: Optional[str]) -> ChildrenPage:
        members = [_member_to_node(m, parent_code) for m in getattr(result, "members", [])]
        return ChildrenPage(
            members=members,
            number_matched=int(getattr(result, "number_matched", len(members))),
            number_returned=int(getattr(result, "number_returned", len(members))),
            limit=limit,
            offset=offset,
        )

    async def roots(self, ctx: Any, *, limit: int = 100, offset: int = 0, language: Optional[str] = None) -> ChildrenPage:
        result = await self._to_thread(
            self._provider.generate, "", "", limit=limit, offset=offset, parent=None,
        )
        return self._page(result, limit, offset, None)

    async def children(self, ctx: Any, parent_code: str, *, limit: int = 100, offset: int = 0, language: Optional[str] = None) -> ChildrenPage:
        result = await self._to_thread(
            self._provider.children, parent_code, limit=limit, offset=offset,
        )
        return self._page(result, limit, offset, parent_code)

    async def ancestors(self, ctx: Any, member_code: str, *, language: Optional[str] = None) -> List[HierarchyNode]:
        chain = await self._to_thread(self._provider.ancestors, member_code)
        nodes: List[HierarchyNode] = []
        for entry in chain or []:
            if hasattr(entry, "code"):
                nodes.append(_member_to_node(entry))
            else:
                nodes.append(HierarchyNode(
                    code=str(entry.get("code")),
                    label=str(entry.get("label") or entry.get("code")),
                    parent_code=entry.get("parent_code"),
                    level=entry.get("level"),
                    labels=dict(entry.get("labels") or {}),
                    extra=dict(entry),
                ))
        return nodes

    async def extent(self, ctx: Any, parent_code: Optional[str]) -> HierarchyExtent:
        # ogc-dimensions extent is member-space, not spatial/temporal — leave empty.
        return HierarchyExtent()

    async def has_children(self, ctx: Any, member_code: str) -> bool:
        return bool(await self._to_thread(self._provider.has_children, member_code))


@register_provider("dimension-backed")
def _build_dimension_backed(config: HierarchyProviderConfig, ctx: Any) -> DimensionBackedProvider:
    return DimensionBackedProvider(config, ctx)
