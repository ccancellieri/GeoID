"""Data-derived generator — wraps `stac_hierarchy_queries` SQL rules.

This is the legacy path (SQL over a collection's rows) exposed behind the
`HierarchyGenerator` protocol. Behaviour is unchanged; only the surface moves.

`ctx` is a namespace with `catalog_id: str`, `collection_id: str`,
`conn: AsyncConnection`.
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Optional

from ..stac_hierarchy_queries import (
    get_distinct_hierarchy_values,
    get_hierarchy_extent,
    get_hierarchy_item_count,
)
from .base import ChildrenPage, HierarchyExtent, HierarchyNode
from .config import HierarchyProviderConfig
from .registry import register_provider


class DataDerivedProvider:
    kind: ClassVar[str] = "data-derived"

    def __init__(self, config: HierarchyProviderConfig, _ctx: Any) -> None:
        assert config.rule is not None  # enforced by model_validator
        self._config = config
        self._rule = config.rule

    def _row_to_node(self, row: dict, parent_code: Optional[str]) -> HierarchyNode:
        code = row.get("code") or row.get(self._rule.item_code_field)
        return HierarchyNode(
            code=str(code),
            label=str(code),
            parent_code=parent_code,
            level=None,
            has_children=False,  # data-derived: unknown without extra query; renderer probes lazily
            labels={},
            extra={k: v for k, v in row.items() if k not in {"code"}},
        )

    async def _page(
        self,
        ctx: Any,
        parent_code: Optional[str],
        limit: int,
        offset: int,
    ) -> ChildrenPage:
        rows = await get_distinct_hierarchy_values(
            ctx.conn, ctx.catalog_id, ctx.collection_id, self._rule,
            parent_value=parent_code, limit=limit + offset,
        )
        total = await get_hierarchy_item_count(
            ctx.conn, ctx.catalog_id, ctx.collection_id, self._rule,
            parent_value=parent_code,
        )
        window = rows[offset : offset + limit]
        members = [self._row_to_node(r, parent_code) for r in window]
        return ChildrenPage(
            members=members,
            number_matched=total,
            number_returned=len(members),
            limit=limit,
            offset=offset,
        )

    async def roots(self, ctx: Any, *, limit: int = 100, offset: int = 0, language: Optional[str] = None) -> ChildrenPage:
        return await self._page(ctx, None, limit, offset)

    async def children(self, ctx: Any, parent_code: str, *, limit: int = 100, offset: int = 0, language: Optional[str] = None) -> ChildrenPage:
        return await self._page(ctx, parent_code, limit, offset)

    async def ancestors(self, ctx: Any, member_code: str, *, language: Optional[str] = None) -> List[HierarchyNode]:
        # Data-derived ancestry is only traversable when rule.parent_code_field is set.
        # Walking rows-by-rows isn't supported in the legacy path; return the member itself.
        return [HierarchyNode(code=member_code, label=member_code)]

    async def extent(self, ctx: Any, parent_code: Optional[str]) -> HierarchyExtent:
        ext = await get_hierarchy_extent(
            ctx.conn, ctx.catalog_id, ctx.collection_id, self._rule, parent_value=parent_code,
        )
        temporal = ext.get("temporal") or [[None, None]]
        return HierarchyExtent(
            spatial_bbox=ext.get("bbox"),
            temporal_interval=temporal[0] if temporal else [None, None],  # type: ignore[arg-type]
        )

    async def has_children(self, ctx: Any, member_code: str) -> bool:
        page = await self._page(ctx, member_code, limit=1, offset=0)
        return page.number_matched > 0


@register_provider("data-derived")
def _build_data_derived(config: HierarchyProviderConfig, ctx: Any) -> DataDerivedProvider:
    return DataDerivedProvider(config, ctx)
