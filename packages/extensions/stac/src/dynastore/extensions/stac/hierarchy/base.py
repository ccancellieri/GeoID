"""Core protocol + value objects for hierarchy providers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, Optional, Protocol, runtime_checkable


@dataclass
class HierarchyNode:
    """A single node in a hierarchy — uniform shape across all provider kinds.

    Mirrors ogc-dimensions' `GeneratedMember` so dimension-backed is a
    pass-through, while also carrying enough for the STAC virtual-Collection
    renderer (labels, level, has_children).
    """

    code: str
    label: str
    parent_code: Optional[str] = None
    level: Optional[int] = None
    has_children: bool = False
    labels: Dict[str, str] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChildrenPage:
    members: List[HierarchyNode]
    number_matched: int
    number_returned: int
    limit: int
    offset: int


@dataclass
class HierarchyExtent:
    """Spatial/temporal extent aggregated over a node's descendants."""

    spatial_bbox: Optional[List[float]] = None
    temporal_interval: Optional[List[Optional[str]]] = None


@runtime_checkable
class HierarchyProvider(Protocol):
    """Uniform interface over any source of hierarchy."""

    kind: ClassVar[str]

    async def roots(
        self,
        ctx: Any,
        *,
        limit: int = 100,
        offset: int = 0,
        language: Optional[str] = None,
    ) -> ChildrenPage: ...

    async def children(
        self,
        ctx: Any,
        parent_code: str,
        *,
        limit: int = 100,
        offset: int = 0,
        language: Optional[str] = None,
    ) -> ChildrenPage: ...

    async def ancestors(
        self,
        ctx: Any,
        member_code: str,
        *,
        language: Optional[str] = None,
    ) -> List[HierarchyNode]: ...

    async def extent(
        self,
        ctx: Any,
        parent_code: Optional[str],
    ) -> HierarchyExtent: ...

    async def has_children(self, ctx: Any, member_code: str) -> bool: ...
