"""Pluggable hierarchy-provider layer for STAC virtual Collections.

A `HierarchyProvider` exposes a uniform API — roots / children / ancestors /
extent / has_children — over any source of hierarchy:

  - data-derived     : SQL rules over a collection's rows (legacy path)
  - dimension-backed : delegates to an ogc-dimensions hierarchical provider
  - static           : embedded JSON tree in config
  - external-skos    : reserved

The STAC virtual-Collection renderer consumes this protocol so new kinds land
as new files under this package without touching the renderer.

Terminology follows ogc-dimensions: both layers use "provider"; the
dimension-backed kind wraps an ogc-dimensions `DimensionProvider`.
"""

from .base import (
    ChildrenPage,
    HierarchyExtent,
    HierarchyNode,
    HierarchyProvider,
)
from .config import HierarchyProviderConfig
from .registry import get_hierarchy_provider, register_provider

# Side-effect imports — register concrete providers.
from . import data_derived  # noqa: F401
from . import dimension_backed  # noqa: F401
from . import static  # noqa: F401

__all__ = [
    "ChildrenPage",
    "HierarchyExtent",
    "HierarchyNode",
    "HierarchyProvider",
    "HierarchyProviderConfig",
    "get_hierarchy_provider",
    "register_provider",
]
