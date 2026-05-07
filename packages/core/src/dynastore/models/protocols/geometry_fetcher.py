"""Protocol for fetching feature geometries from a collection store.

Used by the tile-content pipeline (b3dm / glb) to retrieve polygon WKB
and height attributes for a specific set of feature IDs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, Sequence, runtime_checkable


@dataclass(frozen=True)
class FeatureGeometry:
    """Raw geometry + height attribute for one feature."""

    feature_id: str
    geom_wkb: bytes
    height: float = 0.0


@runtime_checkable
class GeometryFetcherProtocol(Protocol):
    """Fetch WKB geometries + height attrs for an explicit feature-ID list."""

    async def get_geometries(
        self,
        catalog_id: str,
        collection_id: str,
        feature_ids: Sequence[str],
    ) -> Sequence[FeatureGeometry]:
        """Return geometries for *feature_ids*.

        Missing or null-geometry features are silently omitted.
        """
        ...
