"""Protocol for 'give me 3D bounding boxes for a collection' producers.

Used by VolumesService.get_tileset_json to source the FeatureBounds list
the tile-hierarchy builder consumes. Real implementations:

- Sidecar-backed: queries the geometries sidecar for ST_3DExtent of each
  feature in a collection, optionally filtered by bbox/limit.
- Task-backed: pre-computed tileset emitted by VolumesTilesetBuildTask
  (see Pass 5 spec).
- Fixture: tests inject a list directly.

Phase 5d (this PR) ships the Protocol + an EmptyBoundsSource default so
the HTTP surface keeps working when no real producer is registered.
"""

from __future__ import annotations

from typing import Optional, Protocol, Sequence, runtime_checkable

from dynastore.modules.volumes.bounds import FeatureBounds


@runtime_checkable
class BoundsSourceProtocol(Protocol):
    """Producer of 3D bounding boxes for a collection's features."""

    async def get_bounds(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        limit: Optional[int] = None,
    ) -> Sequence[FeatureBounds]:
        """Return per-feature 3D bboxes. Empty sequence when no data."""
        ...


class EmptyBoundsSource:
    """Default no-op source — returns no features.

    Picked up by VolumesService when no other BoundsSourceProtocol is
    registered. Keeps the /tileset.json endpoint functional (empty
    skeleton) without forcing every test or dev environment to wire a
    real producer.
    """

    async def get_bounds(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        limit: Optional[int] = None,
    ) -> Sequence[FeatureBounds]:
        return []
