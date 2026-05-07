"""3D bounding boxes for feature tiling — pure math, no I/O.

FeatureBounds carries a feature's id + its 3D extent in the tileset's
reference frame. Coordinate axes are assumed pre-projected into the
frame the tileset will publish (e.g. EPSG:4978 ECEF for Cesium 3D Tiles
Region — caller's responsibility).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List


@dataclass(frozen=True)
class FeatureBounds:
    """Axis-aligned 3D bbox of a single feature."""
    feature_id: str
    min_x: float
    min_y: float
    min_z: float
    max_x: float
    max_y: float
    max_z: float

    def __post_init__(self) -> None:
        if self.min_x > self.max_x or self.min_y > self.max_y or self.min_z > self.max_z:
            raise ValueError(
                f"FeatureBounds {self.feature_id!r}: min must be <= max on every axis; "
                f"got x=[{self.min_x},{self.max_x}] y=[{self.min_y},{self.max_y}] z=[{self.min_z},{self.max_z}]"
            )

    def volume(self) -> float:
        return (self.max_x - self.min_x) * (self.max_y - self.min_y) * (self.max_z - self.min_z)

    def center(self) -> tuple[float, float, float]:
        return (
            (self.min_x + self.max_x) / 2.0,
            (self.min_y + self.max_y) / 2.0,
            (self.min_z + self.max_z) / 2.0,
        )


def merge_bounds(items: Iterable[FeatureBounds]) -> FeatureBounds:
    """Return the axis-aligned bbox enclosing every input bbox.

    Raises ValueError on empty input.
    """
    lst: List[FeatureBounds] = list(items)
    if not lst:
        raise ValueError("merge_bounds requires at least one FeatureBounds")
    return FeatureBounds(
        feature_id="__merged__",
        min_x=min(b.min_x for b in lst),
        min_y=min(b.min_y for b in lst),
        min_z=min(b.min_z for b in lst),
        max_x=max(b.max_x for b in lst),
        max_y=max(b.max_y for b in lst),
        max_z=max(b.max_z for b in lst),
    )
