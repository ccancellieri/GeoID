#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Compute presets: named bundles of :class:`ComputedField` derivations.

A collection can request a common bundle of computed fields with a short
string (e.g. ``"geometry_stats"``) instead of enumerating every
:class:`ComputedField`. Preset factories generally leave ``storage_mode``
unset (compute-only; a field is persisted only when its entry sets a
store). The one deliberate exception is ``"geometry_density"``, whose
whole point is the physical shape: a filterable ``vertex_count`` column.

Presets are extensible via :func:`register_compute_preset`. Use
:func:`resolve_compute` to expand a spec (a preset name, a heterogeneous
list, or a list of :class:`ComputedField`) into a deduplicated list.
"""

from typing import Callable, List, Union

from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    StatisticStorageMode,
)

# Default resolutions for spatial-cell presets. ~150 m geohash-7 / h3-7
# cells and s2-13 (~1 km) are coarse enough for collection-wide indexing
# while staying inside each scheme's valid range.
_DEFAULT_GEOHASH_RES = 7
_DEFAULT_H3_RES = 7
_DEFAULT_S2_RES = 13

ComputeSpec = Union[str, List[Union[str, ComputedField, dict]]]

_REGISTRY: dict[str, Callable[[], List[ComputedField]]] = {}


def register_compute_preset(
    name: str, factory: Callable[[], List[ComputedField]]
) -> None:
    """Register (or replace) a named preset factory."""
    _REGISTRY[name] = factory


def list_compute_presets() -> List[str]:
    """Return the registered preset names."""
    return list(_REGISTRY)


def _geometry_stats() -> List[ComputedField]:
    return [
        ComputedField(kind=ComputedKind.AREA),
        ComputedField(kind=ComputedKind.PERIMETER),
        ComputedField(kind=ComputedKind.LENGTH),
        ComputedField(kind=ComputedKind.CENTROID),
        ComputedField(kind=ComputedKind.BBOX),
    ]


def _geometry_full() -> List[ComputedField]:
    return _geometry_stats() + [
        ComputedField(kind=ComputedKind.VERTEX_COUNT),
        ComputedField(kind=ComputedKind.HOLE_COUNT),
        ComputedField(kind=ComputedKind.CIRCULARITY),
        ComputedField(kind=ComputedKind.CONVEXITY),
        ComputedField(kind=ComputedKind.ASPECT_RATIO),
    ]


def _geometry_density() -> List[ComputedField]:
    """A filterable per-feature density signal for the tile render path.

    ``vertex_count`` as its own indexed INTEGER column — the shape
    ``TilesConfig.feature_density_column`` needs (the WHERE-clause ceiling
    can only reference a real column; a JSONB key is not indexable here).
    Compose with the compute-only bundles to add the column on top:
    ``compute: ["geometry_full", "geometry_density"]`` — last-wins dedupe
    upgrades the ``vertex_count`` entry in place.
    """
    return [
        ComputedField(
            kind=ComputedKind.VERTEX_COUNT,
            storage_mode=StatisticStorageMode.COLUMNAR,
            indexed=True,
        ),
    ]


def _spatial_cells() -> List[ComputedField]:
    return [
        ComputedField(kind=ComputedKind.GEOHASH, resolution=_DEFAULT_GEOHASH_RES),
        ComputedField(kind=ComputedKind.H3, resolution=_DEFAULT_H3_RES),
        ComputedField(kind=ComputedKind.S2, resolution=_DEFAULT_S2_RES),
    ]


def _place_3d() -> List[ComputedField]:
    return [
        ComputedField(kind=ComputedKind.SURFACE_AREA),
        ComputedField(kind=ComputedKind.Z_RANGE),
        ComputedField(kind=ComputedKind.CENTROID_3D),
    ]


def _all() -> List[ComputedField]:
    return _geometry_full() + _spatial_cells()


register_compute_preset("none", lambda: [])
register_compute_preset("geometry_stats", _geometry_stats)
register_compute_preset("geometry_full", _geometry_full)
register_compute_preset("geometry_density", _geometry_density)
register_compute_preset("spatial_cells", _spatial_cells)
register_compute_preset("place_3d", _place_3d)
register_compute_preset("all", _all)


def _expand_name(name: str) -> List[ComputedField]:
    factory = _REGISTRY.get(name)
    if factory is None:
        raise ValueError(f"unknown compute preset: {name!r}")
    return factory()


def resolve_compute(spec: ComputeSpec) -> List[ComputedField]:
    """Expand a compute spec into a deduplicated list of computed fields.

    ``spec`` may be a preset-name string, or a list whose elements are
    each a preset-name string, a :class:`ComputedField`, or a dict
    (validated via :meth:`ComputedField.model_validate`). The result is
    deduplicated by :attr:`ComputedField.resolved_name`, keeping the LAST
    occurrence so an explicit field overrides a preset-supplied one.
    Order is otherwise preserved.
    """
    collected: List[ComputedField] = []
    if isinstance(spec, str):
        collected.extend(_expand_name(spec))
    else:
        for element in spec:
            if isinstance(element, str):
                collected.extend(_expand_name(element))
            elif isinstance(element, ComputedField):
                collected.append(element)
            else:
                collected.append(ComputedField.model_validate(element))

    by_name: dict[str, ComputedField] = {}
    for field in collected:
        by_name[field.resolved_name] = field
    return list(by_name.values())


__all__ = [
    "register_compute_preset",
    "list_compute_presets",
    "resolve_compute",
]
