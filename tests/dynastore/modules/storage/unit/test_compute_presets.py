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

import pytest

from dynastore.modules.storage.compute_presets import (
    list_compute_presets,
    resolve_compute,
)
from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
)


def _kinds(fields):
    return {cf.kind for cf in fields}


def test_none_expands_empty():
    assert resolve_compute("none") == []


def test_geometry_stats_kinds():
    assert _kinds(resolve_compute("geometry_stats")) == {
        ComputedKind.AREA,
        ComputedKind.PERIMETER,
        ComputedKind.LENGTH,
        ComputedKind.CENTROID,
        ComputedKind.BBOX,
    }


def test_geometry_full_kinds():
    assert _kinds(resolve_compute("geometry_full")) == {
        ComputedKind.AREA,
        ComputedKind.PERIMETER,
        ComputedKind.LENGTH,
        ComputedKind.CENTROID,
        ComputedKind.BBOX,
        ComputedKind.VERTEX_COUNT,
        ComputedKind.HOLE_COUNT,
        ComputedKind.CIRCULARITY,
        ComputedKind.CONVEXITY,
        ComputedKind.ASPECT_RATIO,
    }


def test_spatial_cells_kinds_and_resolutions():
    fields = resolve_compute("spatial_cells")
    assert _kinds(fields) == {
        ComputedKind.GEOHASH,
        ComputedKind.H3,
        ComputedKind.S2,
    }
    by_kind = {cf.kind: cf.resolution for cf in fields}
    assert by_kind[ComputedKind.GEOHASH] == 7
    assert by_kind[ComputedKind.H3] == 7
    assert by_kind[ComputedKind.S2] == 13


def test_place_3d_kinds():
    assert _kinds(resolve_compute("place_3d")) == {
        ComputedKind.SURFACE_AREA,
        ComputedKind.Z_RANGE,
        ComputedKind.CENTROID_3D,
    }


def test_all_kinds():
    assert _kinds(resolve_compute("all")) == {
        ComputedKind.AREA,
        ComputedKind.PERIMETER,
        ComputedKind.LENGTH,
        ComputedKind.CENTROID,
        ComputedKind.BBOX,
        ComputedKind.VERTEX_COUNT,
        ComputedKind.HOLE_COUNT,
        ComputedKind.CIRCULARITY,
        ComputedKind.CONVEXITY,
        ComputedKind.ASPECT_RATIO,
        ComputedKind.GEOHASH,
        ComputedKind.H3,
        ComputedKind.S2,
    }


def test_presets_set_no_storage_mode():
    for preset in ("geometry_stats", "geometry_full", "spatial_cells", "place_3d", "all"):
        for cf in resolve_compute(preset):
            assert cf.storage_mode is None


def test_unknown_preset_raises():
    with pytest.raises(ValueError, match="unknown compute preset"):
        resolve_compute("does_not_exist")


def test_unknown_preset_in_list_raises():
    with pytest.raises(ValueError, match="unknown compute preset"):
        resolve_compute(["geometry_stats", "nope"])


def test_mixed_list_explicit_overrides_preset_h3():
    fields = resolve_compute(["spatial_cells", {"kind": "h3", "resolution": 9}])
    by_name = {cf.resolved_name: cf for cf in fields}
    # Preset H3 is res 7 -> resolved_name "h3_7"; explicit H3 is res 9 ->
    # "h3_9". Distinct names mean both survive dedupe.
    assert by_name["h3_7"].resolution == 7
    assert by_name["h3_9"].resolution == 9


def test_dedupe_keeps_last_occurrence():
    explicit = ComputedField(kind=ComputedKind.CENTROID, name="my_centroid")
    # Two CENTROID fields with the same resolved_name; the explicit one
    # appended last must win.
    fields = resolve_compute(
        [
            ComputedField(kind=ComputedKind.CENTROID, name="my_centroid"),
            explicit,
        ]
    )
    matches = [cf for cf in fields if cf.resolved_name == "my_centroid"]
    assert len(matches) == 1
    assert matches[0] is explicit


def test_explicit_centroid_overrides_geometry_stats():
    explicit = ComputedField(kind=ComputedKind.CENTROID, centroid_type="POINTZ")
    fields = resolve_compute(["geometry_stats", explicit])
    matches = [cf for cf in fields if cf.kind == ComputedKind.CENTROID]
    assert len(matches) == 1
    assert matches[0].centroid_type == "POINTZ"


def test_list_includes_builtins():
    presets = list_compute_presets()
    for name in ("none", "geometry_stats", "geometry_full", "spatial_cells", "place_3d", "all"):
        assert name in presets


def test_list_of_computed_fields_passthrough():
    fields = [
        ComputedField(kind=ComputedKind.AREA),
        ComputedField(kind=ComputedKind.BBOX),
    ]
    assert _kinds(resolve_compute(fields)) == {ComputedKind.AREA, ComputedKind.BBOX}
