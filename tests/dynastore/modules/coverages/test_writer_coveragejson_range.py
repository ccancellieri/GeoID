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

"""Tests for CoverageJSON range correctness (OGC 19-087 §7.5 /req/coveragejson).

Validates that:
- shape reflects actual pixel dimensions (not the old [2,2] sentinel)
- axis num reflects actual grid size
- values are non-empty when data is provided
- empty iterator yields shape [0,0] and empty values (graceful empty response)
"""

import json
import pytest

from dynastore.modules.coverages.writers.coveragejson import write_coveragejson


_DOMAINSET = {
    "type": "DomainSet",
    "generalGrid": {
        "srsName": "OGC:CRS84",
        "axisLabels": ["Lon", "Lat"],
        "axis": [
            {"type": "RegularAxis", "axisLabel": "Lon", "lowerBound": 0.0, "upperBound": 4.0, "uomLabel": "degree"},
            {"type": "RegularAxis", "axisLabel": "Lat", "lowerBound": 0.0, "upperBound": 3.0, "uomLabel": "degree"},
        ],
    },
}

_RANGETYPE_B1 = {
    "type": "DataRecord",
    "field": [{"name": "b1", "definition": "float32"}],
}

_RANGETYPE_TWO_BANDS = {
    "type": "DataRecord",
    "field": [
        {"name": "band1", "definition": "float32"},
        {"name": "band2", "definition": "float32"},
    ],
}


def _parse(chunks):
    return json.loads(b"".join(chunks).decode())


class TestCovJSONRangeShape:
    def test_4x3_grid_shape_reflected(self):
        # 3 rows, 4 cols
        band = [[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0], [9.0, 10.0, 11.0, 12.0]]
        doc = _parse(write_coveragejson(_DOMAINSET, _RANGETYPE_B1, iter([band])))
        assert doc["ranges"]["b1"]["shape"] == [3, 4]
        assert doc["ranges"]["b1"]["values"] == list(range(1, 13))

    def test_axis_num_matches_actual_pixels(self):
        band = [[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]]
        doc = _parse(write_coveragejson(_DOMAINSET, _RANGETYPE_B1, iter([band])))
        # 2 rows, 4 cols
        assert doc["domain"]["axes"]["y"]["num"] == 2
        assert doc["domain"]["axes"]["x"]["num"] == 4

    def test_empty_iterator_yields_zero_shape(self):
        doc = _parse(write_coveragejson(_DOMAINSET, _RANGETYPE_B1, iter([])))
        assert doc["ranges"]["b1"]["shape"] == [0, 0]
        assert doc["ranges"]["b1"]["values"] == []

    def test_two_bands_populated(self):
        band1 = [[1.0, 2.0], [3.0, 4.0]]
        band2 = [[10.0, 20.0], [30.0, 40.0]]
        doc = _parse(write_coveragejson(_DOMAINSET, _RANGETYPE_TWO_BANDS, iter([band1, band2])))
        assert doc["ranges"]["band1"]["values"] == [1.0, 2.0, 3.0, 4.0]
        assert doc["ranges"]["band2"]["values"] == [10.0, 20.0, 30.0, 40.0]
        assert doc["ranges"]["band1"]["shape"] == [2, 2]

    def test_extra_iterator_bands_are_dropped(self):
        # Only one field in rangetype — extra band ignored
        band1 = [[1.0, 2.0], [3.0, 4.0]]
        band2 = [[9.0, 9.0], [9.0, 9.0]]
        doc = _parse(write_coveragejson(_DOMAINSET, _RANGETYPE_B1, iter([band1, band2])))
        assert "b1" in doc["ranges"]
        # No spurious keys from extra band
        assert len(doc["ranges"]) == 1

    def test_no_internal_fields_on_wire(self):
        band = [[5.0, 6.0], [7.0, 8.0]]
        doc = _parse(write_coveragejson(_DOMAINSET, _RANGETYPE_B1, iter([band])))
        for key in ("_total_count", "_driver", "_sidecar", "canonical_envelope"):
            assert key not in doc, f"Internal field {key!r} leaked onto wire response"

    def test_backward_compat_2x2_data(self):
        # Existing test shape: 2x2 data must still produce shape [2,2] not [2,2] sentinel
        band = [[1.0, 2.0], [3.0, 4.0]]
        doc = _parse(write_coveragejson(_DOMAINSET, _RANGETYPE_B1, iter([band])))
        assert doc["type"] == "Coverage"
        assert doc["ranges"]["b1"]["shape"] == [2, 2]
        assert doc["ranges"]["b1"]["values"] == [1.0, 2.0, 3.0, 4.0]

    def test_empty_rangetype_produces_valid_doc(self):
        rt = {"type": "DataRecord", "field": []}
        doc = _parse(write_coveragejson(_DOMAINSET, rt, iter([])))
        assert doc["type"] == "Coverage"
        assert doc["ranges"] == {}


class TestCovJSONScaleIntegration:
    """Integration: read_scaled produces correct data that flows through write_coveragejson."""

    @pytest.mark.skipif(
        __import__("importlib").util.find_spec("rasterio") is None,
        reason="rasterio not installed",
    )
    def test_read_scaled_feeds_coveragejson(self, tmp_path):
        import numpy as np
        import rasterio
        from rasterio.transform import from_origin
        from dynastore.modules.coverages.reader import read_scaled
        from dynastore.modules.coverages.window import RasterGeoRef, resolve_window
        from dynastore.modules.coverages.subset import SubsetRequest

        path = tmp_path / "sample.tif"
        data = np.arange(16, dtype="float32").reshape(4, 4)
        with rasterio.open(
            path, "w", driver="GTiff", width=4, height=4, count=1,
            dtype="float32", transform=from_origin(0, 4, 1, 1),
        ) as dst:
            dst.write(data, 1)

        ref = RasterGeoRef(
            width=4, height=4,
            origin_x=0.0, origin_y=4.0,
            pixel_x=1.0, pixel_y=-1.0,
            crs="EPSG:4326",
            axis_order=("Lon", "Lat"),
        )
        box = resolve_window(SubsetRequest(), ref)
        with rasterio.open(path) as ds:
            arr = read_scaled(ds, box)

        assert arr.shape == (4, 4)

        rt = {"type": "DataRecord", "field": [{"name": "val", "definition": "float32"}]}
        doc = _parse(write_coveragejson(_DOMAINSET, rt, iter([arr.tolist()])))
        assert doc["ranges"]["val"]["shape"] == [4, 4]
        assert len(doc["ranges"]["val"]["values"]) == 16
