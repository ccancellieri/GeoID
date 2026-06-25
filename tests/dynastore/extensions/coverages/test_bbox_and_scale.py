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

"""Unit tests for OGC 19-087 bbox (#7.8), scale-factor (#7.11), and scale-size (#7.12)
helpers added in the coverages conformance truth-up (issue #2320).
"""

import pytest
from fastapi import HTTPException

from dynastore.extensions.coverages.coverages_service import (
    _bbox_to_subset,
    _merge_subset_and_bbox,
    _resolve_scale,
)


# ---------------------------------------------------------------------------
# _bbox_to_subset — OGC 19-087 §7.8 /req/coverage-bbox
# ---------------------------------------------------------------------------


class TestBboxToSubset:
    def test_valid_bbox_produces_lon_lat_axes(self):
        req = _bbox_to_subset("-10,-5,20,45")
        axes = {ar.axis: ar for ar in req.axes}
        assert "Lon" in axes
        assert "Lat" in axes
        assert axes["Lon"].low == -10.0
        assert axes["Lon"].high == 20.0
        assert axes["Lat"].low == -5.0
        assert axes["Lat"].high == 45.0

    def test_too_few_parts_raises_400(self):
        with pytest.raises(HTTPException) as exc:
            _bbox_to_subset("-10,-5,20")
        assert exc.value.status_code == 400

    def test_non_numeric_raises_400(self):
        with pytest.raises(HTTPException) as exc:
            _bbox_to_subset("-10,-5,20,abc")
        assert exc.value.status_code == 400

    def test_min_greater_than_max_raises_400(self):
        with pytest.raises(HTTPException) as exc:
            _bbox_to_subset("20,-5,-10,45")
        assert exc.value.status_code == 400

    def test_point_bbox_allowed(self):
        req = _bbox_to_subset("10,20,10,20")
        assert req.axes[0].low == req.axes[0].high == 10.0


class TestMergeSubsetAndBbox:
    def test_bbox_only_returns_subset_string(self):
        result = _merge_subset_and_bbox(None, "-10,-5,20,45")
        assert result is not None
        assert "Lon(-10.0:20.0)" in result
        assert "Lat(-5.0:45.0)" in result

    def test_subset_only_passes_through(self):
        result = _merge_subset_and_bbox("Time(2024:2025)", None)
        assert result == "Time(2024:2025)"

    def test_none_both_returns_none(self):
        assert _merge_subset_and_bbox(None, None) is None

    def test_subset_and_bbox_no_overlap_merged(self):
        result = _merge_subset_and_bbox("Time(2024:2025)", "0,0,10,10")
        assert result is not None
        assert "Time" in result
        assert "Lon" in result
        assert "Lat" in result

    def test_subset_and_bbox_axis_overlap_raises_400(self):
        # Lon appears in both subset and bbox — must be rejected
        with pytest.raises(HTTPException) as exc:
            _merge_subset_and_bbox("Lon(0:10)", "0,-5,10,45")
        assert exc.value.status_code == 400


# ---------------------------------------------------------------------------
# _resolve_scale — OGC 19-087 §7.11/§7.12 scale-factor / scale-size
# ---------------------------------------------------------------------------


class TestResolveScale:
    def test_no_scale_returns_native(self):
        out_w, out_h = _resolve_scale(
            box_width=100, box_height=50, scale_factor=None, scale_size=None
        )
        assert out_w == 100
        assert out_h == 50

    def test_scale_factor_halves_dimensions(self):
        out_w, out_h = _resolve_scale(
            box_width=100, box_height=50, scale_factor=0.5, scale_size=None
        )
        assert out_w == 50
        assert out_h == 25

    def test_scale_factor_doubles_dimensions(self):
        out_w, out_h = _resolve_scale(
            box_width=10, box_height=8, scale_factor=2.0, scale_size=None
        )
        assert out_w == 20
        assert out_h == 16

    def test_scale_factor_minimum_is_one(self):
        out_w, out_h = _resolve_scale(
            box_width=1, box_height=1, scale_factor=0.001, scale_size=None
        )
        assert out_w >= 1
        assert out_h >= 1

    def test_scale_factor_zero_raises_400(self):
        with pytest.raises(HTTPException) as exc:
            _resolve_scale(box_width=100, box_height=50, scale_factor=0.0, scale_size=None)
        assert exc.value.status_code == 400

    def test_scale_factor_negative_raises_400(self):
        with pytest.raises(HTTPException) as exc:
            _resolve_scale(box_width=100, box_height=50, scale_factor=-1.0, scale_size=None)
        assert exc.value.status_code == 400

    def test_scale_size_lon_only(self):
        out_w, out_h = _resolve_scale(
            box_width=200, box_height=100, scale_factor=None, scale_size="Lon(64)"
        )
        assert out_w == 64
        assert out_h == 100  # unchanged

    def test_scale_size_lat_only(self):
        out_w, out_h = _resolve_scale(
            box_width=200, box_height=100, scale_factor=None, scale_size="Lat(32)"
        )
        assert out_w == 200  # unchanged
        assert out_h == 32

    def test_scale_size_both_axes(self):
        out_w, out_h = _resolve_scale(
            box_width=200, box_height=100, scale_factor=None, scale_size="Lon(64),Lat(32)"
        )
        assert out_w == 64
        assert out_h == 32

    def test_scale_size_x_alias(self):
        out_w, out_h = _resolve_scale(
            box_width=200, box_height=100, scale_factor=None, scale_size="x(50)"
        )
        assert out_w == 50

    def test_scale_size_unknown_axis_raises_400(self):
        with pytest.raises(HTTPException) as exc:
            _resolve_scale(
                box_width=200, box_height=100, scale_factor=None, scale_size="Time(10)"
            )
        assert exc.value.status_code == 400

    def test_scale_size_malformed_raises_400(self):
        with pytest.raises(HTTPException) as exc:
            _resolve_scale(
                box_width=200, box_height=100, scale_factor=None, scale_size="Lon64"
            )
        assert exc.value.status_code == 400

    def test_both_scale_params_raises_400(self):
        with pytest.raises(HTTPException) as exc:
            _resolve_scale(
                box_width=100, box_height=50,
                scale_factor=0.5, scale_size="Lon(50)",
            )
        assert exc.value.status_code == 400
