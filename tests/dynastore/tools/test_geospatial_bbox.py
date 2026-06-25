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

"""Tests for the consolidated parse_bbox_string function in tools/geospatial.py."""

import pytest

from dynastore.tools.geospatial import parse_bbox_string, BboxDimensionality


class TestParseBboxStrict2D:
    """Tests for STRICT_2D dimensionality."""

    def test_valid_4_values(self):
        result = parse_bbox_string(
            "10.0,20.0,30.0,40.0",
            dimensionality=BboxDimensionality.STRICT_2D,
        )
        assert result == (10.0, 20.0, 30.0, 40.0)

    def test_with_spaces(self):
        result = parse_bbox_string(
            "0, 0, 1, 1",
            dimensionality=BboxDimensionality.STRICT_2D,
        )
        assert result == (0.0, 0.0, 1.0, 1.0)

    def test_none_input_allowed(self):
        result = parse_bbox_string(
            None,
            dimensionality=BboxDimensionality.STRICT_2D,
            allow_none=True,
        )
        assert result is None

    def test_empty_string_allowed(self):
        result = parse_bbox_string(
            "",
            dimensionality=BboxDimensionality.STRICT_2D,
            allow_none=True,
        )
        assert result is None

    def test_none_input_not_allowed(self):
        with pytest.raises(ValueError, match="cannot be empty or None"):
            parse_bbox_string(
                None,
                dimensionality=BboxDimensionality.STRICT_2D,
                allow_none=False,
            )

    def test_invalid_count_too_few(self):
        with pytest.raises(ValueError, match="must have exactly 4"):
            parse_bbox_string(
                "10,20,30",
                dimensionality=BboxDimensionality.STRICT_2D,
            )

    def test_invalid_count_too_many(self):
        with pytest.raises(ValueError, match="must have exactly 4"):
            parse_bbox_string(
                "10,20,30,40,50",
                dimensionality=BboxDimensionality.STRICT_2D,
            )

    def test_non_numeric(self):
        with pytest.raises(ValueError, match="must be numeric"):
            parse_bbox_string(
                "a,b,c,d",
                dimensionality=BboxDimensionality.STRICT_2D,
            )

    def test_degenerate_xmin_xmax(self):
        with pytest.raises(ValueError, match="degenerate.*xmin"):
            parse_bbox_string(
                "10,20,5,40",
                dimensionality=BboxDimensionality.STRICT_2D,
            )

    def test_degenerate_ymin_ymax(self):
        with pytest.raises(ValueError, match="degenerate.*ymin"):
            parse_bbox_string(
                "10,20,30,15",
                dimensionality=BboxDimensionality.STRICT_2D,
            )

    def test_no_validation(self):
        result = parse_bbox_string(
            "10,20,5,40",
            dimensionality=BboxDimensionality.STRICT_2D,
            validate_geometry=False,
        )
        assert result == (10.0, 20.0, 5.0, 40.0)


class TestParseBboxAllowExtraDims:
    """Tests for ALLOW_EXTRA_DIMS dimensionality."""

    def test_valid_4_values(self):
        result = parse_bbox_string(
            "10.0,20.0,30.0,40.0",
            dimensionality=BboxDimensionality.ALLOW_EXTRA_DIMS,
        )
        assert result == (10.0, 20.0, 30.0, 40.0)

    def test_6_values_uses_first_4(self):
        result = parse_bbox_string(
            "0,0,1,1,100,200",
            dimensionality=BboxDimensionality.ALLOW_EXTRA_DIMS,
        )
        assert result == (0.0, 0.0, 1.0, 1.0)

    def test_too_few_values(self):
        with pytest.raises(ValueError, match="must have at least 4"):
            parse_bbox_string(
                "0,0,1",
                dimensionality=BboxDimensionality.ALLOW_EXTRA_DIMS,
            )

    def test_degenerate_still_checked(self):
        with pytest.raises(ValueError, match="degenerate"):
            parse_bbox_string(
                "10,20,5,40,100,200",
                dimensionality=BboxDimensionality.ALLOW_EXTRA_DIMS,
            )


class TestParseBboxOptional3D:
    """Tests for OPTIONAL_3D dimensionality."""

    def test_valid_4_values_2d(self):
        result = parse_bbox_string(
            "1.0,2.0,3.0,4.0",
            dimensionality=BboxDimensionality.OPTIONAL_3D,
        )
        assert result == (1.0, 2.0, None, 3.0, 4.0, None)

    def test_valid_6_values_3d(self):
        result = parse_bbox_string(
            "1.0,2.0,0.0,3.0,4.0,10.0",
            dimensionality=BboxDimensionality.OPTIONAL_3D,
        )
        assert result == (1.0, 2.0, 0.0, 3.0, 4.0, 10.0)

    def test_invalid_count_5(self):
        with pytest.raises(ValueError, match="must have 4 or 6"):
            parse_bbox_string(
                "1.0,2.0,3.0,4.0,5.0",
                dimensionality=BboxDimensionality.OPTIONAL_3D,
            )

    def test_invalid_count_7(self):
        with pytest.raises(ValueError, match="must have 4 or 6"):
            parse_bbox_string(
                "1.0,2.0,3.0,4.0,5.0,6.0,7.0",
                dimensionality=BboxDimensionality.OPTIONAL_3D,
            )

    def test_non_numeric_3d(self):
        with pytest.raises(ValueError, match="must be numeric"):
            parse_bbox_string(
                "1.0,2.0,a,3.0,4.0,10.0",
                dimensionality=BboxDimensionality.OPTIONAL_3D,
            )

    def test_degenerate_2d_x(self):
        with pytest.raises(ValueError, match="degenerate.*xmin"):
            parse_bbox_string(
                "5.0,2.0,1.0,4.0",
                dimensionality=BboxDimensionality.OPTIONAL_3D,
            )

    def test_degenerate_3d_z(self):
        with pytest.raises(ValueError, match="degenerate.*zmin"):
            parse_bbox_string(
                "1.0,2.0,10.0,3.0,4.0,5.0",
                dimensionality=BboxDimensionality.OPTIONAL_3D,
            )

    def test_valid_3d_no_z_validation(self):
        result = parse_bbox_string(
            "1.0,2.0,10.0,3.0,4.0,5.0",
            dimensionality=BboxDimensionality.OPTIONAL_3D,
            validate_geometry=False,
        )
        assert result == (1.0, 2.0, 10.0, 3.0, 4.0, 5.0)
