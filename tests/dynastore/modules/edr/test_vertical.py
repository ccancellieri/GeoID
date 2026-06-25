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
#    Company: FAO, Vile delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import pytest
from dynastore.modules.edr.vertical import parse_z_param, select_bands_by_z


def test_parse_z_param_empty():
    low, high = parse_z_param(None)
    assert low is None
    assert high is None

    low, high = parse_z_param("")
    assert low is None
    assert high is None


def test_parse_z_param_single_level():
    low, high = parse_z_param("100")
    assert low == pytest.approx(100.0)
    assert high == pytest.approx(100.0)


def test_parse_z_param_range():
    low, high = parse_z_param("100:200")
    assert low == pytest.approx(100.0)
    assert high == pytest.approx(200.0)


def test_parse_z_param_open_range():
    low, high = parse_z_param("100:")
    assert low == pytest.approx(100.0)
    assert high is None

    low, high = parse_z_param(":200")
    assert low is None
    assert high == pytest.approx(200.0)


def test_parse_z_param_negative():
    low, high = parse_z_param("-50")
    assert low == pytest.approx(-50.0)
    assert high == pytest.approx(-50.0)


def test_parse_z_param_invalid():
    with pytest.raises(ValueError, match="Invalid z value"):
        parse_z_param("abc")


def test_select_bands_by_z_no_filter():
    bands = [{"name": "band1"}, {"name": "band2"}]
    result = select_bands_by_z(bands, None, None)
    assert result == [1, 2]


def test_select_bands_by_z_empty_bands():
    result = select_bands_by_z([], None, None)
    assert result == [1]

    result = select_bands_by_z([], 1.0, 1.0)
    assert result == [1]


def test_select_bands_by_z_with_vertical_metadata():
    bands = [
        {"name": "surface", "vertical": {"value": 0}},
        {"name": "level1", "vertical": {"value": 100}},
        {"name": "level2", "vertical": {"value": 200}},
        {"name": "level3", "vertical": {"value": 300}},
    ]

    result = select_bands_by_z(bands, 100, 100)
    assert result == [2]

    result = select_bands_by_z(bands, 150, 250)
    assert result == [3]

    result = select_bands_by_z(bands, None, 150)
    assert result == [1, 2]

    result = select_bands_by_z(bands, 200, None)
    assert result == [3, 4]


def test_select_bands_by_z_fallback_to_band_index():
    bands = [{"name": "band1"}, {"name": "band2"}, {"name": "band3"}]

    result = select_bands_by_z(bands, 2.0, 2.0)
    assert result == [2]

    result = select_bands_by_z(bands, 1.0, 2.0)
    assert result == [1, 2, 3]


def test_select_bands_by_z_no_match_fallback():
    bands = [
        {"name": "surface", "vertical": {"value": 0}},
        {"name": "level1", "vertical": {"value": 100}},
    ]

    result = select_bands_by_z(bands, 500, 600)
    assert result == [1, 2]
