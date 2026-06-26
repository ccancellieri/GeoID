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
from dynastore.modules.edr.crs import (
    parse_crs_param,
    validate_crs,
    DEFAULT_OUTPUT_CRS,
)


def test_parse_crs_param_empty():
    assert parse_crs_param(None) is None
    assert parse_crs_param("") is None


def test_parse_crs_param_epsg():
    result = parse_crs_param("EPSG:4326")
    assert result == "http://www.opengis.net/def/crs/EPSG/0/4326"

    result = parse_crs_param("epsg:3857")
    assert result == "http://www.opengis.net/def/crs/EPSG/0/3857"


def test_parse_crs_param_ogc_crs():
    result = parse_crs_param("CRS84")
    assert result == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"

    result = parse_crs_param("crs84")
    assert result == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


def test_parse_crs_param_uri():
    uri = "http://www.opengis.net/def/crs/EPSG/0/4326"
    assert parse_crs_param(uri) == uri

    uri = "https://example.com/crs/custom"
    assert parse_crs_param(uri) == uri


def test_parse_crs_param_authority_code():
    result = parse_crs_param("ESRI:54009")
    assert result == "http://www.opengis.net/def/crs/ESRI/0/54009"


def test_validate_crs_default():
    assert validate_crs(DEFAULT_OUTPUT_CRS) is True
    assert validate_crs("http://www.opengis.net/def/crs/OGC/1.3/CRS84") is True


def test_validate_crs_epsg():
    assert validate_crs("http://www.opengis.net/def/crs/EPSG/0/4326") is True
    assert validate_crs("http://www.opengis.net/def/crs/EPSG/0/3857") is True


def test_validate_crs_invalid_epsg():
    with pytest.raises(ValueError, match="Invalid EPSG code"):
        validate_crs("http://www.opengis.net/def/crs/EPSG/0/999999999")


def test_validate_crs_bogus_uri():
    with pytest.raises(ValueError, match="Unrecognised CRS URI"):
        validate_crs("http://example.com/crs/totally-made-up")


@pytest.mark.skipif(
    not pytest.importorskip("pyproj", reason="pyproj not installed"),
    reason="pyproj not installed"
)
def test_transform_point():
    from dynastore.modules.edr.crs import transform_point

    lon, lat = transform_point(0.0, 0.0, "EPSG:4326", "EPSG:4326")
    assert lon == pytest.approx(0.0)
    assert lat == pytest.approx(0.0)

    lon, lat = transform_point(10.0, 45.0, "EPSG:4326", "http://www.opengis.net/def/crs/EPSG/0/3857")
    assert lon != pytest.approx(10.0)
    assert lat != pytest.approx(45.0)
