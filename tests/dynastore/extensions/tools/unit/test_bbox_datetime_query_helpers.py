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

"""Unit tests for the shared ``bbox=``/``datetime=`` parsing helpers (#3295).

``parse_bbox_query_param``/``bbox_filter_condition``/``datetime_filter_conditions``
are shared by the OGC Features ``/items`` endpoint (via ``parse_ogc_query_request``)
and the STAC collection ``/items`` endpoint. These tests pin their standalone
behaviour; the Features regression (byte-identical ``parse_ogc_query_request``
output after the refactor) is covered by
``test_ogc_query_request_attr_filter.py``.
"""

import pytest
from fastapi import HTTPException

from dynastore.extensions.tools.query import (
    bbox_filter_condition,
    datetime_filter_conditions,
    parse_bbox_query_param,
)


def test_parse_bbox_query_param_returns_xmin_ymin_xmax_ymax():
    assert parse_bbox_query_param("1,2,3,4") == (1.0, 2.0, 3.0, 4.0)


def test_parse_bbox_query_param_rejects_wrong_arity():
    with pytest.raises(HTTPException) as excinfo:
        parse_bbox_query_param("1,2,3")
    assert excinfo.value.status_code == 400


def test_parse_bbox_query_param_rejects_non_numeric():
    with pytest.raises(HTTPException) as excinfo:
        parse_bbox_query_param("a,b,c,d")
    assert excinfo.value.status_code == 400


def test_bbox_filter_condition_shape():
    fc = bbox_filter_condition((1.0, 2.0, 3.0, 4.0))
    assert fc.field == "geom"
    assert fc.operator == "&&"
    assert fc.spatial_op is True
    assert fc.value.startswith("SRID=4326;POLYGON((")
    assert "1.0 2.0" in fc.value
    assert "3.0 4.0" in fc.value


def test_bbox_filter_condition_honours_srid():
    fc = bbox_filter_condition((1.0, 2.0, 3.0, 4.0), srid=3857)
    assert fc.value.startswith("SRID=3857;")


def test_datetime_filter_conditions_instant():
    conditions = datetime_filter_conditions("2020-01-01T00:00:00Z")
    assert len(conditions) == 1
    assert conditions[0].field == "validity"
    assert conditions[0].operator == "@>"


def test_datetime_filter_conditions_closed_interval():
    conditions = datetime_filter_conditions(
        "2020-01-01T00:00:00Z/2020-12-31T00:00:00Z"
    )
    assert len(conditions) == 1
    assert conditions[0].operator == "&&"
    assert conditions[0].value.startswith("[")


def test_datetime_filter_conditions_open_start():
    conditions = datetime_filter_conditions("../2020-12-31T00:00:00Z")
    assert len(conditions) == 1
    assert conditions[0].operator == "@>"


def test_datetime_filter_conditions_open_end():
    conditions = datetime_filter_conditions("2020-01-01T00:00:00Z/..")
    assert len(conditions) == 1
    assert conditions[0].operator == "@>"


def test_datetime_filter_conditions_rejects_malformed_value():
    with pytest.raises(HTTPException) as excinfo:
        datetime_filter_conditions("not-a-date")
    assert excinfo.value.status_code == 400
