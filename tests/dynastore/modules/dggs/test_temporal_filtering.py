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

"""Unit tests for DGGS temporal filtering (datetime interval parsing)."""

import pytest
from datetime import datetime

from dynastore.modules.dggs.zone_query import _parse_datetime_filters
from dynastore.models.query_builder import FilterOperator


def test_parse_datetime_instant():
    """Test instant datetime (single value without /)."""
    filters = _parse_datetime_filters("2024-01-01T00:00:00Z")
    
    assert len(filters) == 1
    assert filters[0].field == "datetime"
    assert filters[0].operator == FilterOperator.EQ
    assert filters[0].value == "2024-01-01T00:00:00Z"


def test_parse_datetime_closed_interval():
    """Test closed interval (start/end)."""
    filters = _parse_datetime_filters("2024-01-01T00:00:00Z/2024-12-31T23:59:59Z")
    
    assert len(filters) == 2
    
    assert filters[0].field == "datetime"
    assert filters[0].operator == FilterOperator.GTE
    
    assert filters[1].field == "datetime"
    assert filters[1].operator == FilterOperator.LTE
    
    assert isinstance(filters[0].value, datetime)
    assert isinstance(filters[1].value, datetime)


def test_parse_datetime_open_end():
    """Test open end interval (start/..)."""
    filters = _parse_datetime_filters("2024-01-01T00:00:00Z/..")
    
    assert len(filters) == 1
    assert filters[0].field == "datetime"
    assert filters[0].operator == FilterOperator.GTE
    assert isinstance(filters[0].value, datetime)


def test_parse_datetime_open_start():
    """Test open start interval (../end)."""
    filters = _parse_datetime_filters("../2024-12-31T23:59:59Z")
    
    assert len(filters) == 1
    assert filters[0].field == "datetime"
    assert filters[0].operator == FilterOperator.LTE
    assert isinstance(filters[0].value, datetime)


def test_parse_datetime_closed_interval_simple():
    """Test closed interval with simple date format."""
    filters = _parse_datetime_filters("2024-01-01/2024-12-31")
    
    assert len(filters) == 2
    assert filters[0].operator == FilterOperator.GTE
    assert filters[1].operator == FilterOperator.LTE
    assert isinstance(filters[0].value, datetime)
    assert isinstance(filters[1].value, datetime)


def test_parse_datetime_open_end_simple():
    """Test open end with simple date format."""
    filters = _parse_datetime_filters("2024-01-01/..")
    
    assert len(filters) == 1
    assert filters[0].operator == FilterOperator.GTE
    assert isinstance(filters[0].value, datetime)


def test_parse_datetime_open_start_simple():
    """Test open start with simple date format."""
    filters = _parse_datetime_filters("../2024-12-31")
    
    assert len(filters) == 1
    assert filters[0].operator == FilterOperator.LTE
    assert isinstance(filters[0].value, datetime)
