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

"""Shared RFC3339 open-interval parsing (#2696 duplication consolidation).

Covers the semantics ``dynastore.modules.edr.temporal.parse_datetime_param``,
``dynastore.modules.elasticsearch.items_query.parse_datetime_filter``, and
``dynastore.modules.dggs.zone_query._parse_datetime_filters`` each relied on
before they were consolidated onto this shared helper.
"""
from __future__ import annotations


def test_closed_interval_returns_both_bounds():
    from dynastore.tools.ogc_common import parse_rfc3339_interval
    assert parse_rfc3339_interval("2024-01-01/2024-12-31") == (
        "2024-01-01",
        "2024-12-31",
    )


def test_open_start_returns_none_start():
    from dynastore.tools.ogc_common import parse_rfc3339_interval
    assert parse_rfc3339_interval("../2024-12-31") == (None, "2024-12-31")


def test_open_end_returns_none_end():
    from dynastore.tools.ogc_common import parse_rfc3339_interval
    assert parse_rfc3339_interval("2024-01-01/..") == ("2024-01-01", None)


def test_bare_instant_returns_same_value_for_both_bounds():
    from dynastore.tools.ogc_common import parse_rfc3339_interval
    assert parse_rfc3339_interval("2024-01-01T00:00:00Z") == (
        "2024-01-01T00:00:00Z",
        "2024-01-01T00:00:00Z",
    )


def test_empty_value_returns_none_none():
    from dynastore.tools.ogc_common import parse_rfc3339_interval
    assert parse_rfc3339_interval("") == (None, None)
    assert parse_rfc3339_interval(None) == (None, None)


def test_both_bounds_open_returns_none_none():
    from dynastore.tools.ogc_common import parse_rfc3339_interval
    assert parse_rfc3339_interval("../..") == (None, None)


def test_malformed_value_passes_through_without_raising():
    """No format validation is performed here — callers own that (e.g. via
    ``isoparse``/pydantic). A non-RFC3339 string is split and returned as-is
    rather than rejected."""
    from dynastore.tools.ogc_common import parse_rfc3339_interval
    assert parse_rfc3339_interval("not-a-date") == ("not-a-date", "not-a-date")
    assert parse_rfc3339_interval("not-a-date/also-not-a-date") == (
        "not-a-date",
        "also-not-a-date",
    )


def test_extra_slash_only_splits_on_first():
    """A third ``/`` (e.g. a malformed value with an offset-like suffix) is
    kept as part of the end bound rather than truncated or raising."""
    from dynastore.tools.ogc_common import parse_rfc3339_interval
    assert parse_rfc3339_interval("a/b/c") == ("a", "b/c")
