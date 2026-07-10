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

"""Unit tests for ``_pick_operation`` — operation selection for item streaming.

The OGC features browse always carries an implicit ``validity @> now()``
default (``parse_ogc_query_request`` appends it whenever no ``datetime`` is
supplied). That temporal-validity condition is a READ modifier applied
uniformly on the read backend — it must NOT flip a plain browse onto the
derived-search (INDEX-lane) backend. Only genuine spatial/attribute
predicates select INDEX.

Regression guard for the bug where every ``/items`` browse was misrouted to
Elasticsearch because ``request.filters`` was never empty.
"""
from __future__ import annotations

from datetime import datetime, timezone

from dynastore.models.query_builder import FilterCondition, QueryRequest
from dynastore.modules.catalog.item_query import _pick_operation
from dynastore.modules.storage.routing_config import Operation


def _validity_now() -> FilterCondition:
    return FilterCondition(
        field="validity", operator="@>", value=datetime.now(timezone.utc)
    )


def test_browse_with_implicit_validity_default_is_read() -> None:
    req = QueryRequest(filters=[_validity_now()])
    assert _pick_operation(req) == Operation.READ


def test_user_supplied_datetime_is_still_read() -> None:
    req = QueryRequest(
        filters=[
            FilterCondition(
                field="validity",
                operator="@>",
                value=datetime(2020, 1, 1, tzinfo=timezone.utc),
            )
        ]
    )
    assert _pick_operation(req) == Operation.READ


def test_no_filters_is_read() -> None:
    assert _pick_operation(QueryRequest(filters=[])) == Operation.READ


def test_none_request_is_read() -> None:
    assert _pick_operation(None) == Operation.READ


def test_bbox_spatial_filter_is_search() -> None:
    req = QueryRequest(
        filters=[
            _validity_now(),
            FilterCondition(
                field="geom",
                operator="&&",
                value="SRID=4326;POLYGON((0 0,0 1,1 1,1 0,0 0))",
                spatial_op=True,
            ),
        ]
    )
    assert _pick_operation(req) == Operation.INDEX


def test_cql_filter_is_search() -> None:
    req = QueryRequest(filters=[_validity_now()], cql_filter="CODE = '325'")
    assert _pick_operation(req) == Operation.INDEX


def test_attribute_filter_condition_is_search() -> None:
    req = QueryRequest(
        filters=[
            _validity_now(),
            FilterCondition(field="CODE", operator="=", value="325"),
        ]
    )
    assert _pick_operation(req) == Operation.INDEX
