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

"""Unit coverage for STAC item datetime resolution (#1253).

A STAC item requires a ``datetime``. ``create_item_from_feature`` resolves it
from the (already reserved-member-stripped) feature properties via
``resolve_item_datetime``. The resolver must:

- prefer an explicit item temporal value, in specificity order;
- accept the validity round-trip shape (``start_datetime`` is how
  ``lower(validity)`` is projected back on read);
- accept the ingestion timestamp (``created`` / ``transaction_time``);
- return ``None`` only when no parseable temporal value is present, so the
  caller can stamp an ingestion-timestamp fallback instead of producing an
  invalid STAC item (which previously 500'd for a COLUMNAR collection without a
  validity sink).
"""

from datetime import datetime, timezone

import pytest
from starlette.requests import Request as StarletteRequest

from dynastore.extensions.stac.stac_generator import (
    create_item_from_feature,
    resolve_item_datetime,
)
from dynastore.models.ogc import Feature
from dynastore.modules.stac.stac_config import StacPluginConfig


_EXPECTED = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)


def test_prefers_explicit_datetime_over_validity_bounds():
    dt = resolve_item_datetime(
        {
            "datetime": "2024-01-01T10:00:00Z",
            "start_datetime": "2020-06-06T06:00:00Z",
        }
    )
    assert dt == _EXPECTED


def test_falls_back_to_start_datetime_validity_roundtrip():
    # No ``datetime``; ``start_datetime`` carries lower(validity) on read.
    dt = resolve_item_datetime({"start_datetime": "2024-01-01T10:00:00Z"})
    assert dt == _EXPECTED


def test_uses_valid_from_when_present():
    dt = resolve_item_datetime({"valid_from": "2024-01-01T10:00:00Z"})
    assert dt == _EXPECTED


def test_uses_ingestion_timestamp_created():
    dt = resolve_item_datetime({"created": "2024-01-01T10:00:00Z"})
    assert dt == _EXPECTED


def test_uses_ingestion_timestamp_transaction_time():
    dt = resolve_item_datetime({"transaction_time": "2024-01-01T10:00:00Z"})
    assert dt == _EXPECTED


def test_returns_none_when_no_temporal_value():
    # The #1253 trigger: a COLUMNAR row with only declared attribute columns.
    assert resolve_item_datetime({"adm2_pcode": "PK001"}) is None


def test_returns_none_for_empty_properties():
    assert resolve_item_datetime({}) is None


def test_naive_datetime_is_assumed_utc():
    dt = resolve_item_datetime({"datetime": datetime(2024, 1, 1, 10, 0, 0)})
    assert dt == _EXPECTED


def test_accepts_datetime_instance_directly():
    dt = resolve_item_datetime({"datetime": _EXPECTED})
    assert dt == _EXPECTED


def test_skips_unparseable_then_uses_next_candidate():
    dt = resolve_item_datetime(
        {"datetime": "not-a-real-date", "created": "2024-01-01T10:00:00Z"}
    )
    assert dt == _EXPECTED


def _make_request() -> StarletteRequest:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/stac/catalogs/cat/collections/col/items/item1",
        "query_string": b"",
        "headers": [],
        "server": ("localhost", 80),
    }
    return StarletteRequest(scope)


@pytest.mark.asyncio
async def test_create_item_preserves_source_null_datetime_with_interval():
    feature = Feature(
        type="Feature",
        id="interval-item",
        geometry={
            "type": "Polygon",
            "coordinates": [
                [
                    [180.0, -90.0],
                    [180.0, 90.0],
                    [-180.0, 90.0],
                    [-180.0, -90.0],
                    [180.0, -90.0],
                ]
            ],
        },
        bbox=[-180.0, -90.0, 180.0, 90.0],
        properties={
            "datetime": None,
            "start_datetime": "2024-01-01T00:00:00Z",
            "end_datetime": "2024-01-01T00:00:00Z",
        },
    )

    item = await create_item_from_feature(
        request=_make_request(),
        catalog_id="fao",
        collection_id="agera5-rh12",
        feature=feature,
        stac_config=StacPluginConfig(),
    )

    assert item is not None
    item_dict = item.to_dict()
    assert item_dict["properties"]["datetime"] is None
    assert item_dict["properties"]["start_datetime"] == "2024-01-01T00:00:00Z"
    assert item_dict["properties"]["end_datetime"] == "2024-01-01T00:00:00Z"
    assert "bbox" not in item_dict["geometry"]
