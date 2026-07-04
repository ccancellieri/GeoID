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

"""Regression for #2957: the Records extension must not leak the internal
``_dimensions_`` sentinel catalog id into a public URL.

Covers:
  - ``records_generator._records_collection_url`` resolves dimension-backed
    collections to the platform-tier ``/records/dimensions/{id}`` shape and
    leaves ordinary catalog-scoped collections untouched.
  - ``db_row_to_record`` / ``collection_to_records_collection`` thread that
    through their self/collection links.
  - The new platform-tier route handlers on ``RecordsService`` delegate to
    the existing catalog-scoped handlers with ``catalog_id`` fixed to
    ``DIMENSIONS_CATALOG_ID``.
  - The platform-tier paths are registered and documented in the OpenAPI
    schema.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from geojson_pydantic import Feature as _GeoJSONFeature

from dynastore.extensions.records import records_generator as rgen
from dynastore.models.dimensions import DIMENSIONS_CATALOG_ID


# ---------------------------------------------------------------------------
# records_generator._records_collection_url
# ---------------------------------------------------------------------------


def test_records_collection_url_ordinary_catalog_unchanged():
    url = rgen._records_collection_url("my_catalog", "my_collection", "http://host")
    assert url == "http://host/records/catalogs/my_catalog/collections/my_collection"


def test_records_collection_url_dimensions_sentinel_is_platform_tier():
    url = rgen._records_collection_url(DIMENSIONS_CATALOG_ID, "temporal-dekadal", "http://host")
    assert url == "http://host/records/dimensions/temporal-dekadal"
    # The sentinel value itself must never appear in the resolved URL.
    assert DIMENSIONS_CATALOG_ID not in url


# ---------------------------------------------------------------------------
# db_row_to_record — self / collection links
# ---------------------------------------------------------------------------


def test_db_row_to_record_ordinary_catalog_links_unchanged():
    item = _GeoJSONFeature(type="Feature", geometry=None, properties={}, id="rec1")
    record = rgen.db_row_to_record(item, "my_catalog", "my_collection", "http://host", layer_config=None)
    links = {l.rel: l.href for l in record.links}
    assert links["self"] == "http://host/records/catalogs/my_catalog/collections/my_collection/items/rec1"
    assert links["collection"] == "http://host/records/catalogs/my_catalog/collections/my_collection"


def test_db_row_to_record_dimensions_sentinel_uses_platform_tier_links():
    item = _GeoJSONFeature(type="Feature", geometry=None, properties={}, id="D1")
    record = rgen.db_row_to_record(
        item, DIMENSIONS_CATALOG_ID, "temporal-dekadal", "http://host", layer_config=None,
    )
    links = {l.rel: l.href for l in record.links}
    assert links["self"] == "http://host/records/dimensions/temporal-dekadal/items/D1"
    assert links["collection"] == "http://host/records/dimensions/temporal-dekadal"
    assert DIMENSIONS_CATALOG_ID not in links["self"]
    assert DIMENSIONS_CATALOG_ID not in links["collection"]


# ---------------------------------------------------------------------------
# collection_to_records_collection — self link
# ---------------------------------------------------------------------------


def test_collection_to_records_collection_dimensions_sentinel_uses_platform_tier_link():
    coll = {"id": "temporal-dekadal", "title": "Temporal Dekadal", "type": "Collection"}
    out = rgen.collection_to_records_collection(coll, DIMENSIONS_CATALOG_ID, "http://host")
    self_link = next(l for l in out.links if l.rel == "self")
    assert self_link.href == "http://host/records/dimensions/temporal-dekadal"
    assert DIMENSIONS_CATALOG_ID not in self_link.href


def test_collection_to_records_collection_ordinary_catalog_unchanged():
    coll = {"id": "my_collection", "title": "My Collection", "type": "Collection"}
    out = rgen.collection_to_records_collection(coll, "my_catalog", "http://host")
    self_link = next(l for l in out.links if l.rel == "self")
    assert self_link.href == "http://host/records/catalogs/my_catalog/collections/my_collection"


# ---------------------------------------------------------------------------
# RecordsService platform-tier route delegation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_dimension_records_delegates_with_dimensions_catalog_id():
    from dynastore.extensions.records.records_service import RecordsService

    svc = RecordsService.__new__(RecordsService)
    svc.get_records = AsyncMock(return_value="sentinel-response")

    request = object()
    result = await svc.get_dimension_records(
        request=request,
        dim_id="temporal-dekadal",
        language="en",
        conn=None,
        limit=10,
        offset=0,
        filter=None,
        filter_lang="cql2-text",
        filter_crs=None,
        properties=None,
        skip_geometry=None,
        return_geometry=None,
        sortby=None,
        bbox=None,
        q=None,
        request_hints=frozenset(),
    )

    assert result == "sentinel-response"
    svc.get_records.assert_awaited_once()
    _, kwargs = svc.get_records.await_args
    assert kwargs["catalog_id"] == DIMENSIONS_CATALOG_ID
    assert kwargs["collection_id"] == "temporal-dekadal"


@pytest.mark.asyncio
async def test_get_dimension_record_delegates_with_dimensions_catalog_id():
    from dynastore.extensions.records.records_service import RecordsService

    svc = RecordsService.__new__(RecordsService)
    svc.get_record = AsyncMock(return_value="sentinel-record")

    result = await svc.get_dimension_record(
        dim_id="temporal-dekadal",
        record_id="D1",
        request=object(),
        language="en",
        conn=None,
    )

    assert result == "sentinel-record"
    svc.get_record.assert_awaited_once()
    _, kwargs = svc.get_record.await_args
    assert kwargs["catalog_id"] == DIMENSIONS_CATALOG_ID
    assert kwargs["collection_id"] == "temporal-dekadal"
    assert kwargs["record_id"] == "D1"


@pytest.mark.asyncio
async def test_get_dimension_collection_delegates_with_dimensions_catalog_id():
    from dynastore.extensions.records.records_service import RecordsService

    svc = RecordsService.__new__(RecordsService)
    svc.get_collection = AsyncMock(return_value="sentinel-collection")

    result = await svc.get_dimension_collection(
        dim_id="temporal-dekadal",
        request=object(),
        language="en",
        request_hints=frozenset(),
    )

    assert result == "sentinel-collection"
    svc.get_collection.assert_awaited_once()
    _, kwargs = svc.get_collection.await_args
    assert kwargs["catalog_id"] == DIMENSIONS_CATALOG_ID
    assert kwargs["collection_id"] == "temporal-dekadal"


@pytest.mark.asyncio
async def test_list_dimension_collections_delegates_with_dimensions_catalog_id():
    from dynastore.extensions.records.records_service import RecordsService

    svc = RecordsService.__new__(RecordsService)
    svc.list_collections = AsyncMock(return_value="sentinel-collections")

    result = await svc.list_dimension_collections(
        request=object(),
        language="en",
        limit=None,
        offset=0,
        request_hints=frozenset(),
    )

    assert result == "sentinel-collections"
    svc.list_collections.assert_awaited_once()
    _, kwargs = svc.list_collections.await_args
    assert kwargs["catalog_id"] == DIMENSIONS_CATALOG_ID


# ---------------------------------------------------------------------------
# OpenAPI surface: the new platform-tier paths are registered
# ---------------------------------------------------------------------------


def _build_schema() -> dict:
    from fastapi import FastAPI
    from dynastore.extensions.records.records_service import RecordsService

    app = FastAPI()
    svc = RecordsService()  # type: ignore[reportAbstractUsage]
    app.include_router(svc.router)
    return app.openapi()


def test_dimensions_platform_routes_registered_in_openapi():
    schema = _build_schema()
    paths = schema["paths"]
    assert "/records/dimensions" in paths
    assert "get" in paths["/records/dimensions"]
    assert "/records/dimensions/{dim_id}" in paths
    assert "/records/dimensions/{dim_id}/items" in paths
    assert "/records/dimensions/{dim_id}/items/{record_id}" in paths
