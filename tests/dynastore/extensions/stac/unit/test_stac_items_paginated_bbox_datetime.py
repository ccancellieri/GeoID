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

"""``get_stac_items_paginated`` threads ``bbox=``/``datetime=`` onto the
``QueryRequest`` it builds for the PG fallback path (#3295).

Both shapes are set: ``structural_filters`` (``geom``/``validity``
``FilterCondition`` — consumed by the PostgreSQL ``QueryOptimizer``) AND the
top-level ``QueryRequest.bbox``/``.datetime`` (consumed by an Elasticsearch
driver, in case ``stream_items``'s own routing-aware dispatch resolves one
instead of running the PG SQL path). See ``stac_generator.create_item_collection``
and ``get_stac_items_paginated`` docstrings for why both are threaded.
"""

from __future__ import annotations

from typing import Any

import dynastore.extensions.stac.stac_db as stac_db_mod
from dynastore.models.query_builder import FilterCondition


def _make_query_response():
    from dynastore.models.query_builder import QueryResponse

    async def _empty_gen():
        return
        yield  # pragma: no cover — makes it an async generator

    return QueryResponse(items=_empty_gen(), total_count=0, catalog_id="cat", collection_id="col")


async def test_structural_filters_and_bbox_datetime_reach_query_request(monkeypatch):
    seen: dict = {}

    class _FakeItemsSvc:
        async def stream_items(self, **kwargs: Any):
            seen["request"] = kwargs.get("request")
            return _make_query_response()

    monkeypatch.setattr(stac_db_mod, "get_protocol", lambda _p: _FakeItemsSvc())

    bbox_fc = FilterCondition(
        field="geom", operator="&&", value="SRID=4326;POLYGON((0 0,0 1,1 1,1 0,0 0))",
        spatial_op=True,
    )

    await stac_db_mod.get_stac_items_paginated(
        conn=None,
        catalog_id="cat",
        collection_id="col",
        limit=10,
        offset=0,
        request=None,
        structural_filters=[bbox_fc],
        bbox=[0.0, 0.0, 1.0, 1.0],
        datetime_param="2020-01-01T00:00:00Z",
    )

    built_request = seen["request"]
    assert built_request.filters == [bbox_fc]
    assert built_request.bbox == [0.0, 0.0, 1.0, 1.0]
    assert built_request.datetime == "2020-01-01T00:00:00Z"


async def test_no_structural_filters_leaves_query_request_unfiltered(monkeypatch):
    """Byte-identical default: no bbox/datetime → empty filters, bbox/datetime None."""
    seen: dict = {}

    class _FakeItemsSvc:
        async def stream_items(self, **kwargs: Any):
            seen["request"] = kwargs.get("request")
            return _make_query_response()

    monkeypatch.setattr(stac_db_mod, "get_protocol", lambda _p: _FakeItemsSvc())

    await stac_db_mod.get_stac_items_paginated(
        conn=None,
        catalog_id="cat",
        collection_id="col",
        limit=10,
        offset=0,
        request=None,
    )

    built_request = seen["request"]
    assert built_request.filters == []
    assert built_request.bbox is None
    assert built_request.datetime is None
