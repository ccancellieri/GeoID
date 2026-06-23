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

"""Unit tests for CollectionElasticsearchDriver._enrich_doc.

Regression guard for the production incident (2026-04-22) where
extent.temporal.interval was sent as [[null, null]] to an ES `date_range`
field, causing:

    RequestError(400, 'document_parsing_exception',
        '[1:521] error parsing field [extent.temporal.interval],
        expected an object but got null')

ES `date_range` expects objects {"gte": ..., "lte": ...}, not STAC's
nested-array format [[start, end]].
"""
from __future__ import annotations

import pytest

from dynastore.modules.elasticsearch.collection_es_driver import CollectionElasticsearchDriver


_enrich = CollectionElasticsearchDriver._enrich_doc
_unenrich = CollectionElasticsearchDriver._unenrich_doc


# ---------------------------------------------------------------------------
# Round-trip + no-mutation guarantees (regression for create_collection 422)
# ---------------------------------------------------------------------------


def test_enrich_does_not_mutate_input():
    """_enrich_doc must NOT mutate nested extent dicts.  An earlier
    shallow-copy bug let the gte/lte rewrite leak back into the
    caller's payload, which then failed Pydantic re-validation in
    CollectionService.create_collection with
    ``Input should be a valid list``.
    """
    payload = {
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
        }
    }
    enriched = _enrich(payload)
    # Original interval shape preserved → caller can still re-validate.
    assert payload["extent"]["temporal"]["interval"] == [
        ["2020-01-01T00:00:00Z", None]
    ]
    # And the enriched copy carries the date_range shape.
    assert enriched["extent"]["temporal"]["interval"] == [
        {"gte": "2020-01-01T00:00:00Z"}
    ]
    # No shared references between layers.
    assert enriched["extent"] is not payload["extent"]
    assert enriched["extent"]["temporal"] is not payload["extent"]["temporal"]


def test_unenrich_round_trips_to_stac_shape():
    """ES ``date_range`` shape on read converts back to STAC ``[start, end]``.

    Without this, the catalog metadata router fan-in surfaces the ES
    slice as the merged ``extent`` and ``Collection.model_validate``
    rejects ``interval[0]`` for being a dict instead of a list — exactly
    the 422 observed on POST /collections after create.
    """
    enriched = {
        "extent": {
            "spatial": {
                "bbox": [[-180, -90, 180, 90]],
                "bbox_shape": {"type": "envelope", "coordinates": [[-180, 90], [180, -90]]},
            },
            "temporal": {
                "interval": [
                    {"gte": "2020-01-01T00:00:00Z", "lte": "2025-01-01T00:00:00Z"},
                    {"gte": "2026-01-01T00:00:00Z"},
                ]
            },
        }
    }
    restored = _unenrich(enriched)
    assert restored["extent"]["temporal"]["interval"] == [
        ["2020-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
        ["2026-01-01T00:00:00Z", None],
    ]
    # bbox_shape is ES-internal; stripped on read so STAC consumers
    # don't see it.
    assert "bbox_shape" not in restored["extent"]["spatial"]


def test_unenrich_passes_through_already_stac_shaped_intervals():
    """Defensive: if the stored doc happens to already be in STAC
    shape (e.g. an older write predating _enrich_doc), pass it through.
    """
    src = {
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
        }
    }
    out = _unenrich(src)
    assert out["extent"]["temporal"]["interval"] == [
        ["2020-01-01T00:00:00Z", None]
    ]


# ---------------------------------------------------------------------------
# Temporal interval → date_range conversion
# ---------------------------------------------------------------------------


def test_null_null_interval_is_removed():
    """[[null, null]] (open-ended) must be dropped — ES date_range can't store it."""
    doc = _enrich({
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        }
    })
    assert "interval" not in doc["extent"]["temporal"]


def test_bounded_start_converted():
    """[[start, null]] → [{"gte": start}]"""
    doc = _enrich({
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2020-01-01T00:00:00+00:00", None]]},
        }
    })
    interval = doc["extent"]["temporal"]["interval"]
    assert interval == [{"gte": "2020-01-01T00:00:00+00:00"}]


def test_bounded_both_converted():
    """[[start, end]] → [{"gte": start, "lte": end}]"""
    doc = _enrich({
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2020-01-01T00:00:00+00:00", "2025-12-31T23:59:59+00:00"]]},
        }
    })
    interval = doc["extent"]["temporal"]["interval"]
    assert interval == [{"gte": "2020-01-01T00:00:00+00:00", "lte": "2025-12-31T23:59:59+00:00"}]


def test_bounded_end_only_converted():
    """[[null, end]] → [{"lte": end}]"""
    doc = _enrich({
        "extent": {
            "temporal": {"interval": [[None, "2025-12-31T23:59:59+00:00"]]},
        }
    })
    interval = doc["extent"]["temporal"]["interval"]
    assert interval == [{"lte": "2025-12-31T23:59:59+00:00"}]


def test_multiple_intervals_converted():
    """Multiple intervals — null-null filtered out, bounded ones converted."""
    doc = _enrich({
        "extent": {
            "temporal": {
                "interval": [
                    ["2020-01-01T00:00:00+00:00", None],
                    [None, None],
                    ["2022-01-01T00:00:00+00:00", "2023-01-01T00:00:00+00:00"],
                ]
            },
        }
    })
    interval = doc["extent"]["temporal"]["interval"]
    assert interval == [
        {"gte": "2020-01-01T00:00:00+00:00"},
        {"gte": "2022-01-01T00:00:00+00:00", "lte": "2023-01-01T00:00:00+00:00"},
    ]


def test_missing_temporal_is_noop():
    doc = _enrich({"extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}}})
    assert "temporal" not in doc["extent"]


def test_no_extent_is_noop():
    doc = _enrich({"id": "c1", "title": "Test"})
    assert "extent" not in doc


# ---------------------------------------------------------------------------
# Spatial bbox → geo_shape envelope (existing behaviour must not regress)
# ---------------------------------------------------------------------------


def test_bbox_shape_added():
    doc = _enrich({
        "extent": {
            "spatial": {"bbox": [[-10.0, -20.0, 10.0, 20.0]]},
        }
    })
    shape = doc["extent"]["spatial"]["bbox_shape"]
    assert shape["type"] == "envelope"
    assert shape["coordinates"] == [[-10.0, 20.0], [10.0, -20.0]]


def test_both_spatial_and_temporal_enriched():
    doc = _enrich({
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [["2021-01-01T00:00:00+00:00", None]]},
        }
    })
    assert "bbox_shape" in doc["extent"]["spatial"]
    assert doc["extent"]["temporal"]["interval"] == [{"gte": "2021-01-01T00:00:00+00:00"}]


# ---------------------------------------------------------------------------
# Protocol signature regression — `context` kwarg
#
# Guards against the drift that caused the production log spam:
#     CollectionElasticsearchDriver.get_metadata() got an unexpected keyword
#     argument 'context' — omitting slice from merged envelope
# The router at collection_router.py always forwards `context=`;
# if the driver rejects it, the ES slice is silently dropped on every read.
# ---------------------------------------------------------------------------

import inspect
from unittest.mock import AsyncMock, patch


def test_get_metadata_accepts_context_kwarg():
    params = inspect.signature(CollectionElasticsearchDriver.get_metadata).parameters
    assert "context" in params, (
        "get_metadata must accept `context` per CollectionStore protocol"
    )


def test_search_metadata_accepts_context_kwarg():
    params = inspect.signature(CollectionElasticsearchDriver.search_metadata).parameters
    assert "context" in params, (
        "search_metadata must accept `context` per CollectionStore protocol"
    )


async def test_get_metadata_call_with_context_does_not_raise_typeerror():
    driver = CollectionElasticsearchDriver()
    mock_client = AsyncMock()
    # Singleton index always exists (created at lifespan); a missing
    # collection surfaces as a NotFound from the .get() call.
    mock_client.get = AsyncMock(side_effect=Exception("not_found"))
    with patch.object(driver, "_get_client", return_value=mock_client), \
         patch.object(driver, "_get_prefix", return_value="dynastore"):
        result = await driver.get_metadata(
            "cat", "col", context={"user": "x"},
        )
    assert result is None
    mock_client.get.assert_awaited_once()


async def test_search_metadata_call_with_context_does_not_raise_typeerror():
    driver = CollectionElasticsearchDriver()
    mock_client = AsyncMock()
    mock_client.search = AsyncMock(return_value={"hits": {"hits": [], "total": {"value": 0}}})
    with patch.object(driver, "_get_client", return_value=mock_client), \
         patch.object(driver, "_get_prefix", return_value="dynastore"):
        results, total = await driver.search_metadata(
            "cat", q="foo", context={"user": "x"},
        )
    assert results == []
    assert total == 0
    mock_client.search.assert_awaited_once()


# ---------------------------------------------------------------------------
# Physical-id routing / doc-id contract (P2d)
#
# All ES ops must use the IMMUTABLE physical ids from CatalogsProtocol for
# _routing and _id.  When CatalogsProtocol is not registered (unit-test
# context) the driver falls back to the logical id so tests pass without
# a live DB.
# ---------------------------------------------------------------------------


from dynastore.modules.elasticsearch.collection_es_driver import (
    _doc_id,
    _resolve_physical_ids,
)


def test_doc_id_uses_supplied_values():
    """_doc_id builds a colon-separated composite from its two arguments.

    Callers are responsible for passing physical ids; _doc_id itself is
    a pure formatter.
    """
    assert _doc_id("s_abc123", "t_xyz789") == "s_abc123:t_xyz789"
    # Falls back to logical if callers pass logical (fail-open path).
    assert _doc_id("my_catalog", "my_collection") == "my_catalog:my_collection"


async def test_resolve_physical_ids_falls_back_when_protocol_absent():
    """Without CatalogsProtocol registered the resolver returns logical ids."""
    with patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=None,
    ):
        cat_phys, col_phys = await _resolve_physical_ids("my_cat", "my_col")

    assert cat_phys == "my_cat"
    assert col_phys == "my_col"


async def test_resolve_physical_ids_uses_protocol_when_present():
    """When CatalogsProtocol is available, physical ids are returned."""
    mock_protocol = AsyncMock()
    mock_protocol.resolve_physical_id = AsyncMock(side_effect=[
        "s_abc123",  # first call: catalog
        "t_xyz789",  # second call: collection
    ])

    with patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=mock_protocol,
    ):
        cat_phys, col_phys = await _resolve_physical_ids("my_cat", "my_col")

    assert cat_phys == "s_abc123"
    assert col_phys == "t_xyz789"
    assert mock_protocol.resolve_physical_id.await_count == 2


async def test_resolve_physical_ids_falls_back_on_none_result():
    """If resolve_physical_id returns None, the logical id is used."""
    mock_protocol = AsyncMock()
    mock_protocol.resolve_physical_id = AsyncMock(return_value=None)

    with patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=mock_protocol,
    ):
        cat_phys, col_phys = await _resolve_physical_ids("logical_cat", "logical_col")

    # Both fall back to logical values.
    assert cat_phys == "logical_cat"
    assert col_phys == "logical_col"


async def test_upsert_metadata_uses_physical_routing():
    """upsert_metadata must pass physical catalog id as ES routing param."""
    driver = CollectionElasticsearchDriver()
    mock_client = AsyncMock()
    mock_client.index = AsyncMock(return_value={"result": "created"})

    mock_protocol = AsyncMock()
    mock_protocol.resolve_physical_id = AsyncMock(side_effect=[
        "s_physcat",  # catalog
        "t_physcol",  # collection
    ])

    with patch.object(driver, "_get_client", return_value=mock_client), \
         patch.object(driver, "_get_prefix", return_value="dynastore"), \
         patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_protocol,
         ), \
         patch(
            "dynastore.modules.elasticsearch.collection_canonical.build_canonical_collection_doc",
            return_value={"id": "col1", "catalog_id": "cat1"},
         ), \
         patch(
            "dynastore.modules.elasticsearch.items_projection.build_known_fields",
            return_value=set(),
         ):
        await driver.upsert_metadata("cat1", "col1", {"id": "col1"})

    call_kwargs = mock_client.index.call_args
    assert call_kwargs.kwargs["id"] == "s_physcat:t_physcol", (
        "ES _id must be built from physical catalog + collection ids"
    )
    assert call_kwargs.kwargs["params"]["routing"] == "s_physcat", (
        "ES routing must be the physical catalog id"
    )


async def test_get_metadata_uses_physical_routing():
    """get_metadata must use physical catalog id for routing and doc lookup."""
    driver = CollectionElasticsearchDriver()
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=Exception("not_found"))

    mock_protocol = AsyncMock()
    mock_protocol.resolve_physical_id = AsyncMock(side_effect=[
        "s_physcat",
        "t_physcol",
    ])

    with patch.object(driver, "_get_client", return_value=mock_client), \
         patch.object(driver, "_get_prefix", return_value="dynastore"), \
         patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_protocol,
         ):
        result = await driver.get_metadata("cat1", "col1")

    assert result is None  # not_found → None
    call_kwargs = mock_client.get.call_args
    assert call_kwargs.kwargs["id"] == "s_physcat:t_physcol"
    assert call_kwargs.kwargs["params"]["routing"] == "s_physcat"


async def test_delete_metadata_uses_physical_routing():
    """delete_metadata must use physical ids for routing and doc id."""
    driver = CollectionElasticsearchDriver()
    mock_client = AsyncMock()
    mock_client.delete = AsyncMock(return_value={"result": "deleted"})

    mock_protocol = AsyncMock()
    mock_protocol.resolve_physical_id = AsyncMock(side_effect=[
        "s_physcat",
        "t_physcol",
    ])

    with patch.object(driver, "_get_client", return_value=mock_client), \
         patch.object(driver, "_get_prefix", return_value="dynastore"), \
         patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_protocol,
         ):
        await driver.delete_metadata("cat1", "col1")

    call_kwargs = mock_client.delete.call_args
    assert call_kwargs.kwargs["id"] == "s_physcat:t_physcol"
    assert call_kwargs.kwargs["params"]["routing"] == "s_physcat"


async def test_search_metadata_uses_physical_routing():
    """search_metadata must route to the physical catalog shard."""
    driver = CollectionElasticsearchDriver()
    mock_client = AsyncMock()
    mock_client.search = AsyncMock(
        return_value={"hits": {"hits": [], "total": {"value": 0}}}
    )

    mock_protocol = AsyncMock()
    # search only resolves catalog (no collection arg)
    mock_protocol.resolve_physical_id = AsyncMock(return_value="s_physcat")

    with patch.object(driver, "_get_client", return_value=mock_client), \
         patch.object(driver, "_get_prefix", return_value="dynastore"), \
         patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_protocol,
         ):
        results, total = await driver.search_metadata("cat1")

    call_kwargs = mock_client.search.call_args
    assert call_kwargs.kwargs["params"]["routing"] == "s_physcat", (
        "search routing must use physical catalog id"
    )
