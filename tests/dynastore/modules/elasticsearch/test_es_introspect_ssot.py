#    Copyright 2025 FAO
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
"""SSOT for ES index introspection.

``es_introspect_mapping`` (in ``modules.elasticsearch.items_es_ops``) is the one
place an Elasticsearch index mapping is turned into ``FieldDefinition`` objects:
the ES-type -> canonical ``data_type`` table and the ES-type -> capability table
live there and nowhere else. The base ``introspect_schema`` and every items
driver's ``get_entity_fields`` (public + envelope) go through it, so the two
tables can never drift apart again (the #1216 "no shared source of truth"
defect: the public driver used to keep its own copies that mapped
``object``/``nested`` to ``string`` and dropped ``date_nanos``, while losing the
``unsigned_long`` aggregatable caps the shared copy never had).
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from dynastore.models.protocols.field_definition import FieldCapability
from dynastore.modules.elasticsearch.items_es_ops import (
    _ES_TYPE_TO_CAPS,
    _ES_TYPE_TO_DATA_TYPE,
    es_introspect_mapping,
)

_NUM = {FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.AGGREGATABLE}


class _FakeIndices:
    def __init__(self, mapping: dict):
        self._mapping = mapping

    async def get_mapping(self, index=None):  # noqa: ANN001
        return self._mapping


class _FakeES:
    def __init__(self, properties: dict):
        self.indices = _FakeIndices(
            {"some-index": {"mappings": {"properties": properties}}}
        )


class TestTablesAreColocatedAndComplete:
    def test_type_and_caps_tables_cover_the_same_keys(self):
        # One SSOT means both halves describe the same ES type set; a key in one
        # but not the other is exactly the drift this consolidation removes.
        assert set(_ES_TYPE_TO_DATA_TYPE) == set(_ES_TYPE_TO_CAPS)

    def test_unsigned_long_is_aggregatable_bigint(self):
        assert _ES_TYPE_TO_DATA_TYPE["unsigned_long"] == "bigint"
        assert set(_ES_TYPE_TO_CAPS["unsigned_long"]) == _NUM

    def test_binary_has_no_query_caps(self):
        assert _ES_TYPE_TO_DATA_TYPE["binary"] == "binary"
        assert list(_ES_TYPE_TO_CAPS["binary"]) == []

    def test_object_and_nested_are_jsonb(self):
        assert _ES_TYPE_TO_DATA_TYPE["object"] == "jsonb"
        assert _ES_TYPE_TO_DATA_TYPE["nested"] == "jsonb"

    def test_date_nanos_is_timestamp(self):
        assert _ES_TYPE_TO_DATA_TYPE["date_nanos"] == "timestamp"

    def test_every_canonical_data_type_is_valid(self):
        # Each mapped token must survive FieldDefinition's canonical validator —
        # the whole point of routing every driver through the SSOT.
        from dynastore.models.field_types import canonical_data_type

        for token in _ES_TYPE_TO_DATA_TYPE.values():
            assert canonical_data_type(token) == token


@pytest.mark.asyncio
class TestEsIntrospectMapping:
    async def test_maps_full_type_matrix(self):
        props = {
            "title": {"type": "text"},
            "code": {"type": "keyword"},
            "count": {"type": "unsigned_long"},
            "blob": {"type": "binary"},
            "meta": {"type": "object"},
            "children": {"type": "nested"},
            "ts": {"type": "date_nanos"},
            "geom": {"type": "geo_shape"},
        }
        fields = await es_introspect_mapping(_FakeES(props), "some-index")
        by_name = {f.name: f for f in fields}

        assert by_name["count"].data_type == "bigint"
        assert set(by_name["count"].capabilities) == _NUM
        assert by_name["blob"].data_type == "binary"
        assert list(by_name["blob"].capabilities) == []
        assert by_name["meta"].data_type == "jsonb"
        assert by_name["children"].data_type == "jsonb"
        assert by_name["ts"].data_type == "timestamp"
        assert by_name["geom"].data_type == "geometry"

    async def test_skips_internal_underscore_fields(self):
        props = {
            "name": {"type": "keyword"},
            "_asset_id": {"type": "keyword"},
            "_external_id": {"type": "keyword"},
            "_valid_from": {"type": "date"},
        }
        fields = await es_introspect_mapping(_FakeES(props), "some-index")
        names = {f.name for f in fields}
        assert names == {"name"}


@pytest.mark.asyncio
class TestPublicDriverDelegatesToSsot:
    """The public items driver must produce the SAME field set the SSOT helper
    does — proving it no longer keeps a private, drifted copy of the tables."""

    async def _run(self, props):
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )

        driver = ItemsElasticsearchDriver()
        with patch(
            "dynastore.modules.elasticsearch.client.get_client",
            return_value=_FakeES(props),
        ):
            return await driver.get_entity_fields("cat1", "col1")

    async def test_object_maps_to_jsonb_not_string(self):
        # The drift bug: the old inline driver table fell through to "string".
        result = await self._run({"meta": {"type": "object"}})
        assert result["meta"].data_type == "jsonb"

    async def test_unsigned_long_keeps_aggregatable_caps(self):
        # Regression guard: the shared caps table must carry unsigned_long so
        # delegation does not silently drop SORTABLE/AGGREGATABLE.
        result = await self._run({"count": {"type": "unsigned_long"}})
        assert set(result["count"].capabilities) == _NUM

    async def test_matches_helper_field_for_field(self):
        props = {
            "title": {"type": "text"},
            "count": {"type": "unsigned_long"},
            "meta": {"type": "object"},
            "ts": {"type": "date_nanos"},
        }
        via_driver = await self._run(props)
        via_helper = {
            f.name: f for f in await es_introspect_mapping(_FakeES(props), "i")
        }
        assert set(via_driver) == set(via_helper)
        for name in via_helper:
            assert via_driver[name].data_type == via_helper[name].data_type
            assert list(via_driver[name].capabilities) == list(
                via_helper[name].capabilities
            )

    async def test_non_item_level_returns_empty(self):
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )

        driver = ItemsElasticsearchDriver()
        assert await driver.get_entity_fields("cat1", "col1", entity_level="catalog") == {}
        assert await driver.get_entity_fields("cat1", None) == {}
