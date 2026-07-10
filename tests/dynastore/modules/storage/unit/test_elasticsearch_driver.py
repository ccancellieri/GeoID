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

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.drivers.elasticsearch import (
    ItemsElasticsearchDriver,
    _ElasticsearchBase,
)
from dynastore.modules.storage.drivers.elasticsearch_private import (
    ItemsElasticsearchPrivateDriver,
)
from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError


class TestElasticsearchBase:
    def test_normalize_entities_single_feature(self):
        feature = MagicMock(spec=Feature)
        result = _ElasticsearchBase._normalize_entities(feature)
        assert result == [feature]

    def test_normalize_entities_feature_collection(self):
        fc = MagicMock(spec=FeatureCollection)
        fc.features = [MagicMock(spec=Feature), MagicMock(spec=Feature)]
        result = _ElasticsearchBase._normalize_entities(fc)
        assert len(result) == 2

    def test_normalize_entities_feature_collection_empty(self):
        fc = MagicMock(spec=FeatureCollection)
        fc.features = None
        result = _ElasticsearchBase._normalize_entities(fc)
        assert result == []

    def test_normalize_entities_list(self):
        items = [{"id": "a"}, {"id": "b"}]
        result = _ElasticsearchBase._normalize_entities(items)
        assert result == items

    def test_normalize_entities_dict(self):
        item = {"id": "a"}
        result = _ElasticsearchBase._normalize_entities(item)
        assert result == [item]

    def test_extract_item_id_from_feature(self):
        feature = MagicMock()
        feature.id = "test-id"
        assert _ElasticsearchBase._extract_item_id(feature) == "test-id"

    def test_extract_item_id_from_dict(self):
        assert _ElasticsearchBase._extract_item_id({"id": "test-id"}) == "test-id"

    def test_extract_item_id_none_for_missing(self):
        assert _ElasticsearchBase._extract_item_id({}) is None

    def test_feature_to_stac_item_from_pydantic(self):
        feature = MagicMock()
        feature.model_dump.return_value = {"id": "f1", "type": "Feature"}
        result = _ElasticsearchBase._feature_to_stac_item(feature, "cat1", "col1")
        assert result["collection"] == "col1"
        assert result["id"] == "f1"

    def test_feature_to_stac_item_from_dict(self):
        result = _ElasticsearchBase._feature_to_stac_item(
            {"id": "f1", "type": "Feature"}, "cat1", "col1"
        )
        assert result["collection"] == "col1"
        assert result["id"] == "f1"


class TestItemsElasticsearchDriverMeta:
    """Driver class name / priority / capabilities / read-flavour hints
    are pinned once for all drivers in ``test_driver_meta_contract.py``."""

    @pytest.mark.asyncio
    async def test_export_entities_not_implemented(self):
        driver = ItemsElasticsearchDriver()
        with pytest.raises(NotImplementedError):
            await driver.export_entities("cat1", "col1")


class TestItemsElasticsearchPrivateDriverMeta:
    """Driver class name / priority / capabilities are pinned once for
    all drivers in ``test_driver_meta_contract.py``; ``test_has_search_hints``
    below additionally pins opt-in-only routing behaviour, which is
    driver-specific and stays here."""

    @pytest.mark.asyncio
    async def test_export_entities_not_implemented(self):
        driver = ItemsElasticsearchPrivateDriver()
        with pytest.raises(NotImplementedError):
            await driver.export_entities("cat1", "col1")

    @pytest.mark.asyncio
    async def test_soft_delete_raises(self):
        driver = ItemsElasticsearchPrivateDriver()
        with pytest.raises(SoftDeleteNotSupportedError):
            await driver.delete_entities("cat1", "col1", ["id1"], soft=True)

    @pytest.mark.asyncio
    async def test_soft_drop_raises(self):
        driver = ItemsElasticsearchPrivateDriver()
        with pytest.raises(SoftDeleteNotSupportedError):
            await driver.drop_storage("cat1", "col1", soft=True)

    def test_typed_driver_bind_resolves(self):
        """Regression: ItemsElasticsearchPrivateDriver previously inherited
        only from ``(_ElasticsearchBase, ModuleProtocol)`` and was therefore
        invisible to ``list_registered_configs()`` and the
        ``/configs/registry`` deep-view.  After rebasing onto
        ``TypedDriver[ItemsElasticsearchPrivateDriverConfig]`` the pair
        registers automatically.  Pinning here so a future refactor that
        accidentally drops the TypedDriver base is caught at unit-test time.
        """
        from dynastore.models.protocols.typed_driver import registered_pairs
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchPrivateDriverConfig,
        )

        pairs = registered_pairs()
        assert ItemsElasticsearchPrivateDriverConfig in pairs
        assert pairs[ItemsElasticsearchPrivateDriverConfig] is ItemsElasticsearchPrivateDriver
        assert ItemsElasticsearchPrivateDriverConfig.class_key() == "items_elasticsearch_private_driver"

    def test_visible_in_registry(self):
        """The wire identity is exposed in ``list_registered_configs()`` so
        the ``/configs/registry`` and tree-view endpoints surface the driver.
        """
        from dynastore.models.plugin_config import list_registered_configs
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchPrivateDriverConfig,
        )

        configs = list_registered_configs()
        assert "items_elasticsearch_private_driver" in configs
        assert configs["items_elasticsearch_private_driver"] is ItemsElasticsearchPrivateDriverConfig

    def test_has_search_hints(self):
        """Private driver must expose SEARCH/FILTER/SORT hints so the routing
        dispatcher can select it when an operator explicitly pins it in an
        ItemsRoutingConfig.operations[SEARCH] entry."""
        from dynastore.modules.storage.hints import Hint
        driver = ItemsElasticsearchPrivateDriver()
        for hint in (
            Hint.SEARCH,
            Hint.FULLTEXT,
            Hint.SPATIAL_FILTER,
            Hint.ATTRIBUTE_FILTER,
            Hint.SORT,
        ):
            assert hint in driver.supported_hints, f"missing hint: {hint}"
        # Still opt-in only — never auto-selected.
        assert not driver.preferred_for
        assert not driver.auto_register_for_routing


class TestItemsIndexNameSeam:
    """The single index-name seam (:meth:`_items_index_name`) and the
    collection-routing seam (:meth:`_collection_routing`) that the shared
    :class:`_ItemsElasticsearchBase` data-side ops route through.

    The public per-tenant index is sharded by ``_routing=collection_id``; the
    private index is not (routing ``None``). Pinning the seam resolution here
    so a future change to either driver's index naming or routing is caught.
    """

    def test_public_index_name_is_tenant_items_index(self):
        from dynastore.modules.elasticsearch.mappings import get_tenant_items_index
        from dynastore.modules.elasticsearch.client import get_index_prefix

        driver = ItemsElasticsearchDriver()
        name = driver._items_index_name("cat1")
        assert name == get_tenant_items_index(get_index_prefix(), "cat1")
        assert "private" not in name

    def test_public_routing_is_collection_id(self):
        driver = ItemsElasticsearchDriver()
        assert driver._collection_routing("col1") == "col1"
        assert driver._collection_routing(None) is None

    def test_private_index_name_is_private_index(self):
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            get_private_index_name,
        )
        from dynastore.modules.elasticsearch.client import get_index_prefix

        driver = ItemsElasticsearchPrivateDriver()
        name = driver._items_index_name("cat1")
        assert name == get_private_index_name(get_index_prefix(), "cat1")
        assert name.endswith("-private-items")

    def test_private_routing_is_none(self):
        """The private index is not sharded by collection — never routed."""
        driver = ItemsElasticsearchPrivateDriver()
        assert driver._collection_routing("col1") is None
        assert driver._collection_routing(None) is None

    def test_es_items_marker_present_on_both_drivers(self):
        """The structural marker that ``item_service`` uses to detect an ES
        items driver without importing the classes."""
        assert ItemsElasticsearchDriver().is_es_items_driver is True
        assert ItemsElasticsearchPrivateDriver().is_es_items_driver is True


class TestDataSideOpsRouteThroughSeam:
    """The shared count/extents/aggregate/introspect ops resolve their index
    and routing via the per-driver seams — public passes ``routing``, private
    does not (mirroring the pre-refactor behaviour)."""

    @pytest.mark.asyncio
    async def test_public_count_passes_routing(self):
        driver = ItemsElasticsearchDriver()
        es = MagicMock()
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.items_es_ops.es_count_items",
            new=AsyncMock(return_value=7),
        ) as mock_count:
            result = await driver.count_entities("cat1", "col1")
        assert result == 7
        _, kwargs = mock_count.call_args
        assert kwargs["routing"] == "col1"
        assert kwargs["collection"] == "col1"

    @pytest.mark.asyncio
    async def test_private_count_omits_routing(self):
        driver = ItemsElasticsearchPrivateDriver()
        es = MagicMock()
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.items_es_ops.es_count_items",
            new=AsyncMock(return_value=3),
        ) as mock_count:
            result = await driver.count_entities("cat1", "col1")
        assert result == 3
        _, kwargs = mock_count.call_args
        assert kwargs["routing"] is None
        assert kwargs["collection"] == "col1"

    @pytest.mark.asyncio
    async def test_count_request_passes_inner_query_not_enveloped(self):
        from dynastore.models.query_builder import QueryRequest
        driver = ItemsElasticsearchDriver()
        es = MagicMock()
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.items_es_ops.es_count_items",
            new=AsyncMock(return_value=1),
        ) as mock_count:
            await driver.count_entities(
                "cat1", "col1", request=QueryRequest(item_ids=["x"]),
            )
        _, kwargs = mock_count.call_args
        # es_count_items wants the INNER query (it adds its own scope); a
        # double-enveloped {"query": {...}} would be a malformed count body.
        q = kwargs["query"]
        assert "query" not in q, f"query was double-enveloped: {q}"
        assert "bool" in q or "match_all" in q

    @pytest.mark.asyncio
    async def test_count_multi_collection_omits_single_scope_and_routing(self):
        from dynastore.models.query_builder import QueryRequest
        driver = ItemsElasticsearchDriver()
        es = MagicMock()
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.items_es_ops.es_count_items",
            new=AsyncMock(return_value=9),
        ) as mock_count:
            await driver.count_entities(
                "cat1", "col1", request=QueryRequest(collections=["c1", "c2"]),
            )
        _, kwargs = mock_count.call_args
        # Multi-collection: scoping is in the query's terms filter, so the
        # single-collection scope + routing must be dropped.
        assert kwargs["collection"] is None
        assert kwargs["routing"] is None


class _FakeCollectionsSvc:
    """Stub for ``CatalogsProtocol.collections`` — external → internal only."""

    def __init__(self, collection_map):
        self._map = collection_map

    async def resolve_collection_id(self, catalog_id, collection_id, allow_missing=False):
        return self._map.get(collection_id)


class _FakeCatalogsProtocol:
    """Stub ``CatalogsProtocol`` resolving a fixed external → internal id map,
    mirroring the real ``resolve_catalog_id`` / ``collections.resolve_collection_id``
    contract (``None`` on miss, passthrough left to the caller)."""

    def __init__(self, catalog_map, collection_map):
        self._catalog_map = catalog_map
        self.collections = _FakeCollectionsSvc(collection_map)

    async def resolve_catalog_id(self, catalog_id, allow_missing=False):
        return self._catalog_map.get(catalog_id)


class TestDataSideOpsResolveExternalIds:
    """count_entities / compute_extents / aggregate must resolve an EXTERNAL
    catalog_id/collection_id to internal before computing the index name —
    exactly like read_entities already does (#2325) — so a count/extent
    request targets the SAME index a sibling read_entities call would.
    Before the fix these three ops used the raw (external) id verbatim,
    silently hitting a differently-named index and returning an empty/zero
    result while read_entities (correctly resolved) returned real hits.
    """

    @pytest.mark.asyncio
    async def test_count_entities_resolves_external_catalog_and_collection_id(self):
        fake_catalogs = _FakeCatalogsProtocol(
            catalog_map={"gaulb": "c_internal123"},
            collection_map={"gaul_level_1": "col_internal456"},
        )
        driver = ItemsElasticsearchDriver()
        es = MagicMock()
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=fake_catalogs,
        ), patch(
            "dynastore.modules.elasticsearch.items_es_ops.es_count_items",
            new=AsyncMock(return_value=3103),
        ) as mock_count:
            result = await driver.count_entities("gaulb", "gaul_level_1")

        assert result == 3103
        args, kwargs = mock_count.call_args
        # Second positional arg is the index name — must be built from the
        # INTERNAL catalog id (matches what read_entities would target),
        # not the raw external path-param id.
        index_name = args[1]
        assert "c_internal123" in index_name
        assert "gaulb" not in index_name
        assert kwargs["collection"] == "col_internal456"
        assert kwargs["routing"] == "col_internal456"

    @pytest.mark.asyncio
    async def test_count_entities_passthrough_when_already_internal_or_unmapped(self):
        """No CatalogsProtocol registered (or id unmapped) — behaves exactly
        as before: the raw id is used verbatim (existing passthrough tests
        above must keep passing unmodified)."""
        driver = ItemsElasticsearchDriver()
        es = MagicMock()
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=None,
        ), patch(
            "dynastore.modules.elasticsearch.items_es_ops.es_count_items",
            new=AsyncMock(return_value=7),
        ) as mock_count:
            await driver.count_entities("cat1", "col1")

        args, kwargs = mock_count.call_args
        assert args[1] == driver._items_index_name("cat1")
        assert kwargs["collection"] == "col1"

    @pytest.mark.asyncio
    async def test_compute_extents_resolves_external_catalog_id(self):
        fake_catalogs = _FakeCatalogsProtocol(
            catalog_map={"gaulb": "c_internal123"},
            collection_map={"gaul_level_1": "col_internal456"},
        )
        driver = ItemsElasticsearchDriver()
        es = MagicMock()
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=fake_catalogs,
        ), patch(
            "dynastore.modules.elasticsearch.items_es_ops.es_extents",
            new=AsyncMock(return_value={"spatial": {"bbox": [[1, 2, 3, 4]]}}),
        ) as mock_extents:
            await driver.compute_extents("gaulb", "gaul_level_1")

        args, kwargs = mock_extents.call_args
        assert "c_internal123" in args[1]
        assert kwargs["collection"] == "col_internal456"

    @pytest.mark.asyncio
    async def test_index_available_resolves_external_catalog_id(self):
        """index_available must probe the SAME (internal-keyed) index name
        read_entities/count_entities target — otherwise a pre-check on an
        external catalog_id disagrees with the read it is gating, and a
        genuinely-present index is reported unavailable (or vice versa),
        defeating the PG fallback / ES-selection decision (#2894).
        """
        fake_catalogs = _FakeCatalogsProtocol(
            catalog_map={"gaulb": "c_internal123"},
            collection_map={},
        )
        driver = ItemsElasticsearchDriver()
        es = MagicMock()
        es.indices.exists = AsyncMock(return_value=True)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=fake_catalogs,
        ):
            result = await driver.index_available("gaulb")

        assert result is True
        probed_index = es.indices.exists.call_args.kwargs["index"]
        assert "c_internal123" in probed_index
        assert "gaulb" not in probed_index

    @pytest.mark.asyncio
    async def test_index_available_passthrough_when_already_internal_or_unmapped(self):
        """No CatalogsProtocol registered (or id unmapped) — probes the raw
        id verbatim, matching prior behaviour."""
        driver = ItemsElasticsearchDriver()
        es = MagicMock()
        es.indices.exists = AsyncMock(return_value=False)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=None,
        ):
            result = await driver.index_available("cat1")

        assert result is False
        assert es.indices.exists.call_args.kwargs["index"] == driver._items_index_name("cat1")


class TestQueryRequestToEs:
    def test_empty_request(self):
        from dynastore.models.query_builder import QueryRequest
        request = QueryRequest()
        result = ItemsElasticsearchDriver._query_request_to_es(request)
        assert result == {"query": {"match_all": {}}}

    def test_eq_filter(self):
        from dynastore.models.query_builder import QueryRequest, FilterCondition
        request = QueryRequest(
            filters=[FilterCondition(field="status", operator="eq", value="active")]
        )
        result = ItemsElasticsearchDriver._query_request_to_es(request)
        assert result["query"]["bool"]["must"][0] == {"term": {"status": "active"}}

    def test_bbox_filter(self):
        from dynastore.models.query_builder import QueryRequest, FilterCondition
        request = QueryRequest(
            filters=[
                FilterCondition(
                    field="geometry",
                    operator="bbox",
                    value=[10.0, 40.0, 15.0, 45.0],
                )
            ]
        )
        result = ItemsElasticsearchDriver._query_request_to_es(request)
        geo_filter = result["query"]["bool"]["must"][0]
        # A legacy ``bbox`` FilterCondition now renders as the same
        # ``geo_shape`` envelope the build_items_query SSOT emits, so the
        # streaming read/count path and the structural search agree on one
        # spatial DSL.
        assert "geo_shape" in geo_filter
        assert geo_filter["geo_shape"]["geometry"]["shape"]["type"] == "envelope"

    def test_like_filter(self):
        from dynastore.models.query_builder import QueryRequest, FilterCondition
        request = QueryRequest(
            filters=[FilterCondition(field="name", operator="like", value="test*")]
        )
        result = ItemsElasticsearchDriver._query_request_to_es(request)
        assert result["query"]["bool"]["must"][0] == {"wildcard": {"name": "test*"}}

    def test_multiple_filters(self):
        from dynastore.models.query_builder import QueryRequest, FilterCondition
        request = QueryRequest(
            filters=[
                FilterCondition(field="status", operator="eq", value="active"),
                FilterCondition(field="name", operator="like", value="test*"),
            ]
        )
        result = ItemsElasticsearchDriver._query_request_to_es(request)
        must = result["query"]["bool"]["must"]
        assert len(must) == 2


# ---------------------------------------------------------------------------
# Tenant-index behavior (PR-2b)
# ---------------------------------------------------------------------------


class _StubIndices:
    def __init__(self):
        self.exists_calls: list = []
        self.create_calls: list = []
        self.delete_calls: list = []
        self.exists_result = False

    async def exists(self, *, index, **kwargs):
        self.exists_calls.append({"index": index, "kwargs": kwargs})
        return self.exists_result

    async def create(self, *, index, body=None, **kwargs):
        self.create_calls.append({"index": index, "body": body, "kwargs": kwargs})

    async def delete(self, *, index, params=None, **kwargs):
        self.delete_calls.append({"index": index, "params": params, "kwargs": kwargs})


class _StubEs:
    def __init__(self, exists=False):
        self.indices = _StubIndices()
        self.indices.exists_result = exists
        self.bulk_calls: list = []
        self.delete_calls: list = []
        self.delete_by_query_calls: list = []
        self.get_calls: list = []
        self.search_calls: list = []
        self.index_calls: list = []
        self.count_result = {"count": 0}

    async def bulk(self, *, body, params=None, **kwargs):
        self.bulk_calls.append({"body": body, "params": params, "kwargs": kwargs})
        return getattr(self, "bulk_result", {"items": []})

    async def index(self, *, index, id, body, params=None, **kwargs):
        self.index_calls.append({"index": index, "id": id, "body": body, "params": params})
        return {"result": "created"}

    async def delete(self, *, index, id, params=None, **kwargs):
        self.delete_calls.append({"index": index, "id": id, "params": params})

    async def delete_by_query(self, *, index, body, params=None, **kwargs):
        self.delete_by_query_calls.append({
            "index": index, "body": body, "params": params,
        })

    async def get(self, *, index, id, params=None, **kwargs):
        self.get_calls.append({"index": index, "id": id, "params": params})
        return {"_source": {
            "id": id, "type": "Feature", "collection": "col1",
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
            "properties": {},
        }}

    async def search(self, *, index, body=None, params=None, **kwargs):
        self.search_calls.append({"index": index, "body": body, "params": params})
        return {"hits": {"hits": []}}

    async def count(self, *, body=None, index=None, params=None, **kwargs):
        return self.count_result


class TestEnsureStorageTenantIndex:
    @pytest.mark.asyncio
    async def test_creates_tenant_index_and_adds_to_alias(self):
        es = _StubEs(exists=False)
        added: list = []

        async def _add(index_name):
            added.append(index_name)

        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.modules.elasticsearch.aliases.add_index_to_public_alias",
            new=_add,
        ):
            driver = ItemsElasticsearchDriver()
            await driver.ensure_storage("cat1")

        assert len(es.indices.create_calls) == 1
        call = es.indices.create_calls[0]
        assert call["index"] == "dynastore-cat1-items"
        assert call["kwargs"] == {}
        body = call["body"]
        assert set(body.keys()) == {"settings", "mappings"}
        # ElasticsearchIndexConfig defaults when no PlatformConfigsProtocol is
        # registered (unit-test path): items_total_fields_limit=2000.
        assert body["settings"] == {"index.mapping.total_fields.limit": 2000}
        assert added == ["dynastore-cat1-items"]

    @pytest.mark.asyncio
    async def test_idempotent_when_index_exists(self):
        es = _StubEs(exists=True)
        added: list = []

        async def _add(index_name):
            added.append(index_name)

        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.modules.elasticsearch.aliases.add_index_to_public_alias",
            new=_add,
        ):
            driver = ItemsElasticsearchDriver()
            await driver.ensure_storage("cat1")

        # No create when exists; alias add still called (idempotent itself).
        assert es.indices.create_calls == []
        assert added == ["dynastore-cat1-items"]

    @pytest.mark.asyncio
    async def test_propagates_mapper_parsing_exception(self):
        # Regression for #913: ensure_storage previously swallowed every
        # exception as "concurrent create", masking mapping bugs and leaving
        # the tenant index missing while later writes silently no-oped.
        es = _StubEs(exists=False)

        async def _boom(*, index, body=None, **kwargs):
            raise RuntimeError(
                "RequestError(400, 'mapper_parsing_exception', "
                "'unknown parameter [doc_values] on mapper [foo] of type [text]')"
            )

        es.indices.create = _boom  # type: ignore[method-assign]

        async def _add(index_name):
            pass

        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.modules.elasticsearch.aliases.add_index_to_public_alias",
            new=_add,
        ):
            driver = ItemsElasticsearchDriver()
            with pytest.raises(RuntimeError, match="mapper_parsing_exception"):
                await driver.ensure_storage("cat1")

    @pytest.mark.asyncio
    async def test_swallows_concurrent_create_race(self):
        # The one benign case: a concurrent worker won the create race.
        es = _StubEs(exists=False)

        async def _race(*, index, body=None, **kwargs):
            raise RuntimeError(
                "RequestError(400, 'resource_already_exists_exception', "
                "'index [dynastore-cat1-items/abc] already exists')"
            )

        es.indices.create = _race  # type: ignore[method-assign]
        added: list = []

        async def _add(index_name):
            added.append(index_name)

        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.modules.elasticsearch.aliases.add_index_to_public_alias",
            new=_add,
        ):
            driver = ItemsElasticsearchDriver()
            await driver.ensure_storage("cat1")

        assert added == ["dynastore-cat1-items"]


class TestDeleteEntitiesUsesRouting:
    @pytest.mark.asyncio
    async def test_per_id_delete_with_routing(self):
        es = _StubEs(exists=True)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ):
            driver = ItemsElasticsearchDriver()
            n = await driver.delete_entities("cat1", "col1", ["a"])

        assert n == 1
        assert es.delete_calls == [
            {"index": "dynastore-cat1-items", "id": "a",
             "params": {"routing": "col1", "ignore": "404"}},
        ]

    @pytest.mark.asyncio
    async def test_multi_id_delete_chunk_uses_single_bulk_call(self):
        es = _StubEs(exists=True)
        es.bulk_result = {
            "errors": False,
            "items": [
                {"delete": {"_id": "a", "status": 200}},
                {"delete": {"_id": "b", "status": 200}},
            ],
        }
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ):
            driver = ItemsElasticsearchDriver()
            n = await driver.delete_entities("cat1", "col1", ["a", "b"])

        assert n == 2
        assert es.delete_calls == []
        assert len(es.bulk_calls) == 1
        assert es.bulk_calls[0]["body"] == [
            {"delete": {
                "_index": "dynastore-cat1-items",
                "_id": "a",
                "routing": "col1",
            }},
            {"delete": {
                "_index": "dynastore-cat1-items",
                "_id": "b",
                "routing": "col1",
            }},
        ]


class TestDropStorageScopes:
    @pytest.mark.asyncio
    async def test_collection_drop_uses_delete_by_query(self):
        es = _StubEs(exists=True)
        removed: list = []

        async def _remove(index_name):
            removed.append(index_name)

        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.modules.elasticsearch.aliases.remove_index_from_public_alias",
            new=_remove,
        ):
            driver = ItemsElasticsearchDriver()
            await driver.drop_storage("cat1", "col1")

        assert removed == []  # alias untouched on collection-scope drop
        assert es.delete_by_query_calls == [{
            "index": "dynastore-cat1-items",
            "body": {"query": {"term": {"collection": "col1"}}},
            "params": {"routing": "col1", "refresh": "false"},
        }]
        assert es.indices.delete_calls == []

    @pytest.mark.asyncio
    async def test_catalog_drop_removes_from_alias_then_deletes_index(self):
        es = _StubEs(exists=True)
        removed: list = []

        async def _remove(index_name):
            removed.append(index_name)

        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.modules.elasticsearch.aliases.remove_index_from_public_alias",
            new=_remove,
        ):
            driver = ItemsElasticsearchDriver()
            await driver.drop_storage("cat1")

        assert removed == ["dynastore-cat1-items"]
        assert es.indices.delete_calls == [{
            "index": "dynastore-cat1-items",
            "params": {"ignore_unavailable": "true"},
            "kwargs": {},
        }]


class TestReadEntitiesScopesByCollection:
    @pytest.mark.asyncio
    async def test_by_id_uses_routing(self):
        es = _StubEs(exists=True)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ):
            driver = ItemsElasticsearchDriver()
            results = []
            async for f in driver.read_entities("cat1", "col1", entity_ids=["x"]):
                results.append(f)

        assert es.get_calls == [{
            "index": "dynastore-cat1-items", "id": "x",
            "params": {"routing": "col1"},
        }]
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_query_path_filters_by_collection_term(self):
        es = _StubEs(exists=True)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ):
            driver = ItemsElasticsearchDriver()
            collected = []
            async for f in driver.read_entities(
                "cat1", "col1", limit=10, offset=0,
            ):
                collected.append(f)

        assert es.search_calls
        call = es.search_calls[0]
        assert call["index"] == "dynastore-cat1-items"
        assert call["params"]["routing"] == "col1"
        # Body wraps the base match_all in a bool with a collection filter.
        body_query = call["body"]["query"]
        assert body_query["bool"]["filter"] == [
            {"term": {"collection": "col1"}},
        ]


class TestWriteEntitiesTenantIndex:
    """End-to-end behaviour of the rewritten write_entities path."""

    @staticmethod
    def _feature(item_id="f1", external_id=None):
        from dynastore.models.ogc import Feature
        return Feature.model_validate({
            "id": item_id,
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
            "properties": (
                {"ext_id": external_id} if external_id is not None else {}
            ),
        })

    @pytest.mark.asyncio
    async def test_default_policy_writes_with_routing_and_tracking_fields(self):
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ):
            driver = ItemsElasticsearchDriver()
            written = await driver.write_entities(
                "cat1", "col1",
                [self._feature("f1"), self._feature("f2")],
                context={
                    "asset_id": "asset-7",
                    "valid_from": "2026-04-27T00:00:00Z",
                    "valid_to": None,
                },
            )

        assert len(written) == 2
        assert len(es.bulk_calls) == 1
        body = es.bulk_calls[0]["body"]
        # body alternates [action, doc, action, doc]
        assert len(body) == 4
        for action_idx in (0, 2):
            action = body[action_idx]["index"]
            assert action["_index"] == "dynastore-cat1-items"
            assert action["routing"] == "col1"
        for doc_idx in (1, 3):
            doc = body[doc_idx]
            assert doc["collection"] == "col1"
            # asset_id lives only on the canonical root field; the legacy
            # ``_asset_id`` _source mirror was removed (#1285 identity convergence).
            assert doc["asset_id"] == "asset-7"
            assert "_asset_id" not in doc
            assert doc["_valid_from"] == "2026-04-27T00:00:00Z"
            # _valid_to is None in context → key skipped
            assert "_valid_to" not in doc
        # ES-primary sync write path uses refresh=wait_for so the doc is
        # immediately visible to _search (read-after-write); refresh=false plus
        # ES search-idle would otherwise leave it id-retrievable but unsearchable.
        assert es.bulk_calls[0]["params"] == {"refresh": "wait_for"}

    @pytest.mark.asyncio
    async def test_write_entities_ensures_index_once_per_catalog(self):
        """ES-primary write ensures the index (correct mapping + alias) before
        writing, bounded to once per (catalog, process)."""
        from dynastore.modules.storage.drivers import elasticsearch as es_mod
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        es_mod._ITEMS_INDEX_ENSURED_CATALOGS.discard("catX")
        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ), patch.object(
            ItemsElasticsearchDriver, "ensure_storage", AsyncMock(),
        ) as ensure_mock:
            driver = ItemsElasticsearchDriver()
            await driver.write_entities("catX", "col1", [self._feature("f1")])
            await driver.write_entities("catX", "col1", [self._feature("f2")])

        # ensure_storage runs on the first write, cached for the second.
        ensure_mock.assert_awaited_once_with("catX", "col1")
        assert "catX" in es_mod._ITEMS_INDEX_ENSURED_CATALOGS

    @pytest.mark.asyncio
    async def test_refuse_policy_skips_existing_external_id(self):
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        es = _StubEs(exists=True)
        es.count_result = {"count": 1}
        from dynastore.modules.storage import DeriveSpec
        policy = ItemsWritePolicy(
            on_conflict=WriteConflictPolicy.REFUSE,
            derive=DeriveSpec(external_id="properties.ext_id"),
        )
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ):
            driver = ItemsElasticsearchDriver()
            written = await driver.write_entities(
                "cat1", "col1",
                [self._feature("f1", external_id="EXT-1")],
            )

        # Skipped — no bulk call, no written entries.
        assert written == []
        assert es.bulk_calls == []

    @pytest.mark.asyncio
    async def test_refuse_policy_skip_is_surfaced_in_written_skipped(self):
        """#2826: a REFUSE-on-conflict pre-submit skip must be visible on the
        returned list's ``.skipped`` attribute — not just absorbed into a
        silently-shorter ``written``, with no accounting anywhere."""
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        es = _StubEs(exists=True)
        es.count_result = {"count": 1}
        from dynastore.modules.storage import DeriveSpec
        policy = ItemsWritePolicy(
            on_conflict=WriteConflictPolicy.REFUSE,
            derive=DeriveSpec(external_id="properties.ext_id"),
        )
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=AsyncMock(side_effect=_fake_canonical_inputs),
        ):
            driver = ItemsElasticsearchDriver()
            written = await driver.write_entities(
                "cat1", "col1",
                [self._feature("f1", external_id="EXT-1")],
            )

        assert written == []
        assert written.skipped == [("EXT-1", "refused_on_conflict")]

    @pytest.mark.asyncio
    async def test_missing_id_row_skip_is_surfaced_in_written_skipped(self):
        """#2826: a row with no resolvable id (no top-level id, no geoid, no
        external_id) is skipped before the ``_bulk`` call — the skip must
        land on ``.skipped``, not just an ERROR log line."""
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        no_id_item = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
            "properties": {},
        }
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ):
            driver = ItemsElasticsearchDriver()
            written = await driver.write_entities("cat1", "col1", [no_id_item])

        assert written == []
        assert written.skipped == [(None, "missing_id")]
        assert es.bulk_calls == []

    @pytest.mark.asyncio
    async def test_mixed_batch_written_and_skipped_both_present(self):
        """#2826: a batch with one REFUSE-skipped item and one normally
        written item must return both — the write side unaffected, the skip
        side fully accounted for."""
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )
        from dynastore.modules.storage import DeriveSpec

        class _SelectiveStubEs(_StubEs):
            """count() returns exists=True only for EXT-EXISTS."""

            async def count(self, *, body=None, index=None, params=None, **kwargs):
                terms = body["query"]["bool"]["filter"]
                ext_id = next(
                    t["term"]["external_id"] for t in terms if "term" in t and "external_id" in t["term"]
                )
                return {"count": 1 if ext_id == "EXT-EXISTS" else 0}

        es = _SelectiveStubEs(exists=True)
        policy = ItemsWritePolicy(
            on_conflict=WriteConflictPolicy.REFUSE,
            derive=DeriveSpec(external_id="properties.ext_id"),
        )
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=AsyncMock(side_effect=_fake_canonical_inputs),
        ):
            driver = ItemsElasticsearchDriver()
            written = await driver.write_entities(
                "cat1", "col1",
                [
                    self._feature("f1", external_id="EXT-EXISTS"),
                    self._feature("f2", external_id="EXT-NEW"),
                ],
            )

        assert len(written) == 1
        assert written[0].id == "f2"
        assert written.skipped == [("EXT-EXISTS", "refused_on_conflict")]
        assert len(es.bulk_calls) == 1

    @pytest.mark.asyncio
    async def test_new_version_policy_appends_timestamp_to_doc_id(self):
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )
        from dynastore.modules.storage import DeriveSpec

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(
            on_conflict=WriteConflictPolicy.NEW_VERSION,
            derive=DeriveSpec(external_id="properties.ext_id"),
        )
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ):
            driver = ItemsElasticsearchDriver()
            await driver.write_entities(
                "cat1", "col1",
                [self._feature("f1", external_id="EXT-1")],
            )

        body = es.bulk_calls[0]["body"]
        action_id = body[0]["index"]["_id"]
        # doc id is geoid-based (write_entities: "UPDATE: stable doc_id=geoid"),
        # not external_id-based. With no PG-assigned geoid and no PG canonical
        # for this collection, the top-level feature id ("f1") is the geoid
        # fallback; NEW_VERSION appends an underscore + 14-digit-ish timestamp
        # (YYYYMMDDTHHMMSS + microseconds) suffix to it.
        assert action_id.startswith("f1_")
        assert len(action_id) > len("f1_")


class TestWriteEntitiesResolvesExternalIds:
    """write_entities must resolve an EXTERNAL catalog_id/collection_id to
    internal before computing the index name / ``_routing`` key / stored
    ``collection`` field — exactly like count_entities/compute_extents/
    aggregate/read_entities already do (#2999). Before the fix, write_entities
    used the raw (external) id verbatim: a caller supplying the external id
    wrote to a different index / routing partition than every read path
    resolves to, silently stranding documents on a shard reads never query.
    """

    @staticmethod
    def _feature(item_id="f1"):
        from dynastore.models.ogc import Feature
        return Feature.model_validate({
            "id": item_id,
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
            "properties": {},
        })

    @pytest.mark.asyncio
    async def test_write_entities_resolves_external_catalog_and_collection_id(self):
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        # Mirrors the dev divergence cited in #2999: catalog
        # external_id="gaulb" vs internal id="c_akbbm7cinmqfe".
        fake_catalogs = _FakeCatalogsProtocol(
            catalog_map={"gaulb": "c_akbbm7cinmqfe"},
            collection_map={"gaul_level_1": "col_internal456"},
        )
        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=fake_catalogs,
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ):
            driver = ItemsElasticsearchDriver()
            written = await driver.write_entities(
                "gaulb", "gaul_level_1", [self._feature("f1")],
            )

        assert len(written) == 1
        assert len(es.bulk_calls) == 1
        body = es.bulk_calls[0]["body"]
        action = body[0]["index"]
        doc = body[1]
        # Index name and routing must be built from the INTERNAL ids —
        # matching what read_entities/count_entities would target — not the
        # raw external path-param ids.
        assert action["_index"] == "dynastore-c_akbbm7cinmqfe-items"
        assert "gaulb" not in action["_index"]
        assert action["routing"] == "col_internal456"
        assert doc["collection"] == "col_internal456"

    @pytest.mark.asyncio
    async def test_write_entities_passthrough_when_already_internal_or_unmapped(self):
        """No CatalogsProtocol registered (or id unmapped) — behaves exactly
        as before: the raw id is used verbatim."""
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=None,
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ):
            driver = ItemsElasticsearchDriver()
            await driver.write_entities("cat1", "col1", [self._feature("f1")])

        action = es.bulk_calls[0]["body"][0]["index"]
        assert action["_index"] == "dynastore-cat1-items"
        assert action["routing"] == "col1"

    @pytest.mark.asyncio
    async def test_write_and_read_routing_parity_for_diverged_ids(self):
        """Regression for #2999: write_entities and read_entities must agree
        on the SAME index name and ``_routing`` value for the same logical
        (external) catalog/collection pair, even when the external and
        internal ids diverge. Before the fix, write_entities targeted
        ``dynastore-gaulb-items`` (built from the raw external catalog_id)
        while read_entities (already resolving) targeted
        ``dynastore-c_akbbm7cinmqfe-items`` — two different indices/shards,
        so a write landed on a shard reads never queried.
        """
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        fake_catalogs = _FakeCatalogsProtocol(
            catalog_map={"gaulb": "c_akbbm7cinmqfe"},
            collection_map={"gaul_level_1": "col_internal456"},
        )
        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        driver = ItemsElasticsearchDriver()

        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=fake_catalogs,
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ):
            await driver.write_entities(
                "gaulb", "gaul_level_1", [self._feature("f1")],
            )
            async for _ in driver.read_entities(
                "gaulb", "gaul_level_1", entity_ids=["f1"],
            ):
                pass

        write_action = es.bulk_calls[0]["body"][0]["index"]
        write_index, write_routing = write_action["_index"], write_action["routing"]

        read_call = es.get_calls[0]
        read_index, read_routing = read_call["index"], read_call["params"]["routing"]

        assert write_index == read_index == "dynastore-c_akbbm7cinmqfe-items"
        assert write_routing == read_routing == "col_internal456"


class TestWriteEntitiesPgCanonicalDetection:
    """#2864 — routing-based detection of "does this collection's WRITE
    routing have a PG canonical", and the resulting fork in
    ``write_entities``: PG-canonical present -> unchanged batched-PG-read
    hydration; absent -> feature-derived ``_source``, no PG read attempted,
    no ``_fetch_raw_rows`` "cannot resolve physical table" RuntimeError.
    """

    @staticmethod
    def _feature(item_id="f1", geometry=None, properties=None):
        from dynastore.models.ogc import Feature
        return Feature.model_validate({
            "id": item_id,
            "type": "Feature",
            "geometry": geometry or {"type": "Point", "coordinates": [12.0, 41.9]},
            "properties": properties or {"name": "Rome"},
        })

    @staticmethod
    def _resolved_driver(*, has_resolve_physical_table: bool):
        from dynastore.modules.storage.router import ResolvedDriver

        driver = MagicMock(spec=[] if not has_resolve_physical_table else ["resolve_physical_table"])
        return ResolvedDriver(driver=driver)

    @pytest.mark.asyncio
    async def test_es_only_routing_skips_pg_read_and_writes_from_feature(self):
        """ES-only routing (no PG driver in WRITE fan-out): write_entities
        must NOT call read_canonical_index_inputs at all, must NOT raise the
        _fetch_raw_rows RuntimeError, and must still issue a _bulk write
        whose _source carries the search-critical fields derived from the
        feature (geometry, bbox, properties, id/geoid identity)."""
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        canonical_read_mock = AsyncMock(
            side_effect=RuntimeError(
                "canonical_index_read._fetch_raw_rows: cannot resolve "
                "physical table for cat1/col1"
            )
        )
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=canonical_read_mock,
        ), patch(
            "dynastore.modules.storage.router.get_write_drivers",
            new=AsyncMock(return_value=[
                self._resolved_driver(has_resolve_physical_table=False),
            ]),
        ):
            driver = ItemsElasticsearchDriver()
            written = await driver.write_entities(
                "cat1", "col1", [self._feature("f1")],
            )

        # The batched PG read was never attempted — routing-based detection,
        # not exception-based (would have raised RuntimeError if called).
        canonical_read_mock.assert_not_awaited()
        assert len(written) == 1
        assert len(es.bulk_calls) == 1
        doc = es.bulk_calls[0]["body"][1]
        assert doc["id"] == "f1"
        assert doc["geometry"]["type"] == "Point"
        assert list(doc["geometry"]["coordinates"]) == pytest.approx([12.0, 41.9])
        assert doc["bbox"] == pytest.approx([12.0, 41.9, 12.0, 41.9])
        assert doc["properties"]["extras"]["name"] == "Rome"

    @pytest.mark.asyncio
    async def test_pg_canonical_routing_still_uses_hydration_path_unchanged(self):
        """Pin: a collection whose WRITE routing DOES resolve a PG-capable
        driver must still go through read_canonical_index_inputs — the fix
        must not divert PG-backed writes onto the feature-only path."""
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )
        from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        canonical_read_mock = AsyncMock(
            return_value={
                "f1": CanonicalIndexInput(
                    row={"geoid": "f1"},
                    geometry={"type": "Point", "coordinates": [1.0, 2.0]},
                    bbox=[1.0, 2.0, 1.0, 2.0],
                    user_properties={"from_pg": True},
                )
            }
        )
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=canonical_read_mock,
        ), patch(
            "dynastore.modules.storage.router.get_write_drivers",
            new=AsyncMock(return_value=[
                self._resolved_driver(has_resolve_physical_table=True),
            ]),
        ):
            driver = ItemsElasticsearchDriver()
            await driver.write_entities("cat1", "col1", [self._feature("f1")])

        canonical_read_mock.assert_awaited_once()
        doc = es.bulk_calls[0]["body"][1]
        # Hydrated from the mocked PG canonical row, not the feature.
        assert doc["properties"]["extras"]["from_pg"] is True
        assert doc["geometry"]["type"] == "Point"
        assert list(doc["geometry"]["coordinates"]) == pytest.approx([1.0, 2.0])

    @pytest.mark.asyncio
    async def test_pending_pg_collection_is_activated_before_canonical_read(self):
        """#3046 — routing says PG is in the write fan-out, but the
        collection was never activated (e.g. created by a bulk harvester
        that never goes through ItemService.upsert's own lazy-activation
        gate). write_entities must activate it itself, before the batched
        canonical read — otherwise every batch would deterministically hit
        _fetch_raw_rows's "cannot resolve physical table" RuntimeError."""
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )
        from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        canonical_read_mock = AsyncMock(
            return_value={
                "f1": CanonicalIndexInput(
                    row={"geoid": "f1"},
                    geometry={"type": "Point", "coordinates": [1.0, 2.0]},
                    bbox=[1.0, 2.0, 1.0, 2.0],
                    user_properties={"from_pg": True},
                )
            }
        )
        catalogs = MagicMock()
        catalogs.resolve_catalog_id = AsyncMock(return_value=None)
        catalogs.collections.resolve_collection_id = AsyncMock(return_value=None)
        catalogs.ensure_alive = AsyncMock(return_value=None)
        catalogs.is_active = AsyncMock(return_value=False)
        catalogs.activate_collection = AsyncMock(return_value=None)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=canonical_read_mock,
        ), patch(
            "dynastore.modules.storage.router.get_write_drivers",
            new=AsyncMock(return_value=[
                self._resolved_driver(has_resolve_physical_table=True),
            ]),
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=catalogs,
        ):
            driver = ItemsElasticsearchDriver()
            await driver.write_entities("cat1", "col1", [self._feature("f1")])

        catalogs.ensure_alive.assert_awaited_once_with(
            "cat1", "col1", db_resource=None,
        )
        catalogs.is_active.assert_awaited_once_with(
            "cat1", "col1", db_resource=None,
        )
        catalogs.activate_collection.assert_awaited_once()
        canonical_read_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_already_active_pg_collection_skips_reactivation(self):
        """An already-activated collection must not pay for a redundant
        activate_collection call on every write."""
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )
        from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        canonical_read_mock = AsyncMock(
            return_value={
                "f1": CanonicalIndexInput(
                    row={"geoid": "f1"},
                    geometry={"type": "Point", "coordinates": [1.0, 2.0]},
                    bbox=[1.0, 2.0, 1.0, 2.0],
                    user_properties={"from_pg": True},
                )
            }
        )
        catalogs = MagicMock()
        catalogs.resolve_catalog_id = AsyncMock(return_value=None)
        catalogs.collections.resolve_collection_id = AsyncMock(return_value=None)
        catalogs.ensure_alive = AsyncMock(return_value=None)
        catalogs.is_active = AsyncMock(return_value=True)
        catalogs.activate_collection = AsyncMock(return_value=None)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=canonical_read_mock,
        ), patch(
            "dynastore.modules.storage.router.get_write_drivers",
            new=AsyncMock(return_value=[
                self._resolved_driver(has_resolve_physical_table=True),
            ]),
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=catalogs,
        ):
            driver = ItemsElasticsearchDriver()
            await driver.write_entities("cat1", "col1", [self._feature("f1")])

        catalogs.is_active.assert_awaited_once()
        catalogs.activate_collection.assert_not_awaited()
        canonical_read_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_activation_lifecycle_failure_degrades_to_feature_only(self):
        """A collection still in a transitional lifecycle (e.g.
        PROVISIONING) must not crash the write. ensure_canonical_source_ready
        swallows a CollectionNotAliveError from ensure_alive the same way
        has_canonical_source swallows a routing-resolution error — this is a
        background/secondary-index write path with no HTTP client to hand a
        409 to; the caller falls back to the feature-derived doc."""
        from dynastore.modules.catalog.collection_service import CollectionNotAliveError
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        canonical_read_mock = AsyncMock(return_value={})
        catalogs = MagicMock()
        catalogs.resolve_catalog_id = AsyncMock(return_value=None)
        catalogs.collections.resolve_collection_id = AsyncMock(return_value=None)
        catalogs.ensure_alive = AsyncMock(
            side_effect=CollectionNotAliveError("cat1", "col1", "provisioning")
        )
        catalogs.is_active = AsyncMock(return_value=False)
        catalogs.activate_collection = AsyncMock(return_value=None)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=canonical_read_mock,
        ), patch(
            "dynastore.modules.storage.router.get_write_drivers",
            new=AsyncMock(return_value=[
                self._resolved_driver(has_resolve_physical_table=True),
            ]),
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=catalogs,
        ):
            driver = ItemsElasticsearchDriver()
            # Must not raise CollectionNotAliveError out of write_entities.
            await driver.write_entities("cat1", "col1", [self._feature("f1")])

        catalogs.ensure_alive.assert_awaited_once()
        catalogs.activate_collection.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_write_drivers_resolution_failure_degrades_to_feature_only(self):
        """A routing-resolution error (e.g. ConfigResolutionError) must not
        block the ES write — it degrades to "no PG canonical" and the write
        still succeeds via the feature-derived fallback."""
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ), patch(
            "dynastore.modules.storage.router.get_write_drivers",
            new=AsyncMock(side_effect=RuntimeError("routing unavailable")),
        ):
            driver = ItemsElasticsearchDriver()
            written = await driver.write_entities(
                "cat1", "col1", [self._feature("f1")],
            )

        assert len(written) == 1
        assert len(es.bulk_calls) == 1


class TestWriteEntitiesGeometryPolicy:
    """#1248 — ES simplifies oversized geometry by default; exact geometry is
    the explicit opt-out (``simplify_geometry: false`` in the driver config)."""

    @staticmethod
    def _big_polygon_feature(item_id="big1"):
        """A feature whose geometry serializes well over the 10 MB limit
        when indexed exactly."""
        import math

        from dynastore.models.ogc import Feature

        # 300k vertices → GeoJSON serialization busts the 10 MB ES limit, so
        # simplify-by-default is observably different from the exact-geometry path.
        n = 300_000
        ring = [
            [math.cos(2 * math.pi * i / n), math.sin(2 * math.pi * i / n)]
            for i in range(n)
        ] + [[1.0, 0.0]]
        return Feature.model_validate({
            "id": item_id,
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "properties": {},
        })

    async def _run_write(self, driver_config, feature):
        from dynastore.modules.storage.driver_config import (
            ItemsWritePolicy, WriteConflictPolicy,
        )

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(on_conflict=WriteConflictPolicy.UPDATE)
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_write_policy",
            AsyncMock(return_value=policy),
        ), patch.object(
            ItemsElasticsearchDriver, "_enforce_field_constraints",
            AsyncMock(return_value=None),
        ), patch.object(
            ItemsElasticsearchDriver, "get_driver_config",
            AsyncMock(return_value=driver_config),
        ):
            driver = ItemsElasticsearchDriver()
            await driver.write_entities("cat1", "col1", [feature])
        return es

    @pytest.mark.asyncio
    async def test_oversized_geometry_simplified_by_default(self):
        """Default config (simplify_geometry not set → True): writing a
        300k-vertex polygon must produce a SHRUNK geometry (not 300_001
        vertices) and stamp ``system.geometry_simplification``.

        ``maybe_simplify_for_es`` is patched to return a deterministic result so
        the test does not require shapely to be installed in this environment.
        """
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchDriverConfig,
        )

        feature = self._big_polygon_feature()
        simplified_geom = {"type": "Point", "coordinates": [0.0, 0.0]}  # stub shrunk
        with patch(
            "dynastore.modules.storage.drivers.elasticsearch.maybe_simplify_for_es",
            side_effect=lambda doc, *, simplify, **kwargs: (
                {**doc, "geometry": simplified_geom} if simplify else doc,
                0.001 if simplify else 1.0,
                "tolerance" if simplify else "none",
            ),
        ):
            es = await self._run_write(ItemsElasticsearchDriverConfig(), feature)

        assert len(es.bulk_calls) == 1
        body = es.bulk_calls[0]["body"]
        doc = body[1]
        geom = doc.get("geometry")
        assert geom, "geometry must not be empty/{} on default write"
        # Geometry was shrunk by the simplification stub — not the raw 300_001.
        assert geom["type"] == "Point", "oversized geometry must be simplified by default"
        # Simplification metadata stamped in the canonical system container (#1828).
        gs = doc.get("system", {}).get("geometry_simplification", {})
        assert gs.get("mode") == "tolerance"

    @pytest.mark.asyncio
    async def test_exact_geometry_round_trips_when_disabled(self):
        """simplify_geometry=False: the indexed doc carries the FULL geometry
        — the 300k-vertex polygon is indexed verbatim (explicit opt-out)."""
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchDriverConfig,
        )

        feature = self._big_polygon_feature()
        es = await self._run_write(
            ItemsElasticsearchDriverConfig(simplify_geometry=False), feature,
        )

        assert len(es.bulk_calls) == 1
        body = es.bulk_calls[0]["body"]
        doc = body[1]
        geom = doc.get("geometry")
        assert geom, "geometry must not be empty/{} when simplification is disabled"
        assert geom["type"] == "Polygon"
        # Full vertex count preserved — geometry indexed verbatim.
        assert len(geom["coordinates"][0]) == 300_001
        # No simplification metadata stamped when simplification is disabled.
        # Since #1828 the canonical location is system.geometry_simplification;
        # the legacy flat keys are no longer written.
        system = doc.get("system", {})
        assert "geometry_simplification" not in system

    @pytest.mark.asyncio
    async def test_simplification_runs_only_when_flag_enabled(self):
        """simplify_geometry=True routes through _apply_geometry_simplification,
        which writes the canonical system.geometry_simplification container
        (#1828 Phase 2 — flat _simplification_mode root key no longer written).

        ``maybe_simplify_for_es`` is patched to return a deterministic result so
        the test does not require shapely to be installed in this environment.
        """
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchDriverConfig,
        )

        feature = self._big_polygon_feature()
        simplified_geom = {"type": "Point", "coordinates": [0.0, 0.0]}  # stub shrunk
        with patch(
            "dynastore.modules.storage.drivers.elasticsearch.maybe_simplify_for_es",
            side_effect=lambda doc, *, simplify, **kwargs: (
                {**doc, "geometry": simplified_geom} if simplify else doc,
                0.001 if simplify else 1.0,
                "tolerance" if simplify else "none",
            ),
        ):
            es = await self._run_write(
                ItemsElasticsearchDriverConfig(simplify_geometry=True), feature,
            )

        body = es.bulk_calls[0]["body"]
        doc = body[1]
        # Canonical system container carries the simplification metadata.
        gs = doc.get("system", {}).get("geometry_simplification", {})
        assert gs.get("mode") == "tolerance"
        assert gs.get("factor") == pytest.approx(0.001)
        # Old flat keys must NOT be present on new writes.
        assert "_simplification_mode" not in doc
        assert "_simplification_factor" not in doc


class TestLocationReportsTenantIndex:
    @pytest.mark.asyncio
    async def test_includes_routing_in_canonical_uri(self):
        with patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ):
            driver = ItemsElasticsearchDriver()
            loc = await driver.location("cat1", "col1")

        assert loc.identifiers["index"] == "dynastore-cat1-items"
        assert loc.identifiers["routing"] == "col1"
        assert loc.canonical_uri == "es://dynastore-cat1-items?routing=col1"


# --- #914 — pin index_bulk response-shape parsing + silent no-op WARN ---

class _StubEsBulk:
    """Minimal ES client stub whose ``bulk`` returns a caller-provided shape."""

    def __init__(self, bulk_response):
        self._bulk_response = bulk_response
        self.bulk_calls: list = []

    async def bulk(self, *, body, params=None, **kwargs):
        self.bulk_calls.append({"body": body, "params": params})
        return self._bulk_response


def _make_op(entity_id="f1", *, op_type="upsert", entity_type="item", payload=None):
    from dynastore.models.protocols.indexer import IndexOp

    return IndexOp(
        op_type=op_type,
        entity_type=entity_type,
        entity_id=entity_id,
        payload=payload or {
            "id": entity_id, "type": "Feature", "collection": "col1",
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
            "properties": {},
        },
    )


def _make_ctx():
    from dynastore.models.protocols.indexer import IndexContext

    return IndexContext(catalog="cat1", collection="col1", entity_type="item")


def _fake_canonical_inputs(catalog_id, collection_id, geoids, db_resource=None):
    """Stand in for the raw-PG read (#1800): one minimal canonical input per
    geoid so ``build_canonical_index_doc`` runs for real with ``id``/
    ``catalog_id``/``collection_id`` populated, exercising the same body
    construction the response-shape assertions depend on. ``op.payload`` is
    ignored by ``index_bulk`` (the canonical raw-row path supersedes it)."""
    from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput

    return {g: CanonicalIndexInput(row={"geoid": g}) for g in geoids}


def _patch_bulk_dependencies(es, *, has_pg_write_canonical=True):
    """Wire the module-level helpers used inside ``index_bulk``/``index``.

    ``has_pg_write_canonical`` defaults to True so the existing response-shape
    tests keep exercising the canonical-hydration path unchanged; pass False
    to exercise the ES-only skip-the-canonical-read branch (#2884 follow-up).
    """
    return [
        patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ),
        patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ),
        patch(
            "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "dynastore.modules.elasticsearch.items_projection.resolve_catalog_known_fields",
            new=AsyncMock(return_value={}),
        ),
        patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=AsyncMock(side_effect=_fake_canonical_inputs),
        ),
        patch(
            "dynastore.modules.storage.drivers.elasticsearch.has_canonical_source",
            new=AsyncMock(return_value=has_pg_write_canonical),
        ),
        patch(
            "dynastore.modules.storage.drivers.elasticsearch.ensure_canonical_source_ready",
            new=AsyncMock(return_value=None),
        ),
    ]


class TestIndexBulkResponseShapes:
    """Pin response-shape parsing for ``ItemsElasticsearchDriver.index_bulk``.

    The dispatcher logs ``post_commit_inline`` whenever ``BulkResult.failed == 0``
    (``index_dispatcher.py``), so a ``(total>0, succeeded=0, failed=0)`` result
    is invisible without the #914 WARN. These tests pin:

    * happy path → no WARN, succeeded matches op count
    * per-item error → no WARN, failed matches
    * silent no-op (resp.items empty) → WARN fires, BulkResult shape preserved
    * resp is not a dict → WARN fires with ``resp_type`` reflecting actual type
    """

    @pytest.mark.asyncio
    async def test_happy_path_succeeded_matches_ops(self, caplog):
        es = _StubEsBulk({
            "errors": False,
            "items": [
                {"index": {"_id": "f1", "result": "created", "status": 201}},
                {"index": {"_id": "f2", "result": "created", "status": 201}},
            ],
        })
        ops = [_make_op("f1"), _make_op("f2")]
        ctx = _make_ctx()

        with caplog.at_level("WARNING"):
            patches = _patch_bulk_dependencies(es)
            for p in patches:
                p.start()
            try:
                driver = ItemsElasticsearchDriver()
                result = await driver.index_bulk(ctx, ops)
            finally:
                for p in patches:
                    p.stop()

        assert result.total == 2
        assert result.succeeded == 2
        assert result.failed == 0
        assert "ES bulk returned a shape" not in caplog.text

    @pytest.mark.asyncio
    async def test_per_item_error_counted_as_failed(self, caplog):
        es = _StubEsBulk({
            "errors": True,
            "items": [
                {"index": {"_id": "f1", "result": "created", "status": 201}},
                {"index": {
                    "_id": "f2", "status": 400,
                    "error": {"type": "mapper_parsing", "reason": "boom"},
                }},
            ],
        })
        ops = [_make_op("f1"), _make_op("f2")]
        ctx = _make_ctx()

        with caplog.at_level("WARNING"):
            patches = _patch_bulk_dependencies(es)
            for p in patches:
                p.start()
            try:
                driver = ItemsElasticsearchDriver()
                result = await driver.index_bulk(ctx, ops)
            finally:
                for p in patches:
                    p.stop()

        assert result.total == 2
        assert result.succeeded == 1
        assert result.failed == 1
        assert result.failures[0]["id"] == "f2"
        assert "boom" in result.failures[0]["reason"]
        assert "ES bulk returned a shape" not in caplog.text

    @pytest.mark.asyncio
    async def test_silent_noop_empty_items_triggers_warn(self, caplog):
        """The #914 fingerprint: bulk responded but ``items`` is empty.

        Either the request didn't reach ES (network shape bug) or ES
        rejected every op at a layer that returns no per-item rows. The
        WARN line dumps ``resp_type`` / ``resp_keys`` / ``items_len`` /
        ``errors`` so an operator can disambiguate from the log.
        """
        es = _StubEsBulk({"errors": False, "items": []})
        ops = [_make_op("f1"), _make_op("f2")]
        ctx = _make_ctx()

        with caplog.at_level("WARNING"):
            patches = _patch_bulk_dependencies(es)
            for p in patches:
                p.start()
            try:
                driver = ItemsElasticsearchDriver()
                result = await driver.index_bulk(ctx, ops)
            finally:
                for p in patches:
                    p.stop()

        assert result.total == 2
        assert result.succeeded == 0
        assert result.failed == 0
        assert "ES bulk returned a shape" in caplog.text
        assert "items_len=0" in caplog.text
        assert "resp_type=dict" in caplog.text

    @pytest.mark.asyncio
    async def test_silent_noop_non_dict_response_triggers_warn(self, caplog):
        """Defence-in-depth: if some future client returns a non-dict (e.g. an
        ObjectApiResponse-like wrapper), the parser short-circuits ``items``
        to ``[]`` and the WARN must surface the actual type."""

        class _NotADict:
            def __repr__(self):
                return "<NotADict>"

        es = _StubEsBulk(_NotADict())
        ops = [_make_op("f1")]
        ctx = _make_ctx()

        with caplog.at_level("WARNING"):
            patches = _patch_bulk_dependencies(es)
            for p in patches:
                p.start()
            try:
                driver = ItemsElasticsearchDriver()
                result = await driver.index_bulk(ctx, ops)
            finally:
                for p in patches:
                    p.stop()

        assert result.total == 1
        assert result.succeeded == 0
        assert result.failed == 0
        assert "ES bulk returned a shape" in caplog.text
        assert "resp_type=_NotADict" in caplog.text

    @pytest.mark.asyncio
    async def test_all_ops_skipped_by_entity_type_filter_returns_early(self, caplog):
        """When every op has ``entity_type != 'item'``, ``body`` stays empty
        and the early ``return BulkResult(total=len(ops))`` at the
        ``if not body:`` guard fires — BEFORE the silent-no-op WARN block.

        Pinning this gap so a future refactor that swaps the early-return
        for the parse path forces an explicit decision: should a misrouted
        batch (no items reached ES) WARN or stay silent?
        """
        ops = [
            _make_op("c1", entity_type="catalog"),
            _make_op("co1", entity_type="collection"),
        ]
        ctx = _make_ctx()
        es = _StubEsBulk({"errors": False, "items": []})  # would WARN if reached

        with caplog.at_level("WARNING"):
            patches = _patch_bulk_dependencies(es)
            for p in patches:
                p.start()
            try:
                driver = ItemsElasticsearchDriver()
                result = await driver.index_bulk(ctx, ops)
            finally:
                for p in patches:
                    p.stop()

        assert result.total == 2
        assert result.succeeded == 0
        assert result.failed == 0
        assert es.bulk_calls == []
        assert "ES bulk returned a shape" not in caplog.text

    @pytest.mark.asyncio
    async def test_indexed_doc_carries_catalog_id_for_search_filter(self):
        """#914 — ``SearchService._build_item_query`` appends
        ``{"term": {"catalog_id": body.catalog_id}}`` when a catalog is
        scoped, so the indexed doc MUST expose ``catalog_id`` at top-level
        or the filter excludes every hit even when the tenant-scoped index
        is the one being queried. Pin both code paths that build the doc:

        * ``op.payload`` carrying only the Feature shape (the upstream
          ``item_service.upsert`` path that dumps ``Feature`` via
          ``model_dump`` — never sets ``catalog_id``).
        * ``_serialize_item`` fallback when ``op.payload`` is ``None``
          (covered by a separate test that injects the serializer; here
          we focus on the payload path which is the production hot path).
        """
        es = _StubEsBulk({
            "errors": False,
            "items": [
                {"index": {"_id": "f1", "result": "created", "status": 201}},
            ],
        })
        ops = [_make_op(
            "f1",
            payload={
                "id": "f1", "type": "Feature", "collection": "col1",
                "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                "properties": {},
            },
        )]
        ctx = _make_ctx()  # catalog="cat1", collection="col1"

        patches = _patch_bulk_dependencies(es)
        for p in patches:
            p.start()
        try:
            driver = ItemsElasticsearchDriver()
            await driver.index_bulk(ctx, ops)
        finally:
            for p in patches:
                p.stop()

        assert len(es.bulk_calls) == 1
        body = es.bulk_calls[0]["body"]
        # body alternates [action, doc, action, doc, ...]
        docs = [body[i] for i in range(1, len(body), 2)]
        assert len(docs) == 1
        assert docs[0].get("catalog_id") == "cat1", (
            "indexed doc must carry top-level catalog_id so "
            "SearchService's term filter matches (#914 fix)"
        )


class TestIndexAndIndexBulkPgCanonicalDetection:
    """Follow-up to #2864/#2884: ``write_entities`` was fixed to skip the PG
    canonical read for ES-only routing, but ``index`` and ``index_bulk`` —
    reached via the index-dispatcher fan-out for collections whose only
    materialization target is the ES driver — called
    ``read_canonical_index_inputs`` unconditionally. On a real ES-only
    collection this deterministically raised ``_fetch_raw_rows``'s "cannot
    resolve physical table" RuntimeError, which propagated out of
    ``ItemService.upsert`` even though the primary write had already
    committed — surfacing as a stuck/looping harvest job that never
    progressed."""

    @pytest.mark.asyncio
    async def test_index_bulk_skips_pg_read_and_builds_feature_derived_doc(self):
        es = _StubEsBulk({
            "errors": False,
            "items": [{"index": {"_id": "f1", "result": "created", "status": 201}}],
        })
        ops = [_make_op("f1", payload={
            "id": "f1", "type": "Feature", "collection": "col1",
            "geometry": {"type": "Point", "coordinates": [12.0, 41.9]},
            "properties": {"name": "Rome"},
        })]
        ctx = _make_ctx()
        canonical_read_mock = AsyncMock(
            side_effect=RuntimeError(
                "canonical_index_read._fetch_raw_rows: cannot resolve "
                "physical table for cat1/col1"
            )
        )

        patches = _patch_bulk_dependencies(es, has_pg_write_canonical=False)
        with patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=canonical_read_mock,
        ):
            for p in patches:
                if p.attribute != "read_canonical_index_inputs":
                    p.start()
            try:
                driver = ItemsElasticsearchDriver()
                result = await driver.index_bulk(ctx, ops)
            finally:
                for p in patches:
                    if p.attribute != "read_canonical_index_inputs":
                        p.stop()

        canonical_read_mock.assert_not_awaited()
        assert result.total == 1
        assert result.succeeded == 1
        doc = es.bulk_calls[0]["body"][1]
        assert doc["id"] == "f1"
        assert doc["properties"]["extras"]["name"] == "Rome"

    @pytest.mark.asyncio
    async def test_index_single_op_skips_pg_read_and_builds_feature_derived_doc(self):
        es = _StubEs(exists=True)
        ctx = _make_ctx()
        op = _make_op("f1", payload={
            "id": "f1", "type": "Feature", "collection": "col1",
            "geometry": {"type": "Point", "coordinates": [12.0, 41.9]},
            "properties": {"name": "Rome"},
        })
        canonical_read_mock = AsyncMock(
            side_effect=RuntimeError(
                "canonical_index_read._fetch_raw_rows: cannot resolve "
                "physical table for cat1/col1"
            )
        )
        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
            new=AsyncMock(return_value=None),
        ), patch(
            "dynastore.modules.elasticsearch.items_projection.resolve_catalog_known_fields",
            new=AsyncMock(return_value={}),
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=canonical_read_mock,
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.has_canonical_source",
            new=AsyncMock(return_value=False),
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_simplify_geometry",
            new=AsyncMock(return_value=False),
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_simplify_max_bytes",
            new=AsyncMock(return_value=10_000_000),
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_snap_to_grid_config",
            new=AsyncMock(return_value=(False, 0.00001)),
        ):
            driver = ItemsElasticsearchDriver()
            await driver.index(ctx, op)

        canonical_read_mock.assert_not_awaited()
        assert len(es.index_calls) == 1
        doc = es.index_calls[0]["body"]
        assert doc["id"] == "f1"
        assert doc["properties"]["extras"]["name"] == "Rome"

    @pytest.mark.asyncio
    async def test_index_bulk_activates_pending_collection_before_canonical_read(self):
        """#3046 — index_bulk's dispatch fan-out must lazily activate a
        pending collection before the canonical read, mirroring
        write_entities's gate. _patch_bulk_dependencies normally mocks
        ensure_canonical_source_ready as an unconditional no-op, which left
        this call site's db_resource/catalog/collection wiring untested."""
        from sqlalchemy.ext.asyncio import AsyncConnection
        from dynastore.models.protocols.indexer import IndexContext

        es = _StubEsBulk({
            "errors": False,
            "items": [{"index": {"_id": "f1", "result": "created", "status": 201}}],
        })
        ops = [_make_op("f1")]
        sentinel_conn = MagicMock(spec=AsyncConnection)
        ctx = IndexContext(
            catalog="cat1", collection="col1", entity_type="item",
            pg_conn=sentinel_conn,
        )
        catalogs = MagicMock()
        catalogs.ensure_alive = AsyncMock(return_value=None)
        catalogs.is_active = AsyncMock(return_value=False)
        catalogs.activate_collection = AsyncMock(return_value=None)

        patches = _patch_bulk_dependencies(es, has_pg_write_canonical=True)
        with patch(
            "dynastore.tools.discovery.get_protocol", return_value=catalogs,
        ):
            for p in patches:
                if p.attribute != "ensure_canonical_source_ready":
                    p.start()
            try:
                driver = ItemsElasticsearchDriver()
                result = await driver.index_bulk(ctx, ops)
            finally:
                for p in patches:
                    if p.attribute != "ensure_canonical_source_ready":
                        p.stop()

        catalogs.ensure_alive.assert_awaited_once_with(
            "cat1", "col1", db_resource=sentinel_conn,
        )
        catalogs.is_active.assert_awaited_once_with(
            "cat1", "col1", db_resource=sentinel_conn,
        )
        catalogs.activate_collection.assert_awaited_once()
        assert result.succeeded == 1

    @pytest.mark.asyncio
    async def test_index_single_op_activates_pending_collection_before_canonical_read(self):
        """Same #3046 coverage as the index_bulk variant above, for the
        single-op ``index()`` dispatch path."""
        from sqlalchemy.ext.asyncio import AsyncConnection
        from dynastore.models.protocols.indexer import IndexContext
        from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput

        es = _StubEs(exists=True)
        sentinel_conn = MagicMock(spec=AsyncConnection)
        ctx = IndexContext(
            catalog="cat1", collection="col1", entity_type="item",
            pg_conn=sentinel_conn,
        )
        op = _make_op("f1", payload={
            "id": "f1", "type": "Feature", "collection": "col1",
            "geometry": {"type": "Point", "coordinates": [12.0, 41.9]},
            "properties": {"name": "Rome"},
        })
        canonical_read_mock = AsyncMock(
            return_value={"f1": CanonicalIndexInput(row={"geoid": "f1"})}
        )
        catalogs = MagicMock()
        catalogs.ensure_alive = AsyncMock(return_value=None)
        catalogs.is_active = AsyncMock(return_value=False)
        catalogs.activate_collection = AsyncMock(return_value=None)

        with patch(
            "dynastore.modules.elasticsearch.client.get_client", return_value=es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
            new=AsyncMock(return_value=None),
        ), patch(
            "dynastore.modules.elasticsearch.items_projection.resolve_catalog_known_fields",
            new=AsyncMock(return_value={}),
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=canonical_read_mock,
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.has_canonical_source",
            new=AsyncMock(return_value=True),
        ), patch(
            "dynastore.tools.discovery.get_protocol", return_value=catalogs,
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_simplify_geometry",
            new=AsyncMock(return_value=False),
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_snap_to_grid_config",
            new=AsyncMock(return_value=(False, 0.00001)),
        ), patch.object(
            ItemsElasticsearchDriver, "_resolve_simplify_max_bytes",
            new=AsyncMock(return_value=10_000_000),
        ):
            driver = ItemsElasticsearchDriver()
            await driver.index(ctx, op)

        catalogs.ensure_alive.assert_awaited_once_with(
            "cat1", "col1", db_resource=sentinel_conn,
        )
        catalogs.is_active.assert_awaited_once_with(
            "cat1", "col1", db_resource=sentinel_conn,
        )
        catalogs.activate_collection.assert_awaited_once()
        canonical_read_mock.assert_awaited_once()


# ---------------------------------------------------------------------------
# get_driver_config dispatches through _driver_config_class (#2049)
# ---------------------------------------------------------------------------


class TestGetDriverConfigDispatchesPerSubclass:
    """Each ES driver subclass returns its own config type from
    ``get_driver_config``, not the hardcoded ``ItemsElasticsearchDriverConfig``
    that the base used before #2049.
    """

    @pytest.mark.asyncio
    async def test_public_driver_returns_items_config(self):
        from dynastore.modules.storage.driver_config import ItemsElasticsearchDriverConfig

        driver = ItemsElasticsearchDriver()
        # No ConfigsProtocol registered → falls back to config_cls().
        config = await driver.get_driver_config("cat1", "col1")
        assert isinstance(config, ItemsElasticsearchDriverConfig), (
            f"public items driver must return ItemsElasticsearchDriverConfig, "
            f"got {type(config).__name__}"
        )

    @pytest.mark.asyncio
    async def test_private_driver_returns_private_config(self):
        from dynastore.modules.storage.driver_config import ItemsElasticsearchPrivateDriverConfig

        driver = ItemsElasticsearchPrivateDriver()
        config = await driver.get_driver_config("cat1", "col1")
        assert isinstance(config, ItemsElasticsearchPrivateDriverConfig), (
            f"private driver must return ItemsElasticsearchPrivateDriverConfig, "
            f"got {type(config).__name__}"
        )

    @pytest.mark.asyncio
    async def test_envelope_driver_returns_envelope_config(self):
        from dynastore.modules.storage.driver_config import ItemsElasticsearchEnvelopeDriverConfig
        from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
            ItemsElasticsearchEnvelopeDriver,
        )

        driver = ItemsElasticsearchEnvelopeDriver()
        config = await driver.get_driver_config("cat1", "col1")
        assert isinstance(config, ItemsElasticsearchEnvelopeDriverConfig), (
            f"envelope driver must return ItemsElasticsearchEnvelopeDriverConfig, "
            f"got {type(config).__name__}"
        )

    @pytest.mark.asyncio
    async def test_asset_driver_returns_asset_config(self):
        """Before #2049, AssetElasticsearchDriver.get_driver_config fell through
        to the _ElasticsearchBase fallback and parsed config via
        ItemsElasticsearchDriverConfig — silently wrong type.  After the fix
        it must return AssetElasticsearchDriverConfig."""
        from dynastore.modules.storage.driver_config import AssetElasticsearchDriverConfig
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

        driver = AssetElasticsearchDriver()
        config = await driver.get_driver_config("cat1", "col1")
        assert isinstance(config, AssetElasticsearchDriverConfig), (
            f"asset driver must return AssetElasticsearchDriverConfig, "
            f"got {type(config).__name__}"
        )

    def test_driver_config_class_attributes(self):
        """Each driver class declares the correct _driver_config_class — pinning
        the ClassVar so an accidental revert is caught at import time."""
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchDriverConfig,
            ItemsElasticsearchPrivateDriverConfig,
            ItemsElasticsearchEnvelopeDriverConfig,
            AssetElasticsearchDriverConfig,
        )
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver
        from dynastore.modules.storage.drivers.elasticsearch_envelope.driver import (
            ItemsElasticsearchEnvelopeDriver,
        )

        assert ItemsElasticsearchDriver._driver_config_class is ItemsElasticsearchDriverConfig
        assert ItemsElasticsearchPrivateDriver._driver_config_class is ItemsElasticsearchPrivateDriverConfig
        assert ItemsElasticsearchEnvelopeDriver._driver_config_class is ItemsElasticsearchEnvelopeDriverConfig
        assert AssetElasticsearchDriver._driver_config_class is AssetElasticsearchDriverConfig


# ---------------------------------------------------------------------------
# #2863 — asset driver index-existence caching + bulk batching
# ---------------------------------------------------------------------------
#
# Refs #2863: harvest with_assets=true into an ES-primary catalog indexed
# items and especially assets one document at a time, with an
# ``indices.exists`` HEAD before EVERY PUT (index creation is a
# once-per-catalog event) — millions of sequential HEAD+PUT round trips
# exhausted the shared AsyncOpenSearch client's connection pool and, since
# harvest tolerates per-write failures (#2828), silently dropped data.
#
# Each test below uses a catalog id unique to that test (mirroring
# ``test_write_entities_ensures_index_once_per_catalog``'s
# ``_ITEMS_INDEX_ENSURED_CATALOGS.discard`` idiom) so the process-lifetime
# ``@cached`` entries one test creates cannot leak into another.


def _index_not_found_error(index_name: str) -> Exception:
    """Build an exception shaped like opensearch-py's
    ``index_not_found_exception`` (``exc.info['error']['type']``), the
    signal :func:`dynastore.modules.storage.drivers.elasticsearch._is_index_not_found`
    / ``_bulk_response_missing_index`` key off of."""
    exc = Exception(f"index_not_found_exception: no such index [{index_name}]")
    exc.info = {"error": {"type": "index_not_found_exception", "index": index_name}}  # type: ignore[attr-defined]
    return exc


class TestAssetIndexExistenceCache:
    """``AssetElasticsearchDriver`` HEAD-before-write caching (#2863)."""

    def _patches(self, es):
        return [
            patch(
                "dynastore.modules.elasticsearch.client.get_client", return_value=es,
            ),
            patch(
                "dynastore.modules.elasticsearch.client.get_index_prefix",
                return_value="dynastore",
            ),
        ]

    @pytest.mark.asyncio
    async def test_index_asset_head_fires_once_per_catalog(self):
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

        es = _StubEs(exists=False)
        es.index = AsyncMock(return_value={"result": "created"})
        patches = self._patches(es)
        for p in patches:
            p.start()
        try:
            driver = AssetElasticsearchDriver()
            await driver.index_asset("catAssetCache1", {"asset_id": "a1"})
            await driver.index_asset("catAssetCache1", {"asset_id": "a2"})
            await driver.index_asset("catAssetCache1", {"asset_id": "a3"})
        finally:
            for p in patches:
                p.stop()

        # HEAD + create fire once; the third write skips both entirely.
        assert len(es.indices.exists_calls) == 1
        assert len(es.indices.create_calls) == 1

    @pytest.mark.asyncio
    async def test_write_entities_head_fires_once_per_catalog(self):
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

        es = _StubEs(exists=False)
        patches = self._patches(es)
        for p in patches:
            p.start()
        try:
            driver = AssetElasticsearchDriver()
            await driver.write_entities(
                "catAssetCache2", "col1", [{"asset_id": "a1"}],
            )
            await driver.write_entities(
                "catAssetCache2", "col1", [{"asset_id": "a2"}],
            )
        finally:
            for p in patches:
                p.stop()

        assert len(es.indices.exists_calls) == 1
        assert len(es.indices.create_calls) == 1
        assert len(es.bulk_calls) == 2  # the bulk write itself is never cached

    @pytest.mark.asyncio
    async def test_index_bulk_head_fires_once_and_batches_ops(self):
        """``index_bulk`` reuses ``_ensure_index_cached`` AND issues exactly
        one ``_bulk`` call for the whole op batch instead of one HEAD+PUT
        per op (#2863)."""
        from dynastore.models.protocols.indexer import IndexContext, IndexOp
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

        es = _StubEs(exists=False)
        es.bulk_result = {
            "items": [
                {"index": {"_id": "a1", "status": 201}},
                {"index": {"_id": "a2", "status": 201}},
                {"delete": {"_id": "a3", "status": 200}},
            ],
        }

        async def _bulk(*, body, params=None, **kwargs):
            es.bulk_calls.append({"body": body, "params": params})
            return es.bulk_result

        es.bulk = _bulk

        ctx = IndexContext(catalog="catAssetCache3", collection="col1", entity_type="asset")
        ops = [
            IndexOp(op_type="upsert", entity_type="asset", entity_id="a1",
                    payload={"asset_id": "a1"}),
            IndexOp(op_type="upsert", entity_type="asset", entity_id="a2",
                    payload={"asset_id": "a2"}),
            IndexOp(op_type="delete", entity_type="asset", entity_id="a3"),
        ]

        patches = self._patches(es)
        for p in patches:
            p.start()
        try:
            driver = AssetElasticsearchDriver()
            result = await driver.index_bulk(ctx, ops)
        finally:
            for p in patches:
                p.stop()

        assert len(es.indices.exists_calls) == 1
        assert len(es.indices.create_calls) == 1
        assert len(es.bulk_calls) == 1  # ONE _bulk call for the whole batch
        # 2 upsert actions (action + doc) + 1 delete action = 5 body entries.
        assert len(es.bulk_calls[0]["body"]) == 5
        assert result.total == 3
        assert result.succeeded == 3
        assert result.failed == 0

    @pytest.mark.asyncio
    async def test_index_bulk_per_item_error_does_not_fail_whole_batch(self):
        """One poison doc in a batch is reported in ``BulkResult.failures``
        — it must not take the other, good docs down with it (#2828 parity)."""
        from dynastore.models.protocols.indexer import IndexContext, IndexOp
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

        es = _StubEs(exists=True)
        es.bulk_result = {
            "errors": True,
            "items": [
                {"index": {"_id": "a1", "status": 201}},
                {"index": {
                    "_id": "a2", "status": 400,
                    "error": {"type": "mapper_parsing_exception", "reason": "boom"},
                }},
            ],
        }

        async def _bulk(*, body, params=None, **kwargs):
            es.bulk_calls.append({"body": body, "params": params})
            return es.bulk_result

        es.bulk = _bulk

        ctx = IndexContext(catalog="catAssetCache4", collection="col1", entity_type="asset")
        ops = [
            IndexOp(op_type="upsert", entity_type="asset", entity_id="a1",
                    payload={"asset_id": "a1"}),
            IndexOp(op_type="upsert", entity_type="asset", entity_id="a2",
                    payload={"asset_id": "a2"}),
        ]

        patches = self._patches(es)
        for p in patches:
            p.start()
        try:
            driver = AssetElasticsearchDriver()
            result = await driver.index_bulk(ctx, ops)
        finally:
            for p in patches:
                p.stop()

        assert result.total == 2
        assert result.succeeded == 1
        assert result.failed == 1
        assert result.failures[0]["id"] == "a2"
        assert "boom" in result.failures[0]["reason"]

    @pytest.mark.asyncio
    async def test_index_asset_invalidates_cache_on_index_not_found(self):
        """A write that fails with ``index_not_found_exception`` (the index
        was cached as existing, then dropped/rotated out from under this
        worker) forces the NEXT write to re-check instead of trusting the
        stale cache entry forever (#2863)."""
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

        es = _StubEs(exists=True)  # first ensure_index call: index already exists
        boom = _index_not_found_error("dynastore-catAssetCache5-assets")

        async def _index_boom(*, index, id, body, **kwargs):
            raise boom

        es.index = _index_boom

        patches = self._patches(es)
        for p in patches:
            p.start()
        try:
            driver = AssetElasticsearchDriver()
            with pytest.raises(Exception):
                await driver.index_asset("catAssetCache5", {"asset_id": "a1"})
            assert len(es.indices.exists_calls) == 1  # first ensure: cache miss

            # Cache was invalidated by the index_not_found failure — the next
            # write re-checks (a second HEAD) instead of trusting the stale
            # "exists" entry and PUTting straight into the same 404 forever.
            async def _index_ok(*, index, id, body, **kwargs):
                return {"result": "created"}

            es.index = _index_ok
            await driver.index_asset("catAssetCache5", {"asset_id": "a2"})
        finally:
            for p in patches:
                p.stop()

        assert len(es.indices.exists_calls) == 2

    @pytest.mark.asyncio
    async def test_write_entities_bulk_missing_index_invalidates_cache(self):
        """Same invalidation contract as above, through the ``_bulk`` response
        shape (``resp["items"][i]["index"]["error"]["type"]``) rather than a
        raised exception."""
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

        es = _StubEs(exists=True)
        es.bulk_result = {
            "errors": True,
            "items": [{
                "index": {
                    "_id": "a1", "status": 404,
                    "error": {"type": "index_not_found_exception", "reason": "no such index"},
                },
            }],
        }

        async def _bulk(*, body, params=None, **kwargs):
            es.bulk_calls.append({"body": body, "params": params})
            return es.bulk_result

        es.bulk = _bulk

        patches = self._patches(es)
        for p in patches:
            p.start()
        try:
            driver = AssetElasticsearchDriver()
            with pytest.raises(Exception):
                await driver.write_entities(
                    "catAssetCache6", "col1", [{"asset_id": "a1"}],
                )
            assert len(es.indices.exists_calls) == 1

            es.bulk_result = {"errors": False, "items": [
                {"index": {"_id": "a2", "status": 201}},
            ]}
            await driver.write_entities(
                "catAssetCache6", "col1", [{"asset_id": "a2"}],
            )
        finally:
            for p in patches:
                p.stop()

        # Second write re-checked the index (cache was invalidated).
        assert len(es.indices.exists_calls) == 2

    @pytest.mark.asyncio
    async def test_drop_storage_catalog_scope_invalidates_cache(self):
        """Dropping the whole per-catalog assets index must evict it from
        the existence cache — otherwise the next write skips the HEAD and
        PUTs straight into a 404 (#2863)."""
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

        es = _StubEs(exists=False)
        es.index = AsyncMock(return_value={"result": "created"})
        patches = self._patches(es)
        for p in patches:
            p.start()
        try:
            driver = AssetElasticsearchDriver()
            await driver.index_asset("catAssetCache7", {"asset_id": "a1"})
            assert len(es.indices.exists_calls) == 1

            await driver.drop_storage("catAssetCache7")

            await driver.index_asset("catAssetCache7", {"asset_id": "a2"})
        finally:
            for p in patches:
                p.stop()

        # drop_storage evicted the cache entry — the write after it re-checks.
        assert len(es.indices.exists_calls) == 2


class TestAssetDeleteIdempotent:
    """``AssetElasticsearchDriver.delete_asset`` redelivery safety.

    ``AssetEntitySyncSubscriber`` (#2494) now raises on indexer failure so
    the durable events plane retries — every entry in a retried batch is
    re-attempted, including ones that already succeeded. A delete must
    therefore tolerate deleting an already-absent document without raising
    (idempotent), while a real backend failure must still propagate so the
    subscriber's failure policy applies.
    """

    def _patches(self, es):
        return [
            patch(
                "dynastore.modules.elasticsearch.client.get_client", return_value=es,
            ),
            patch(
                "dynastore.modules.elasticsearch.client.get_index_prefix",
                return_value="dynastore",
            ),
        ]

    @pytest.mark.asyncio
    async def test_delete_asset_passes_ignore_404(self):
        """Every delete_asset call opts the ES client out of raising on a
        missing document — a redelivered delete for an already-deleted
        asset must be a no-op, not an error."""
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

        es = _StubEs()
        patches = self._patches(es)
        for p in patches:
            p.start()
        try:
            driver = AssetElasticsearchDriver()
            await driver.delete_asset("cat1", "asset-1")
        finally:
            for p in patches:
                p.stop()

        assert len(es.delete_calls) == 1
        assert es.delete_calls[0]["params"] == {"ignore": "404"}
        assert es.delete_calls[0]["id"] == "asset-1"

    @pytest.mark.asyncio
    async def test_delete_asset_redelivery_is_idempotent(self):
        """Deleting the same asset id twice (redelivery) never raises."""
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

        es = _StubEs()
        patches = self._patches(es)
        for p in patches:
            p.start()
        try:
            driver = AssetElasticsearchDriver()
            await driver.delete_asset("cat1", "asset-1")
            await driver.delete_asset("cat1", "asset-1")
        finally:
            for p in patches:
                p.stop()

        assert len(es.delete_calls) == 2

    @pytest.mark.asyncio
    async def test_delete_asset_real_failure_propagates(self):
        """A non-404 backend failure must NOT be swallowed — the caller
        (``AssetEntitySyncSubscriber``) needs to see it to apply its
        ``on_failure`` policy and trigger a durable retry."""
        from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

        es = _StubEs()
        es.delete = AsyncMock(side_effect=ConnectionError("cluster unreachable"))
        patches = self._patches(es)
        for p in patches:
            p.start()
        try:
            driver = AssetElasticsearchDriver()
            with pytest.raises(ConnectionError):
                await driver.delete_asset("cat1", "asset-1")
        finally:
            for p in patches:
                p.stop()
