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
from dynastore.models.protocols.storage_driver import Capability
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


class TestIsSecondaryFor:
    @pytest.mark.asyncio
    async def test_returns_true_when_listed(self):
        from dynastore.modules.storage.routing_config import (
            ItemsRoutingConfig, Operation, OperationDriverEntry,
        )

        mock_configs = AsyncMock()
        routing = ItemsRoutingConfig(operations={
            Operation.WRITE: [OperationDriverEntry(driver_ref="items_postgresql_driver")],
            Operation.READ: [OperationDriverEntry(driver_ref="items_elasticsearch_driver")],
        })
        mock_configs.get_config = AsyncMock(return_value=routing)

        with patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs):
            result = await _ElasticsearchBase._is_secondary_for(
                "items_elasticsearch_driver", "cat1", "col1"
            )
            assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_not_listed(self):
        from dynastore.modules.storage.routing_config import (
            ItemsRoutingConfig, Operation, OperationDriverEntry,
        )

        mock_configs = AsyncMock()
        routing = ItemsRoutingConfig(operations={
            Operation.WRITE: [OperationDriverEntry(driver_ref="items_postgresql_driver")],
            Operation.READ: [OperationDriverEntry(driver_ref="items_duckdb_driver")],
        })
        mock_configs.get_config = AsyncMock(return_value=routing)

        with patch("dynastore.tools.discovery.get_protocol", return_value=mock_configs):
            result = await _ElasticsearchBase._is_secondary_for(
                "elasticsearch", "cat1", "col1"
            )
            assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_error(self):
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=Exception("boom"),
        ):
            result = await _ElasticsearchBase._is_secondary_for(
                "elasticsearch", "cat1", "col1"
            )
            assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_no_configs(self):
        with patch("dynastore.tools.discovery.get_protocol", return_value=None):
            result = await _ElasticsearchBase._is_secondary_for(
                "elasticsearch", "cat1", "col1"
            )
            assert result is False


class TestItemsElasticsearchDriverMeta:
    def test_driver_class_name(self):
        driver = ItemsElasticsearchDriver()
        assert type(driver).__name__ == "ItemsElasticsearchDriver"

    def test_priority(self):
        driver = ItemsElasticsearchDriver()
        assert driver.priority == 50

    def test_capabilities(self):
        driver = ItemsElasticsearchDriver()
        assert Capability.STREAMING in driver.capabilities
        assert Capability.SOFT_DELETE in driver.capabilities

    def test_read_flavour_hints(self):
        """Read-flavour capabilities moved from ``Capability`` to
        ``Hint`` in PR #3b — the driver self-declares them via
        ``supported_hints``."""
        from dynastore.modules.storage.hints import Hint
        driver = ItemsElasticsearchDriver()
        assert Hint.FULLTEXT in driver.supported_hints
        assert Hint.SPATIAL_FILTER in driver.supported_hints
        assert Hint.AGGREGATION in driver.supported_hints

    @pytest.mark.asyncio
    async def test_export_entities_not_implemented(self):
        driver = ItemsElasticsearchDriver()
        with pytest.raises(NotImplementedError):
            await driver.export_entities("cat1", "col1")


class TestItemsElasticsearchPrivateDriverMeta:
    def test_driver_class_name(self):
        driver = ItemsElasticsearchPrivateDriver()
        assert type(driver).__name__ == "ItemsElasticsearchPrivateDriver"

    def test_priority(self):
        driver = ItemsElasticsearchPrivateDriver()
        assert driver.priority == 51

    def test_capabilities(self):
        driver = ItemsElasticsearchPrivateDriver()
        assert Capability.STREAMING in driver.capabilities
        assert Capability.SOFT_DELETE not in driver.capabilities

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
        from dynastore.models.protocols.typed_driver import _registered_pairs
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchPrivateDriverConfig,
        )

        pairs = _registered_pairs()
        assert ItemsElasticsearchPrivateDriverConfig in pairs
        assert pairs[ItemsElasticsearchPrivateDriverConfig] is ItemsElasticsearchPrivateDriver
        assert ItemsElasticsearchPrivateDriverConfig.class_key() == "items_elasticsearch_private_driver"

    def test_visible_in_registry(self):
        """The wire identity is exposed in ``list_registered_configs()`` so
        the ``/configs/registry`` and tree-view endpoints surface the driver.
        """
        from dynastore.modules.db_config.plugin_config import list_registered_configs
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
        assert "geo_bounding_box" in geo_filter

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
        self.count_result = {"count": 0}

    async def bulk(self, *, body, params=None, **kwargs):
        self.bulk_calls.append({"body": body, "params": params, "kwargs": kwargs})
        return {"items": []}

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
            n = await driver.delete_entities("cat1", "col1", ["a", "b"])

        assert n == 2
        assert es.delete_calls == [
            {"index": "dynastore-cat1-items", "id": "a",
             "params": {"routing": "col1", "ignore": "404"}},
            {"index": "dynastore-cat1-items", "id": "b",
             "params": {"routing": "col1", "ignore": "404"}},
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
            assert doc["_asset_id"] == "asset-7"
            assert doc["_valid_from"] == "2026-04-27T00:00:00Z"
            # _valid_to is None in context → key skipped
            assert "_valid_to" not in doc
        assert es.bulk_calls[0]["params"] == {"refresh": "false"}

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
        # doc id should be EXT-1 followed by an underscore + 14-digit-ish
        # timestamp suffix (YYYYMMDDTHHMMSS + microseconds).
        assert action_id.startswith("EXT-1_")
        assert len(action_id) > len("EXT-1_")


class TestWriteEntitiesGeometryPolicy:
    """#1248 — ES indexes EXACT geometry by default; simplification opt-in."""

    @staticmethod
    def _big_polygon_feature(item_id="big1"):
        """A feature whose geometry serializes well over the 10 MB limit
        when indexed exactly."""
        import math

        from dynastore.models.ogc import Feature

        # 300k vertices → GeoJSON serialization busts the 10 MB ES limit, so
        # exact-by-default is observably different from the lossy shrink path.
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
    async def test_exact_geometry_round_trips_by_default(self):
        """Default config (simplify_geometry=False): the indexed doc carries
        the FULL geometry — never an empty ``{}`` — so the geometry round-trips
        into the ES index payload (the #1248 symptom)."""
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchDriverConfig,
        )

        feature = self._big_polygon_feature()
        es = await self._run_write(ItemsElasticsearchDriverConfig(), feature)

        assert len(es.bulk_calls) == 1
        body = es.bulk_calls[0]["body"]
        doc = body[1]
        geom = doc.get("geometry")
        assert geom, "geometry must not be empty/{} on exact-by-default write"
        assert geom["type"] == "Polygon"
        # Full vertex count preserved — geometry indexed verbatim even though
        # it busts the 10 MB ES limit (simplify_to_fit was NOT called).
        assert len(geom["coordinates"][0]) == 300_001
        # No simplification metadata stamped when simplify disabled.
        assert "_simplification_mode" not in doc
        assert "_simplification_factor" not in doc

    @pytest.mark.asyncio
    async def test_simplification_runs_only_when_flag_enabled(self):
        """simplify_geometry=True restores the lossy shrink path."""
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchDriverConfig,
        )

        feature = self._big_polygon_feature()
        es = await self._run_write(
            ItemsElasticsearchDriverConfig(simplify_geometry=True), feature,
        )

        body = es.bulk_calls[0]["body"]
        doc = body[1]
        # Lossy path stamped the metadata and shrank the geometry.
        assert doc.get("_simplification_mode") in ("tolerance", "bbox")
        geom = doc.get("geometry")
        assert geom
        assert len(geom["coordinates"][0]) < 300_001


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


def _patch_bulk_dependencies(es):
    """Wire the module-level helpers used inside ``index_bulk``."""
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
