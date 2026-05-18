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
        policy = ItemsWritePolicy(
            on_conflict=WriteConflictPolicy.REFUSE,
            external_id_field="properties.ext_id",
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

        es = _StubEs(exists=True)
        policy = ItemsWritePolicy(
            on_conflict=WriteConflictPolicy.NEW_VERSION,
            external_id_field="properties.ext_id",
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
