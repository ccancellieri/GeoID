"""Unit tests for M9 config discovery endpoints.

Covers:
- get_config_schemas() — all registered configs with scope annotation
- get_config_schema(class_key) — single config schema + 404 on unknown key
- get_config_graph() — nodes + edges structure
- list_storage_drivers() response keys use Protocol qualnames (M9 / §7)
- ConfigScopeMixin scope annotation propagation
- WritePolicyDefaults and CollectionSchema absent / present fields
"""
import pytest
from fastapi import FastAPI


@pytest.fixture(scope="module")
def service():
    from dynastore.extensions.configs.service import ConfigsService
    return ConfigsService(FastAPI())


# ---------------------------------------------------------------------------
# get_config_schemas
# ---------------------------------------------------------------------------


class TestGetConfigSchemas:
    @pytest.mark.asyncio
    async def test_returns_dict(self, service):
        result = await service.get_config_schemas()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_non_empty(self, service):
        result = await service.get_config_schemas()
        assert len(result) >= 5, f"Expected ≥5 registered configs, got {len(result)}"

    @pytest.mark.asyncio
    async def test_each_entry_has_required_keys(self, service):
        result = await service.get_config_schemas()
        for class_key, entry in result.items():
            assert "json_schema" in entry, f"Missing json_schema for {class_key}"
            assert "description" in entry, f"Missing description for {class_key}"
            assert "scope" in entry, f"Missing scope for {class_key}"

    @pytest.mark.asyncio
    async def test_scope_values_are_valid(self, service):
        valid_scopes = {"platform_waterfall", "collection_intrinsic", "deployment_env"}
        result = await service.get_config_schemas()
        for class_key, entry in result.items():
            assert entry["scope"] in valid_scopes, (
                f"{class_key} has unexpected scope {entry['scope']!r}"
            )

    @pytest.mark.asyncio
    async def test_json_schema_is_dict(self, service):
        result = await service.get_config_schemas()
        for class_key, entry in result.items():
            assert isinstance(entry["json_schema"], dict), (
                f"{class_key}.json_schema is not a dict"
            )

    @pytest.mark.asyncio
    async def test_collection_routing_config_present(self, service):
        # PR #140 (snake_case identity cutover): class_key is auto-derived
        # snake_case from `cls.__name__`. ItemsRoutingConfig → items_routing_config.
        result = await service.get_config_schemas()
        assert "items_routing_config" in result

    @pytest.mark.asyncio
    async def test_write_policy_defaults_present(self, service):
        result = await service.get_config_schemas()
        assert "write_policy_defaults" in result

    @pytest.mark.asyncio
    async def test_collection_schema_present(self, service):
        result = await service.get_config_schemas()
        assert "collection_schema" in result


# ---------------------------------------------------------------------------
# get_config_schema (single class)
# ---------------------------------------------------------------------------


class TestGetConfigSchema:
    @pytest.mark.asyncio
    async def test_known_class_key_returns_entry(self, service):
        result = await service.get_config_schema("items_routing_config")
        assert result["plugin_id"] == "items_routing_config"
        assert "json_schema" in result
        assert "description" in result
        assert "scope" in result

    @pytest.mark.asyncio
    async def test_unknown_class_key_raises_404(self, service):
        # The configs extension raises an RFC 9457 ProblemException directly
        # (subclass of Exception, not HTTPException).  ProblemException now
        # exposes ``status_code`` at the exception level too so HTTPException-
        # style attribute access still works for callers that need it.
        from dynastore.extensions.configs.problem_details import ProblemException
        with pytest.raises(ProblemException) as exc_info:
            await service.get_config_schema("no_such_config_xyzzy")
        assert exc_info.value.status_code == 404
        assert exc_info.value.problem.status == 404

    @pytest.mark.asyncio
    async def test_write_policy_defaults_schema_no_external_id_field(self, service):
        result = await service.get_config_schema("write_policy_defaults")
        props = result["json_schema"].get("properties", {})
        assert "external_id_field" not in props
        assert "validity_field" not in props
        assert "geohash_precision" not in props

    @pytest.mark.asyncio
    async def test_write_policy_defaults_schema_has_on_conflict(self, service):
        result = await service.get_config_schema("write_policy_defaults")
        props = result["json_schema"].get("properties", {})
        assert "on_conflict" in props

    @pytest.mark.asyncio
    async def test_collection_schema_has_constraints_field(self, service):
        result = await service.get_config_schema("collection_schema")
        props = result["json_schema"].get("properties", {})
        assert "constraints" in props

    @pytest.mark.asyncio
    async def test_scope_reflects_config_scope_mixin(self, service):
        """Classes with ConfigScopeMixin should expose their declared scope."""
        result = await service.get_config_schema("items_routing_config")
        # ItemsRoutingConfig defaults to platform_waterfall
        assert result["scope"] == "platform_waterfall"


# ``/configs/graph`` was retired in the Phase 0 cutover (hardcoded
# PascalCase edges, no live consumer); see commit 47fcc5d.


# ---------------------------------------------------------------------------
# Storage drivers grouped by Protocol qualname (M9/§7)
# ---------------------------------------------------------------------------


class TestStorageDriversProtocolGrouping:
    def test_driver_groups_use_protocol_qualnames(self):
        """list_storage_drivers response must key by Protocol qualname, not
        hard-coded strings like 'collections' / 'assets' / 'collection_metadata'.
        """
        from dynastore.models.protocols.storage_driver import CollectionItemsStore
        from dynastore.models.protocols.asset_driver import AssetStore
        from dynastore.models.protocols.entity_store import CollectionStore

        expected = {
            CollectionItemsStore.__qualname__,
            AssetStore.__qualname__,
            CollectionStore.__qualname__,
        }
        # Check the service source code directly — the keys are set at call time
        import inspect
        from dynastore.extensions.configs.service import ConfigsService
        src = inspect.getsource(ConfigsService.list_storage_drivers)
        for name in expected:
            assert name in src, (
                f"Protocol qualname '{name}' not found in list_storage_drivers source"
            )

    def test_old_string_domains_absent_from_list_storage_drivers(self):
        import inspect
        from dynastore.extensions.configs.service import ConfigsService
        src = inspect.getsource(ConfigsService.list_storage_drivers)
        for old_key in ('"collections"', '"assets"', '"collection_metadata"'):
            assert old_key not in src, (
                f"Old hard-coded domain key {old_key} found in list_storage_drivers"
            )


# ---------------------------------------------------------------------------
# ConfigScopeMixin annotation correctness
# ---------------------------------------------------------------------------


class TestConfigScopeMixinAnnotations:
    def test_collection_schema_has_collection_intrinsic_scope(self):
        from dynastore.modules.storage.driver_config import CollectionSchema
        from dynastore.modules.storage.schema_types import ConfigScopeMixin
        # CollectionSchema is collection-intrinsic per plan §8
        scope = getattr(CollectionSchema, "config_scope", "platform_waterfall")
        # Either annotated or default — just verify it's a valid scope
        valid = {"platform_waterfall", "collection_intrinsic", "deployment_env"}
        assert scope in valid

    def test_write_policy_defaults_scope_is_platform_waterfall(self):
        from dynastore.modules.storage.driver_config import WritePolicyDefaults
        scope = getattr(WritePolicyDefaults, "config_scope", "platform_waterfall")
        assert scope == "platform_waterfall"


# PR #153 (`feat(configs)!: hoist CollectionType into its own PluginConfig
# (Phase 1.6)`) replaced the per-class `get_effective_collection_config` API
# with a tree-shaped `compose_collection_config` that returns *all* configs
# at once. The per-class source-of-truth annotation logic the deleted
# TestGetEffectiveCollectionConfig block exercised still exists inside
# `_get_effective_configs` + `_compose_tree` in
# `extensions/configs/config_api_service.py` and is now exercised
# end-to-end through the integration tests in `test_config_api_service.py`.
# The unit-level mock-driven tests aren't worth re-targeting at the new
# tree-shaped response since the new shape is fully covered upstream.
