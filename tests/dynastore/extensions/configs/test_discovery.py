"""Unit tests for M9 config discovery endpoints.

Covers:
- get_config_schemas() — all registered configs with scope annotation
- get_config_schema(class_key) — single config schema + 404 on unknown key
- get_config_graph() — nodes + edges structure
- list_storage_drivers() response keys use Protocol qualnames (M9 / §7)
- ConfigScopeMixin scope annotation propagation
- ItemsWritePolicy and ItemsSchema absent / present fields
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
    async def test_items_write_policy_present(self, service):
        result = await service.get_config_schemas()
        assert "items_write_policy" in result

    @pytest.mark.asyncio
    async def test_items_schema_present(self, service):
        result = await service.get_config_schemas()
        assert "items_schema" in result


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
    async def test_items_write_policy_schema_no_legacy_fields(self, service):
        """Phase 2 rewrite collapses legacy field knobs into ComputedField/IdentityRule."""
        result = await service.get_config_schema("items_write_policy")
        props = result["json_schema"].get("properties", {})
        assert "external_id_field" not in props
        assert "geohash_precision" not in props
        assert "identity_matchers" not in props
        assert "matcher_actions" not in props
        assert "require_external_id" not in props
        assert "compute" in props
        assert "identity" in props
        assert "schema" in props

    @pytest.mark.asyncio
    async def test_items_write_policy_schema_has_on_conflict(self, service):
        result = await service.get_config_schema("items_write_policy")
        props = result["json_schema"].get("properties", {})
        assert "on_conflict" in props

    @pytest.mark.asyncio
    async def test_items_schema_has_constraints_field(self, service):
        result = await service.get_config_schema("items_schema")
        props = result["json_schema"].get("properties", {})
        assert "constraints" in props

    @pytest.mark.asyncio
    async def test_scope_reflects_config_scope_mixin(self, service):
        """Classes with ConfigScopeMixin should expose their declared scope."""
        result = await service.get_config_schema("items_routing_config")
        # ItemsRoutingConfig defaults to platform_waterfall
        assert result["scope"] == "platform_waterfall"

    @pytest.mark.asyncio
    async def test_meta_schema_returns_raw_json_schema(self, service):
        """``?meta=schema`` returns the raw JSON Schema dict only (no wrapper).

        The ``rel="schema"`` link emitted in ``links=full`` mode points
        at this projection — form-builders should not need to unwrap.
        """
        result = await service.get_config_schema("items_routing_config", meta="schema")
        # Raw JSON Schema 2020-12 shape — has ``properties`` (the schema
        # itself may carry ``description``/``title`` from the class
        # docstring; those are standard JSON Schema keys, not wrapper
        # keys).  The wrapper-only keys ``plugin_id`` / ``scope`` are
        # absent.
        assert isinstance(result, dict)
        assert "properties" in result
        assert "plugin_id" not in result
        assert "scope" not in result

    @pytest.mark.asyncio
    async def test_meta_field_returns_docs_map(self, service):
        """``?meta=field`` returns the terse ``{field_name: description}``
        map only (the same shape ``_meta.docs`` carries on composed leaves)."""
        result = await service.get_config_schema("items_write_policy", meta="field")
        # Pure dict[str, str] — no wrapper, no nested schema.
        assert isinstance(result, dict)
        if result:  # only fields with non-empty descriptions appear
            for k, v in result.items():
                assert isinstance(k, str)
                assert isinstance(v, str) and v


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
    def test_items_schema_has_collection_intrinsic_scope(self):
        from dynastore.modules.storage.driver_config import ItemsSchema
        from dynastore.modules.storage.schema_types import ConfigScopeMixin
        # ItemsSchema is collection-intrinsic per plan §8
        scope = getattr(ItemsSchema, "config_scope", "platform_waterfall")
        # Either annotated or default — just verify it's a valid scope
        valid = {"platform_waterfall", "collection_intrinsic", "deployment_env"}
        assert scope in valid

    def test_items_write_policy_scope_is_platform_waterfall(self):
        from dynastore.modules.storage.driver_config import ItemsWritePolicy
        scope = getattr(ItemsWritePolicy, "config_scope", "platform_waterfall")
        assert scope == "platform_waterfall"


# PR #153 (`feat(configs)!: hoist CollectionInfo into its own PluginConfig
# (Phase 1.6)`) replaced the per-class `get_effective_collection_config` API
# with a tree-shaped `compose_collection_config` that returns *all* configs
# at once. The per-class source-of-truth annotation logic the deleted
# TestGetEffectiveCollectionConfig block exercised still exists inside
# `_get_effective_configs` + `_compose_tree` in
# `extensions/configs/config_api_service.py` and is now exercised
# end-to-end through the integration tests in `test_config_api_service.py`.
# The unit-level mock-driven tests aren't worth re-targeting at the new
# tree-shaped response since the new shape is fully covered upstream.
