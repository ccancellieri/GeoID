"""Tests for the reshaped composed-config composer.

After the reshape:
  * ``_get_effective_configs`` returns ``(by_class, sources)``.
  * ``_compose_tree`` nests configs into ``scope -> topic -> [sub ->] ClassName -> payload``
    in one pass, dropping classes that don't belong at the active scope
    and optionally emitting a ``meta`` dict.
  * ``_build_routing_refs`` rewrites ``operations[OP][*]`` into slim
    ``DriverRef`` dicts, resolving driver→config via the
    ``{DriverName}Config`` naming convention against the PluginConfig
    registry.
  * ``compose_*_config`` endpoints accept ``meta: bool`` to opt into tier
    diagnostics.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.configs.config_api_service import ConfigApiService


def _default_config_side_effect(config_cls, catalog_id=None, collection_id=None, **_):
    if isinstance(config_cls, str):
        from dynastore.modules.db_config.platform_config_service import (
            resolve_config_class,
        )
        config_cls = resolve_config_class(config_cls)
    return config_cls() if config_cls else None


@pytest.fixture()
def mock_config_service():
    svc = MagicMock()
    svc.list_configs = AsyncMock(return_value={"items": [], "total": 0})
    svc.get_config = AsyncMock(side_effect=_default_config_side_effect)
    return svc


# ---------------------------------------------------------------------------
# _get_effective_configs — returns (by_class, sources)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_effective_configs_source_default(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    by_class, sources, _tier_data = await svc._get_effective_configs(
        catalog_id=None, collection_id=None, resolved=True,
    )
    assert isinstance(by_class, dict)
    assert len(by_class) > 0
    for class_name in by_class:
        assert sources[class_name] == "default"


@pytest.mark.asyncio
async def test_get_effective_configs_resolved_batches_to_three_list_calls(mock_config_service):
    """Resolved-mode loader must do exactly THREE list_configs (one per tier)
    and ZERO per-class ``get_config`` calls — replaces the previous N+3 loop.
    """
    svc = ConfigApiService(config_service=mock_config_service)
    await svc._get_effective_configs(
        catalog_id="cat-x", collection_id="coll-y", resolved=True,
    )
    assert mock_config_service.list_configs.await_count == 3
    assert mock_config_service.get_config.await_count == 0


@pytest.mark.asyncio
async def test_get_effective_configs_resolved_merges_tier_deltas():
    """Tier rows are deltas; the loader merges them onto the code default in
    waterfall order (platform > catalog > collection, last wins) — no per-class
    ``get_config`` round-trip.
    """
    from dynastore.modules.storage.driver_config import WritePolicyDefaults

    async def list_side_effect(catalog_id=None, collection_id=None, **_):
        if catalog_id and collection_id:
            return {
                "items": [
                    {"plugin_id": "write_policy_defaults",
                     "config_data": {"require_identity_key": True}},
                ],
                "total": 1,
            }
        return {"items": [], "total": 0}

    svc_mock = MagicMock()
    svc_mock.list_configs = AsyncMock(side_effect=list_side_effect)
    svc_mock.get_config = AsyncMock(side_effect=AssertionError(
        "batched loader must NOT call get_config in resolved mode"
    ))

    svc = ConfigApiService(config_service=svc_mock)
    by_class, sources, _tier_data = await svc._get_effective_configs(
        catalog_id="cat-x", collection_id="coll-y", resolved=True,
    )
    assert sources["write_policy_defaults"] == "collection"
    assert by_class["write_policy_defaults"]["require_identity_key"] is True
    # round-trips through the model so other defaults are present
    default_keys = set(WritePolicyDefaults().model_dump().keys())
    assert set(by_class["write_policy_defaults"].keys()) == default_keys


@pytest.mark.asyncio
async def test_get_effective_configs_catalog_source(mock_config_service):
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    async def list_side_effect(catalog_id=None, collection_id=None, **_):
        if catalog_id and not collection_id:
            return {
                "items": [
                    {"plugin_id": "items_routing_config",
                     "config_data": {"enabled": False}},
                ],
                "total": 1,
            }
        return {"items": [], "total": 0}

    async def get_side_effect(config_cls, catalog_id=None, collection_id=None, **_):
        if isinstance(config_cls, str):
            from dynastore.modules.db_config.platform_config_service import (
                resolve_config_class,
            )
            config_cls = resolve_config_class(config_cls)
        if config_cls is ItemsRoutingConfig and catalog_id and not collection_id:
            return ItemsRoutingConfig()
        return config_cls() if config_cls else None

    mock_config_service.list_configs.side_effect = list_side_effect
    mock_config_service.get_config.side_effect = get_side_effect

    svc = ConfigApiService(config_service=mock_config_service)
    _, sources, _tier_data = await svc._get_effective_configs(
        catalog_id="my-catalog", collection_id=None, resolved=True,
    )
    assert sources["items_routing_config"] == "catalog"


# ---------------------------------------------------------------------------
# _compose_tree — scope/topic tree + optional meta in a single pass
# ---------------------------------------------------------------------------

def _stub_registry(**classes):
    """Build a fake ``list_registered_configs()`` dict.

    Each entry is ``name -> {"_address": (s,t,sub), "_visibility": str|None,
                              "abstract": <bool>?, "__module__": "..."?}``.
    Placement is now driven by the explicit ``_address`` ClassVar on each
    concrete config (mandatory in production, enforced by
    ``PluginConfig.__init_subclass__``).  ``_visibility`` is the optional
    scope filter; ``abstract=True`` flips ``is_abstract_base``.
    """
    out = {}
    for name, attrs in classes.items():
        body = {}
        if attrs.get("abstract"):
            body["is_abstract_base"] = True
        if "_address" in attrs:
            body["_address"] = attrs["_address"]
        if "_visibility" in attrs:
            body["_visibility"] = attrs["_visibility"]
        cls = type(name, (), body)
        cls.__module__ = attrs.get("__module__", "test.stub")
        out[name] = cls
    return out


def test_compose_tree_places_classes_by_address():
    by_class = {
        "WebConfig": {"brand_name": "x"},
        "catalog_core_postgresql_driver": {"enabled": True},
    }
    registry = _stub_registry(
        WebConfig={"_address": ("platform", "web", None)},
        catalog_core_postgresql_driver={
            "_address": ("storage", "drivers", "catalog"),
            "_visibility": "catalog",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        # ``include_mode="upstream"`` exercises the verbose path — tests
        # focused on address-based placement should not also be exercising
        # the slim filter (covered by dedicated slim-mode tests below).
        tree, meta, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="catalog",
            include_mode="upstream",
        )
    assert tree["platform"]["web"]["WebConfig"] == {"brand_name": "x"}
    assert "catalog_core_postgresql_driver" in tree["storage"]["drivers"]["catalog"]
    assert meta is None


def test_compose_tree_filters_collection_only_from_catalog():
    # ``_visibility = "collection"`` → hidden at non-collection scopes.
    by_class = {"items_write_policy": {"on_conflict": "update"}}
    registry = _stub_registry(
        items_write_policy={
            "_address": ("storage", "policy", None),
            "_visibility": "collection",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="catalog",
        )
    assert "storage" not in tree or "policy" not in tree.get("storage", {})


def test_compose_tree_includes_collection_only_at_collection_scope():
    by_class = {"items_write_policy": {"on_conflict": "update"}}
    registry = _stub_registry(
        items_write_policy={
            "_address": ("storage", "policy", None),
            "_visibility": "collection",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="collection",
        )
    assert tree["storage"]["policy"]["items_write_policy"] == {"on_conflict": "update"}


def test_compose_tree_drops_abstract_bases():
    # All four legacy abstract bases (PluginConfig, _PluginDriverConfig,
    # DriverPluginConfig, CollectionDriverConfig, AssetDriverConfig) MUST
    # be filtered out at every active scope.  Filter is now driven by the
    # ``is_abstract_base = True`` ClassVar on the class itself.
    by_class = {
        "DriverPluginConfig":      {"enabled": True},
        "_PluginDriverConfig":     {"enabled": True},
        "CollectionDriverConfig":  {"enabled": True},
        "AssetDriverConfig":       {"enabled": True},
    }
    registry = _stub_registry(
        DriverPluginConfig={"__module__": "dynastore.modules.storage.driver_config", "abstract": True},
        _PluginDriverConfig={"__module__": "dynastore.models.protocols.typed_driver", "abstract": True},
        CollectionDriverConfig={"__module__": "dynastore.modules.storage.driver_config", "abstract": True},
        AssetDriverConfig={"__module__": "dynastore.modules.storage.driver_config", "abstract": True},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        for scope in ("platform", "catalog", "collection"):
            tree, _, _ = ConfigApiService._compose_tree(
                by_class, sources={}, active_scope=scope,
            )
            assert tree == {}, (
                f"abstract bases leaked into the tree at scope={scope!r}: {tree!r}"
            )


def test_compose_tree_real_plugin_driver_config_does_not_leak():
    """Regression: ``_PluginDriverConfig`` must never appear in the composed tree.

    The legacy ``_ABSTRACT_BASES`` frozenset in this module was missing the
    underscore-prefixed name, so it leaked as ``platform.misc._PluginDriverConfig:
    {enabled: true}`` in the production deep view.  After the marker move, the
    filter reads ``cls.__dict__.get("is_abstract_base", False)`` and the real
    class declares it; the leak is structurally impossible.
    """
    from dynastore.models.protocols.typed_driver import _PluginDriverConfig

    assert _PluginDriverConfig.__dict__.get("is_abstract_base") is True

    by_class = {"_PluginDriverConfig": {"enabled": True}}
    registry = {"_PluginDriverConfig": _PluginDriverConfig}
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        for scope in ("platform", "catalog", "collection"):
            tree, _, _ = ConfigApiService._compose_tree(
                by_class, sources={}, active_scope=scope,
            )
            assert tree == {}, f"_PluginDriverConfig leaked at scope={scope!r}: {tree!r}"


# ---------------------------------------------------------------------------
# _build_routing_refs — slim DriverRef dicts, no inline driver-config
# ---------------------------------------------------------------------------

def test_build_routing_refs_replaces_entries_with_slim_refs():
    by_class = {
        "catalog_routing_config": {
            "enabled": True,
            "operations": {
                "WRITE": [
                    {"driver_id": "catalog_core_postgresql_driver",
                     "on_failure": "fatal", "write_mode": "sync",
                     "hints": [], "sla": {"foo": 1}},
                ],
            },
        },
        "catalog_core_postgresql_driver": {"enabled": True},
    }
    # Stub key matches the wire key (TypedDriver bind drops Config suffix).
    registry = _stub_registry(catalog_core_postgresql_driver={"__module__": "m"})
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        svc = ConfigApiService(config_service=MagicMock())
        svc._build_routing_refs(by_class)

    write = by_class["catalog_routing_config"]["operations"]["WRITE"]
    assert len(write) == 1
    ref = write[0]
    assert ref["driver_id"] == "catalog_core_postgresql_driver"
    assert ref["config_ref"] == "catalog_core_postgresql_driver"
    assert ref["on_failure"] == "fatal"
    assert ref["write_mode"] == "sync"
    # Hints / sla / other legacy fields must not survive into the ref.
    assert "hints" not in ref
    assert "sla" not in ref


def test_build_routing_refs_missing_driver_yields_null_config_ref():
    by_class = {
        "catalog_routing_config": {
            "operations": {
                "WRITE": [{"driver_id": "UnknownDriver",
                           "on_failure": "warn", "write_mode": "sync"}]
            },
        },
    }
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value={},
    ):
        svc = ConfigApiService(config_service=MagicMock())
        svc._build_routing_refs(by_class)
    ref = by_class["catalog_routing_config"]["operations"]["WRITE"][0]
    assert ref["driver_id"] == "UnknownDriver"
    assert ref["config_ref"] is None


# ---------------------------------------------------------------------------
# Pagination helpers (unchanged behaviour, retained)
# ---------------------------------------------------------------------------

# NOTE: Pagination helper tests (test_next_link_on_first_page,
# test_prev_link_on_page_3, test_no_next_on_last_page) and
# ``_build_config_page`` were retired in Cycle C alongside the
# ``categories`` field and the depth-expansion machinery.


# ---------------------------------------------------------------------------
# compose_* — meta wiring
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compose_collection_config_meta_none(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=({}, {}, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()):
        response = await svc.compose_collection_config(
            base_url="http://test", catalog_id="c", collection_id="col",
            meta="none",
        )
    assert response.meta is None


@pytest.mark.asyncio
async def test_compose_catalog_meta_field_populates_hierarchical_meta(mock_config_service):
    """Cycle B: ``meta=field`` produces a hierarchical meta tree mirroring
    ``configs`` with ``{field_docs}`` leaves per class."""
    svc = ConfigApiService(config_service=mock_config_service)

    class FakeWebConfig:
        _address = ("platform", "web", None)
        _visibility = None

        @classmethod
        def model_json_schema(cls):
            return {"properties": {"brand_name": {"description": "Brand label."}}}

    by_class = {"WebConfig": {"brand_name": "x"}}
    sources = {"WebConfig": "default"}
    registry = {"WebConfig": FakeWebConfig}
    # Bust the lru_cache so our stub class's schema is re-extracted.
    ConfigApiService._extract_field_docs.cache_clear()
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=(by_class, sources, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch(
             "dynastore.extensions.configs.config_api_service.list_registered_configs",
             return_value=registry,
         ):
        r = await svc.compose_catalog_config(
            base_url="http://test", catalog_id="c",
            meta="field", include="upstream",
        )
    assert r.meta is not None
    # Meta mirrors the configs tree shape.
    assert r.meta["platform"]["web"]["WebConfig"]["field_docs"] == {"brand_name": "Brand label."}


@pytest.mark.asyncio
async def test_compose_platform_config_sets_platform_scope(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=({}, {}, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()):
        r = await svc.compose_platform_config(base_url="http://test")
    assert r.scope == "platform"


def test_compose_tree_address_visibility_filters_correctly():
    """``_visibility = "catalog"`` hides the class from the *main* tree at
    collection scope (it surfaces under ``inherited_from_catalog`` instead
    — see ``test_compose_tree_surfaces_catalog_configs_as_inherited_at_collection_scope``).
    """
    by_class = {"CatalogOnly": {"x": 1}}
    registry = _stub_registry(
        CatalogOnly={
            "_address": ("storage", "drivers", "catalog"),
            "_visibility": "catalog",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        # Visible at platform / catalog
        for scope in ("platform", "catalog"):
            tree, _, _ = ConfigApiService._compose_tree(
                by_class, sources={}, active_scope=scope,
            )
            assert "CatalogOnly" in tree["storage"]["drivers"]["catalog"]
            assert "inherited_from_catalog" not in tree
        # At collection scope: NOT in main tree, but in inherited_from_catalog
        tree, _, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="collection",
        )
        assert "storage" not in tree
        assert tree["inherited_from_catalog"]["storage"]["drivers"]["catalog"]["CatalogOnly"] == {"x": 1}


def test_compose_tree_surfaces_catalog_configs_as_inherited_at_collection_scope():
    """At collection scope, every catalog-visibility config that would have
    been dropped now surfaces under the sibling ``inherited_from_catalog``
    block, using the SAME ``scope/topic/sub`` shape as the main tree so
    the dashboard form-builder renders both with the same code path.
    """
    by_class = {
        "items_postgresql_driver":      {"sidecars": []},
        "elasticsearch_catalog_config": {"private": True},
        "catalog_routing_config":       {"enabled": True},
        "catalog_postgresql_driver":    {},
        "web_config":                   {"brand_name": "X"},
    }
    registry = _stub_registry(
        items_postgresql_driver={
            "_address": ("storage", "drivers", "items"),
            "_visibility": "collection",
        },
        elasticsearch_catalog_config={
            "_address": ("catalog", "elasticsearch", None),
            "_visibility": "catalog",
        },
        catalog_routing_config={
            "_address": ("storage", "routing", None),
            "_visibility": "catalog",
        },
        catalog_postgresql_driver={
            "_address": ("storage", "drivers", "catalog"),
            "_visibility": "catalog",
        },
        web_config={
            "_address": ("platform", "web", None),
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _, inherited = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="collection",
        )

    # Collection-vis stays in main tree
    assert tree["storage"]["drivers"]["items"]["items_postgresql_driver"] == {"sidecars": []}
    # Universal-visibility configs are upstream-tier under default slim mode —
    # they go to the ``inherited`` summary, NOT inlined in the body
    assert "platform" not in tree
    assert inherited is not None
    assert "web_config" in inherited
    # Catalog-vis configs ALL surface under inherited_from_catalog with the same shape
    inh = tree["inherited_from_catalog"]
    assert inh["catalog"]["elasticsearch"]["elasticsearch_catalog_config"] == {"private": True}
    assert inh["storage"]["routing"]["catalog_routing_config"] == {"enabled": True}
    assert inh["storage"]["drivers"]["catalog"]["catalog_postgresql_driver"] == {}


def test_compose_tree_no_inherited_from_catalog_at_non_collection_scopes():
    """The ``inherited_from_catalog`` block is collection-scope only — at
    catalog and platform scopes the catalog-tier configs land in the main
    tree, and the inherited block must not appear.
    """
    by_class = {"catalog_routing_config": {"enabled": True}}
    registry = _stub_registry(
        catalog_routing_config={
            "_address": ("storage", "routing", None),
            "_visibility": "catalog",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        for scope in ("platform", "catalog"):
            tree, _, _ = ConfigApiService._compose_tree(
                by_class, sources={}, active_scope=scope,
            )
            assert "inherited_from_catalog" not in tree, (
                f"inherited_from_catalog must NOT appear at scope={scope!r}"
            )
            assert "catalog_routing_config" in tree["storage"]["routing"]


def test_compose_tree_inherited_from_catalog_meta_mirrors_path():
    """Cycle B: the ``meta`` tree mirrors the ``configs`` tree — including
    the ``inherited_from_catalog`` block.  Each leaf carries
    ``{field_docs}`` (or ``{json_schema}``) at the same path that produces
    the payload in ``configs``.
    """
    class FakeESCatConfig:
        _address = ("catalog", "elasticsearch", None)
        _visibility = "catalog"

        @classmethod
        def model_json_schema(cls):
            return {"properties": {"private": {"description": "Private mode."}}}

    by_class = {"elasticsearch_catalog_config": {"private": True}}
    sources = {"elasticsearch_catalog_config": "catalog"}
    registry = {"elasticsearch_catalog_config": FakeESCatConfig}
    ConfigApiService._extract_field_docs.cache_clear()
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, meta, _ = ConfigApiService._compose_tree(
            by_class, sources=sources, active_scope="collection", meta_mode="field",
        )
    assert tree["inherited_from_catalog"]["catalog"]["elasticsearch"]["elasticsearch_catalog_config"] == {"private": True}
    assert meta is not None
    # Meta mirrors the configs path under inherited_from_catalog.
    assert meta["inherited_from_catalog"]["catalog"]["elasticsearch"]["elasticsearch_catalog_config"]["field_docs"] == {"private": "Private mode."}


# ---------------------------------------------------------------------------
# Slim mode (?include=scope, default) — body shows configs owned by the
# active scope; upstream-tier ones are summarised in `inherited` instead.
# ---------------------------------------------------------------------------


def test_compose_tree_slim_default_diverts_universal_visibility_to_inherited():
    """Universal-visibility (_visibility=None) configs at collection scope
    flow into the slim ``inherited`` summary; only collection-owned configs
    stay in the body.
    """
    by_class = {
        "items_postgresql_driver": {"sidecars": []},  # _visibility=collection
        "web_config": {"brand_name": "X"},            # _visibility=None (universal)
    }
    registry = _stub_registry(
        items_postgresql_driver={
            "_address": ("storage", "drivers", "items"),
            "_visibility": "collection",
        },
        web_config={"_address": ("platform", "web", None)},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _, inherited = ConfigApiService._compose_tree(
            by_class, sources={"web_config": "platform"},
            active_scope="collection",
            # default include_mode="scope"
        )
    # Collection-owned config stays in the body
    assert tree["storage"]["drivers"]["items"]["items_postgresql_driver"] == {"sidecars": []}
    # Universal-vis config is diverted to the slim summary, NOT inlined
    assert "platform" not in tree
    assert inherited == {"web_config": "platform"}


def test_compose_tree_slim_keeps_collection_overrides_in_body():
    """A class with universal visibility BUT an explicit collection-scope
    row IS in-scope — it stays in the body."""
    by_class = {"web_config": {"brand_name": "Tenant Override"}}
    registry = _stub_registry(
        web_config={"_address": ("platform", "web", None)},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _, inherited = ConfigApiService._compose_tree(
            by_class, sources={"web_config": "collection"},
            active_scope="collection",
        )
    # source==collection means an explicit override exists → stays in body
    assert tree["platform"]["web"]["web_config"] == {"brand_name": "Tenant Override"}
    # Nothing was diverted
    assert inherited is None


def test_compose_tree_upstream_mode_renders_everything_in_body():
    """``include_mode="upstream"`` restores the verbose pre-slim default —
    every visible class lands in the tree regardless of source/visibility."""
    by_class = {
        "web_config": {"brand_name": "X"},
        "items_postgresql_driver": {"sidecars": []},
    }
    registry = _stub_registry(
        web_config={"_address": ("platform", "web", None)},
        items_postgresql_driver={
            "_address": ("storage", "drivers", "items"),
            "_visibility": "collection",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _, inherited = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="collection",
            include_mode="upstream",
        )
    # Both rendered in body, no inherited summary
    assert tree["platform"]["web"]["web_config"] == {"brand_name": "X"}
    assert tree["storage"]["drivers"]["items"]["items_postgresql_driver"] == {"sidecars": []}
    assert inherited is None


# NOTE: The "entity" annotation on ConfigMeta and the per-class
# ``ConfigMeta.source`` / ``.layers`` waterfall were retired in Cycle B.
# Driver-tier entity grouping (items vs collection vs assets) now lives
# in the tree path itself: a driver lives at
# ``configs.storage.drivers.<entity>`` so the path encodes the bucket
# directly — no per-entry annotation needed.


def test_compose_tree_slim_at_platform_scope_is_a_noop():
    """At platform scope the slim filter is a no-op — platform IS the top
    tier, nothing is upstream."""
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry(
        web_config={"_address": ("platform", "web", None)},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _, inherited = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform",
        )
    assert tree["platform"]["web"]["web_config"] == {"brand_name": "X"}
    assert inherited is None


# ---------------------------------------------------------------------------
# Production placement-bug regression tests — were misplaced under the
# heuristic; explicit `_address` fixes them.
# ---------------------------------------------------------------------------

def test_catalog_policy_config_lands_at_catalog_scope():
    """``CatalogPolicyConfig`` (Cycle E.1 — replaces
    ``ElasticsearchCatalogConfig``) carries the catalog-tier privacy
    default; address pins it under ``catalog.policy``.
    """
    from dynastore.modules.catalog.catalog_config import CatalogPolicyConfig

    assert CatalogPolicyConfig._address == ("catalog", "policy", None)
    assert CatalogPolicyConfig._visibility == "catalog"


# NOTE: ``ElasticsearchCollectionConfig`` was retired in Cycle C and
# ``ElasticsearchCatalogConfig`` was retired in Cycle E.1.  Privacy
# moves to ``CatalogPolicyConfig.default_collection_privacy`` (catalog
# tier) and (Cycle E.2) ``is_private: bool`` on ``CollectionPluginConfig``.


def test_catalog_es_driver_lands_under_storage_drivers_catalog():
    """``CatalogElasticsearchDriverConfig`` was leaking to ``platform.misc``."""
    from dynastore.modules.elasticsearch.catalog_es_driver import (
        CatalogElasticsearchDriverConfig,
    )

    assert CatalogElasticsearchDriverConfig._address == ("storage", "drivers", "catalog")
    assert CatalogElasticsearchDriverConfig._visibility == "catalog"


def test_collection_es_driver_lands_under_storage_drivers_collection():
    """``CollectionElasticsearchDriverConfig`` was leaking to ``platform.misc``."""
    from dynastore.modules.elasticsearch.collection_es_driver import (
        CollectionElasticsearchDriverConfig,
    )

    assert CollectionElasticsearchDriverConfig._address == ("storage", "drivers", "collection")
    assert CollectionElasticsearchDriverConfig._visibility == "catalog"


def test_assets_plugin_config_visible_at_all_scopes():
    """Extension config was incorrectly gated to collection by ``Asset*`` name."""
    from dynastore.extensions.assets.config import AssetsPluginConfig

    assert AssetsPluginConfig._address == ("extensions", "assets", None)
    assert AssetsPluginConfig._visibility is None  # visible everywhere


# ---------------------------------------------------------------------------
# Enforcement — every concrete PluginConfig must declare _address.
# ---------------------------------------------------------------------------

def test_concrete_subclass_without_address_raises():
    """``PluginConfig.__init_subclass__`` enforces ``_address`` on concrete subclasses."""
    from typing import ClassVar

    from dynastore.modules.db_config.platform_config_service import PluginConfig

    with pytest.raises(TypeError, match=r"does not declare ``_address``"):
        class _BadConcreteConfig(PluginConfig):  # noqa: F841 — deliberate
            field: ClassVar[int] = 1


def test_concrete_subclass_with_address_ok():
    from typing import ClassVar, Optional, Tuple

    from dynastore.modules.db_config.platform_config_service import PluginConfig

    class _GoodConcreteConfig(PluginConfig):
        _address: ClassVar[Tuple[str, str, Optional[str]]] = ("platform", "misc", None)

    assert _GoodConcreteConfig._address == ("platform", "misc", None)


def test_abstract_subclass_without_address_ok():
    """Abstract bases (``is_abstract_base = True``) opt out of the check."""
    from typing import ClassVar

    from dynastore.modules.db_config.platform_config_service import PluginConfig

    class _AbstractIntermediate(PluginConfig):
        is_abstract_base: ClassVar[bool] = True

    assert _AbstractIntermediate.is_abstract_base is True


# NOTE: The synthetic ``routing_resolution`` field and its
# ``_build_routing_resolution`` resolver were retired in Cycle C
# (2026-05-05).  The resolver hard-coded ES public/private per op
# without consulting ``ItemsRoutingConfig``, which made it lie post
# PR #254 (PG is the WRITE primary, not ES).  Operators read the
# truth from the routing tree under ``configs.platform.catalog.
# collection.storage.routing`` directly.  Dropped tests:
#   - test_routing_resolution_public_mode
#   - test_routing_resolution_private_mode
#   - test_routing_resolution_failure_returns_empty
#   - test_compose_collection_config_includes_routing_resolution_when_meta_true
#   - test_compose_collection_config_no_routing_resolution_when_meta_false


# NOTE: The Phase 4 waterfall trace (``meta.<class>.layers``) and the
# ``_build_meta_entry`` helper were retired in Cycle B of the
# config-API restructure (2026-05-05).  ``meta`` is now a hierarchical
# tree mirroring ``configs`` with ``{field_docs}`` or ``{json_schema}``
# leaves; tier-of-origin is communicated via the top-level
# ``inherited`` map.  The dropped tests covered:
#   - test_build_meta_entry_default_only
#   - test_build_meta_entry_platform_only
#   - test_build_meta_entry_platform_then_catalog_overrides
#   - test_build_meta_entry_full_waterfall
#   - test_build_meta_entry_no_tier_data_returns_source_only
#   - test_compose_tree_meta_includes_layers_when_tier_data_provided
