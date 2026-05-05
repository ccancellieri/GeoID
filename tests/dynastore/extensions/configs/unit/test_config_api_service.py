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
            by_class, sources={}, active_scope="catalog", include_meta=False,
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
            by_class, sources={}, active_scope="catalog", include_meta=False,
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
            by_class, sources={}, active_scope="collection", include_meta=False,
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
                by_class, sources={}, active_scope=scope, include_meta=False,
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
                by_class, sources={}, active_scope=scope, include_meta=False,
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

def test_next_link_on_first_page():
    svc = ConfigApiService(config_service=MagicMock())
    page = svc._build_config_page(
        "http://test/config", "collections", 30, 1, 15, {"depth": 1},
    )
    assert any(link["rel"] == "next" for link in page.links)
    assert not any(link["rel"] == "prev" for link in page.links)
    next_href = next(link["href"] for link in page.links if link["rel"] == "next")
    assert "collections_page=2" in next_href
    assert "depth=1" in next_href


def test_prev_link_on_page_3():
    svc = ConfigApiService(config_service=MagicMock())
    page = svc._build_config_page("http://test/config", "collections", 100, 3, 15, {})
    assert any(link["rel"] == "prev" for link in page.links)


def test_no_next_on_last_page():
    svc = ConfigApiService(config_service=MagicMock())
    page = svc._build_config_page("http://test/config", "collections", 10, 1, 15, {})
    assert not any(link["rel"] == "next" for link in page.links)


# ---------------------------------------------------------------------------
# compose_* — categories + meta wiring
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compose_collection_config_depth0_no_categories(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=({}, {}, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()):
        response = await svc.compose_collection_config(
            base_url="http://test", catalog_id="c", collection_id="col", depth=0,
        )
    assert response.categories is None
    assert response.meta is None


@pytest.mark.asyncio
async def test_compose_catalog_meta_flag_populates_meta_dict(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    by_class = {"WebConfig": {"brand_name": "x"}}
    sources = {"WebConfig": "default"}
    registry = _stub_registry(
        WebConfig={"_address": ("platform", "web", None)},
    )
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=(by_class, sources, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch(
             "dynastore.extensions.configs.config_api_service.list_registered_configs",
             return_value=registry,
         ):
        r = await svc.compose_catalog_config(
            base_url="http://test", catalog_id="c", depth=0, meta=True,
        )
    assert r.meta is not None
    assert r.meta["WebConfig"].source == "default"


@pytest.mark.asyncio
async def test_compose_platform_config_sets_platform_scope(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=({}, {}, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()):
        r = await svc.compose_platform_config(base_url="http://test", depth=0)
    assert r.scope == "platform"
    assert r.categories is None


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
                by_class, sources={}, active_scope=scope, include_meta=False,
            )
            assert "CatalogOnly" in tree["storage"]["drivers"]["catalog"]
            assert "inherited_from_catalog" not in tree
        # At collection scope: NOT in main tree, but in inherited_from_catalog
        tree, _, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="collection", include_meta=False,
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
            by_class, sources={}, active_scope="collection", include_meta=False,
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
                by_class, sources={}, active_scope=scope, include_meta=False,
            )
            assert "inherited_from_catalog" not in tree, (
                f"inherited_from_catalog must NOT appear at scope={scope!r}"
            )
            assert "catalog_routing_config" in tree["storage"]["routing"]


def test_compose_tree_inherited_from_catalog_meta_carries_source():
    """The ``meta`` block still carries ``source`` for inherited configs,
    so the dashboard can render the inheritance chain alongside the value.
    """
    by_class = {"elasticsearch_catalog_config": {"private": True}}
    sources = {"elasticsearch_catalog_config": "catalog"}
    registry = _stub_registry(
        elasticsearch_catalog_config={
            "_address": ("catalog", "elasticsearch", None),
            "_visibility": "catalog",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, meta, _ = ConfigApiService._compose_tree(
            by_class, sources=sources, active_scope="collection", include_meta=True,
        )
    assert tree["inherited_from_catalog"]["catalog"]["elasticsearch"]["elasticsearch_catalog_config"] == {"private": True}
    assert meta is not None
    assert meta["elasticsearch_catalog_config"].source == "catalog"


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
            active_scope="collection", include_meta=False,
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
            active_scope="collection", include_meta=False,
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
            by_class, sources={}, active_scope="collection", include_meta=False,
            include_mode="upstream",
        )
    # Both rendered in body, no inherited summary
    assert tree["platform"]["web"]["web_config"] == {"brand_name": "X"}
    assert tree["storage"]["drivers"]["items"]["items_postgresql_driver"] == {"sidecars": []}
    assert inherited is None


def test_compose_tree_entity_annotation_on_driver_tier_configs():
    """Driver-tier configs (``_address[1] == "drivers"``) get an ``entity``
    field on their ConfigMeta — extracted from ``_address[2]`` — so
    dashboards can group sidecars/driver settings per entity (items vs
    collection-metadata vs assets).
    """
    by_class = {
        "items_postgresql_driver":      {"sidecars": [{"sidecar_type": "geometries"}]},
        "collection_postgresql_driver": {"sidecars": []},
        "asset_postgresql_driver":      {},
        "web_config":                   {"brand_name": "X"},  # non-driver, no entity
    }
    registry = _stub_registry(
        items_postgresql_driver={
            "_address": ("storage", "drivers", "items"),
            "_visibility": "collection",
        },
        collection_postgresql_driver={
            "_address": ("storage", "drivers", "collection"),
            "_visibility": "collection",
        },
        asset_postgresql_driver={
            "_address": ("storage", "drivers", "assets"),
            "_visibility": "collection",
        },
        web_config={"_address": ("platform", "web", None)},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        _, meta, _ = ConfigApiService._compose_tree(
            by_class, sources={
                "items_postgresql_driver": "collection",
                "collection_postgresql_driver": "collection",
                "asset_postgresql_driver": "collection",
                "web_config": "platform",
            },
            active_scope="collection", include_meta=True,
            tier_data={
                "platform": {"web_config": {"brand_name": "X"}},
                "catalog": {},
                "collection": {
                    "items_postgresql_driver": {"sidecars": [{}]},
                    "collection_postgresql_driver": {"sidecars": []},
                    "asset_postgresql_driver": {},
                },
            },
            include_mode="upstream",
        )
    assert meta is not None
    assert meta["items_postgresql_driver"].entity == "items"
    assert meta["collection_postgresql_driver"].entity == "collection"
    assert meta["asset_postgresql_driver"].entity == "assets"
    # Non-driver configs have no entity context
    assert meta["web_config"].entity is None


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
            by_class, sources={}, active_scope="platform", include_meta=False,
        )
    assert tree["platform"]["web"]["web_config"] == {"brand_name": "X"}
    assert inherited is None


# ---------------------------------------------------------------------------
# Production placement-bug regression tests — were misplaced under the
# heuristic; explicit `_address` fixes them.
# ---------------------------------------------------------------------------

def test_es_catalog_config_lands_at_catalog_scope():
    """``ElasticsearchCatalogConfig`` was leaking to ``platform.misc``."""
    from dynastore.modules.elasticsearch.es_catalog_config import (
        ElasticsearchCatalogConfig,
    )

    assert ElasticsearchCatalogConfig._address == ("catalog", "elasticsearch", None)
    assert ElasticsearchCatalogConfig._visibility == "catalog"


def test_es_collection_config_lands_at_collection_scope():
    """``ElasticsearchCollectionConfig`` was leaking to ``platform.misc``."""
    from dynastore.modules.elasticsearch.es_collection_config import (
        ElasticsearchCollectionConfig,
    )

    assert ElasticsearchCollectionConfig._address == ("collection", "elasticsearch", None)
    assert ElasticsearchCollectionConfig._visibility == "collection"


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


# ---------------------------------------------------------------------------
# Phase 1.5d: routing_resolution meta block at collection scope.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_routing_resolution_public_mode():
    """When ``is_collection_private`` returns False, items operations
    resolve to the PUBLIC ES driver with a clear reason string.
    """
    with patch(
        "dynastore.modules.elasticsearch.es_collection_config.is_collection_private",
        new=AsyncMock(return_value=False),
    ):
        rr = await ConfigApiService._build_routing_resolution("cat", "col")
    assert rr["items"]["WRITE"]["driver_id"] == "items_elasticsearch_driver"
    assert rr["items"]["READ"]["driver_id"] == "items_elasticsearch_driver"
    assert rr["items"]["SEARCH"]["driver_id"] == "items_elasticsearch_driver"
    assert "private mode not active" in rr["items"]["WRITE"]["reason"]


@pytest.mark.asyncio
async def test_routing_resolution_private_mode():
    """When ``is_collection_private`` returns True, items operations
    resolve to the PRIVATE ES driver — answers the original "where do
    my items go?" operator question.
    """
    with patch(
        "dynastore.modules.elasticsearch.es_collection_config.is_collection_private",
        new=AsyncMock(return_value=True),
    ):
        rr = await ConfigApiService._build_routing_resolution("cat", "col")
    assert rr["items"]["WRITE"]["driver_id"] == "items_elasticsearch_private_driver"
    assert rr["items"]["READ"]["driver_id"] == "items_elasticsearch_private_driver"
    assert rr["items"]["SEARCH"]["driver_id"] == "items_elasticsearch_private_driver"
    assert "private mode active" in rr["items"]["WRITE"]["reason"]


@pytest.mark.asyncio
async def test_routing_resolution_failure_returns_empty():
    """Any failure resolving private-mode (no ConfigsProtocol, ES not
    configured, etc.) is non-fatal — returns ``{}`` so the deep view
    still renders without the routing_resolution block.
    """
    with patch(
        "dynastore.modules.elasticsearch.es_collection_config.is_collection_private",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ):
        rr = await ConfigApiService._build_routing_resolution("cat", "col")
    assert rr == {}


@pytest.mark.asyncio
async def test_compose_collection_config_includes_routing_resolution_when_meta_true(
    mock_config_service,
):
    """``?meta=true`` populates the new ``routing_resolution`` field on
    the response (alongside the existing per-class ``meta`` dict).
    """
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=({}, {}, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch(
             "dynastore.modules.elasticsearch.es_collection_config.is_collection_private",
             new=AsyncMock(return_value=True),
         ):
        r = await svc.compose_collection_config(
            base_url="http://test", catalog_id="c", collection_id="x",
            depth=0, meta=True,
        )
    assert r.routing_resolution is not None
    assert r.routing_resolution["items"]["WRITE"]["driver_id"] == "items_elasticsearch_private_driver"


@pytest.mark.asyncio
async def test_compose_collection_config_no_routing_resolution_when_meta_false(
    mock_config_service,
):
    """``?meta=false`` (default) keeps the cheap path cheap — no async
    ``is_collection_private`` call, ``routing_resolution`` is None.
    """
    svc = ConfigApiService(config_service=mock_config_service)
    # is_collection_private must NOT be awaited under meta=False
    privacy_mock = AsyncMock(side_effect=AssertionError(
        "compose_collection_config(meta=False) must NOT call is_collection_private"
    ))
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=({}, {}, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch(
             "dynastore.modules.elasticsearch.es_collection_config.is_collection_private",
             new=privacy_mock,
         ):
        r = await svc.compose_collection_config(
            base_url="http://test", catalog_id="c", collection_id="x",
            depth=0, meta=False,
        )
    assert r.routing_resolution is None


# ---------------------------------------------------------------------------
# Phase 4 layer trace: meta.<class>.layers waterfall.
# ---------------------------------------------------------------------------

def test_build_meta_entry_default_only():
    """When the class has no row at any tier, only the 'default' layer is
    present and source='default'."""
    sources = {"foo": "default"}
    tier_data = {"platform": {}, "catalog": {}, "collection": {}}
    meta = ConfigApiService._build_meta_entry("foo", sources, tier_data)
    assert meta.source == "default"
    layers = {l.level: l.present for l in meta.layers or []}
    assert layers == {
        "default": True, "platform": False, "catalog": False, "collection": False,
    }


def test_build_meta_entry_platform_only():
    sources = {"foo": "platform"}
    tier_data = {"platform": {"foo": {"a": 1}}, "catalog": {}, "collection": {}}
    meta = ConfigApiService._build_meta_entry("foo", sources, tier_data)
    assert meta.source == "platform"
    layers = {l.level: l.present for l in meta.layers or []}
    assert layers == {
        "default": False, "platform": True, "catalog": False, "collection": False,
    }


def test_build_meta_entry_platform_then_catalog_overrides():
    """Platform AND catalog have rows; catalog wins as source.  Both layers
    must show present=True so the operator sees the override chain."""
    sources = {"foo": "catalog"}
    tier_data = {
        "platform": {"foo": {"a": 1}},
        "catalog":  {"foo": {"a": 2}},
        "collection": {},
    }
    meta = ConfigApiService._build_meta_entry("foo", sources, tier_data)
    assert meta.source == "catalog"
    layers = {l.level: l.present for l in meta.layers or []}
    assert layers == {
        "default": False, "platform": True, "catalog": True, "collection": False,
    }


def test_build_meta_entry_full_waterfall():
    """All 4 tiers contributing: source is the top (collection) and every
    tier's present flag is True (default is implicit-present only when no
    other tier contributed)."""
    sources = {"foo": "collection"}
    tier_data = {
        "platform":   {"foo": {"a": 1}},
        "catalog":    {"foo": {"a": 2}},
        "collection": {"foo": {"a": 3}},
    }
    meta = ConfigApiService._build_meta_entry("foo", sources, tier_data)
    assert meta.source == "collection"
    layers = {l.level: l.present for l in meta.layers or []}
    assert layers == {
        "default": False, "platform": True, "catalog": True, "collection": True,
    }


def test_build_meta_entry_no_tier_data_returns_source_only():
    """Backward-compat path: no tier_data → meta.layers is None, source is set."""
    meta = ConfigApiService._build_meta_entry("foo", {"foo": "platform"}, None)
    assert meta.source == "platform"
    assert meta.layers is None


def test_compose_tree_meta_includes_layers_when_tier_data_provided():
    """End-to-end through `_compose_tree`: with tier_data, every meta entry
    carries layers."""
    by_class = {"WebConfig": {"brand_name": "x"}}
    sources = {"WebConfig": "platform"}
    tier_data = {
        "platform":   {"WebConfig": {"brand_name": "x"}},
        "catalog":    {},
        "collection": {},
    }
    registry = _stub_registry(WebConfig={"_address": ("platform", "web", None)})
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        _, meta, _ = ConfigApiService._compose_tree(
            by_class, sources, "platform", include_meta=True,
            tier_data=tier_data,
        )
    assert meta is not None
    entry = meta["WebConfig"]
    assert entry.source == "platform"
    assert entry.layers is not None
    assert [l.level for l in entry.layers] == [
        "default", "platform", "catalog", "collection",
    ]
    assert entry.layers[1].present is True   # platform
    assert entry.layers[2].present is False  # catalog
