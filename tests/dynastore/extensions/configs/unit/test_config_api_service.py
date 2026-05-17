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
        from dynastore.modules.db_config.plugin_config import resolve_config_class
        config_cls = resolve_config_class(config_cls)
    return config_cls() if config_cls else None


@pytest.fixture()
def mock_config_service():
    svc = MagicMock()
    svc.list_configs = AsyncMock(return_value={"results": [], "total": 0})
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
                "results": [
                    {"plugin_id": "write_policy_defaults",
                     "config": {"require_identity_key": True}},
                ],
                "total": 1,
            }
        return {"results": [], "total": 0}

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
                "results": [
                    {"plugin_id": "items_routing_config",
                     "config": {"enabled": False}},
                ],
                "total": 1,
            }
        return {"results": [], "total": 0}

    async def get_side_effect(config_cls, catalog_id=None, collection_id=None, **_):
        if isinstance(config_cls, str):
            from dynastore.modules.db_config.plugin_config import resolve_config_class
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


@pytest.mark.asyncio
@pytest.mark.parametrize("resolved", [True, False])
async def test_get_effective_configs_consumes_real_list_configs_row_shape(resolved):
    """Locks the loader's contract against ``ConfigService.list_configs``.

    ``ConfigService.list_configs`` returns ``{"total", "results": [{"plugin_id",
    "config"}, ...]}`` (config_service.py). An earlier draft of the composed
    loader read ``{"items": [{"plugin_id", "config_data"}]}`` instead — every
    composed view (both ``resolved=true`` and ``resolved=false``) silently
    ignored stored tier rows and rendered code defaults, so a collection-level
    write-policy override never surfaced in
    ``GET /configs/.../collections/{c}``. This test feeds the real wire shape
    and asserts the override survives the merge in both branches.
    """
    from dynastore.modules.storage.driver_config import WritePolicyDefaults

    async def list_side_effect(catalog_id=None, collection_id=None, **_):
        if catalog_id and collection_id:
            return {
                "total": 1,
                "results": [
                    {"plugin_id": "write_policy_defaults",
                     "config": {"require_identity_key": True}},
                ],
            }
        return {"total": 0, "results": []}

    svc_mock = MagicMock()
    svc_mock.list_configs = AsyncMock(side_effect=list_side_effect)

    svc = ConfigApiService(config_service=svc_mock)
    by_class, sources, tier_data = await svc._get_effective_configs(
        catalog_id="cat-x", collection_id="coll-y", resolved=resolved,
    )

    assert sources["write_policy_defaults"] == "collection", (
        "stored collection-tier row must surface as source=collection; "
        "regression for items/results & config_data/config field-name drift"
    )
    assert by_class["write_policy_defaults"]["require_identity_key"] is True
    assert tier_data["collection"]["write_policy_defaults"] == {
        "require_identity_key": True,
    }
    if resolved:
        # resolved path round-trips through the model so other defaults appear
        assert set(by_class["write_policy_defaults"].keys()) == set(
            WritePolicyDefaults().model_dump().keys()
        )


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
        WebConfig={"_address": ("platform", "web")},
        catalog_core_postgresql_driver={
            "_address": ("platform", "catalog", "drivers"),
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
        tree = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="catalog",
            include_mode="upstream",
        )
    assert {k: v for k, v in tree["platform"]["web"]["WebConfig"].items() if not k.startswith("_")} == {"brand_name": "x"}
    assert "catalog_core_postgresql_driver" in tree["platform"]["catalog"]["drivers"]
    # Per #665 slice 3 every leaf carries ``_meta = {tier, source}`` even
    # under ``meta_mode="none"``.
    assert {k: v for k, v in tree["platform"]["web"]["WebConfig"]["_meta"].items() if not k.startswith("_")} == {"tier": "catalog", "source": "default"}


def test_compose_tree_filters_collection_only_from_catalog():
    # ``_visibility = "collection"`` → hidden at non-collection scopes.
    by_class = {"items_write_policy": {"on_conflict": "update"}}
    registry = _stub_registry(
        items_write_policy={
            "_address": ("platform", "catalog", "collection", "items", "policy"),
            "_visibility": "collection",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="catalog",
        )
    assert "storage" not in tree or "policy" not in tree.get("storage", {})


def test_compose_tree_includes_collection_only_at_collection_scope():
    by_class = {"items_write_policy": {"on_conflict": "update"}}
    registry = _stub_registry(
        items_write_policy={
            "_address": ("platform", "catalog", "collection", "items", "policy"),
            "_visibility": "collection",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="collection",
        )
    assert {k: v for k, v in tree["platform"]["catalog"]["collection"]["items"]["policy"]["items_write_policy"].items() if not k.startswith("_")} == {"on_conflict": "update"}


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
            tree = ConfigApiService._compose_tree(
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
            tree = ConfigApiService._compose_tree(
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
                    {"driver_ref": "catalog_core_postgresql_driver",
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
        svc._build_routing_refs(by_class, base_url="http://h/configs")

    write = by_class["catalog_routing_config"]["operations"]["WRITE"]
    assert len(write) == 1
    ref = write[0]
    assert ref["driver_ref"] == "catalog_core_postgresql_driver"
    # Cycle F.7d.3: HATEOAS driver-config link replaces the
    # ``config_ref`` scalar.  Single link with rel=driver-config when
    # the driver_ref binds to a registered config.
    assert "config_ref" not in ref
    # #520: routing-entry _links use exclude_none=True, matching the
    # shape emitted by ``_leaf_links``.  ``hrefSchema`` (and any other
    # None field) is dropped; ``templated: False`` survives because it
    # has a non-None default.
    assert ref["_links"] == [
        {
            "rel": "driver-config",
            "href": "http://h/configs/plugins/catalog_core_postgresql_driver",
            "method": "PUT",
            "title": "PUT this driver's config at platform scope",
            "templated": False,
        }
    ]
    assert ref["on_failure"] == "fatal"
    assert ref["write_mode"] == "sync"
    # Cycle F.7d.3-fixup: hints + source surface on the slim ref so
    # operators can distinguish hint-gated entries that share the same
    # driver_ref under one operation.
    assert ref["hints"] == []
    # ``sla`` is internal — must NOT appear on the slim ref.
    assert "sla" not in ref


def test_build_routing_refs_forwards_hints_and_source():
    """Cycle F.7d.3-fixup — ``hints`` and ``source`` flow from the
    routing entry through to the slim ``DriverRef``.  Without this,
    operators reading the configs API can't tell which of two
    same-class entries fires for which hint, nor which entries the
    apply-handler self-registered vs operator-authored."""
    by_class = {
        "items_routing_config": {
            "operations": {
                "SEARCH": [
                    {"driver_ref": "items_elasticsearch_driver",
                     "hints": ["geometry_simplified"],
                     "on_failure": "fatal", "write_mode": "sync",
                     "source": "auto"},
                    {"driver_ref": "items_postgresql_driver",
                     "hints": ["geometry_exact"],
                     "on_failure": "fatal", "write_mode": "sync",
                     "source": "operator"},
                ],
            },
        },
    }
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=_stub_registry(
            items_elasticsearch_driver={"__module__": "m"},
            items_postgresql_driver={"__module__": "m"},
        ),
    ):
        ConfigApiService(config_service=MagicMock())._build_routing_refs(
            by_class, base_url="http://h/configs",
        )
    [es, pg] = by_class["items_routing_config"]["operations"]["SEARCH"]
    assert es["hints"] == ["geometry_simplified"]
    assert es["_meta"]["source"] == "auto"
    assert es["_meta"]["tier"] == "platform"
    assert "source" not in es
    assert pg["hints"] == ["geometry_exact"]
    assert pg["_meta"]["source"] == "operator"
    assert pg["_meta"]["tier"] == "platform"


def test_build_routing_refs_link_title_reflects_active_scope():
    """Cycle F.7d.3-fixup — operators reading a routing entry's
    ``driver-config`` link from the API response see whether they're
    about to PATCH a platform default, a catalog override, or a
    collection override.  Title carries the scope label."""
    by_class = {
        "items_routing_config": {
            "operations": {
                "WRITE": [{"driver_ref": "items_postgresql_driver",
                           "on_failure": "fatal", "write_mode": "sync"}]
            },
        },
    }
    registry = _stub_registry(items_postgresql_driver={"__module__": "m"})
    cases = [
        ("http://h/configs", "platform"),
        ("http://h/configs/catalogs/cat1", "catalog"),
        ("http://h/configs/catalogs/cat1/collections/coll1", "collection"),
    ]
    for base_url, expected_scope in cases:
        local = {"items_routing_config": {"operations": {
            "WRITE": [{"driver_ref": "items_postgresql_driver",
                       "on_failure": "fatal", "write_mode": "sync"}]
        }}}
        with patch(
            "dynastore.extensions.configs.config_api_service.list_registered_configs",
            return_value=registry,
        ):
            ConfigApiService(config_service=MagicMock())._build_routing_refs(
                local, base_url=base_url,
            )
        ref = local["items_routing_config"]["operations"]["WRITE"][0]
        assert ref["_links"][0]["title"] == (
            f"PUT this driver's config at {expected_scope} scope"
        ), f"base_url={base_url} expected scope={expected_scope}"


def test_build_routing_refs_meta_tier_reflects_active_scope():
    """#585 — `_meta.tier` mirrors the active composed scope (platform |
    catalog | collection) inferred from ``base_url``.  Lets operators
    distinguish "this entry surfaced at collection scope" from "this
    entry surfaced at platform scope" without a second request."""
    registry = _stub_registry(items_postgresql_driver={"__module__": "m"})
    cases = [
        ("http://h/configs", "platform"),
        ("http://h/configs/catalogs/cat1", "catalog"),
        ("http://h/configs/catalogs/cat1/collections/coll1", "collection"),
    ]
    for base_url, expected_tier in cases:
        local = {"items_routing_config": {"operations": {
            "WRITE": [{"driver_ref": "items_postgresql_driver",
                       "on_failure": "fatal", "write_mode": "sync",
                       "source": "auto"}]
        }}}
        with patch(
            "dynastore.extensions.configs.config_api_service.list_registered_configs",
            return_value=registry,
        ):
            ConfigApiService(config_service=MagicMock())._build_routing_refs(
                local, base_url=base_url,
            )
        ref = local["items_routing_config"]["operations"]["WRITE"][0]
        assert ref["_meta"]["tier"] == expected_tier, (
            f"base_url={base_url} expected tier={expected_tier}"
        )
        assert ref["_meta"]["source"] == "auto"


def test_build_routing_refs_meta_omits_source_when_missing():
    """#585 — legacy / test-fixture entries without ``source`` get a
    ``_meta`` block with only ``tier`` (no ``source`` key)."""
    local = {"items_routing_config": {"operations": {
        "WRITE": [{"driver_ref": "items_postgresql_driver",
                   "on_failure": "fatal", "write_mode": "sync"}]
    }}}
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=_stub_registry(items_postgresql_driver={"__module__": "m"}),
    ):
        ConfigApiService(config_service=MagicMock())._build_routing_refs(
            local, base_url="http://h/configs",
        )
    ref = local["items_routing_config"]["operations"]["WRITE"][0]
    assert ref["_meta"] == {"tier": "platform"}
    assert "source" not in ref


def test_build_routing_refs_surfaces_transformer_attachments():
    """#501 followup — ``input_transformers`` and ``output_transformers``
    on each ``OperationDriverEntry`` flow through to the slim
    ``DriverRef``.  Without this, operators reading the configs API
    can't see which transformer chain attaches to which (operation,
    driver) pair — the attachment is invisible until something blows up
    at runtime."""
    by_class = {
        "items_routing_config": {
            "operations": {
                "INDEX": [{
                    "driver_ref": "items_elasticsearch_private_driver",
                    "input_transformers": ("private_entity_transformer",),
                    "output_transformers": (),
                    "on_failure": "outbox", "write_mode": "sync",
                    "source": "auto",
                }],
                "SEARCH": [{
                    "driver_ref": "items_elasticsearch_private_driver",
                    "input_transformers": (),
                    "output_transformers": ("private_entity_transformer",),
                    "on_failure": "fatal", "write_mode": "sync",
                    "source": "auto",
                }],
            },
        },
    }
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=_stub_registry(
            items_elasticsearch_private_driver={"__module__": "m"},
        ),
    ):
        ConfigApiService(config_service=MagicMock())._build_routing_refs(
            by_class, base_url="http://h/configs",
        )
    [idx] = by_class["items_routing_config"]["operations"]["INDEX"]
    [srch] = by_class["items_routing_config"]["operations"]["SEARCH"]
    assert idx["input_transformers"] == ["private_entity_transformer"]
    assert idx["output_transformers"] == []
    assert srch["input_transformers"] == []
    assert srch["output_transformers"] == ["private_entity_transformer"]


def test_build_routing_refs_transformer_lists_default_empty():
    """Entries without transformer attachment surface empty lists, not
    missing keys — keeps the response shape stable across migrations."""
    local = {"items_routing_config": {"operations": {
        "WRITE": [{"driver_ref": "items_postgresql_driver",
                   "on_failure": "fatal", "write_mode": "sync"}]
    }}}
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=_stub_registry(items_postgresql_driver={"__module__": "m"}),
    ):
        ConfigApiService(config_service=MagicMock())._build_routing_refs(
            local, base_url="http://h/configs",
        )
    ref = local["items_routing_config"]["operations"]["WRITE"][0]
    assert ref["input_transformers"] == []
    assert ref["output_transformers"] == []


def test_build_routing_refs_unregistered_driver_emits_no_link():
    """Cycle F.7d.3 — composition sub-drivers with no registered config
    emit zero links.  Drops the old confusing ``config_ref: null`` shape."""
    by_class = {
        "catalog_routing_config": {
            "operations": {
                "WRITE": [{"driver_ref": "UnknownDriver",
                           "on_failure": "warn", "write_mode": "sync"}]
            },
        },
    }
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value={},
    ):
        svc = ConfigApiService(config_service=MagicMock())
        svc._build_routing_refs(by_class, base_url="http://h/configs")
    ref = by_class["catalog_routing_config"]["operations"]["WRITE"][0]
    assert ref["driver_ref"] == "UnknownDriver"
    assert "config_ref" not in ref
    assert ref["_links"] == []


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
    """``meta=none`` suppresses ``_meta`` siblings on every leaf and the
    response model no longer carries the retired top-level ``meta`` field."""
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=({}, {}, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_get_extra_refs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()):
        response = await svc.compose_collection_config(
            base_url="http://test", catalog_id="c", collection_id="col",
            meta="none",
        )
    assert not hasattr(response, "meta")


@pytest.mark.asyncio
async def test_compose_catalog_meta_field_inlines_meta_on_leaf(mock_config_service):
    """#517: ``meta=field`` injects ``_meta = {docs: {...}}`` INLINE
    on each in-scope plugin leaf — replacing the retired parallel ``meta``
    tree.  Path through ``configs`` resolves to the leaf, which carries
    its plugin fields alongside the ``_meta`` sibling."""
    svc = ConfigApiService(config_service=mock_config_service)

    class FakeWebConfig:
        _address = ("platform", "web")
        _visibility = None

        @classmethod
        def model_json_schema(cls):
            return {"properties": {"brand_name": {"description": "Brand label."}}}

    by_class = {"WebConfig": {"brand_name": "x"}}
    sources = {"WebConfig": "default"}
    registry = {"WebConfig": FakeWebConfig}
    ConfigApiService._extract_docs.cache_clear()
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=(by_class, sources, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_get_extra_refs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch.object(svc, "_get_extra_refs", new=AsyncMock(return_value={})), \
         patch(
             "dynastore.extensions.configs.config_api_service.list_registered_configs",
             return_value=registry,
         ):
        r = await svc.compose_catalog_config(
            base_url="http://test", catalog_id="c",
            meta="field", include="upstream",
        )
    leaf = r.configs["platform"]["web"]["WebConfig"]
    assert leaf["brand_name"] == "x"
    # Per #665 slice 3 ``_meta`` carries ``{tier, source}`` always; the
    # ``docs`` extras merge in when ``meta=field``.
    assert leaf["_meta"]["docs"] == {"brand_name": "Brand label."}
    assert leaf["_meta"]["tier"] == "catalog"
    # Top-level ``meta`` field is gone.
    assert not hasattr(r, "meta")


@pytest.mark.asyncio
async def test_compose_catalog_meta_schema_inlines_full_json_schema(mock_config_service):
    """``meta=schema`` injects ``_meta = {json_schema: <full schema>}``."""
    svc = ConfigApiService(config_service=mock_config_service)

    class FakeWebConfig:
        _address = ("platform", "web")
        _visibility = None

        @classmethod
        def model_json_schema(cls):
            return {"title": "WebConfig", "type": "object",
                    "properties": {"brand_name": {"type": "string"}}}

    by_class = {"WebConfig": {"brand_name": "x"}}
    sources = {"WebConfig": "default"}
    registry = {"WebConfig": FakeWebConfig}
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=(by_class, sources, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_get_extra_refs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch(
             "dynastore.extensions.configs.config_api_service.list_registered_configs",
             return_value=registry,
         ):
        r = await svc.compose_catalog_config(
            base_url="http://test", catalog_id="c",
            meta="schema", include="upstream",
        )
    leaf = r.configs["platform"]["web"]["WebConfig"]
    assert leaf["_meta"]["json_schema"]["title"] == "WebConfig"


# ---------------------------------------------------------------------------
# compose_* — links= query parameter (inline _links per leaf)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compose_catalog_links_none_opt_out_suppresses_leaf_links(mock_config_service):
    """``links=none`` is the opt-out path: no leaf carries ``_links``;
    response-level ``_links`` holds only the ``self`` discovery entry."""
    svc = ConfigApiService(config_service=mock_config_service)

    class FakeWebConfig:
        _address = ("platform", "web")
        _visibility = None

        @classmethod
        def model_json_schema(cls):
            return {"properties": {}}

    by_class = {"WebConfig": {"brand_name": "x"}}
    sources = {"WebConfig": "default"}
    registry = {"WebConfig": FakeWebConfig}
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=(by_class, sources, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_get_extra_refs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch(
             "dynastore.extensions.configs.config_api_service.list_registered_configs",
             return_value=registry,
         ):
        r = await svc.compose_catalog_config(
            base_url="http://test/configs/catalogs/c", catalog_id="c",
            meta="none", include="upstream", links="none",
        )
    leaf = r.configs["platform"]["web"]["WebConfig"]
    assert "_links" not in leaf
    # Response-level ``links`` field was retired in #665 slice 3 — root
    # response no longer carries an array (the single ``self`` entry
    # was a runtime copy of OpenAPI's query-param schema).
    assert not hasattr(r, "links")


@pytest.mark.asyncio
async def test_compose_catalog_links_minimal_emits_four_rels_no_titles(mock_config_service):
    """``links=minimal`` emits the 4-rel set per in-scope leaf with no
    ``title`` keys.  URLs reflect the active scope."""
    svc = ConfigApiService(config_service=mock_config_service)

    class FakeWebConfig:
        _address = ("platform", "web")
        _visibility = None

        @classmethod
        def model_json_schema(cls):
            return {"properties": {}}

    by_class = {"WebConfig": {"brand_name": "x"}}
    sources = {"WebConfig": "default"}
    registry = {"WebConfig": FakeWebConfig}
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=(by_class, sources, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_get_extra_refs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch(
             "dynastore.extensions.configs.config_api_service.list_registered_configs",
             return_value=registry,
         ):
        r = await svc.compose_catalog_config(
            base_url="http://test/configs/catalogs/c", catalog_id="c",
            meta="none", include="upstream", links="minimal",
        )
    leaf = r.configs["platform"]["web"]["WebConfig"]
    rels_methods = {(lk["rel"], lk["method"]) for lk in leaf["_links"]}
    assert rels_methods == {
        ("self", "GET"), ("edit", "PUT"), ("edit", "DELETE"),
        ("describedby", "GET"),
    }
    # No titles in minimal mode.
    assert all("title" not in lk for lk in leaf["_links"])
    # edit hrefs are scope-correct (catalog scope, not registry root).
    edit_put = next(lk for lk in leaf["_links"]
                    if lk["rel"] == "edit" and lk["method"] == "PUT")
    assert edit_put["href"] == "http://test/configs/catalogs/c/plugins/WebConfig"
    # describedby drops the scope path — registry is scope-agnostic.
    describedby = next(lk for lk in leaf["_links"] if lk["rel"] == "describedby")
    assert describedby["href"] == "http://test/configs/registry/WebConfig"


@pytest.mark.asyncio
async def test_compose_collection_links_full_titles_name_scope(mock_config_service):
    """``links=full`` adds a contextual ``title`` per link naming the
    class key and the collection's tier phrase (catalog/collection)."""
    svc = ConfigApiService(config_service=mock_config_service)

    class FakeRoutingConfig:
        _address = ("storage", "routing")
        _visibility = "collection"

        @classmethod
        def model_json_schema(cls):
            return {"properties": {}}

    by_class = {"items_routing": {"operations": {}}}
    sources = {"items_routing": "default"}
    registry = {"items_routing": FakeRoutingConfig}
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=(by_class, sources, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_get_extra_refs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch(
             "dynastore.extensions.configs.config_api_service.list_registered_configs",
             return_value=registry,
         ):
        r = await svc.compose_collection_config(
            base_url="http://test/configs/catalogs/cat/collections/col",
            catalog_id="cat", collection_id="col",
            meta="none", include="upstream", links="full",
        )
    leaf = r.configs["storage"]["routing"]["items_routing"]
    assert all("title" in lk for lk in leaf["_links"])
    edit_put = next(lk for lk in leaf["_links"]
                    if lk["rel"] == "edit" and lk["method"] == "PUT")
    # Title names both catalog and collection at collection scope.
    assert "items_routing" in edit_put["title"]
    assert "cat" in edit_put["title"]
    assert "col" in edit_put["title"]


@pytest.mark.asyncio
async def test_compose_collection_links_full_adds_schema_and_engine_rels(mock_config_service):
    """``links=full`` adds two cross-link affordances per leaf:
    ``rel="schema"`` (raw JSON Schema via ``?meta=schema``) always, and
    ``rel="engine"`` only when the leaf payload carries a non-null
    ``engine_ref``.  ``links=minimal`` does NOT emit either."""
    svc = ConfigApiService(config_service=mock_config_service)

    class FakeDriverConfig:
        _address = ("platform", "catalog", "collection", "drivers")
        _visibility = "collection"

        @classmethod
        def model_json_schema(cls):
            return {"properties": {}}

    by_class = {
        "collection_postgresql_driver": {"engine_ref": "postgresql_engine"},
        "items_routing": {"operations": {}},
    }
    sources = {"collection_postgresql_driver": "default", "items_routing": "default"}
    registry = {
        "collection_postgresql_driver": FakeDriverConfig,
        "items_routing": FakeDriverConfig,
    }
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=(by_class, sources, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_get_extra_refs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch(
             "dynastore.extensions.configs.config_api_service.list_registered_configs",
             return_value=registry,
         ):
        r = await svc.compose_collection_config(
            base_url="http://test/configs/catalogs/cat/collections/col",
            catalog_id="cat", collection_id="col",
            meta="none", include="upstream", links="full",
        )
    driver_leaf = r.configs["platform"]["catalog"]["collection"]["drivers"]["collection_postgresql_driver"]
    rels = {(lk["rel"], lk["method"]) for lk in driver_leaf["_links"]}
    # The 4 always-on affordances + schema + engine.
    assert ("self", "GET") in rels
    assert ("edit", "PUT") in rels
    assert ("edit", "DELETE") in rels
    assert ("describedby", "GET") in rels
    assert ("schema", "GET") in rels
    assert ("engine", "GET") in rels

    schema_link = next(lk for lk in driver_leaf["_links"] if lk["rel"] == "schema")
    assert schema_link["href"].endswith("/registry/collection_postgresql_driver?meta=schema")

    engine_link = next(lk for lk in driver_leaf["_links"] if lk["rel"] == "engine")
    # Engine link targets the platform-root configs (no /catalogs/.../ segment).
    assert engine_link["href"] == "http://test/configs/plugins/postgresql_engine"

    # Non-driver leaf has no engine_ref → no rel="engine".
    routing_leaf = r.configs["platform"]["catalog"]["collection"]["drivers"]["items_routing"]
    routing_rels = {lk["rel"] for lk in routing_leaf["_links"]}
    assert "engine" not in routing_rels
    # But rel="schema" is always present in full mode.
    assert "schema" in routing_rels


@pytest.mark.asyncio
async def test_compose_collection_links_minimal_skips_schema_and_engine(mock_config_service):
    """``links=minimal`` keeps the 4 always-on affordances and does NOT
    emit ``rel="schema"`` / ``rel="engine"`` — those are full-mode only."""
    svc = ConfigApiService(config_service=mock_config_service)

    class FakeDriverConfig:
        _address = ("platform", "catalog", "collection", "drivers")
        _visibility = "collection"

        @classmethod
        def model_json_schema(cls):
            return {"properties": {}}

    by_class = {"collection_postgresql_driver": {"engine_ref": "postgresql_engine"}}
    sources = {"collection_postgresql_driver": "default"}
    registry = {"collection_postgresql_driver": FakeDriverConfig}
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=(by_class, sources, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_get_extra_refs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch(
             "dynastore.extensions.configs.config_api_service.list_registered_configs",
             return_value=registry,
         ):
        r = await svc.compose_collection_config(
            base_url="http://test/configs/catalogs/cat/collections/col",
            catalog_id="cat", collection_id="col",
            meta="none", include="upstream", links="minimal",
        )
    leaf = r.configs["platform"]["catalog"]["collection"]["drivers"]["collection_postgresql_driver"]
    rels = {lk["rel"] for lk in leaf["_links"]}
    assert {"self", "edit", "describedby"}.issubset(rels)
    assert "schema" not in rels
    assert "engine" not in rels


def test_configs_root_url_strips_scope_segments():
    """``_configs_root_url`` strips ``/catalogs/{x}[/collections/{y}]``
    so per-leaf ``describedby`` links can target the scope-agnostic
    registry endpoint."""
    f = ConfigApiService._configs_root_url
    assert f("http://h/configs/") == "http://h/configs"
    assert f("http://h/configs/catalogs/c") == "http://h/configs"
    assert f("http://h/configs/catalogs/c/collections/col") == "http://h/configs"


@pytest.mark.asyncio
async def test_compose_platform_config_sets_platform_scope(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=({}, {}, {"platform":{},"catalog":{},"collection":{}}))), \
         patch.object(svc, "_get_extra_refs", new=AsyncMock(return_value={})), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()):
        r = await svc.compose_platform_config(base_url="http://test")
    assert r.scope == "platform"


def test_compose_tree_address_visibility_filters_correctly():
    """``_visibility = "catalog"`` keeps the class out of the *main* tree
    at collection scope under default slim mode — it surfaces in the
    hierarchical ``inherited`` tree at its natural address with a
    ``{source}`` leaf instead.
    """
    by_class = {"CatalogOnly": {"x": 1}}
    registry = _stub_registry(
        CatalogOnly={
            "_address": ("platform", "catalog", "drivers"),
            "_visibility": "catalog",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        # At platform scope under strict=True (default, Cycle F.7d.2):
        # _visibility="catalog" routes to ``inherited`` instead of inlining.
        tree = ConfigApiService._compose_tree(
            by_class, sources={"CatalogOnly": "platform"}, active_scope="platform",
        )
        assert "platform" not in tree or "CatalogOnly" not in tree.get(
            "platform", {}
        ).get("catalog", {}).get("drivers", {})
        # Platform scope under strict=False restores inclusive behavior.
        tree = ConfigApiService._compose_tree(
            by_class, sources={"CatalogOnly": "platform"}, active_scope="platform",
            strict=False,
        )
        assert "CatalogOnly" in tree["platform"]["catalog"]["drivers"]
        # At catalog scope: rendered in body via visibility match.
        tree = ConfigApiService._compose_tree(
            by_class, sources={"CatalogOnly": "catalog"}, active_scope="catalog",
        )
        assert "CatalogOnly" in tree["platform"]["catalog"]["drivers"]
        # At collection scope: NOT inlined in body; surfaces in the hierarchical
        # inherited tree at the same natural address with {source} leaf.
        tree = ConfigApiService._compose_tree(
            by_class, sources={"CatalogOnly": "catalog"}, active_scope="collection",
        )
        assert "storage" not in tree


def test_compose_tree_strict_at_platform_routes_catalog_visibility_to_inherited():
    """Cycle F.7d.2 — under ``strict=True`` (default), a catalog-tier
    template (``_visibility="catalog"``) at platform scope drops out of
    the body and surfaces in ``inherited`` at its natural address.

    Pin against the user-mental-model: platform scope shows only
    platform-intrinsic configs; catalog-tier defaults are visible only
    on demand.
    """
    by_class = {
        "platform_intrinsic": {"value": 1},
        "catalog_template":   {"value": 2},
    }
    registry = _stub_registry(
        platform_intrinsic={
            "_address": ("platform", "modules", "web"),
            "_visibility": None,
        },
        catalog_template={
            "_address": ("platform", "catalog", "drivers"),
            "_visibility": "catalog",
        },
    )
    sources = {"platform_intrinsic": "platform", "catalog_template": "platform"}
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        # Default strict=True
        tree = ConfigApiService._compose_tree(
            by_class, sources=sources, active_scope="platform",
        )
        # platform_intrinsic stays in body
        assert "platform_intrinsic" in tree["platform"]["modules"]["web"]
        # catalog_template filtered out (per #665 slice 3 — no parallel inherited tree).
        assert (
            "platform" not in tree
            or "catalog" not in tree.get("platform", {})
            or "catalog_template" not in tree["platform"]["catalog"].get("drivers", {})
        )


def test_compose_tree_strict_keeps_platform_visibility_in_body():
    """Cycle F.7d.2-fixup — ``_visibility="platform"`` configs (engines,
    security, etc.) stay in body at platform scope under strict.  The
    F.7d.2 cut only allowed ``_visibility=None`` and silently routed
    engine configs to ``inherited``, which was a bug — engines ARE
    platform-tier resources by definition.
    """
    by_class = {"engine_a": {"value": 1}}
    registry = _stub_registry(
        engine_a={
            "_address": ("platform", "engines"),
            "_visibility": "platform",
        },
    )
    sources = {"engine_a": "platform"}
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources=sources, active_scope="platform",
        )
        # Engine stays in body under strict=True default.
        assert "engine_a" in tree["platform"]["engines"]


def test_compose_tree_strict_false_restores_inclusive_platform_behavior():
    """Cycle F.7d.2 — ``strict=False`` brings back the previous always-true
    platform-scope inclusion: catalog-tier templates inline in the body,
    no inherited tree."""
    by_class = {"catalog_template": {"value": 2}}
    registry = _stub_registry(
        catalog_template={
            "_address": ("platform", "catalog", "drivers"),
            "_visibility": "catalog",
        },
    )
    sources = {"catalog_template": "platform"}
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources=sources, active_scope="platform", strict=False,
        )
        assert "catalog_template" in tree["platform"]["catalog"]["drivers"]


def test_compose_tree_strict_no_op_at_catalog_and_collection_scope():
    """Cycle F.7d.2 — ``strict`` only narrows platform scope.  At catalog
    and collection scope, the per-tier ``_visibility`` filter already
    runs; ``strict`` is accepted for API symmetry but doesn't change
    behavior there."""
    by_class = {"catalog_template": {"value": 2}}
    registry = _stub_registry(
        catalog_template={
            "_address": ("platform", "catalog", "drivers"),
            "_visibility": "catalog",
        },
    )
    sources = {"catalog_template": "catalog"}
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        for strict in (True, False):
            tree = ConfigApiService._compose_tree(
                by_class, sources=sources, active_scope="catalog", strict=strict,
            )
            assert "catalog_template" in tree["platform"]["catalog"]["drivers"]


def test_compose_tree_collection_scope_surfaces_full_config_tree():
    """At collection scope under the default mode the response surfaces every
    leaf that ``_place()`` accepts — collection-vis, catalog-vis (read-only
    context), and null-visibility (overrideable).  ``_meta.source`` on each
    leaf reports the effective tier.  Issue #761.
    """
    by_class = {
        "items_postgresql_driver":      {"sidecars": []},
        "elasticsearch_catalog_config": {"private": True},
        "catalog_routing_config":       {"enabled": True},
        "catalog_postgresql_driver":    {},
        "web_config":                   {"brand_name": "X"},
    }
    sources = {
        "items_postgresql_driver":      "collection",
        "elasticsearch_catalog_config": "catalog",
        "catalog_routing_config":       "catalog",
        "catalog_postgresql_driver":    "catalog",
        "web_config":                   "platform",
    }
    registry = _stub_registry(
        items_postgresql_driver={
            "_address": ("platform", "catalog", "collection", "items", "drivers"),
            "_visibility": "collection",
        },
        elasticsearch_catalog_config={
            "_address": ("platform", "catalog", "elasticsearch"),
            "_visibility": "catalog",
        },
        catalog_routing_config={
            "_address": ("platform", "catalog", "routing"),
            "_visibility": "catalog",
        },
        catalog_postgresql_driver={
            "_address": ("platform", "catalog", "drivers"),
            "_visibility": "catalog",
        },
        web_config={
            "_address": ("platform", "web"),
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources=sources, active_scope="collection",
        )

    # Collection-vis stays in body.
    coll_node = tree["platform"]["catalog"]["collection"]["items"]["drivers"]
    assert {k: v for k, v in coll_node["items_postgresql_driver"].items() if not k.startswith("_")} == {"sidecars": []}
    # No sibling ``inherited_from_catalog`` block (Cycle D.3 dropped it).
    assert "inherited_from_catalog" not in tree
    # Null-visibility (web_config) included; source reports the effective tier.
    assert {k: v for k, v in tree["platform"]["web"]["web_config"].items() if not k.startswith("_")} == {"brand_name": "X"}
    assert tree["platform"]["web"]["web_config"]["_meta"]["source"] == "platform"
    # Catalog-vis configs ARE included at collection scope (#761) — operators
    # see the full waterfall.  Each leaf's _meta.source reports source="catalog".
    assert tree["platform"]["catalog"]["elasticsearch"]["elasticsearch_catalog_config"]["_meta"]["source"] == "catalog"
    assert tree["platform"]["catalog"]["routing"]["catalog_routing_config"]["_meta"]["source"] == "catalog"
    assert tree["platform"]["catalog"]["drivers"]["catalog_postgresql_driver"]["_meta"]["source"] == "catalog"


def test_compose_tree_catalog_scope_slim_mode_visibility():
    """At catalog scope under slim mode:

    - ``_visibility="catalog"`` configs stay in body.
    - ``_visibility=None`` (universal) configs stay in body — null-visibility
      means "overrideable at any tier"; ``_meta.source`` reports where the
      effective value comes from.
    """
    by_class = {
        "catalog_routing_config": {"enabled": True},
        "web_config":             {"brand_name": "X"},
    }
    sources = {
        "catalog_routing_config": "catalog",
        "web_config":             "platform",
    }
    registry = _stub_registry(
        catalog_routing_config={
            "_address": ("platform", "catalog", "routing"),
            "_visibility": "catalog",
        },
        web_config={"_address": ("platform", "web")},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources=sources, active_scope="catalog",
        )
    # Catalog-vis stays in body.
    assert {k: v for k, v in tree["platform"]["catalog"]["routing"]["catalog_routing_config"].items() if not k.startswith("_")} == {"enabled": True}
    # Null-visibility config included at catalog scope; source="platform" (no catalog override).
    assert {k: v for k, v in tree["platform"]["web"]["web_config"].items() if not k.startswith("_")} == {"brand_name": "X"}
    assert tree["platform"]["web"]["web_config"]["_meta"]["source"] == "platform"


def test_compose_tree_catalog_vis_surfaces_at_collection_scope_with_docs():
    """Post-#761: catalog-vis configs surface in the collection response with
    full ``_meta`` (docs/source/tier).  Operators see the catalog-tier
    config alongside what their collection inherits.
    """
    class FakeESCatConfig:
        _address = ("platform", "catalog", "elasticsearch")
        _visibility = "catalog"

        @classmethod
        def model_json_schema(cls):
            return {"properties": {"private": {"description": "Private mode."}}}

    by_class = {"elasticsearch_catalog_config": {"private": True}}
    sources = {"elasticsearch_catalog_config": "catalog"}
    registry = {"elasticsearch_catalog_config": FakeESCatConfig}
    ConfigApiService._extract_docs.cache_clear()
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources=sources, active_scope="collection", meta_mode="field",
        )
    leaf = tree["platform"]["catalog"]["elasticsearch"]["elasticsearch_catalog_config"]
    assert leaf["private"] is True
    assert leaf["_meta"]["source"] == "catalog"
    assert leaf["_meta"]["tier"] == "collection"
    assert leaf["_meta"]["docs"] == {"private": "Private mode."}


# ---------------------------------------------------------------------------
# Slim mode (?include=scope, default) — body shows configs visible at the
# active scope: explicitly scoped, null-visibility, or stored at that scope.
# ---------------------------------------------------------------------------


def test_compose_tree_slim_null_visibility_included_at_non_platform_scopes():
    """``_visibility=None`` (null / universal) configs appear in the body at
    catalog and collection scope under the default ``include="scope"`` mode.

    Null-visibility means "overrideable at any tier."  Module, extension,
    and task configs carry this default so catalog/collection admins can
    inspect and override them at their scope.  ``_meta.source`` on each
    leaf reports the effective tier.
    """
    by_class = {
        "items_postgresql_driver": {"sidecars": []},  # _visibility=collection
        "web_config": {"brand_name": "X"},            # _visibility=None (universal)
    }
    registry = _stub_registry(
        items_postgresql_driver={
            "_address": ("platform", "catalog", "collection", "items", "drivers"),
            "_visibility": "collection",
        },
        web_config={"_address": ("platform", "web")},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={"web_config": "platform"},
            active_scope="collection",
            # default include_mode="scope"
        )
    # Collection-owned config stays in the body.
    assert {k: v for k, v in tree["platform"]["catalog"]["collection"]["items"]["drivers"]["items_postgresql_driver"].items() if not k.startswith("_")} == {"sidecars": []}
    # Null-visibility config is now included at collection scope.
    assert {k: v for k, v in tree["platform"]["web"]["web_config"].items() if not k.startswith("_")} == {"brand_name": "X"}
    # Source shows where the effective value comes from.
    assert tree["platform"]["web"]["web_config"]["_meta"]["source"] == "platform"


def test_compose_tree_slim_keeps_collection_overrides_in_body():
    """A class with universal visibility BUT an explicit collection-scope
    row IS in-scope — it stays in the body."""
    by_class = {"web_config": {"brand_name": "Tenant Override"}}
    registry = _stub_registry(
        web_config={"_address": ("platform", "web")},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={"web_config": "collection"},
            active_scope="collection",
        )
    # source==collection means an explicit override exists → stays in body
    assert {k: v for k, v in tree["platform"]["web"]["web_config"].items() if not k.startswith("_")} == {"brand_name": "Tenant Override"}
    # Nothing was diverted


def test_compose_tree_upstream_mode_renders_everything_in_body():
    """``include_mode="upstream"`` restores the verbose pre-slim default —
    every visible class lands in the tree regardless of source/visibility."""
    by_class = {
        "web_config": {"brand_name": "X"},
        "items_postgresql_driver": {"sidecars": []},
    }
    registry = _stub_registry(
        web_config={"_address": ("platform", "web")},
        items_postgresql_driver={
            "_address": ("platform", "catalog", "collection", "items", "drivers"),
            "_visibility": "collection",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="collection",
            include_mode="upstream",
        )
    # Both rendered in body, no inherited summary
    assert {k: v for k, v in tree["platform"]["web"]["web_config"].items() if not k.startswith("_")} == {"brand_name": "X"}
    assert {k: v for k, v in tree["platform"]["catalog"]["collection"]["items"]["drivers"]["items_postgresql_driver"].items() if not k.startswith("_")} == {"sidecars": []}


# NOTE: The "entity" annotation on ConfigMeta and the per-class
# ``ConfigMeta.source`` / ``.layers`` waterfall were retired in Cycle B.
# Driver-tier entity grouping (items vs collection vs assets) now lives
# in the tree path itself: a driver lives at
# ``configs.platform.catalog.{tier}.drivers`` so the path encodes the bucket
# directly — no per-entry annotation needed.


def test_compose_tree_slim_at_platform_scope_is_a_noop():
    """At platform scope the slim filter is a no-op — platform IS the top
    tier, nothing is upstream."""
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry(
        web_config={"_address": ("platform", "web")},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform",
        )
    assert {k: v for k, v in tree["platform"]["web"]["web_config"].items() if not k.startswith("_")} == {"brand_name": "X"}


# ---------------------------------------------------------------------------
# Production placement-bug regression tests — were misplaced under the
# heuristic; explicit `_address` fixes them.
# ---------------------------------------------------------------------------

def test_catalog_policy_config_lands_at_catalog_scope():
    """``CatalogPrivacy`` (Cycle E.1 — replaces
    ``ElasticsearchCatalogConfig``) carries the catalog-tier privacy
    default; address pins it under ``catalog.policy``.
    """
    from dynastore.modules.catalog.catalog_config import CatalogPrivacy

    assert CatalogPrivacy._address == ("platform", "catalog", "privacy")
    assert CatalogPrivacy._visibility == "catalog"


# NOTE: ``ElasticsearchCollectionConfig`` was retired in Cycle C and
# ``ElasticsearchCatalogConfig`` was retired in Cycle E.1.  Privacy
# moves to ``CatalogPrivacy.collection_defaults.is_private`` (catalog
# tier) and (Cycle F.0d) ``is_private: bool`` on ``CollectionPrivacy``
# at ``(platform, catalog, collection, privacy)``.


def test_catalog_es_driver_lands_under_storage_drivers_catalog():
    """``CatalogElasticsearchDriverConfig`` was leaking to ``platform.misc``."""
    from dynastore.modules.elasticsearch.catalog_es_driver import (
        CatalogElasticsearchDriverConfig,
    )

    assert CatalogElasticsearchDriverConfig._address == ("platform", "catalog", "drivers")
    assert CatalogElasticsearchDriverConfig._visibility == "catalog"


def test_collection_es_driver_lands_under_storage_drivers_collection():
    """``CollectionElasticsearchDriverConfig`` was leaking to ``platform.misc``."""
    from dynastore.modules.elasticsearch.collection_es_driver import (
        CollectionElasticsearchDriverConfig,
    )

    assert CollectionElasticsearchDriverConfig._address == ("platform", "catalog", "collection", "drivers")
    assert CollectionElasticsearchDriverConfig._visibility == "catalog"


def test_assets_plugin_config_visible_at_all_scopes():
    """Extension config was incorrectly gated to collection by ``Asset*`` name."""
    from dynastore.extensions.assets.config import AssetsPluginConfig

    assert AssetsPluginConfig._address == ("platform", "extensions", "assets")
    assert AssetsPluginConfig._visibility is None  # visible everywhere


def test_761_full_config_surface_at_catalog_and_collection_scope():
    """Regression for #761: the catalog and collection composed response
    must surface the full configurable surface — modules, extensions,
    tasks, engines, drivers — not just configs that explicitly carry
    ``_visibility="catalog"``/``"collection"``.

    Before the fix, ``_is_in_scope()`` at non-platform scopes filtered out
    every leaf that lacked a stored row at the active scope AND didn't
    match ``_visibility=active_scope``.  The ``gcp`` config was the only
    leaf because it happened to declare ``_visibility="catalog"``;
    engines, modules, extensions, tasks disappeared.

    After the fix, every leaf placed by ``_place()`` appears.  ``_meta.source``
    on each reports where the effective value comes from; ``_visibility``
    continues to gate writability via the service layer.
    """
    from dynastore.modules.tasks.tasks_config import TasksPluginConfig
    from dynastore.modules.web.models import WebPageSettingsConfig
    from dynastore.modules.cache.cache_config import CachePluginConfig
    from dynastore.modules.db_config.engine_config import (
        PostgresqlEngineConfig,
        ElasticsearchEngineConfig,
    )

    null_vis = (TasksPluginConfig, WebPageSettingsConfig, CachePluginConfig)
    platform_vis = (PostgresqlEngineConfig, ElasticsearchEngineConfig)
    for cls in null_vis:
        assert getattr(cls, "_visibility", None) is None, (
            f"{cls.__name__} unexpectedly declares _visibility"
        )
    for cls in platform_vis:
        assert getattr(cls, "_visibility", None) == "platform", (
            f"{cls.__name__} expected _visibility='platform'"
        )

    from dynastore.extensions.configs.config_api_service import (
        ConfigApiService,
        _place,
    )

    # Build a representative by_class with one of each kind.
    representative = null_vis + platform_vis
    by_class = {cls.class_key(): cls().model_dump() for cls in representative}
    sources = {k: "default" for k in by_class}

    for scope in ("catalog", "collection"):
        tree = ConfigApiService._compose_tree(
            by_class, sources=sources, active_scope=scope,
        )
        assert len(tree) > 0, f"empty tree at {scope} scope"
        for cls in representative:
            address = _place(cls, scope)
            assert address is not None, (
                f"{cls.__name__} dropped by _place at scope={scope}"
            )
            node = tree
            for seg in address:
                if seg is None:
                    continue
                assert seg in node, (
                    f"{cls.__name__} address segment '{seg}' missing at scope={scope}; "
                    f"keys: {list(node.keys())}"
                )
                node = node[seg]
            assert cls.class_key() in node, (
                f"{cls.__name__} ({cls.class_key()}) missing from tree at scope={scope}"
            )
            leaf = node[cls.class_key()]
            assert "_meta" in leaf
            assert leaf["_meta"]["source"] == "default"


# ---------------------------------------------------------------------------
# Enforcement — every concrete PluginConfig must declare _address.
# ---------------------------------------------------------------------------

def test_concrete_subclass_without_address_raises():
    """``PluginConfig.__init_subclass__`` enforces ``_address`` on concrete subclasses."""
    from typing import ClassVar

    from dynastore.modules.db_config.plugin_config import PluginConfig

    with pytest.raises(TypeError, match=r"does not declare ``_address``"):
        class _BadConcreteConfig(PluginConfig):  # noqa: F841 — deliberate
            field: ClassVar[int] = 1


def test_concrete_subclass_with_address_ok():
    from typing import ClassVar, Optional, Tuple

    from dynastore.modules.db_config.plugin_config import PluginConfig

    class _GoodConcreteConfig(PluginConfig):
        _address: ClassVar[Tuple[str, str, Optional[str]]] = ("platform", "misc", None)

    assert _GoodConcreteConfig._address == ("platform", "misc", None)


def test_abstract_subclass_without_address_ok():
    """Abstract bases (``is_abstract_base = True``) opt out of the check."""
    from typing import ClassVar

    from dynastore.modules.db_config.plugin_config import PluginConfig

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
# tree mirroring ``configs`` with ``{docs}`` or ``{json_schema}``
# leaves; tier-of-origin is communicated via the top-level
# ``inherited`` map.  The dropped tests covered:
#   - test_build_meta_entry_default_only
#   - test_build_meta_entry_platform_only
#   - test_build_meta_entry_platform_then_catalog_overrides
#   - test_build_meta_entry_full_waterfall
#   - test_build_meta_entry_no_tier_data_returns_source_only
#   - test_compose_tree_meta_includes_layers_when_tier_data_provided
