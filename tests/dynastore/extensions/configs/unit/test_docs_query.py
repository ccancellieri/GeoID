"""Tests for the ``?meta=`` query mode on the composed-config endpoints.

Cycle B (2026-05-05) renamed ``?docs=`` to ``?meta=`` and dropped the
waterfall trace — ``meta`` now mirrors the ``configs`` tree shape with
``{field_docs}`` or ``{json_schema}`` leaves.

  * ``?meta=none``           — ``meta`` returned as ``None``.
  * ``?meta=field`` (default) — ``{field_docs: {field_name: description}}``
                                leaf at the path mirroring ``configs``.
  * ``?meta=schema``         — ``{json_schema: <full Pydantic schema>}`` leaf.
"""

from unittest.mock import patch

from dynastore.extensions.configs.config_api_service import ConfigApiService


def _stub_registry_with_schema(**classes):
    """Build a fake registry where each class also exposes a callable
    ``model_json_schema()`` returning a deterministic stub document.
    """
    out = {}
    for name, attrs in classes.items():
        attrs = dict(attrs)
        schema = attrs.pop("schema", {"title": name, "type": "object"})

        class _Cls:
            _address = attrs.get("_address")
            _visibility = attrs.get("_visibility")

        _Cls.__name__ = name

        @classmethod
        def _model_json_schema(cls, _s=schema):
            return _s

        _Cls.model_json_schema = _model_json_schema  # type: ignore[assignment]
        out[name] = _Cls
    return out


def test_compose_tree_meta_none_suppresses_meta():
    """``meta_mode="none"`` returns ``meta=None`` regardless of payload."""
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web", None)},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        _, meta, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", meta_mode="none",
        )
    assert meta is None


def test_compose_tree_meta_field_attaches_field_docs():
    """``meta_mode="field"`` attaches ``{field_docs}`` at the path mirroring
    ``configs``."""
    schema = {
        "properties": {
            "brand_name": {"description": "Display name."},
            "version":    {"description": "Schema version."},
        },
    }
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web", None), "schema": schema},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        # Bust the lru_cache so our stub class's schema is re-extracted.
        ConfigApiService._extract_field_docs.cache_clear()
        tree, meta, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", meta_mode="field",
        )
    # configs path
    assert tree["platform"]["web"]["web_config"] == {"brand_name": "X"}
    # meta mirrors the same path with the field_docs leaf
    assert meta is not None
    assert meta["platform"]["web"]["web_config"]["field_docs"] == {
        "brand_name": "Display name.",
        "version":    "Schema version.",
    }


def test_compose_tree_meta_schema_attaches_full_json_schema():
    """``meta_mode="schema"`` attaches the full ``model_json_schema()``."""
    schema = {"title": "WebConfig", "type": "object", "properties": {}}
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web", None), "schema": schema},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, meta, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", meta_mode="schema",
        )
    assert meta is not None
    assert meta["platform"]["web"]["web_config"]["json_schema"] == schema
    # Field-docs leaf must not appear under schema mode.
    assert "field_docs" not in meta["platform"]["web"]["web_config"]


def test_compose_tree_meta_field_skips_inherited_classes():
    """Slim mode: configs deferred to the hierarchical ``inherited`` tree
    do NOT get meta entries — they're breadcrumbs, not a docs surface.
    """
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web", None)},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        _, meta, inherited = ConfigApiService._compose_tree(
            by_class, sources={"web_config": "platform"},
            active_scope="collection", meta_mode="field",
            include_mode="scope",
        )
    # The class moved to the hierarchical inherited tree (not in scope at collection).
    assert inherited is not None
    assert inherited["platform"]["web"]["web_config"] == {"source": "platform"}
    # ... and meta should NOT have an entry for it.
    assert meta is not None
    assert "platform" not in meta or "web" not in meta.get("platform", {})


def test_compose_tree_catalog_tier_under_upstream_mode_gets_meta():
    """Cycle D.3: catalog-tier configs at collection scope surface in
    ``inherited`` under slim mode (no meta), but render inlined in the
    main tree under ``include=upstream`` — and THEN they get meta
    entries at the same address.
    """
    schema = {"properties": {"private": {"description": "Private mode."}}}
    by_class = {"elasticsearch_catalog_config": {"private": True}}
    registry = _stub_registry_with_schema(
        elasticsearch_catalog_config={
            "_address": ("catalog", "elasticsearch", None),
            "_visibility": "catalog",
            "schema": schema,
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        ConfigApiService._extract_field_docs.cache_clear()
        tree, meta, inherited = ConfigApiService._compose_tree(
            by_class, sources={"elasticsearch_catalog_config": "catalog"},
            active_scope="collection", meta_mode="field",
            include_mode="upstream",
        )
    # Upstream mode: rendered inlined at its natural address.
    assert tree["catalog"]["elasticsearch"]["elasticsearch_catalog_config"] == {"private": True}
    # No inherited tree under upstream mode.
    assert inherited is None
    # Meta mirrors the configs path.
    assert meta is not None
    assert meta["catalog"]["elasticsearch"]["elasticsearch_catalog_config"]["field_docs"] == {
        "private": "Private mode.",
    }
