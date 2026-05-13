"""Tests for the ``?meta=`` query mode on the composed-config endpoints.

Post #517 (2026-05-11): per-class field documentation is injected INLINE
on each in-scope plugin leaf as a ``_meta`` sibling — replacing the
retired parallel ``meta`` tree mirroring ``configs``.

  * ``?meta=none``           — no ``_meta`` key on any leaf.
  * ``?meta=field`` (default) — leaf carries ``_meta = {docs:
                                {field_name: description}}``.
  * ``?meta=schema``         — leaf carries ``_meta = {json_schema:
                                <full Pydantic schema>}``.
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
    """``meta_mode="none"`` injects no ``_meta`` sibling on any leaf."""
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web")},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", meta_mode="none",
        )
    assert "_meta" not in tree["platform"]["web"]["web_config"]


def test_compose_tree_meta_field_attaches_docs():
    """``meta_mode="field"`` injects ``_meta = {docs: …}`` ON each
    in-scope plugin leaf (sibling of the plugin's own fields)."""
    schema = {
        "properties": {
            "brand_name": {"description": "Display name."},
            "version":    {"description": "Schema version."},
        },
    }
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web"), "schema": schema},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        ConfigApiService._extract_docs.cache_clear()
        tree, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", meta_mode="field",
        )
    leaf = tree["platform"]["web"]["web_config"]
    assert leaf["brand_name"] == "X"
    assert leaf["_meta"]["docs"] == {
        "brand_name": "Display name.",
        "version":    "Schema version.",
    }


def test_compose_tree_meta_schema_attaches_full_json_schema():
    """``meta_mode="schema"`` injects ``_meta = {json_schema: …}``."""
    schema = {"title": "WebConfig", "type": "object", "properties": {}}
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web"), "schema": schema},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", meta_mode="schema",
        )
    leaf = tree["platform"]["web"]["web_config"]
    assert leaf["_meta"]["json_schema"] == schema
    # Field-docs leaf must not appear under schema mode.
    assert "docs" not in leaf["_meta"]


def test_compose_tree_meta_field_skips_inherited_classes():
    """Slim mode: configs routed to the hierarchical ``inherited`` tree
    are breadcrumbs only — they carry no ``_meta`` (not a docs surface)
    AND no ``_links`` (not actionable at the active scope).
    """
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web")},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        _, inherited = ConfigApiService._compose_tree(
            by_class, sources={"web_config": "platform"},
            active_scope="collection", meta_mode="field",
            include_mode="scope",
        )
    assert inherited is not None
    breadcrumb = inherited["platform"]["web"]["web_config"]
    assert breadcrumb == {"source": "platform"}
    assert "_meta" not in breadcrumb
    assert "_links" not in breadcrumb


def test_compose_tree_catalog_tier_under_upstream_mode_gets_meta():
    """Cycle D.3: catalog-tier configs at collection scope surface in
    ``inherited`` under slim mode (no _meta), but render inlined in the
    main tree under ``include=upstream`` — and THEN they get ``_meta``
    inline on the leaf.
    """
    schema = {"properties": {"private": {"description": "Private mode."}}}
    by_class = {"elasticsearch_catalog_config": {"private": True}}
    registry = _stub_registry_with_schema(
        elasticsearch_catalog_config={
            "_address": ("platform", "catalog", "elasticsearch"),
            "_visibility": "catalog",
            "schema": schema,
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        ConfigApiService._extract_docs.cache_clear()
        tree, inherited = ConfigApiService._compose_tree(
            by_class, sources={"elasticsearch_catalog_config": "catalog"},
            active_scope="collection", meta_mode="field",
            include_mode="upstream",
        )
    leaf = tree["platform"]["catalog"]["elasticsearch"]["elasticsearch_catalog_config"]
    assert leaf["private"] is True
    # No inherited tree under upstream mode.
    assert inherited is None
    # Meta is inline on the leaf.
    assert leaf["_meta"]["docs"] == {"private": "Private mode."}
