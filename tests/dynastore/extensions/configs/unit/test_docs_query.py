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


def test_compose_tree_meta_none_keeps_provenance():
    """Per #665 slice 3: every leaf carries ``_meta = {tier, source}``
    even under ``meta_mode="none"`` (provenance is structural).  Only
    the field-level extras (``docs`` / ``json_schema``) are suppressed.
    """
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web")},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", meta_mode="none",
        )
    leaf = tree["platform"]["web"]["web_config"]
    assert leaf["_meta"] == {"tier": "platform", "source": "default"}
    assert "docs" not in leaf["_meta"]
    assert "json_schema" not in leaf["_meta"]


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
        tree = ConfigApiService._compose_tree(
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
        tree = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", meta_mode="schema",
        )
    leaf = tree["platform"]["web"]["web_config"]
    assert leaf["_meta"]["json_schema"] == schema
    # Field-docs leaf must not appear under schema mode.
    assert "docs" not in leaf["_meta"]


def test_compose_tree_slim_filters_upstream_tier_configs():
    """Slim mode (default ``include=scope``): upstream-tier configs are
    filtered out of the tree entirely.  Provenance for the configs that
    DO render at the active scope still lives on their own
    ``_meta.source`` — per #665 slice 3 the parallel ``inherited`` tree
    is retired.
    """
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web")},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree = ConfigApiService._compose_tree(
            by_class, sources={"web_config": "platform"},
            active_scope="collection", meta_mode="field",
            include_mode="scope",
        )
    # Platform-tier config dropped from collection-scope slim view.
    assert tree == {}


def test_compose_tree_catalog_tier_under_upstream_mode_gets_meta():
    """Catalog-tier configs at collection scope are filtered out under
    slim mode, but render inlined under ``include=upstream`` with
    ``_meta = {tier=collection, source=catalog, docs={...}}``.
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
        tree = ConfigApiService._compose_tree(
            by_class, sources={"elasticsearch_catalog_config": "catalog"},
            active_scope="collection", meta_mode="field",
            include_mode="upstream",
        )
    leaf = tree["platform"]["catalog"]["elasticsearch"]["elasticsearch_catalog_config"]
    assert leaf["private"] is True
    assert leaf["_meta"]["tier"] == "collection"
    assert leaf["_meta"]["source"] == "catalog"
    assert leaf["_meta"]["docs"] == {"private": "Private mode."}


def test_compose_tree_meta_tier_source_always_present_across_modes():
    """#665 slice 3 lockdown: every rendered leaf carries
    ``_meta = {tier, source}`` regardless of ``meta_mode``.  Provenance
    is structural, not opt-in.  Field-level extras (``docs`` /
    ``json_schema``) gate on the mode; ``tier`` + ``source`` do not.
    """
    schema = {"properties": {"brand_name": {"description": "Brand label."}}}
    by_class = {"web_config": {"brand_name": "X"}}
    registry = _stub_registry_with_schema(
        web_config={"_address": ("platform", "web"), "schema": schema},
    )
    for mode, must_have, must_not_have in [
        ("none",   set(),               {"docs", "json_schema"}),
        ("field",  {"docs"},            {"json_schema"}),
        ("schema", {"json_schema"},     {"docs"}),
    ]:
        with patch(
            "dynastore.extensions.configs.config_api_service.list_registered_configs",
            return_value=registry,
        ):
            ConfigApiService._extract_docs.cache_clear()
            tree = ConfigApiService._compose_tree(
                {"web_config": {"brand_name": "X"}},
                sources={"web_config": "platform"},
                active_scope="catalog",
                meta_mode=mode,
                include_mode="upstream",
            )
        leaf = tree["platform"]["web"]["web_config"]
        meta = leaf["_meta"]
        assert meta["tier"] == "catalog", f"mode={mode}: tier missing/wrong"
        assert meta["source"] == "platform", f"mode={mode}: source missing/wrong"
        for k in must_have:
            assert k in meta, f"mode={mode}: missing extra {k}"
        for k in must_not_have:
            assert k not in meta, f"mode={mode}: leaked extra {k}"
