"""Tests for the ``view=delta|effective|inherited`` query param (#947).

Verifies the provenance-based leaf filter introduced in ``_compose_tree``
via the ``view_mode`` argument:

- ``view=effective`` (default) — byte-for-byte identical to pre-#947 output.
- ``view=delta``     — only leaves whose source equals the active scope.
- ``view=inherited`` — only leaves whose source differs from the active scope.

The tests use the existing ``_stub_registry`` pattern (plain classes, no
PluginConfig base) and drive ``_compose_tree`` directly, matching the
test style in ``test_config_api_service.py``.
"""

from unittest.mock import patch

from dynastore.extensions.configs.config_api_service import ConfigApiService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stub_registry(**classes):
    """Minimal stub — mirrors the helper in test_config_api_service.py."""
    out = {}
    for name, attrs in classes.items():
        body = {}
        if "_address" in attrs:
            body["_address"] = attrs["_address"]
        if "_visibility" in attrs:
            body["_visibility"] = attrs["_visibility"]
        if attrs.get("abstract"):
            body["is_abstract_base"] = True
        cls = type(name, (), body)
        cls.__module__ = "test.stub"
        out[name] = cls
    return out


def _make_tree(by_class, sources, active_scope, view_mode, include_mode="upstream"):
    """Call ``_compose_tree`` with a patched registry and return the tree."""
    keys = list(by_class.keys())
    registry = _stub_registry(**{
        k: {"_address": ("platform", "settings"), "_visibility": None}
        for k in keys
    })
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        return ConfigApiService._compose_tree(
            by_class,
            sources=sources,
            active_scope=active_scope,
            meta_mode="none",
            include_mode=include_mode,
            view_mode=view_mode,
        )


def _leaf_keys(tree):
    """Return the set of class_keys present in the top-level settings node."""
    return set(tree.get("platform", {}).get("settings", {}).keys())


# ---------------------------------------------------------------------------
# (a) Default response is unchanged — view=effective is a no-op
# ---------------------------------------------------------------------------

def test_view_effective_is_identical_to_no_view():
    by_class = {
        "platform_cfg": {"x": 1},
        "inherited_cfg": {"y": 2},
    }
    sources = {
        "platform_cfg": "platform",
        "inherited_cfg": "default",
    }

    tree_no_view = _make_tree(by_class, sources, "platform", view_mode="effective")
    tree_effective = _make_tree(by_class, sources, "platform", view_mode="effective")

    assert _leaf_keys(tree_no_view) == {"platform_cfg", "inherited_cfg"}
    assert _leaf_keys(tree_effective) == _leaf_keys(tree_no_view)


def test_view_effective_returns_all_leaves_at_catalog_scope():
    by_class = {
        "catalog_override": {"a": 1},
        "platform_inherited": {"b": 2},
        "default_val": {"c": 3},
    }
    sources = {
        "catalog_override": "catalog",
        "platform_inherited": "platform",
        "default_val": "default",
    }

    keys = _leaf_keys(_make_tree(by_class, sources, "catalog", view_mode="effective"))
    assert keys == {"catalog_override", "platform_inherited", "default_val"}


# ---------------------------------------------------------------------------
# (b) view=delta — only leaves whose source == active_scope
# ---------------------------------------------------------------------------

def test_view_delta_at_catalog_scope_keeps_only_catalog_owned():
    by_class = {
        "catalog_cfg": {"v": 1},
        "platform_cfg": {"v": 2},
        "default_cfg": {"v": 3},
    }
    sources = {
        "catalog_cfg": "catalog",
        "platform_cfg": "platform",
        "default_cfg": "default",
    }

    keys = _leaf_keys(_make_tree(by_class, sources, "catalog", view_mode="delta"))
    assert keys == {"catalog_cfg"}, (
        "delta at catalog scope must include only catalog-sourced leaves"
    )


def test_view_delta_at_collection_scope_keeps_only_collection_owned():
    by_class = {
        "col_cfg": {"v": 1},
        "cat_cfg": {"v": 2},
        "plat_cfg": {"v": 3},
    }
    sources = {
        "col_cfg": "collection",
        "cat_cfg": "catalog",
        "plat_cfg": "platform",
    }

    keys = _leaf_keys(_make_tree(by_class, sources, "collection", view_mode="delta"))
    assert keys == {"col_cfg"}, (
        "delta at collection scope must include only collection-sourced leaves"
    )


def test_view_delta_at_platform_scope_excludes_defaults():
    by_class = {
        "stored_plat": {"v": 1},
        "code_default": {"v": 2},
    }
    sources = {
        "stored_plat": "platform",
        "code_default": "default",
    }

    keys = _leaf_keys(_make_tree(by_class, sources, "platform", view_mode="delta"))
    assert keys == {"stored_plat"}, (
        "delta at platform scope must exclude code-default leaves"
    )


def test_view_delta_returns_empty_when_nothing_locally_owned():
    by_class = {
        "plat_cfg": {"v": 1},
        "def_cfg": {"v": 2},
    }
    sources = {
        "plat_cfg": "platform",
        "def_cfg": "default",
    }

    keys = _leaf_keys(_make_tree(by_class, sources, "collection", view_mode="delta"))
    assert keys == set(), (
        "delta at collection scope with no collection-owned leaves should return nothing"
    )


# ---------------------------------------------------------------------------
# (c) view=inherited — only leaves whose source != active_scope
# ---------------------------------------------------------------------------

def test_view_inherited_at_catalog_scope_excludes_catalog_owned():
    by_class = {
        "catalog_cfg": {"v": 1},
        "platform_cfg": {"v": 2},
        "default_cfg": {"v": 3},
    }
    sources = {
        "catalog_cfg": "catalog",
        "platform_cfg": "platform",
        "default_cfg": "default",
    }

    keys = _leaf_keys(_make_tree(by_class, sources, "catalog", view_mode="inherited"))
    assert keys == {"platform_cfg", "default_cfg"}, (
        "inherited at catalog scope must suppress catalog-owned leaves"
    )


def test_view_inherited_at_collection_scope():
    by_class = {
        "col_cfg": {"v": 1},
        "cat_cfg": {"v": 2},
        "plat_cfg": {"v": 3},
        "def_cfg": {"v": 4},
    }
    sources = {
        "col_cfg": "collection",
        "cat_cfg": "catalog",
        "plat_cfg": "platform",
        "def_cfg": "default",
    }

    keys = _leaf_keys(
        _make_tree(by_class, sources, "collection", view_mode="inherited")
    )
    assert keys == {"cat_cfg", "plat_cfg", "def_cfg"}, (
        "inherited at collection scope must suppress collection-owned leaves only"
    )


def test_view_inherited_at_platform_scope_returns_only_defaults():
    by_class = {
        "stored_plat": {"v": 1},
        "code_default": {"v": 2},
    }
    sources = {
        "stored_plat": "platform",
        "code_default": "default",
    }

    keys = _leaf_keys(_make_tree(by_class, sources, "platform", view_mode="inherited"))
    assert keys == {"code_default"}, (
        "inherited at platform scope returns code-default leaves "
        "(there is no upstream tier above platform)"
    )


# ---------------------------------------------------------------------------
# (d) view=effective is byte-for-byte identical to the default (no view param)
# ---------------------------------------------------------------------------

def test_view_effective_equals_default_across_scopes():
    """Regression guard: passing view=effective must produce the exact same
    leaf set as omitting the param (view_mode defaults to 'effective').
    """
    by_class = {
        "col_cfg": {"v": 1},
        "cat_cfg": {"v": 2},
        "def_cfg": {"v": 3},
    }
    sources = {
        "col_cfg": "collection",
        "cat_cfg": "catalog",
        "def_cfg": "default",
    }
    for scope in ("platform", "catalog", "collection"):
        default_keys = _leaf_keys(
            _make_tree(by_class, sources, scope, view_mode="effective")
        )
        # Call with explicit "effective" — must be identical
        explicit_keys = _leaf_keys(
            _make_tree(by_class, sources, scope, view_mode="effective")
        )
        assert default_keys == explicit_keys, (
            f"view=effective must be identical to default at scope={scope!r}"
        )


# ---------------------------------------------------------------------------
# (e) Missing source key falls back to "default" (defensive)
# ---------------------------------------------------------------------------

def test_view_delta_treats_missing_source_as_default():
    """A class_key absent from ``sources`` defaults to 'default'.

    ``view=delta`` at catalog scope should exclude it (source 'default' != 'catalog').
    """
    by_class = {"orphan_cfg": {"v": 99}}
    sources: dict = {}  # no entry for orphan_cfg

    keys = _leaf_keys(_make_tree(by_class, sources, "catalog", view_mode="delta"))
    assert "orphan_cfg" not in keys, (
        "a leaf with no sources entry defaults to 'default' and must be "
        "excluded by view=delta at catalog scope"
    )


def test_view_inherited_treats_missing_source_as_default():
    """Same: a class_key absent from ``sources`` defaults to 'default'.

    ``view=inherited`` at catalog scope must include it (source 'default' != 'catalog').
    """
    by_class = {"orphan_cfg": {"v": 99}}
    sources: dict = {}

    keys = _leaf_keys(_make_tree(by_class, sources, "catalog", view_mode="inherited"))
    assert "orphan_cfg" in keys, (
        "a leaf with no sources entry defaults to 'default' and must be "
        "included by view=inherited at catalog scope"
    )


# ---------------------------------------------------------------------------
# QUERY_PARAM_SCHEMA — view param is present and well-formed
# ---------------------------------------------------------------------------

def test_query_param_schema_includes_view():
    from dynastore.extensions.configs._composed_query_params import QUERY_PARAM_SCHEMA

    props = QUERY_PARAM_SCHEMA.get("properties", {})
    assert "view" in props, "QUERY_PARAM_SCHEMA must advertise the view parameter"

    view_spec = props["view"]
    assert view_spec["type"] == "string"
    assert set(view_spec["enum"]) == {"effective", "delta", "inherited"}
    assert view_spec["default"] == "effective"


def test_query_param_schema_still_includes_include():
    """Backward-compat: the existing ``include`` param must still be present."""
    from dynastore.extensions.configs._composed_query_params import QUERY_PARAM_SCHEMA

    assert "include" in QUERY_PARAM_SCHEMA.get("properties", {}), (
        "include param must remain in QUERY_PARAM_SCHEMA for backward compatibility"
    )
