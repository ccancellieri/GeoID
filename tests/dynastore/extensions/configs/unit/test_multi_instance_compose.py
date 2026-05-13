"""Cycle F.4d.1 — pin the multi-instance ref surfacing in the configs
tree composer.

When ``ConfigService.list_refs_at_scope`` returns refs whose ``ref_key``
differs from the row's ``class_key`` (multi-instance writes), the
composer must place each as a sibling leaf under the parent class's
``_address`` (operator-visible alongside the canonical row).

This is a static-shape pin: the composer is exercised directly with a
synthetic ``extra_refs`` dict — no DB hit.  Live round-trip lands in
the integration suite (F.4c.5 covers storage; the API-level live test
arrives with the URL-PATCH composer rewrite slice).
"""

from __future__ import annotations

from dynastore.extensions.configs.config_api_service import ConfigApiService
from dynastore.modules.tiles.tiles_config import TilesConfig


CANONICAL_KEY = TilesConfig.class_key()


def _by_class_with_canonical_tiles() -> tuple[dict, dict]:
    payload = TilesConfig().model_dump()
    return ({CANONICAL_KEY: payload}, {CANONICAL_KEY: "platform"})


# ---------------------------------------------------------------------------
# Tree shape
# ---------------------------------------------------------------------------


def test_compose_tree_places_extra_ref_as_sibling_of_canonical_class():
    """``tiles_secondary`` (multi-instance ref pointing at TilesConfig)
    lands at ``platform.modules.tiles.tiles_secondary`` next to the
    canonical ``platform.modules.tiles.tiles_config`` leaf."""
    by_class, sources = _by_class_with_canonical_tiles()
    secondary_payload = TilesConfig(min_zoom=5, max_zoom=15).model_dump()
    extra_refs = {"tiles_secondary": (CANONICAL_KEY, secondary_payload)}

    tree, _ = ConfigApiService._compose_tree(
        by_class=by_class,
        sources=sources,
        active_scope="platform",
        meta_mode="none",
        include_mode="upstream",
        strict=False,
        extra_refs=extra_refs,
    )

    # Both leaves live under platform.modules.tiles.*.
    tiles_node = tree.get("platform", {}).get("modules", {}).get("tiles", {})
    assert CANONICAL_KEY in tiles_node, (
        f"canonical class leaf missing from tiles node: {tiles_node!r}"
    )
    assert "tiles_secondary" in tiles_node, (
        f"multi-instance ref leaf missing from tiles node: {tiles_node!r}"
    )
    assert tiles_node["tiles_secondary"]["min_zoom"] == 5
    assert tiles_node["tiles_secondary"]["max_zoom"] == 15


def test_compose_tree_no_extra_refs_unchanged():
    """``extra_refs=None`` (or empty) leaves the canonical tree untouched —
    the canonical-only path is the regression invariant since
    pre-F.4d.1 deployments take this branch unconditionally."""
    by_class, sources = _by_class_with_canonical_tiles()
    tree_a, _ = ConfigApiService._compose_tree(
        by_class=by_class, sources=sources, active_scope="platform",
        meta_mode="none", include_mode="upstream", strict=False,
    )
    tree_b, _ = ConfigApiService._compose_tree(
        by_class=by_class, sources=sources, active_scope="platform",
        meta_mode="none", include_mode="upstream", strict=False,
        extra_refs={},
    )
    assert tree_a == tree_b


def test_compose_tree_extra_ref_with_unknown_class_skipped():
    """A ref whose stored class_key is not in the live registry gets
    silently dropped — same defensive shape as the canonical loop's
    ``if cls is None: continue`` skip.

    Operators see the row absent from the tree (it'd surface in a
    warning-log via get_config_by_ref).  The composer doesn't crash
    or render a leaf with no parent address.
    """
    by_class, sources = _by_class_with_canonical_tiles()
    extra_refs = {
        "ghost_ref": ("class_that_was_unregistered", {"some": "payload"}),
    }
    tree, _ = ConfigApiService._compose_tree(
        by_class=by_class, sources=sources, active_scope="platform",
        meta_mode="none", include_mode="upstream", strict=False,
        extra_refs=extra_refs,
    )
    tiles_node = tree.get("platform", {}).get("modules", {}).get("tiles", {})
    assert "ghost_ref" not in tiles_node


def test_compose_tree_meta_inlined_on_extra_ref_under_field_mode():
    """Post-#517: a multi-instance ref leaf carries its ``_meta`` inline
    alongside the plugin payload — same docs as the canonical
    class (schema is per-class, not per-instance)."""
    by_class, sources = _by_class_with_canonical_tiles()
    extra_refs = {
        "tiles_secondary": (CANONICAL_KEY, TilesConfig().model_dump()),
    }
    tree, _ = ConfigApiService._compose_tree(
        by_class=by_class, sources=sources, active_scope="platform",
        meta_mode="field", include_mode="upstream", strict=False,
        extra_refs=extra_refs,
    )
    tiles_node = tree["platform"]["modules"]["tiles"]
    assert "tiles_secondary" in tiles_node
    leaf = tiles_node["tiles_secondary"]
    assert "_meta" in leaf
    assert "docs" in leaf["_meta"]
