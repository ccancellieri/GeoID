#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""Unit tests for StyleBindingConfig, StyleBindingResolver, and resolve_binding_style_id.

Pure: no DB, no HTTP, no pystac.  Mocks PlatformConfigsProtocol to avoid
storage dependency.
"""

from __future__ import annotations

import pytest

from dynastore.modules.styles.binding_config import StyleBinding, StyleBindingConfig
from dynastore.modules.styles.binding_resolver import (
    StyleBindingResolver,
    _eval_cql2_json_selector,
    resolve_binding_style_id,
)


# ---------------------------------------------------------------------------
# StyleBinding model
# ---------------------------------------------------------------------------


class TestStyleBinding:
    def test_default_priority_is_zero(self):
        b = StyleBinding(style_id="my-style")
        assert b.priority == 0

    def test_selector_defaults_to_none(self):
        b = StyleBinding(style_id="my-style")
        assert b.selector is None

    def test_explicit_selector(self):
        sel = {"op": "=", "args": [{"property": "theme"}, "fire"]}
        b = StyleBinding(style_id="fire-style", selector=sel, priority=10)
        assert b.selector == sel
        assert b.priority == 10


# ---------------------------------------------------------------------------
# StyleBindingConfig
# ---------------------------------------------------------------------------


class TestStyleBindingConfig:
    def test_defaults_are_empty(self):
        cfg = StyleBindingConfig()
        assert cfg.default_style_id is None
        assert cfg.bindings == []

    def test_address(self):
        assert StyleBindingConfig._address == (
            "platform",
            "catalog",
            "styles",
            "bindings",
        )

    def test_config_accepts_bindings(self):
        b = StyleBinding(style_id="s1", priority=5)
        cfg = StyleBindingConfig(default_style_id="s0", bindings=[b])
        assert cfg.default_style_id == "s0"
        assert len(cfg.bindings) == 1
        assert cfg.bindings[0].style_id == "s1"


# ---------------------------------------------------------------------------
# _eval_cql2_json_selector
# ---------------------------------------------------------------------------


class TestEvalCql2JsonSelector:
    def test_simple_equality_match(self):
        selector = {"op": "=", "args": [{"property": "theme"}, "fire"]}
        assert _eval_cql2_json_selector(selector, {"theme": "fire"}) is True

    def test_simple_equality_no_match(self):
        selector = {"op": "=", "args": [{"property": "theme"}, "fire"]}
        assert _eval_cql2_json_selector(selector, {"theme": "water"}) is False

    def test_missing_property_returns_false(self):
        selector = {"op": "=", "args": [{"property": "theme"}, "fire"]}
        # No 'theme' key — evaluator returns False, not an exception.
        assert _eval_cql2_json_selector(selector, {}) is False

    def test_invalid_selector_returns_false(self):
        # Malformed CQL2-JSON should not raise; returns False (fail-open).
        assert _eval_cql2_json_selector({"totally": "invalid"}, {"x": 1}) is False

    def test_numeric_comparison_match(self):
        selector = {"op": ">", "args": [{"property": "count"}, 5]}
        assert _eval_cql2_json_selector(selector, {"count": 10}) is True

    def test_numeric_comparison_no_match(self):
        selector = {"op": ">", "args": [{"property": "count"}, 5]}
        assert _eval_cql2_json_selector(selector, {"count": 3}) is False


# ---------------------------------------------------------------------------
# StyleBindingResolver
# ---------------------------------------------------------------------------


class TestStyleBindingResolver:
    def test_empty_bindings_returns_none(self):
        resolver = StyleBindingResolver()
        assert resolver.resolve([], {"theme": "fire"}) is None

    def test_unconditional_binding_matches(self):
        """selector=None matches every item."""
        bindings = [StyleBinding(style_id="default-style")]
        resolver = StyleBindingResolver()
        assert resolver.resolve(bindings, {"theme": "anything"}) == "default-style"

    def test_unconditional_binding_matches_with_no_properties(self):
        """selector=None matches even when item_properties is None."""
        bindings = [StyleBinding(style_id="default-style")]
        resolver = StyleBindingResolver()
        assert resolver.resolve(bindings, None) == "default-style"

    def test_selector_binding_matches_when_predicate_true(self):
        selector = {"op": "=", "args": [{"property": "theme"}, "fire"]}
        bindings = [StyleBinding(style_id="fire-style", selector=selector)]
        resolver = StyleBindingResolver()
        assert resolver.resolve(bindings, {"theme": "fire"}) == "fire-style"

    def test_selector_binding_skipped_when_predicate_false(self):
        selector = {"op": "=", "args": [{"property": "theme"}, "fire"]}
        bindings = [StyleBinding(style_id="fire-style", selector=selector)]
        resolver = StyleBindingResolver()
        assert resolver.resolve(bindings, {"theme": "water"}) is None

    def test_selector_binding_skipped_when_no_properties(self):
        """Selector-gated bindings are skipped when item_properties is None."""
        selector = {"op": "=", "args": [{"property": "theme"}, "fire"]}
        bindings = [StyleBinding(style_id="fire-style", selector=selector)]
        resolver = StyleBindingResolver()
        assert resolver.resolve(bindings, None) is None

    def test_priority_order_respected(self):
        """Higher priority wins over lower priority."""
        sel_fire = {"op": "=", "args": [{"property": "theme"}, "fire"]}
        sel_always = None  # matches everything
        bindings = [
            StyleBinding(style_id="high-priority-style", selector=sel_fire, priority=10),
            StyleBinding(style_id="low-priority-style", selector=sel_always, priority=1),
        ]
        # Already sorted descending by priority.
        resolver = StyleBindingResolver()
        assert resolver.resolve(bindings, {"theme": "fire"}) == "high-priority-style"

    def test_fallback_when_first_selector_misses(self):
        """Second binding fires when first selector doesn't match."""
        sel_fire = {"op": "=", "args": [{"property": "theme"}, "fire"]}
        bindings = [
            StyleBinding(style_id="fire-style", selector=sel_fire, priority=10),
            StyleBinding(style_id="default-style", priority=0),  # unconditional
        ]
        resolver = StyleBindingResolver()
        assert resolver.resolve(bindings, {"theme": "water"}) == "default-style"

    def test_rename_immune_internal_id(self):
        """Bindings key on internal collection id (rename-immune by design).

        This test documents the contract: the resolver is called with
        already-resolved internal IDs, so the caller is responsible for
        external → internal resolution at the request boundary.
        """
        bindings = [StyleBinding(style_id="s1")]
        resolver = StyleBindingResolver()
        # Simulate that the binding was found under the internal id and is now
        # being evaluated — the internal id is not part of the resolver's concern.
        assert resolver.resolve(bindings, {}) == "s1"


# ---------------------------------------------------------------------------
# StylesResolver integration — binding_default_id wins
# ---------------------------------------------------------------------------


class TestStylesResolverWithBinding:
    """Verify that StylesResolver respects binding_default_id precedence."""

    def test_binding_wins_over_coverages_config(self):
        from dynastore.modules.styles.resolver import StylesResolver

        res = StylesResolver().resolve(
            available={"binding-style": ["sheet"], "coverages-style": ["sheet"]},
            binding_default_id="binding-style",
            coverages_config_default_id="coverages-style",
            item_assets_default_id=None,
        )
        assert res.default_style_id == "binding-style"

    def test_binding_wins_over_item_assets(self):
        from dynastore.modules.styles.resolver import StylesResolver

        res = StylesResolver().resolve(
            available={"binding-style": ["sheet"], "item-assets-style": ["sheet"]},
            binding_default_id="binding-style",
            coverages_config_default_id=None,
            item_assets_default_id="item-assets-style",
        )
        assert res.default_style_id == "binding-style"

    def test_stale_binding_falls_through_to_coverages_config(self):
        from dynastore.modules.styles.resolver import StylesResolver

        res = StylesResolver().resolve(
            available={"coverages-style": ["sheet"]},
            binding_default_id="deleted-binding-style",
            coverages_config_default_id="coverages-style",
            item_assets_default_id=None,
        )
        assert res.default_style_id == "coverages-style"

    def test_none_binding_falls_through(self):
        from dynastore.modules.styles.resolver import StylesResolver

        res = StylesResolver().resolve(
            available={"item-style": ["sheet"]},
            binding_default_id=None,
            coverages_config_default_id=None,
            item_assets_default_id="item-style",
        )
        assert res.default_style_id == "item-style"

    def test_existing_callers_without_binding_param_still_work(self):
        """binding_default_id defaults to None — backwards compatible."""
        from dynastore.modules.styles.resolver import StylesResolver

        res = StylesResolver().resolve(
            available={"s": ["sheet"]},
            coverages_config_default_id="s",
            item_assets_default_id=None,
        )
        assert res.default_style_id == "s"


# ---------------------------------------------------------------------------
# resolve_binding_style_id (async) — with mocked config manager
# ---------------------------------------------------------------------------


class TestResolveBindingStyleId:
    @pytest.fixture
    def cat_binding(self):
        """Catalog-tier binding: only a default_style_id, no selector rules."""
        return StyleBindingConfig(
            default_style_id="cat-default",
            bindings=[],
        )

    @pytest.fixture
    def col_binding(self):
        """Collection-tier binding: theme=fire rule + default."""
        sel = {"op": "=", "args": [{"property": "theme"}, "fire"]}
        return StyleBindingConfig(
            default_style_id="col-default",
            bindings=[StyleBinding(style_id="fire-style", selector=sel, priority=5)],
        )

    @pytest.mark.asyncio
    async def test_collection_selector_wins(self, cat_binding, col_binding, monkeypatch):
        """Collection-tier CQL2 selector match beats catalog default_style_id."""
        from unittest.mock import AsyncMock, MagicMock
        import dynastore.modules.styles.binding_resolver as mod

        mgr = MagicMock()
        mgr.get_config = AsyncMock(side_effect=lambda cls, catalog_id=None, collection_id=None: (
            col_binding if collection_id else cat_binding
        ))
        monkeypatch.setattr(mod, "get_protocol", lambda _cls: mgr)  # noqa: ARG005

        result = await resolve_binding_style_id(
            "internal-cat", "internal-col", {"theme": "fire"}
        )
        assert result == "fire-style"

    @pytest.mark.asyncio
    async def test_collection_default_wins_when_no_selector_matches(
        self, cat_binding, col_binding, monkeypatch
    ):
        from unittest.mock import AsyncMock, MagicMock
        import dynastore.modules.styles.binding_resolver as mod

        mgr = MagicMock()
        mgr.get_config = AsyncMock(side_effect=lambda cls, catalog_id=None, collection_id=None: (
            col_binding if collection_id else cat_binding
        ))
        monkeypatch.setattr(mod, "get_protocol", lambda _cls: mgr)  # noqa: ARG005

        result = await resolve_binding_style_id(
            "internal-cat", "internal-col", {"theme": "water"}
        )
        assert result == "col-default"

    @pytest.mark.asyncio
    async def test_catalog_default_wins_when_collection_default_absent(
        self, cat_binding, monkeypatch
    ):
        from unittest.mock import AsyncMock, MagicMock
        import dynastore.modules.styles.binding_resolver as mod

        empty_col = StyleBindingConfig()

        mgr = MagicMock()
        mgr.get_config = AsyncMock(side_effect=lambda cls, catalog_id=None, collection_id=None: (
            empty_col if collection_id else cat_binding
        ))
        monkeypatch.setattr(mod, "get_protocol", lambda _cls: mgr)  # noqa: ARG005

        result = await resolve_binding_style_id("internal-cat", "internal-col", {})
        assert result == "cat-default"

    @pytest.mark.asyncio
    async def test_returns_none_when_no_bindings_set(self, monkeypatch):
        from unittest.mock import AsyncMock, MagicMock
        import dynastore.modules.styles.binding_resolver as mod

        empty = StyleBindingConfig()
        mgr = MagicMock()
        mgr.get_config = AsyncMock(return_value=empty)
        monkeypatch.setattr(mod, "get_protocol", lambda _cls: mgr)  # noqa: ARG005

        result = await resolve_binding_style_id("cat", "col", {})
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_config_manager_unavailable(self, monkeypatch):
        import dynastore.modules.styles.binding_resolver as mod

        monkeypatch.setattr(mod, "get_protocol", lambda _cls: None)

        result = await resolve_binding_style_id("cat", "col", {"x": 1})
        assert result is None

    @pytest.mark.asyncio
    async def test_rename_immune_uses_internal_ids(self, col_binding, monkeypatch):
        """The internal catalog/collection id is passed directly to get_config.

        If external_id changes, the binding is still found because the
        config is keyed on the internal id.
        """
        from unittest.mock import AsyncMock, MagicMock
        import dynastore.modules.styles.binding_resolver as mod

        empty = StyleBindingConfig()
        mgr = MagicMock()
        call_log: list = []

        async def _get(cls, catalog_id=None, collection_id=None):
            call_log.append((catalog_id, collection_id))
            return col_binding if collection_id == "internal-col-id" else empty

        mgr.get_config = AsyncMock(side_effect=_get)
        monkeypatch.setattr(mod, "get_protocol", lambda _cls: mgr)  # noqa: ARG005

        # Simulate that external_id was "old-name", internal is "internal-col-id".
        # rename: external_id becomes "new-name" — internal id unchanged.
        result = await resolve_binding_style_id(
            "cat", "internal-col-id", {"theme": "fire"}
        )
        # Binding still resolves because we used the internal id.
        assert result == "fire-style"
        # Verify the internal id was passed to the config manager.
        col_calls = [c for c in call_log if c[1] == "internal-col-id"]
        assert len(col_calls) == 1
