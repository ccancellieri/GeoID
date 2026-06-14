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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
Tests for the request filter inspection framework.

Covers:
- FilterConditionHandler orchestration
- GeospatialFilterInspector with CQL and bbox
- Inspector registration SPI
"""

import pytest

from dynastore.modules.iam.conditions import EvaluationContext
from dynastore.modules.iam.filter_inspectors import (
    FilterConditionHandler,
    FilterInspectionResult,
    GeospatialFilterInspector,
    register_filter_inspector,
    filter_handler,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ctx(
    query_params: dict | None = None,
    effect: str | None = None,
) -> EvaluationContext:
    """Build a minimal EvaluationContext with query params and optional policy effect."""
    return EvaluationContext(
        request=None,
        storage=None,  # type: ignore[arg-type]
        query_params=query_params or {},
        effect=effect,
    )


# Italy bounding box (approximate)
ITALY_BBOX = {
    "type": "Polygon",
    "coordinates": [
        [[6.6, 35.5], [18.5, 35.5], [18.5, 47.1], [6.6, 47.1], [6.6, 35.5]]
    ],
}

GEO_CONFIG = {
    "inspector": "geospatial",
    "allowed_geometry": ITALY_BBOX,
    "operation": "within",
}


# ---------------------------------------------------------------------------
# FilterConditionHandler tests
# ---------------------------------------------------------------------------


class TestFilterConditionHandler:
    # --- Effect-aware fail-closed for unknown/missing inspector ---

    @pytest.mark.asyncio
    async def test_unknown_inspector_allow_ctx_returns_false(self):
        """Unknown inspector + ALLOW policy → condition not met → ALLOW does not fire."""
        handler = FilterConditionHandler()
        ctx = _make_ctx(effect="ALLOW")
        assert await handler.evaluate({"inspector": "nonexistent"}, ctx) is False

    @pytest.mark.asyncio
    async def test_unknown_inspector_deny_ctx_returns_true(self):
        """Unknown inspector + DENY policy → condition treated as met → DENY fires."""
        handler = FilterConditionHandler()
        ctx = _make_ctx(effect="DENY")
        assert await handler.evaluate({"inspector": "nonexistent"}, ctx) is True

    @pytest.mark.asyncio
    async def test_unknown_inspector_no_effect_returns_false(self):
        """Unknown inspector + no effect stamped → defaults to False (safe: no grant)."""
        handler = FilterConditionHandler()
        ctx = _make_ctx(effect=None)
        assert await handler.evaluate({"inspector": "nonexistent"}, ctx) is False

    @pytest.mark.asyncio
    async def test_missing_inspector_key_allow_ctx_returns_false(self):
        """Missing 'inspector' key + ALLOW policy → condition not met → ALLOW does not fire."""
        handler = FilterConditionHandler()
        ctx = _make_ctx(effect="ALLOW")
        assert await handler.evaluate({}, ctx) is False

    @pytest.mark.asyncio
    async def test_missing_inspector_key_deny_ctx_returns_true(self):
        """Missing 'inspector' key + DENY policy → condition treated as met → DENY fires."""
        handler = FilterConditionHandler()
        ctx = _make_ctx(effect="DENY")
        assert await handler.evaluate({}, ctx) is True

    @pytest.mark.asyncio
    async def test_missing_inspector_key_no_effect_returns_false(self):
        """Missing 'inspector' key + no effect → defaults to False (safe: no grant)."""
        handler = FilterConditionHandler()
        ctx = _make_ctx(effect=None)
        assert await handler.evaluate({}, ctx) is False

    @pytest.mark.asyncio
    async def test_type_is_filter(self):
        assert filter_handler.type == "filter"


# ---------------------------------------------------------------------------
# GeospatialFilterInspector tests
# ---------------------------------------------------------------------------


class TestGeospatialFilterInspector:
    def setup_method(self):
        self.inspector = GeospatialFilterInspector()

    def test_inspector_id(self):
        assert self.inspector.inspector_id == "geospatial"

    def test_can_inspect_with_filter_param(self):
        ctx = _make_ctx({"filter": "S_INTERSECTS(geometry, POINT(12.5 41.9))"})
        assert self.inspector.can_inspect(ctx, GEO_CONFIG) is True

    def test_can_inspect_with_bbox_param(self):
        ctx = _make_ctx({"bbox": "12.0,41.0,13.0,42.0"})
        assert self.inspector.can_inspect(ctx, GEO_CONFIG) is True

    def test_can_inspect_with_cql_filter_param(self):
        ctx = _make_ctx({"cql_filter": "INTERSECTS(geometry, POINT(12.5 41.9))"})
        assert self.inspector.can_inspect(ctx, GEO_CONFIG) is True

    def test_cannot_inspect_without_params(self):
        ctx = _make_ctx({"page": "1"})
        assert self.inspector.can_inspect(ctx, GEO_CONFIG) is False

    def test_cannot_inspect_empty(self):
        ctx = _make_ctx({})
        assert self.inspector.can_inspect(ctx, GEO_CONFIG) is False

    # --- bbox tests ---

    @pytest.mark.asyncio
    async def test_bbox_inside_allowed(self):
        """Rome area bbox — inside Italy."""
        ctx = _make_ctx({"bbox": "12.0,41.0,13.0,42.0"})
        result = await self.inspector.inspect_filter(ctx, GEO_CONFIG)
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_bbox_outside_allowed(self):
        """Paris area bbox — outside Italy."""
        ctx = _make_ctx({"bbox": "2.0,48.0,3.0,49.0"})
        result = await self.inspector.inspect_filter(ctx, GEO_CONFIG)
        assert result.allowed is False

    @pytest.mark.asyncio
    async def test_bbox_partially_outside(self):
        """Bbox crossing Italy's border — within fails, intersects passes."""
        ctx = _make_ctx({"bbox": "6.0,43.0,8.0,48.0"})
        result_within = await self.inspector.inspect_filter(ctx, GEO_CONFIG)
        assert result_within.allowed is False

        intersect_config = {**GEO_CONFIG, "operation": "intersects"}
        result_intersect = await self.inspector.inspect_filter(ctx, intersect_config)
        assert result_intersect.allowed is True

    # --- CQL filter tests ---

    @pytest.mark.asyncio
    async def test_cql_intersects_inside(self):
        """CQL spatial predicate with geometry inside Italy."""
        cql = "S_INTERSECTS(geometry, POLYGON((12 41, 13 41, 13 42, 12 42, 12 41)))"
        ctx = _make_ctx({"filter": cql})
        result = await self.inspector.inspect_filter(ctx, GEO_CONFIG)
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_cql_intersects_outside(self):
        """CQL spatial predicate with geometry outside Italy."""
        cql = "S_INTERSECTS(geometry, POLYGON((2 48, 3 48, 3 49, 2 49, 2 48)))"
        ctx = _make_ctx({"filter": cql})
        result = await self.inspector.inspect_filter(ctx, GEO_CONFIG)
        assert result.allowed is False

    @pytest.mark.asyncio
    async def test_non_spatial_cql_passes(self):
        """Non-spatial CQL filter should pass through."""
        ctx = _make_ctx({"filter": "name = 'test'"})
        result = await self.inspector.inspect_filter(ctx, GEO_CONFIG)
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_no_allowed_geometry_passes(self):
        """Config without allowed_geometry should pass everything."""
        ctx = _make_ctx({"bbox": "2.0,48.0,3.0,49.0"})
        config = {"inspector": "geospatial"}
        result = await self.inspector.inspect_filter(ctx, config)
        assert result.allowed is True


# ---------------------------------------------------------------------------
# End-to-end: FilterConditionHandler + GeospatialFilterInspector
# ---------------------------------------------------------------------------


class TestFilterConditionHandlerE2E:
    @pytest.mark.asyncio
    async def test_evaluate_geospatial_allowed(self):
        """Full pipeline: filter handler → geospatial inspector → allowed."""
        ctx = _make_ctx({"bbox": "12.0,41.0,13.0,42.0"})
        assert await filter_handler.evaluate(GEO_CONFIG, ctx) is True

    @pytest.mark.asyncio
    async def test_evaluate_geospatial_denied(self):
        """Full pipeline: filter handler → geospatial inspector → denied."""
        ctx = _make_ctx({"bbox": "2.0,48.0,3.0,49.0"})
        assert await filter_handler.evaluate(GEO_CONFIG, ctx) is False

    @pytest.mark.asyncio
    async def test_evaluate_no_spatial_param_passes(self):
        """No spatial params → inspector doesn't apply → passes."""
        ctx = _make_ctx({"page": "1"})
        assert await filter_handler.evaluate(GEO_CONFIG, ctx) is True


# ---------------------------------------------------------------------------
# SPI: Custom inspector registration
# ---------------------------------------------------------------------------


class TestCustomInspectorRegistration:
    def test_register_and_use_custom_inspector(self):
        """Verify the register_filter_inspector SPI works."""

        class AlwaysDenyInspector:
            inspector_id = "always_deny"

            def can_inspect(self, ctx, config):
                return True

            async def inspect_filter(self, ctx, config):
                return FilterInspectionResult(
                    allowed=False, reason="always deny", inspector_id="always_deny"
                )

        handler = FilterConditionHandler()
        handler.register_inspector(AlwaysDenyInspector())

        import asyncio

        ctx = _make_ctx()
        result = asyncio.get_event_loop().run_until_complete(
            handler.evaluate({"inspector": "always_deny"}, ctx)
        )
        assert result is False
