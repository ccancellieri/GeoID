"""
Request Filter Inspection Framework

Provides a pluggable condition handler that delegates to registered
``RequestFilterInspector`` implementations.  Each inspector validates a
specific class of request filter (geospatial, temporal, attribute, etc.)
against policy constraints.

Usage in policy conditions::

    {
        "type": "filter",
        "config": {
            "inspector": "geospatial",
            "allowed_geometry": { "type": "Polygon", ... },
            "operation": "within"
        }
    }

Custom inspectors are registered via ``register_filter_inspector()``.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from dynastore.modules.iam.conditions import ConditionHandler, EvaluationContext

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Protocol & result types
# ---------------------------------------------------------------------------


@dataclass
class FilterInspectionResult:
    """Outcome of a single filter inspection."""

    allowed: bool
    reason: Optional[str] = None
    inspector_id: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


@runtime_checkable
class RequestFilterInspector(Protocol):
    """
    Protocol for inspecting request filters against policy constraints.

    Implementations extract filter expressions from requests (CQL, ECQL,
    bbox params, geometry POST bodies) and validate them against allowed
    boundaries or constraints.
    """

    @property
    def inspector_id(self) -> str:
        """Unique identifier (e.g. 'geospatial', 'temporal')."""
        ...

    def can_inspect(self, ctx: EvaluationContext, config: Dict[str, Any]) -> bool:
        """Return True if this inspector applies to the current request."""
        ...

    async def inspect_filter(
        self, ctx: EvaluationContext, config: Dict[str, Any]
    ) -> FilterInspectionResult:
        """Inspect and return allowed/denied with reason."""
        ...


# ---------------------------------------------------------------------------
# Orchestrating condition handler
# ---------------------------------------------------------------------------


class FilterConditionHandler(ConditionHandler):
    """
    Condition handler that delegates to registered ``RequestFilterInspector``
    instances.

    Policy condition config must include ``"inspector": "<id>"``; the
    remaining keys are forwarded to the matching inspector.
    """

    def __init__(self) -> None:
        self._inspectors: Dict[str, RequestFilterInspector] = {}

    def register_inspector(self, inspector: RequestFilterInspector) -> None:
        self._inspectors[inspector.inspector_id] = inspector

    @property
    def type(self) -> str:
        return "filter"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        inspector_id = config.get("inspector")
        if not inspector_id:
            logger.warning("FilterConditionHandler: missing 'inspector' in config")
            return True

        inspector = self._inspectors.get(inspector_id)
        if not inspector:
            logger.warning(f"Unknown filter inspector: {inspector_id}")
            return True  # fail-open for unknown inspectors

        if not inspector.can_inspect(ctx, config):
            return True  # inspector does not apply to this request

        result = await inspector.inspect_filter(ctx, config)
        if not result.allowed:
            logger.info(f"Filter denied by {inspector_id}: {result.reason}")
        return result.allowed

    async def inspect(
        self, config: Dict[str, Any], ctx: EvaluationContext
    ) -> Optional[Dict[str, Any]]:
        inspector_id = config.get("inspector")
        return {"type": "filter", "inspector": inspector_id}


# ---------------------------------------------------------------------------
# Built-in: Geospatial filter inspector
# ---------------------------------------------------------------------------

# Geometry cache — keyed by JSON hash of the allowed_geometry config
_GEOMETRY_CACHE_SIZE = 256


@lru_cache(maxsize=_GEOMETRY_CACHE_SIZE)
def _cached_shapely_geom(geojson_hash: str, geojson_str: str):
    """Build and cache a shapely geometry from GeoJSON string."""
    from shapely.geometry import shape

    return shape(json.loads(geojson_str))


def _geojson_hash(geojson: Dict[str, Any]) -> str:
    raw = json.dumps(geojson, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()


def _extract_spatial_geometries(node) -> List[Dict[str, Any]]:
    """
    Walk a pygeofilter AST and extract geometry literals from spatial
    predicate nodes.

    Returns a list of GeoJSON-like dicts from the ``rhs`` of each
    ``SpatialComparisonPredicate``.
    """
    from pygeofilter.ast import SpatialComparisonPredicate, BBox
    from pygeofilter.values import Geometry, Envelope

    results: List[Dict[str, Any]] = []

    if isinstance(node, BBox):
        # BBox(lhs, minx, miny, maxx, maxy, crs)
        results.append(
            {
                "type": "Polygon",
                "coordinates": [
                    [
                        [node.minx, node.miny],
                        [node.maxx, node.miny],
                        [node.maxx, node.maxy],
                        [node.minx, node.maxy],
                        [node.minx, node.miny],
                    ]
                ],
            }
        )
        return results

    if isinstance(node, SpatialComparisonPredicate):
        rhs = node.rhs
        if isinstance(rhs, Geometry):
            # Convert tuple coords to lists for shapely compatibility
            geom = dict(rhs.geometry)
            results.append(geom)
        elif isinstance(rhs, Envelope):
            results.append(
                {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [rhs.x1, rhs.y1],
                            [rhs.x2, rhs.y1],
                            [rhs.x2, rhs.y2],
                            [rhs.x1, rhs.y2],
                            [rhs.x1, rhs.y1],
                        ]
                    ],
                }
            )
        return results

    # Recurse into composite nodes
    for attr in ("lhs", "rhs", "node"):
        child = getattr(node, attr, None)
        if child is not None:
            results.extend(_extract_spatial_geometries(child))

    for attr in ("nodes", "sub_nodes", "arguments"):
        children = getattr(node, attr, None)
        if children:
            for child in children:
                results.extend(_extract_spatial_geometries(child))

    return results


class GeospatialFilterInspector:
    """
    Validates spatial predicates in CQL/ECQL queries against an allowed
    geometry boundary.

    Handles multiple filter sources used across protocols:
    - OGC Features / STAC: ``?filter=...`` with ``filter-lang=cql2-text``
    - WFS: ``?cql_filter=...``
    - Tiles: ``?filter=...`` with ``filter-lang=cql2-text``
    - bbox: ``?bbox=minx,miny,maxx,maxy``

    Config keys:
        allowed_geometry: GeoJSON dict — the permitted spatial boundary
        operation: ``"within"`` | ``"intersects"`` (default: ``"within"``)
        filter_params: list of query param names (default: ``["filter", "cql_filter"]``)
        bbox_param: query param for bbox (default: ``"bbox"``)
        parser_type: ``"cql2"`` | ``"ecql"`` (default: ``"cql2"``)
    """

    inspector_id = "geospatial"

    _DEFAULT_FILTER_PARAMS = ["filter", "cql_filter"]
    _DEFAULT_BBOX_PARAM = "bbox"

    def can_inspect(self, ctx: EvaluationContext, config: Dict[str, Any]) -> bool:
        if not ctx.query_params:
            return False
        filter_params = config.get("filter_params", self._DEFAULT_FILTER_PARAMS)
        bbox_param = config.get("bbox_param", self._DEFAULT_BBOX_PARAM)
        return any(ctx.query_params.get(p) for p in filter_params) or bool(
            ctx.query_params.get(bbox_param)
        )

    async def inspect_filter(
        self, ctx: EvaluationContext, config: Dict[str, Any]
    ) -> FilterInspectionResult:
        from shapely.geometry import shape, box

        allowed_geojson = config.get("allowed_geometry")
        if not allowed_geojson:
            return FilterInspectionResult(allowed=True, inspector_id=self.inspector_id)

        operation = config.get("operation", "within")

        # Build (cached) allowed geometry
        h = _geojson_hash(allowed_geojson)
        allowed_geom = _cached_shapely_geom(h, json.dumps(allowed_geojson))

        request_geometries: List = []

        # 1. Check bbox param
        bbox_param = config.get("bbox_param", self._DEFAULT_BBOX_PARAM)
        bbox_str = (ctx.query_params or {}).get(bbox_param)
        if bbox_str:
            try:
                parts = [float(x.strip()) for x in bbox_str.split(",")]
                if len(parts) >= 4:
                    request_geometries.append(box(parts[0], parts[1], parts[2], parts[3]))
            except (ValueError, IndexError):
                pass

        # 2. Check CQL filter params
        filter_params = config.get("filter_params", self._DEFAULT_FILTER_PARAMS)
        parser_type = config.get("parser_type", "cql2")
        for param in filter_params:
            cql_text = (ctx.query_params or {}).get(param)
            if not cql_text:
                continue
            try:
                if parser_type == "cql2":
                    from pygeofilter.parsers.cql2_text import parse as parse_cql2

                    ast = parse_cql2(cql_text)
                else:
                    from pygeofilter.parsers.ecql import parse as parse_ecql

                    ast = parse_ecql(cql_text)

                geojson_list = _extract_spatial_geometries(ast)
                for geojson_dict in geojson_list:
                    request_geometries.append(shape(geojson_dict))
            except Exception as e:
                logger.debug(f"CQL parse error in geospatial inspector: {e}")
                continue

        # If no spatial content found, pass through
        if not request_geometries:
            return FilterInspectionResult(allowed=True, inspector_id=self.inspector_id)

        # 3. Validate each request geometry against the allowed boundary
        for req_geom in request_geometries:
            if operation == "within":
                if not req_geom.within(allowed_geom):
                    return FilterInspectionResult(
                        allowed=False,
                        reason=f"Requested geometry is not within the allowed boundary",
                        inspector_id=self.inspector_id,
                        details={"operation": operation},
                    )
            elif operation == "intersects":
                if not allowed_geom.intersects(req_geom):
                    return FilterInspectionResult(
                        allowed=False,
                        reason=f"Requested geometry does not intersect the allowed boundary",
                        inspector_id=self.inspector_id,
                        details={"operation": operation},
                    )

        return FilterInspectionResult(allowed=True, inspector_id=self.inspector_id)


# ---------------------------------------------------------------------------
# Module-level singleton & public SPI
# ---------------------------------------------------------------------------

filter_handler = FilterConditionHandler()
filter_handler.register_inspector(GeospatialFilterInspector())


def register_filter_inspector(inspector: RequestFilterInspector) -> None:
    """Public SPI — register a custom filter inspector."""
    filter_handler.register_inspector(inspector)
