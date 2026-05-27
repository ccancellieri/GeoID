#    Copyright 2025 FAO
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
MVT Query Transformer

Transforms queries for Mapbox Vector Tile (MVT) generation by:
- Replacing geometry selection with ST_AsMVTGeom
- Adding spatial filter for tile bounds
- Injecting MVT-specific parameters (extent, buffer, tile_wkb, target_srid)
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from dynastore.models.query_builder import QueryRequest

if TYPE_CHECKING:
    from dynastore.modules.storage.drivers.pg_sidecars import GeometriesSidecar

logger = logging.getLogger(__name__)


class MVTQueryTransform:
    """
    Query transformer for MVT tile generation.
    
    Applies ST_AsMVTGeom transformation and tile bounds filtering
    when geom_format == "MVT".
    """

    priority: int = 100  # Run after basic geometry transforms

    @property
    def transform_id(self) -> str:
        return "mvt"

    def can_transform(self, context: Dict[str, Any]) -> bool:
        """Check if this is an MVT query"""
        return context.get("geom_format") == "MVT"

    def transform_query(
        self, query_request: QueryRequest, context: Dict[str, Any]
    ) -> QueryRequest:
        """
        Transform query for MVT generation.

        Resolves the geometry projection through the collection's geometry
        sidecar so the physical geometry column is never hardcoded here, then
        adds the tile-bounds spatial filter. The per-row ``ST_AsMVTGeom`` is
        produced on the fly by the driver; the wrapping ``ST_AsMVT`` aggregates
        the ``geom`` output column into the tile.
        """
        target_srid = context.get("target_srid")
        tile_wkb = context.get("tile_wkb")
        srid = context.get("srid")  # Source SRID

        if not all([target_srid, tile_wkb, srid]):
            logger.warning(
                "MVT transform requires target_srid, tile_wkb, and srid in context"
            )
            return query_request

        geom_sidecar = self._resolve_geometry_sidecar(context.get("col_config"))
        if geom_sidecar is None:
            logger.warning("MVT transform: collection has no geometry sidecar; skipping")
            return query_request

        geom_field = geom_sidecar.get_main_geometry_field() or "geom"
        sidecar_alias = f"sc_{geom_sidecar.sidecar_id}"
        qualified_geom = f"{sidecar_alias}.{geom_field}"

        # On-the-fly MVT geometry from the geometry sidecar (storage-agnostic;
        # the physical column is resolved by the sidecar, not hardcoded here).
        mvt_geom_expr = geom_sidecar.get_geometry_select(context, sidecar_alias)
        if not mvt_geom_expr:
            logger.warning(
                "MVT transform: geometry sidecar produced no geometry expression; skipping"
            )
            return query_request

        # Drop any existing geometry selection; the actual MVT geometry comes
        # from the raw select below, aliased ``geom`` for the wrapping
        # ST_AsMVT(..., 'geom'). The geometry sidecar is JOINed unconditionally
        # by ``QueryOptimizer.determine_required_sidecars`` (``require_geometry``
        # defaults to True), so no placeholder selection is needed — and a
        # placeholder would leak the raw geometry column into the tile as a
        # property (every column selected by the inner SELECT becomes an MVT
        # property; the previous ``_geom_source`` alias surfaced WKB hex bytes
        # in every feature on the live tile path).
        query_request.select = [s for s in query_request.select if s.field != geom_field]
        query_request.raw_selects.append(f"{mvt_geom_expr} AS geom")

        # Spatial filter for tile bounds on the sidecar's qualified geometry
        # (source SRID for efficient spatial-index use).
        spatial_filter = (
            f"ST_Intersects({qualified_geom}, "
            "ST_Transform(ST_SetSRID(ST_GeomFromWKB(:tile_wkb), "
            "CAST(:target_srid AS INTEGER)), CAST(:srid AS INTEGER)))"
        )

        if query_request.raw_where:
            query_request.raw_where += f" AND {spatial_filter}"
        else:
            query_request.raw_where = spatial_filter

        # Bind params referenced by the geometry expression and the filter.
        # ``extent``/``buffer`` are emitted as literals by ``get_geometry_select``
        # (matching the wrapping ST_AsMVT extent), so only the SRIDs, tile
        # envelope, and optional simplification tolerance need binding.
        query_request.raw_params.update(
            {
                "target_srid": target_srid,
                "tile_wkb": tile_wkb,
                "srid": srid,
            }
        )
        simplification = context.get("simplification")
        if simplification:
            query_request.raw_params["simplification"] = simplification

        logger.debug(
            "MVT transform applied: geom=%s, target_srid=%s, srid=%s",
            qualified_geom,
            target_srid,
            srid,
        )

        return query_request

    def _resolve_geometry_sidecar(
        self, col_config: Any
    ) -> Optional["GeometriesSidecar"]:
        """Resolve the collection's geometry sidecar, or ``None``.

        Mirrors the QueryOptimizer's sidecar resolution so the MVT geometry
        expression is sourced from the same sidecar the optimizer joins. Returns
        ``None`` for non-PG drivers (which carry no sidecars) or collections
        without a geometry sidecar.
        """
        if col_config is None:
            return None

        from dynastore.modules.storage.drivers.pg_sidecars import (
            GeometriesSidecar,
            SidecarRegistry,
            driver_sidecars,
        )

        for cfg in driver_sidecars(col_config):
            sidecar = SidecarRegistry.get_sidecar(cfg, lenient=True)
            if isinstance(sidecar, GeometriesSidecar):
                return sidecar
        return None

    def post_process_sql(
        self, sql: str, params: Dict[str, Any], context: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Restrict the tile's per-feature columns to what ``feature_type`` allows.

        ST_AsMVT emits every column of the inner SELECT as a tile property; the
        QueryOptimizer auto-adds ``h.geoid`` whenever ``query.select`` is
        non-empty and the consumer did not include geoid explicitly. Wrap the
        subquery so only ``geom`` plus the policy-authorised columns reach the
        outer ``ST_AsMVT(mvtgeom.*, ...)``. Without this wrap the legacy tile
        path leaked ``geoid`` (and, before the geometry-placeholder removal,
        raw WKB) into every feature regardless of ``feature_type.expose``.

        Trinary ``feature_type.expose`` (see :class:`FeatureType`):
        ``None`` → schema-declared fields (from ``context['schema_fields']``);
        ``[]`` → none (geometry-only tile);
        non-empty list → schema fields PLUS the listed computed names.
        """
        feature_type = context.get("feature_type")
        if feature_type is None:
            return sql, params

        schema_fields = list(context.get("schema_fields") or [])

        keep: list[str] = ["geom"]
        if feature_type.expose_geoid:
            keep.append("geoid")
        if feature_type.expose_created:
            keep.append("created")

        if feature_type.expose is None:
            keep.extend(schema_fields)
        elif len(feature_type.expose) == 0:
            pass  # explicit suppression: geometry-only
        else:
            keep.extend(schema_fields)
            keep.extend(feature_type.expose)

        # ``identifier`` quote each column name so JSONB-derived names with
        # mixed-case (``CODE``/``NAME``) survive Postgres' lowercase-folding.
        quoted = ", ".join(f'"{c}"' for c in keep)
        wrapped = f"SELECT {quoted} FROM ({sql}) _mvt_inner"
        return wrapped, params
