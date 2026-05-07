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
from typing import Dict, Any, Tuple
from dynastore.models.query_builder import QueryRequest, FieldSelection

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
        
        Replaces geometry selection with ST_AsMVTGeom and adds spatial filter.
        """
        extent = context.get("extent", 4096)
        buffer = context.get("buffer", 256)
        target_srid = context.get("target_srid")
        tile_wkb = context.get("tile_wkb")
        srid = context.get("srid")  # Source SRID

        if not all([target_srid, tile_wkb, srid]):
            logger.warning(
                "MVT transform requires target_srid, tile_wkb, and srid in context"
            )
            return query_request

        # Remove existing geometry selections
        query_request.select = [s for s in query_request.select if s.field != "geom"]

        # Add a placeholder geometry selection to ensure GeometrySidecar joins
        # The actual geometry will come from raw_selects below
        query_request.select.append(FieldSelection(field="geom", alias="_geom_source"))

        # Add MVT geometry transformation via raw SQL
        # QueryOptimizer will resolve 'geom' to the correct sidecar table alias
        mvt_geom_expr = (
            f"ST_AsMVTGeom("
            f"ST_Transform(geom, CAST(:target_srid AS INTEGER)), "
            f"ST_SetSRID(ST_GeomFromWKB(:tile_wkb), CAST(:target_srid AS INTEGER)), "
            f":extent, :buffer, true) AS geom"
        )

        query_request.raw_selects.append(mvt_geom_expr)

        # Add spatial filter for tile bounds
        # This uses the source SRID for efficient index usage
        spatial_filter = (
            "ST_Intersects(geom, "
            "ST_Transform(ST_SetSRID(ST_GeomFromWKB(:tile_wkb), "
            "CAST(:target_srid AS INTEGER)), CAST(:srid AS INTEGER)))"
        )

        if query_request.raw_where:
            query_request.raw_where += f" AND {spatial_filter}"
        else:
            query_request.raw_where = spatial_filter

        # Inject MVT parameters
        query_request.raw_params.update(
            {
                "target_srid": target_srid,
                "tile_wkb": tile_wkb,
                "extent": extent,
                "buffer": buffer,
                "srid": srid,
            }
        )

        logger.debug(
            f"MVT transform applied: extent={extent}, buffer={buffer}, "
            f"target_srid={target_srid}, srid={srid}"
        )

        return query_request

    def post_process_sql(
        self, sql: str, params: Dict[str, Any], context: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """No SQL post-processing needed for MVT"""
        return sql, params
