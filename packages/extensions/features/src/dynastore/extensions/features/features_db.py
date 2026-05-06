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

# dynastore/extensions/features/features_db.py

import logging
import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol

from dynastore.modules.catalog.validation import get_valid_properties
from dynastore.models.query_builder import QueryRequest, FieldSelection, SortOrder, FilterCondition, FilterOperator

logger = logging.getLogger(__name__)

from dynastore.modules.tools.cql import parse_cql_filter


from dynastore.models.query_builder import QueryRequest, FieldSelection, SortOrder, FilterCondition

logger = logging.getLogger(__name__)

from dynastore.modules.tools.cql import parse_cql_filter
from fastapi import HTTPException, status


async def get_items_filtered(
    conn: AsyncConnection,
    schema: str,  # Kept for compatibility
    table: str,   # Kept for compatibility
    catalog_id: str, # Logical ID
    collection_id: str, # Logical ID
    limit: int,
    offset: int,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    datetime_str: Optional[str] = None,
    cql_filter: Optional[str] = None,
    bbox_crs_srid: Optional[int] = None,
    target_crs_srid: Optional[int] = None,
    sortby: Optional[str] = None,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Fetches and filters items from a single collection table, leveraging the new ItemService search pipeline.
    Supports reprojection and CQL filters utilizing automated sidecar joins via the QueryOptimizer.
    """
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        raise HTTPException(status_code=503, detail="CatalogsProtocol unavailable.")
    
    # Construct robust QueryRequest utilizing the optimized execution pipeline
    request = QueryRequest(
        limit=limit,
        offset=offset,
        select=[FieldSelection(field="*")],
        include_total_count=True,
        raw_selects=[],
        filters=[],
        raw_params={}
    )
    
    raw_where_parts = []

    # --- 1. BBOX Filter ---
    if bbox:
        input_srid = bbox_crs_srid or 4326
        # Represent BBOX as EWKT for the structured filter
        ewkt = f"SRID={input_srid};POLYGON(({bbox[0]} {bbox[1]}, {bbox[0]} {bbox[3]}, {bbox[2]} {bbox[3]}, {bbox[2]} {bbox[1]}, {bbox[0]} {bbox[1]}))"
        request.filters.append(FilterCondition(
            field="geom",
            operator=FilterOperator.BBOX,
            value=ewkt,
            spatial_op=True
        ))

    # --- 2. Datetime Filter ---
    if datetime_str:
        if "/" in datetime_str:
            start_str, end_str = datetime_str.split('/')
            start_dt, end_dt = (start_str if start_str != '..' else None), (end_str if end_str != '..' else None)
            
            if start_dt and end_dt:
                request.filters.append(FilterCondition(
                    field="validity",
                    operator=FilterOperator.RANGE_OVERLAPS,  # &&: ranges overlap
                    value=f"[{start_dt}, {end_dt}]"
                ))
            elif start_dt:
                request.filters.append(FilterCondition(
                    field="validity", 
                    operator=FilterOperator.GTE, 
                    value=start_dt
                ))
            elif end_dt:
                request.filters.append(FilterCondition(
                    field="validity", 
                    operator=FilterOperator.LTE, 
                    value=end_dt
                ))
        else:
            request.filters.append(FilterCondition(
                field="validity",
                operator=FilterOperator.RANGE_CONTAINS,  # @>: range contains point
                value=datetime_str
            ))

    # --- 3. CQL Filter ---
    if cql_filter:
        try:
            from sqlalchemy import text
            from dynastore.models.protocols import ItemsProtocol
            
            items_svc = get_protocol(ItemsProtocol)
            if items_svc is None:
                raise HTTPException(status_code=503, detail="ItemsProtocol unavailable.")

            # Fetch full field definitions including sidecars
            # We don't need physical schema/table here as we pass logical IDs
            field_defs = await items_svc.get_collection_fields(
                catalog_id=catalog_id,
                collection_id=collection_id,
                db_resource=conn
            )

            valid_props = set(field_defs.keys())
            field_mapping = {}
            
            for name, defn in field_defs.items():
                # Map to SQL expression defined by sidecar (e.g. "a.asset_id", "h.geoid")
                field_mapping[name] = text(defn.sql_expression or name)

            cql_where, cql_params = parse_cql_filter(
                cql_filter, 
                field_mapping=field_mapping, 
                valid_props=valid_props, 
                parser_type='cql2'
            )

            if cql_where:
                raw_where_parts.append(f"({cql_where})")
                request.raw_params.update(cql_params)

        except ValueError as ve:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
        except ImportError:
            raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="CQL filtering is not available on this server.")
        except Exception as e:
            logger.error(f"Failed to parse CQL2 filter: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid CQL2-TEXT filter: {e}")

    if raw_where_parts:
        request.raw_where = " AND ".join(raw_where_parts)

    # --- 4. Sort Order ---
    if sortby:
        request.sort = []
        for field in sortby.split(','):
            field = field.strip()
            if not field: continue
            direction = "DESC" if field.startswith('-') else "ASC"
            field = field.lstrip('+-')
            if not re.match(r'^[a-zA-Z0-9_]+$', field):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid sort field: '{field}'")
            request.sort.append(SortOrder(field=field, direction=direction))

    # --- 5. Projection (Reprojection) ---
    if target_crs_srid:
        # Reprojection transformation: QueryOptimizer will skip redundant ST_Transform internally
        request.select.append(FieldSelection(
            field="geom", 
            transformation="ST_Transform", 
            transform_args={"srid": target_crs_srid},
            alias="geom"
        ))

    # --- Execute Single Query utilizing the Optimized Services ---
    from dynastore.models.driver_context import DriverContext
    rows_proxy = await catalogs.search_items(
        catalog_id=catalog_id,
        collection_id=collection_id,
        request=request,
        ctx=DriverContext(db_resource=conn) if conn is not None else None,
    )

    rows = []
    total_count = 0
    if rows_proxy:
        total_count = (rows_proxy[0].properties or {}).get("_total_count", 0)
        for row in rows_proxy:
            r = row.model_dump()
            r["properties"].pop("_total_count", None)
            rows.append(r)

    return total_count, rows