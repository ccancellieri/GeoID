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

# dynastore/extensions/stac/stac_hierarchy_queries.py

import logging
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy import text
from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
from dynastore.modules.stac.stac_config import HierarchyRule, HierarchyStrategy
from dynastore.tools.cache import cached

logger = logging.getLogger(__name__)


def build_hierarchy_where_clause(
    rule: HierarchyRule,
    parent_value: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None
) -> str:
    """
    Builds the SQL WHERE fragment for a hierarchy rule.
    
    Args:
        rule: The hierarchy rule to apply
        parent_value: Optional parent value to filter children
        params: Parameters dictionary to update with bind values
        
    Returns:
        SQL WHERE fragment (without 'WHERE' or 'AND' prefix)
    """
    if params is None:
        params = {}
        
    where_clauses = []
    
    if rule.strategy == HierarchyStrategy.FIXED:
        # Apply the rule's condition if present
        if rule.condition:
            where_clauses.append(f"({rule.condition})")
        
        # If parent_value is provided, filter by parent_code_field
        if parent_value and rule.parent_code_field:
            where_clauses.append(f"attributes->>'{rule.parent_code_field}' = :parent_value")
            params["parent_value"] = parent_value
            
    elif rule.strategy == HierarchyStrategy.RECURSIVE:
        # For recursive strategy, filter by parent relationship
        if parent_value:
            if rule.parent_code_field:
                where_clauses.append(f"attributes->>'{rule.parent_code_field}' = :parent_value")
                params["parent_value"] = parent_value
        else:
            # No parent means we want root nodes
            if rule.root_condition:
                where_clauses.append(f"({rule.root_condition})")
            elif rule.parent_code_field:
                where_clauses.append(f"attributes->>'{rule.parent_code_field}' IS NULL")
                
    return " AND ".join(where_clauses) if where_clauses else "TRUE"


async def build_hierarchy_items_query(
    catalog_id: str,
    collection_id: str,
    rule: HierarchyRule,
    parent_value: Optional[str] = None,
    limit: int = 10,
    offset: int = 0
) -> Tuple[str, Dict[str, Any]]:
    """
    Builds a SQL query to retrieve items matching a hierarchy rule.
    """
    params: Dict[str, Any] = {
        "limit": limit,
        "offset": offset
    }
    
    where_clause = build_hierarchy_where_clause(rule, parent_value, params)
    
    # Base query selecting all columns plus geometry
    base_select = f"""
        SELECT *, ST_AsEWKB(geom) as geom
        FROM "{catalog_id}"."{collection_id}"
        WHERE deleted_at IS NULL AND {where_clause}
        ORDER BY attributes->>'{rule.item_code_field}'
        LIMIT :limit OFFSET :offset
    """
    
    return base_select, params


async def get_hierarchy_item_count(
    conn: AsyncConnection,
    catalog_id: str,
    collection_id: str,
    rule: HierarchyRule,
    parent_value: Optional[str] = None
) -> int:
    """Returns total count of items matching hierarchy rule."""
    params: Dict[str, Any] = {}
    where_clause = build_hierarchy_where_clause(rule, parent_value, params)
    
    base_query = f"""
        SELECT COUNT(*) as total
        FROM "{catalog_id}"."{collection_id}"
        WHERE deleted_at IS NULL AND {where_clause}
    """
    
    query = DQLQuery(text(base_query), result_handler=ResultHandler.SCALAR_ONE)
    result = await query.execute(conn, **params)
    return int(result) if result else 0


@cached(
    maxsize=256,
    ttl=300,
    jitter=15,
    namespace="stac_hierarchy",
    ignore=["conn"],
)
async def get_distinct_hierarchy_values(
    conn: AsyncConnection,
    catalog_id: str,
    collection_id: str,
    rule: HierarchyRule,
    parent_value: Optional[str] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Returns distinct values for the hierarchy level.
    Used to generate child collection links.
    """
    params: Dict[str, Any] = {"limit": limit}
    where_clause = build_hierarchy_where_clause(rule, parent_value, params)
    
    # Select distinct values of the item_code_field
    select_fields = [f"attributes->>'{rule.item_code_field}' as code"]
    if rule.link_properties:
        for prop in rule.link_properties:
            select_fields.append(f"attributes->>'{prop}' as {prop}")
    
    base_query = f"""
        SELECT DISTINCT {', '.join(select_fields)}, COUNT(*) OVER (PARTITION BY attributes->>'{rule.item_code_field}') as item_count
        FROM "{catalog_id}"."{collection_id}"
        WHERE deleted_at IS NULL
          AND attributes->>'{rule.item_code_field}' IS NOT NULL
          AND {where_clause}
        ORDER BY code
        LIMIT :limit
    """
    
    query = DQLQuery(text(base_query), result_handler=ResultHandler.ALL_DICTS)
    results = await query.execute(conn, **params)
    return results if results else []


@cached(
    maxsize=256,
    ttl=300,
    jitter=15,
    namespace="stac_hierarchy_ext",
    ignore=["conn"],
)
async def get_hierarchy_extent(
    conn: AsyncConnection,
    catalog_id: str,
    collection_id: str,
    rule: HierarchyRule,
    parent_value: Optional[str] = None
) -> Dict[str, Any]:
    """Calculates bbox and temporal extent for items matching hierarchy rule."""
    params: Dict[str, Any] = {}
    where_clause = build_hierarchy_where_clause(rule, parent_value, params)
    
    base_query = f"""
        SELECT 
            ST_XMin(ST_Extent(geom)) as minx,
            ST_YMin(ST_Extent(geom)) as miny,
            ST_XMax(ST_Extent(geom)) as maxx,
            ST_YMax(ST_Extent(geom)) as maxy,
            MIN((attributes->>'datetime')::timestamptz) as min_datetime,
            MAX((attributes->>'datetime')::timestamptz) as max_datetime,
            MIN((attributes->>'start_datetime')::timestamptz) as min_start_datetime,
            MAX((attributes->>'end_datetime')::timestamptz) as max_end_datetime
        FROM "{catalog_id}"."{collection_id}"
        WHERE deleted_at IS NULL
          AND geom IS NOT NULL
          AND {where_clause}
    """
    
    query = DQLQuery(text(base_query), result_handler=ResultHandler.ONE_OR_NONE)
    result = await query.execute(conn, **params)
    
    # Default extent
    extent = {
        "bbox": [-180, -90, 180, 90],
        "temporal": [[None, None]]
    }
    
    if result:
        if result.get('minx') is not None:
            extent["bbox"] = [
                float(result['minx']), float(result['miny']),
                float(result['maxx']), float(result['maxy'])
            ]
        
        start_time = result.get('min_datetime') or result.get('min_start_datetime')
        end_time = result.get('max_datetime') or result.get('max_end_datetime')
        
        extent["temporal"] = [[
            start_time.isoformat() if start_time else None,
            end_time.isoformat() if end_time else None
        ]]
    
    return extent
