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
Query Transform Protocol

Allows modules and extensions to register query transformations that are
applied conditionally based on context. This enables pluggable query building
without hardcoding module-specific logic in ItemService.

Example use cases:
- MVT tile generation (ST_AsMVTGeom transformation)
- DWH joins (ensuring join columns are selected)
- Custom output formats (CSV, Parquet, etc.)
"""

from typing import Protocol, Dict, Any, Tuple, runtime_checkable
from dynastore.models.query_builder import QueryRequest


@runtime_checkable
class QueryTransformProtocol(Protocol):
    """
    Protocol for modules to contribute query transformations.
    
    Transformations are applied in priority order before SQL generation,
    allowing extensions to modify SELECT fields, WHERE clauses, and parameters
    without modifying core ItemService code.
    
    Example:
        class MVTTransform:
            @property
            def transform_id(self) -> str:
                return "mvt"
            
            @property
            def priority(self) -> int:
                return 100
            
            def can_transform(self, context: Dict[str, Any]) -> bool:
                return context.get("geom_format") == "MVT"
            
            def transform_query(
                self, 
                query_request: QueryRequest, 
                context: Dict[str, Any]
            ) -> QueryRequest:
                # Add ST_AsMVTGeom to raw_selects
                query_request.raw_selects.append("ST_AsMVTGeom(...) AS geom")
                return query_request
    """

    @property
    def transform_id(self) -> str:
        """
        Unique identifier for this transformation.
        
        Used for logging and debugging. Should be lowercase with underscores.
        Examples: "mvt", "dwh_join", "csv_export"
        """
        ...

    @property
    def priority(self) -> int:
        """
        Execution priority (lower values execute first).
        
        Allows control over transformation order when multiple transformers
        are applicable. Suggested ranges:
        - 0-49: Pre-processing (e.g., parameter normalization)
        - 50-99: Data selection (e.g., ensuring columns are selected)
        - 100-149: Format transformations (e.g., MVT geometry)
        - 150-199: Post-processing (e.g., result limiting)
        
        Default: 100
        """
        ...

    def can_transform(self, context: Dict[str, Any]) -> bool:
        """
        Determines if this transformation should be applied.
        
        Args:
            context: Query context containing:
                - catalog_id: str
                - collection_id: str
                - col_config: ItemsPostgresqlDriverConfig
                - params: Dict[str, Any] (all request parameters)
                - geom_format: Optional[str] (e.g., "MVT", "WKB", "GeoJSON")
                - output_format: Optional[str] (e.g., "geojson", "csv")
                - ... (any other context from params)
        
        Returns:
            True if this transformation should be applied to the query
        """
        ...

    def transform_query(
        self, query_request: QueryRequest, context: Dict[str, Any]
    ) -> QueryRequest:
        """
        Transforms the query request before SQL generation.
        
        Modify the QueryRequest object by:
        - Adding/removing fields from `select` list
        - Adding raw SQL expressions to `raw_selects`
        - Adding WHERE conditions to `raw_where`
        - Injecting parameters into `raw_params`
        - Modifying limit, offset, order_by, etc.
        
        IMPORTANT: Avoid string manipulation. Use QueryRequest's structured
        fields (select, raw_selects, raw_where, raw_params) instead.
        
        Args:
            query_request: The current query request (may be modified in-place)
            context: Query context (same as can_transform)
        
        Returns:
            Modified QueryRequest (can return same object or new instance)
        """
        ...

    def post_process_sql(
        self, sql: str, params: Dict[str, Any], context: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Optional: Post-process generated SQL directly.
        
        Use sparingly - prefer transform_query when possible. This is for
        cases where SQL-level manipulation is unavoidable (e.g., wrapping
        entire query in a CTE or adding UNION clauses).
        
        Default implementation returns sql and params unchanged.
        
        Args:
            sql: Generated SQL string
            params: Query parameters
            context: Query context
        
        Returns:
            (modified_sql, modified_params)
        """
        ...
