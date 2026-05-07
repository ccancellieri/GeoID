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

import logging
from typing import Dict, Any, List, Optional, Tuple, Set

from sqlalchemy import text
from pydantic import BaseModel

from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarProtocol
from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars
from dynastore.modules.db_config.query_executor import DbResource, GeoDQLQuery, ResultHandler

logger = logging.getLogger(__name__)

class CatalogQueryOrchestrator:
    """
    Orchestrates queries across the Hub and multiple Sidecars.
    
    Responsibilities:
    1. Resolve attribute paths to specific sidecars (e.g. 'h3_res9' -> geometry sidecar).
    2. Construct efficient SQL JOINs.
    3. execute queries transparently.
    """
    
    def __init__(self, collection_config: ItemsPostgresqlDriverConfig):
        self.config = collection_config
        self.sidecars: List[SidecarProtocol] = []
        self._init_sidecars()
        
    def _init_sidecars(self):
        # We need to instantiate sidecars from config
        # This duplicates logic in ItemService, maybe we should centralize the factory? 
        # For now, inline.
        if driver_sidecars(self.config):
            from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry
            for sc_config in driver_sidecars(self.config):
                try:
                    sidecar = SidecarRegistry.get_sidecar(sc_config)
                    if sidecar is not None:
                        self.sidecars.append(sidecar)
                except ValueError as e:
                    logger.warning(f"Skipping sidecar orchestration: {e}")

    def _get_required_joins(self, attributes: Set[str]) -> Tuple[Set[SidecarProtocol], Dict[str, str]]:
        """
        Determines which sidecars need to be joined based on requested attributes.
        Returns:
            - Set of sidecar IDs to join
            - Dict of {attribute_alias -> sql_expression}
        """
        sidecars_to_join = set()
        resolved_columns = {}
        
        # Always include Hub columns (assumed to be available as 'h.*')
        # We don't have a strict list of hub columns here easily without checking schema
        # but we can try resolving via sidecars first.
        
        for attr in attributes:
            resolved = False
            for sidecar in self.sidecars:
                res = sidecar.resolve_query_path(attr)
                if res:
                    col_expr, alias = res # e.g. "g.geom", "g"
                    resolved_columns[attr] = col_expr
                    sidecars_to_join.add(sidecar)
                    resolved = True
                    break
            
            if not resolved:
                # Assume Hub column
                resolved_columns[attr] = f"h.{attr}"
                
        return sidecars_to_join, resolved_columns

    def build_select_query(
        self,
        schema: str,
        table: str,
        columns: List[str],
        where_clauses: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> str:
        """
        Builds a SELECT query with necessary JOINs.
        """
        # 1. Identify necessary sidecars
        # We look at columns AND where clauses to find dependencies
        # Simple extraction of identifiers from where clauses is hard without parsing
        # For now, we only assume 'columns' drives the JOINs, 
        # OR we join ALL sidecars if we can't determine dependency (safer but slower).
        # Optimization: User must provide list of attributes used in WHERE if they want optimized joins.
        
        # Making a simplification: Join ALL configured sidecars for now to ensure correctness
        # until we have a better SQL parser.
        # But wait, `resolve_query_path` is powerful.
        
        needed_sidecars, resolved_cols = self._get_required_joins(set(columns))

        # 2. Construct SQL
        select_parts = [f"{expr} AS {alias}" for alias, expr in resolved_cols.items()]
        select_clause = ", ".join(select_parts)

        joins = []
        for sidecar in needed_sidecars:
            joins.append(sidecar.get_join_clause(schema=schema, hub_table=table, hub_alias="h"))

        join_clause = " ".join(joins)

        # SAFETY: where_clauses must use bind-parameter syntax (:param),
        # never interpolated user input.
        effective_where = where_clauses or []
        where_clause = " AND ".join(effective_where)
        if where_clause:
            where_clause = f"WHERE {where_clause}"

        limit_clause = f"LIMIT :_qo_limit" if limit is not None else ""

        query = f"""
            SELECT {select_clause}
            FROM "{schema}"."{table}" h
            {join_clause}
            {where_clause}
            {limit_clause}
        """
        return query

