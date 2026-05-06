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
DWH Join Query Transformer

Ensures join columns are selected and optionally filters by join keys
for Data Warehouse join operations.
"""

import logging
from typing import Dict, Any, Tuple
from dynastore.models.query_builder import QueryRequest, FieldSelection

logger = logging.getLogger(__name__)


class DWHJoinQueryTransform:
    """
    Query transformer for DWH join operations.
    
    Ensures join column is selected and optionally filters by join keys
    when dwh_join_column is present in context.
    """

    @property
    def transform_id(self) -> str:
        return "dwh_join"

    @property
    def priority(self) -> int:
        return 50  # Run early to ensure join column is available

    def can_transform(self, context: Dict[str, Any]) -> bool:
        """Check if this is a DWH join query"""
        return "dwh_join_column" in context

    def transform_query(
        self, query_request: QueryRequest, context: Dict[str, Any]
    ) -> QueryRequest:
        """
        Transform query for DWH join.
        
        Ensures join column is selected and optionally filters by join keys.
        """
        join_column = context["dwh_join_column"]

        # Ensure join column is selected
        existing_fields = {s.field for s in query_request.select}
        existing_aliases = {s.alias for s in query_request.select if s.alias}

        if join_column not in existing_fields and join_column not in existing_aliases:
            query_request.select.append(FieldSelection(field=join_column))
            logger.debug(f"DWH transform: Added join column '{join_column}' to select")

        # Optional: Filter by join keys if DWH result is small
        # This reduces data transfer when DWH result set is smaller than spatial data
        join_keys = context.get("dwh_join_keys")
        if join_keys and len(join_keys) < 1000:  # Threshold for efficiency
            # Use ANY array operator for efficient filtering
            filter_clause = f"{join_column} = ANY(:dwh_join_keys)"

            if query_request.raw_where:
                query_request.raw_where += f" AND {filter_clause}"
            else:
                query_request.raw_where = filter_clause

            query_request.raw_params["dwh_join_keys"] = list(join_keys)
            logger.debug(
                f"DWH transform: Added join key filter for {len(join_keys)} keys"
            )

        return query_request

    def post_process_sql(
        self, sql: str, params: Dict[str, Any], context: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """No SQL post-processing needed for DWH join"""
        return sql, params
