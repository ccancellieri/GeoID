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

from dynastore.models.protocols import ItemsProtocol
from dynastore.models.query_builder import QueryRequest
from dynastore.models.driver_context import DriverContext
from dynastore.tools.discovery import get_protocol
from dynastore.modules.stac.stac_config import StacPluginConfig
import logging

logger = logging.getLogger(__name__)

from typing import Any, List, Optional, Tuple


async def get_stac_items_paginated(
    conn: Any,
    catalog_id: str,
    collection_id: str,
    limit: int,
    offset: int,
    stac_config: Optional[StacPluginConfig] = None,
) -> Tuple[list, int]:
    """
    Fetches a paginated list of items for STAC using the optimised QueryOptimizer path.

    Geometries are simplified at the database level based on the StacPluginConfig.
    The total count is derived from COUNT(*) OVER() embedded in the same query,
    so no separate count query is needed.
    """
    items_svc = get_protocol(ItemsProtocol)
    if not items_svc:
        raise RuntimeError("ItemsProtocol not available")

    simplification: float = 0.0001
    if stac_config and stac_config.simplification:
        simplification = stac_config.simplification.default_tolerance

    request = QueryRequest(
        limit=limit,
        offset=offset,
        include_total_count=True,
        raw_params={
            "geom_format": "WKB",
            "simplification": simplification,
            "lang": "*",  # stac_generator handles specific language selection
        },
    )

    query_response = await items_svc.stream_items(
        catalog_id=catalog_id,
        collection_id=collection_id,
        request=request,
        ctx=DriverContext(db_resource=conn) if conn is not None else None,
    )

    if query_response is None:
        return [], 0

    processed_result = [feature async for feature in query_response]
    total_count: int = query_response.total_count or 0

    return processed_result, total_count
