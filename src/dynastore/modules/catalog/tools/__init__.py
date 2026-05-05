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

import logging
from typing import Optional

from dynastore.modules.catalog.models import SpatialExtent, TemporalExtent
from dynastore.modules.db_config.query_executor import (
    DbResource,
    managed_transaction,
)
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)


async def recalculate_and_update_extents(
    db_resource: DbResource, catalog_id: str, collection_id: str
):
    """
    Recalculates the spatial and temporal extents of a collection's data and
    updates its metadata record via the storage driver.
    """
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation
    from dynastore.modules.storage.hints import Hint

    catalogs = get_protocol(CatalogsProtocol)
    if not catalogs:
        logger.warning("CatalogsProtocol not found. Cannot recalculate extents.")
        return

    async with managed_transaction(db_resource) as conn:
        try:
            driver = await get_driver(Operation.READ, catalog_id, collection_id)
        except ValueError:
            logger.warning(
                f"No READ driver for '{catalog_id}:{collection_id}'. Cannot recalculate extents."
            )
            return

        # Drivers self-declare aggregation/extents support via the Hint
        # catalogue (post PR #3b — the read-flavour Capabilities moved to
        # ``Hint``).  Falling back to ``True`` when neither is present
        # treats the driver optimistically; ``compute_extents`` handles
        # NotImplementedError downstream.
        supported = getattr(driver, "supported_hints", None)
        if supported is not None and Hint.AGGREGATION not in supported:
            logger.warning(
                f"Driver for '{catalog_id}:{collection_id}' does not support compute_extents."
            )
            return

        extents = await driver.compute_extents(
            catalog_id, collection_id, db_resource=conn
        )
        if not extents:
            return

        spatial = extents.get("spatial", {})
        temporal = extents.get("temporal", {})

        # compute_extents already returns bbox as [[x1,y1,x2,y2]] (list of bboxes)
        bbox = spatial.get("bbox", [[-180.0, -90.0, 180.0, 90.0]])
        new_spatial_extent = SpatialExtent(bbox=bbox)

        interval = temporal.get("interval", [[None, None]])
        new_temporal_extent = TemporalExtent(interval=interval)

        collection = await catalogs.get_collection(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
        )
        if collection:
            if not collection.extent:
                from dynastore.models.shared_models import Extent
                collection.extent = Extent(
                    spatial=new_spatial_extent, temporal=new_temporal_extent
                )
            else:
                collection.extent.spatial = new_spatial_extent
                collection.extent.temporal = new_temporal_extent

            await catalogs.update_collection(
                catalog_id,
                collection_id,
                collection.model_dump(),
                lang="*",
                ctx=DriverContext(db_resource=conn),
            )
            logger.info(
                f"Successfully updated extents for collection '{catalog_id}:{collection_id}'."
            )


def get_engine() -> Optional[DbResource]:
    from dynastore.tools.protocol_helpers import get_engine as get_platform_engine

    return get_platform_engine()
