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
import asyncio
from typing import List, Optional, Dict, Any, Union, Callable, Awaitable

# Hard runtime dep — see modules/elasticsearch/module.py for rationale.
# Forces entry-point load to fail on services without ``GDAL`` (the osgeo
# package, transitively required by the ``module_gdal`` extra) so the
# CapabilityMap doesn't list this task as claimable on services lacking it.
# ``modules.gdal.service`` itself uses a ``try: from osgeo import gdal except
# ImportError`` pattern (lines 23-29) — that's intentional for read paths
# that don't open rasters; we need the strict gate at the task tier.
from osgeo import gdal as _gdal  # noqa: F401

from dynastore.modules.catalog.asset_service import (
    Asset,
    AssetTypeEnum,
    AssetService,
    AssetUpdate,
)
from dynastore.modules.tasks.models import TaskPayload
from dynastore.modules.processes.models import ExecuteRequest, Process
from dynastore.modules.catalog.asset_tasks_spi import AssetTasksSPI
from dynastore.tasks.protocols import TaskProtocol
from dynastore.models.protocols import AssetsProtocol
from dynastore.modules import get_protocol
from dynastore.modules.gdal import service as gdal_module
from dynastore.tasks.gdal.definition import GDALINFO_PROCESS_DEFINITION

logger = logging.getLogger(__name__)


class GdalInfoTask(TaskProtocol, AssetTasksSPI):
    """
    Task that calculates GDAL/OGR info for an asset and enriches its metadata.
    """

    @staticmethod
    def get_definition() -> Process:
        return GDALINFO_PROCESS_DEFINITION

    def __init__(self, app_state: object):
        self.app_state = app_state

    async def can_run_on_asset(self, asset: Asset) -> bool:
        """
        GdalInfo can run on any asset that GDAL can handle (Raster or Vectorial).
        """
        if not gdal_module.GDAL_AVAILABLE:
            return False
        return asset.asset_type in [AssetTypeEnum.RASTER, AssetTypeEnum.VECTORIAL]

    async def get_execution_request(
        self, asset: Asset, execution_request: ExecuteRequest
    ) -> ExecuteRequest:
        """
        Injects asset information into the execution request inputs.
        """
        inputs = execution_request.inputs.copy() or {}
        inputs.update(
            {
                "asset_uri": str(asset.uri),
                "asset_id": asset.asset_id,
                "catalog_id": asset.catalog_id,
                "collection_id": asset.collection_id,
                "asset_type": asset.asset_type.value,
            }
        )
        # Create a new request with updated inputs (pydantic model is immutable by default but we are creating new)
        return ExecuteRequest(
            inputs=inputs,
            outputs=execution_request.outputs,
            response=execution_request.response,
        )

    async def run(self, payload: TaskPayload[ExecuteRequest]) -> Any:
        # payload.inputs is a generic InputType which might be a dict or a model
        # checks to ensure we can access the 'inputs' field
        raw_inputs = payload.inputs
        inputs: Dict[str, Any]
        if isinstance(raw_inputs, ExecuteRequest):
            inputs = raw_inputs.inputs
        elif isinstance(raw_inputs, dict):
            nested = raw_inputs.get("inputs", raw_inputs)
            inputs = nested if isinstance(nested, dict) else {}
        else:
            inputs = {}

        asset_uri = inputs.get("asset_uri")
        asset_id = inputs.get("asset_id")

        if not asset_uri or not asset_id:
            raise ValueError(
                "Asset information (uri, id) is missing in task inputs for GdalInfoTask."
            )

        # Ensure we have the asset object (passed via payload)
        asset: Optional[Asset] = payload.asset

        if asset is None:
            # Fallback attempt to fetch if missing in payload but present in inputs
            catalog_id = inputs.get("catalog_id")
            collection_id = inputs.get("collection_id")
            assets = get_protocol(AssetsProtocol)
            if assets and catalog_id and asset_id:
                asset = await assets.get_asset(
                    asset_id=asset_id,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                )

        if asset is None:
            raise ValueError(f"Asset '{asset_id}' could not be resolved.")

        from dynastore.modules.gcp.tools import bucket as bucket_tool

        gdal_path = bucket_tool.get_gdal_path(asset_uri)

        logger.info(f"Running GDAL info for asset '{asset_id}' at '{gdal_path}'")

        info = {}
        try:
            asset_type = inputs.get("asset_type")
            if asset_type == AssetTypeEnum.RASTER.value:
                info = await asyncio.to_thread(gdal_module.get_raster_info, gdal_path)
            else:
                info = await asyncio.to_thread(gdal_module.get_vector_info, gdal_path)

            # Enrich Metadata
            # Note: We need to re-fetch or construct enough context to update the asset.
            # We have IDs, so we can call update_asset.
            assets = get_protocol(AssetsProtocol)
            if assets is None:
                raise ValueError("AssetsProtocol implementation is not available.")

            catalog_id = inputs.get("catalog_id")
            collection_id = inputs.get("collection_id")
            if not catalog_id:
                raise ValueError("catalog_id is missing in task inputs for GdalInfoTask.")

            # Fetch fresh asset to ensure we are not overwriting concurrent changes
            # and to get the base for our merge.
            fresh_asset = await assets.get_asset(
                asset_id=asset_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )

            if not fresh_asset:
                raise ValueError(
                    f"Asset '{asset_id}' not found during execution of GdalInfoTask."
                )

            current_metadata = fresh_asset.metadata or {}

            # If the USER provided 'asset_metadata' in inputs (as an override), we apply it.
            # This allows the caller to patch other metadata fields alongside the info generation.
            user_metadata_override = inputs.get("asset_metadata") or {}

            # 1. Start with fresh
            # 2. Update with user overrides
            # 3. Update with calculated info (ensure this takes precedence or is just a key)
            final_metadata = current_metadata.copy()
            final_metadata.update(user_metadata_override)
            final_metadata["gdalinfo"] = info

            await assets.update_asset(
                asset_id=fresh_asset.asset_id,
                update=AssetUpdate(metadata=final_metadata),
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            logger.info(f"Asset '{asset_id}' metadata enriched with GDAL info.")

            return {"info": info}

        except Exception as e:
            msg = str(e)
            # Check for expected file not found errors in tests/integration
            if "No such file or directory" in msg or "does not exist" in msg:
                logger.warning(
                    f"GdalInfoTask: File not found for asset '{asset_id}'. This might be expected in test environments without real buckets. Error: {e}"
                )
            else:
                logger.error(
                    f"GdalInfoTask failed for asset '{asset_id}': {e}", exc_info=True
                )
            raise
