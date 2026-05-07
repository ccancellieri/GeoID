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
from typing import Any, Optional

# Hard runtime dep — see modules/elasticsearch/module.py for rationale.
# Forces entry-point load to fail on services without ``geopandas``
# (transitively required by the ``geospatial_io`` extra in worker_task_ingestion
# → ingestion → task_ingestion_deps → geospatial → geospatial_io) so the
# CapabilityMap doesn't list this task as claimable on services lacking it.
# ``main_ingestion.py`` itself imports fiona lazily inside functions, but we
# need the strict gate at the task entry-point layer.
import geopandas  # noqa: F401

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task
from dynastore.tasks.ingestion.ingestion_models import TaskIngestionRequest
from dynastore.tools.protocol_helpers import get_engine
from .definition import INGESTION_PROCESS_DEFINITION
from dynastore.modules.processes.models import Process, ExecuteRequest, StatusInfo

# Asset SPI imports
from dynastore.modules.catalog.asset_tasks_spi import AssetTasksSPI
from dynastore.modules.catalog.asset_service import Asset, AssetTypeEnum

logger = logging.getLogger(__name__)
class IngestionTask(ProcessTaskProtocol[Process, TaskPayload[ExecuteRequest], Optional[StatusInfo]], AssetTasksSPI):
    priority: int = 100
    """
    A formal, stateful task responsible for running the data ingestion process.
    It is discovered and executed by the main_task.py entrypoint.
    It also implements AssetTasksSPI to allow context injection when triggered via Asset Service.
    """
    @staticmethod
    def get_definition() -> Process:
        """Exposes the OGC Process definition for this task."""
        return INGESTION_PROCESS_DEFINITION

    def __init__(self, app_state: object):
        self.app_state = app_state
        logger.info("IngestionTask initialized.")

    # --- AssetTasksSPI Implementation ---

    async def can_run_on_asset(self, asset: Asset) -> bool:
        # Ingestion generally works on Vectorial assets (CSV, etc) or maybe others in future.
        # For now, let's allow it if it's VECTORIAL or if we relax this.
        # VectorialIngestionTask was specific to VECTORIAL.
        # Let's keep it broad or restrict? The task handles vectors.
        return asset.asset_type == AssetTypeEnum.VECTORIAL

    async def get_execution_request(self, asset: Asset, execution_request: ExecuteRequest) -> ExecuteRequest:
        """
        Injects asset context (catalog, collection, asset code) into the ingestion request inputs.
        """
        inputs = execution_request.inputs.copy()
        
        # Inject context if missing
        if "catalog_id" not in inputs:
            inputs["catalog_id"] = asset.catalog_id
        if "collection_id" not in inputs:
            inputs["collection_id"] = asset.collection_id

        # Handle nested ingestion_request
        ing_req = inputs.get("ingestion_request", {})
        if not isinstance(ing_req, dict):
            ing_req = {}
        
        # Ensure 'asset' field in ingestion_request
        ing_asset = ing_req.get("asset", {})
        if "asset_id" not in ing_asset and "uri" not in ing_asset:
             ing_asset["asset_id"] = asset.asset_id
             ing_req["asset"] = ing_asset
        
        inputs["ingestion_request"] = ing_req
        
        return ExecuteRequest(
            inputs=inputs,
            outputs=execution_request.outputs,
            response=execution_request.response
        )
    
    # Note: run method signature in SPI includes 'asset', but ProcessTaskProtocol does not.
    # The AssetService uses get_execution_request but triggers the task via standard Process flow (no asset arg).
    # So we keep the standard run signature. 
    # If AssetService ever calls run(..., asset=...), we would need to handle it, but currently it doesn't.

    async def run(self, payload: TaskPayload[ExecuteRequest]) -> Optional[StatusInfo]:
        """The core execution logic. It parses the payload and calls the main ingestion function."""
        task_id: str = str(payload.task_id)
        # A background task MUST use a synchronous engine to avoid event loop conflicts.
        sync_engine = get_engine()
        if not sync_engine:
            raise RuntimeError("IngestionTask requires a database engine.")
        try:
            # The payload.inputs field is an ExecuteRequest Pydantic model.
            # The actual OGC process inputs are in the 'inputs' attribute of that model.
            ogc_process_inputs = payload.inputs
            caller_id = payload.caller_id

            # Handle both object (Pydantic model) and dict (from generic runners)
            # This is necessary because TaskPayload[ExecuteRequest] might receive a dict
            # when instantiated via generic runners that don't cast inputs to the generic type.
            if hasattr(ogc_process_inputs, 'inputs'):
                inputs_dict = ogc_process_inputs.inputs
            elif isinstance(ogc_process_inputs, dict):
                inputs_dict = ogc_process_inputs.get('inputs', {})
            else:
                raise ValueError(f"Unexpected type for payload.inputs: {type(ogc_process_inputs)}")

            catalog_id = inputs_dict['catalog_id']
            collection_id = inputs_dict['collection_id']
            task_request = TaskIngestionRequest(**inputs_dict['ingestion_request'])

            logger.info(f"Running ingestion task '{task_id}' for {catalog_id}:{collection_id}")

            # This is now a direct, blocking call. The runner is responsible for thread management.
            await run_ingestion_task(
                engine=sync_engine,
                task_id=task_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
                task_request=task_request,
                caller_id=caller_id
            )
            logger.info(f"Ingestion task '{task_id}' complete.")
            return StatusInfo(jobID=payload.task_id, status="successful", message="Ingestion task completed successfully.", progress=100, links=[])
        except Exception as e:
            logger.error(f"Ingestion task '{task_id}' failed catastrophically: {e}", exc_info=True)
            # The DatabaseStatusReporter will catch this exception and mark the task as FAILED.
            raise e