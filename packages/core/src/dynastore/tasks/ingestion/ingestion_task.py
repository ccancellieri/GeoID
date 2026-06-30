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
from typing import Optional

# Hard runtime dep — see modules/elasticsearch/module.py for rationale.
# Forces entry-point load to fail on services without ``geopandas``
# (transitively required by the ``geospatial_io`` extra in worker_task_ingestion
# → ingestion → task_ingestion_deps → geospatial → geospatial_io) so the
# CapabilityMap doesn't list this task as claimable on services lacking it.
# ``main_ingestion.py`` / ``tools/file_io.py`` import geopandas + pyogrio
# lazily inside functions, but we need the strict gate at the task
# entry-point layer.
import geopandas  # noqa: F401

from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks import result_message
from dynastore.tasks.ingestion.main_ingestion import run_ingestion_task
from dynastore.tasks.ingestion.ingestion_models import TaskIngestionRequest
from dynastore.tools.protocol_helpers import get_engine
from .definition import INGESTION_PROCESS_DEFINITION
from dynastore.modules.processes.models import Process, ExecuteRequest, StatusInfo

logger = logging.getLogger(__name__)


async def _maybe_enqueue_tile_preseed(
    catalog_id: str,
    collection_id: str,
    spec: object,
) -> None:
    """Chain a ``tiles_preseed`` job after a successful ingestion (opt-in).

    ``spec`` is the ``preseed_on_success`` value carried in the ingestion
    inputs: falsy/absent → no preseed (the default for ordinary ingestions);
    a truthy flag → MVT defaults; a dict → overrides (``output_format``
    default ``"mvt"``, optional ``tms_ids``).

    Submitted through the canonical OGC process-execution path
    (:func:`execute_process`) so it is validated, mode-resolved and
    dedup-guarded exactly like a ``POST /processes/tiles_preseed/execution``.
    A short-lived async engine is built for the enqueue (mirrors the event
    drain) because the ingestion task itself runs on a synchronous engine.
    The ``dedup_key`` collapses redundant chains for the same target while the
    prior job is non-terminal; a fresh ingestion after a terminal preseed
    enqueues a new one.
    """
    if not spec:
        return
    opts = spec if isinstance(spec, dict) else {}
    preseed_inputs: dict = {
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "output_format": opts.get("output_format", "mvt"),
        "operation": "seed",
    }
    if opts.get("tms_ids"):
        preseed_inputs["tms_ids"] = opts["tms_ids"]

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    from dynastore.modules.db_config.db_config import DBConfig
    from dynastore.modules.db_config.tools import normalize_db_url
    from dynastore.modules.processes import models as _proc_models
    from dynastore.modules.processes.processes_module import execute_process

    db_url = normalize_db_url(DBConfig.database_url, is_async=True)
    engine = create_async_engine(db_url, poolclass=NullPool)
    try:
        await execute_process(
            "tiles_preseed",
            _proc_models.ExecuteRequest(inputs=preseed_inputs),
            engine=engine,
            caller_id="ingestion:preseed_on_success",
            preferred_mode=_proc_models.JobControlOptions.ASYNC_EXECUTE,
            catalog_id=catalog_id,
            collection_id=collection_id,
            dedup_key=f"preseed_on_ingest:{catalog_id}:{collection_id}",
        )
        logger.info(
            "Ingestion chained tiles_preseed (%s) for %s/%s",
            preseed_inputs["output_format"], catalog_id, collection_id,
        )
    finally:
        await engine.dispose()


class IngestionTask(ProcessTaskProtocol[Process, TaskPayload[ExecuteRequest], Optional[StatusInfo]]):
    priority: int = 100
    """
    A formal, stateful task responsible for running the data ingestion process.
    It is discovered and executed by the main_task.py entrypoint.

    A plain OGC process: ingestion context (``catalog_id``/``collection_id`` and
    the nested ``ingestion_request``) is supplied in ``inputs``. GCS event-driven
    ingestion calls ``processes_module.execute_process`` directly.
    """
    @staticmethod
    def get_definition() -> Process:
        """Exposes the OGC Process definition for this task."""
        return INGESTION_PROCESS_DEFINITION

    def __init__(self, app_state: object):
        self.app_state = app_state
        logger.info("IngestionTask initialized.")

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

            # Opt-in completion chain: once the features are committed, enqueue a
            # tile preseed for the collection so the tiles render against real
            # data (a preseed submitted up-front would race this async job and
            # tile an empty collection). Best-effort — a failure to enqueue must
            # never fail the already-committed ingestion.
            try:
                await _maybe_enqueue_tile_preseed(
                    catalog_id, collection_id,
                    inputs_dict.get("preseed_on_success"),
                )
            except Exception:  # noqa: BLE001 — preseed chaining is best-effort
                logger.warning(
                    "Ingestion task '%s': tiles_preseed chaining failed for "
                    "%s/%s; ingestion itself succeeded.",
                    task_id, catalog_id, collection_id, exc_info=True,
                )

            # Standard message: a URL to verify the ingested output — the
            # feature collection (items listing) for the target collection,
            # filtered to the source asset's features when its id is known.
            # Root-relative unless an operator sets PublicUrlConfig, so the link
            # resolves against whatever host/proxy the consumer browses through.
            asset_id = task_request.asset.asset_id if task_request.asset else None
            verify_url = await result_message.items_verify_url(
                catalog_id, collection_id, asset_id=asset_id
            )
            return result_message.completed(payload.task_id, message=verify_url)
        except Exception as e:
            logger.error(f"Ingestion task '{task_id}' failed catastrophically: {e}", exc_info=True)
            # The DatabaseStatusReporter will catch this exception and mark the task as FAILED.
            raise e