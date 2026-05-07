import logging
import asyncio
from typing import Any, Optional, List

# Hard runtime dep — see modules/elasticsearch/module.py for rationale.
# Forces entry-point load to fail on services without ``google-cloud-bigquery``
# (transitively required by the ``dwh`` extra → ``joins``) so the
# CapabilityMap doesn't list this task as claimable on services lacking it.
import google.cloud.bigquery  # noqa: F401

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.processes.models import (
    Process,
    ExecuteRequest,
    StatusInfo,
)
from dynastore.tasks.tools import initialize_reporters
from dynastore.tools.async_utils import SyncQueueIterator
from dynastore.tools.file_io import get_features_as_byte_stream
from dynastore.modules.gcp.tools.bucket import upload_stream_to_gcs
from dynastore.modules.tools.features import FeatureStreamConfig, stream_features
from dynastore.models.shared_models import OutputFormatEnum
from dynastore.extensions.dwh.dwh import execute_bigquery_async
from dynastore.modules.concurrency import get_concurrency_backend
from dynastore.extensions.tools.formatters import format_map

from .definition import DWH_JOIN_EXPORT_PROCESS_DEFINITION
from .models import DwhJoinExportRequest

logger = logging.getLogger(__name__)


class DwhJoinExportTask(TaskProtocol[Process, TaskPayload[ExecuteRequest], Optional[StatusInfo]]):
    priority: int = 100
    @staticmethod
    def get_definition() -> Process:
        return DWH_JOIN_EXPORT_PROCESS_DEFINITION

    def __init__(self, app_state: object):
        self.app_state = app_state

    async def run(self, payload: TaskPayload[ExecuteRequest]) -> Optional[StatusInfo]:
        task_id = payload.task_id
        engine = get_engine()
        if engine is None:
            raise RuntimeError("No database engine available.")

        try:
            inputs = payload.inputs.inputs
            request = DwhJoinExportRequest(**inputs)
            
            logger.info(f"Starting DwhJoinExportTask {task_id} for {request.catalog}:{request.collection}")

            reporters = initialize_reporters(
                engine=engine,
                task_id=str(task_id),
                task_request=request,
                reporting_config=request.reporting
            )

            for r in reporters:
                await r.task_started(str(task_id), request.collection, request.catalog, request.destination_uri)

            # --- DWH Join Logic ---
            # 1. Execute BigQuery to get join data
            join_values = await execute_bigquery_async(
                query=request.dwh_query, 
                join_column=request.dwh_join_column, 
                project_id=request.dwh_project_id
            )
            
            if not join_values:
                logger.warning("DWH query returned no join values. Exporting empty set or base features only depending on requirements.")
                # If join_values is empty, we might still want to stream base features if it's a LEFT JOIN.
                # But usually DWH join implies we only want joined features.
                # Let's assume we proceed with empty join result (filtering out all features).

            # --- Producer: Stream features with DWH data injected ---
            queue = asyncio.Queue(maxsize=1000)
            
            async def producer():
                try:
                    # We only stream if we have join values (acting as an INNER JOIN filter)
                    if not join_values:
                        return

                    # Configure base feature stream
                    stream_config = FeatureStreamConfig(
                        catalog=request.catalog,
                        collection=request.collection,
                        cql_filter=request.where, # DWHJoinRequest uses 'where' field for CQL
                        property_names=request.attributes, # Map to attributes
                        limit=request.limit,
                        offset=request.offset,
                        include_geometry=request.with_geometry,
                        target_srid=request.destination_crs
                    )
                    
                    # Stream features and join with DWH data
                    async for feature in stream_features(stream_config, engine):
                        # Apply Join
                        join_key_value = feature.get(request.join_column)
                        if join_key_value is not None:
                            supp_row = join_values.get(join_key_value)
                            if supp_row:
                                # Merge DWH data into attributes
                                attrs = feature.get("attributes", {})
                                if not isinstance(attrs, dict):
                                    attrs = {}
                                attrs.update(supp_row)
                                feature["attributes"] = attrs
                                
                                await queue.put(feature)
                        # If no join match, feature is skipped (Inner Join behavior)
                        
                except Exception as e:
                    logger.error(f"Producer failed: {e}", exc_info=True)
                    raise e
                finally:
                    await queue.put(None)

            # --- Consumer: Sync file writing and upload ---
            def blocking_export_logic(iterator):
                # 1. Get the byte stream
                byte_stream = get_features_as_byte_stream(
                    features=iterator,
                    output_format=request.output_format,
                    target_srid=request.destination_crs,
                    encoding=request.output_encoding
                )
                
                # 2. Upload to storage
                formatter = format_map.get(request.output_format)
                if formatter is None:
                    raise RuntimeError(f"No formatter registered for {request.output_format}")
                upload_stream_to_gcs(
                    byte_stream=byte_stream,
                    destination_uri=request.destination_uri,
                    content_type=formatter["media_type"]
                )
            
            # Start tasks
            producer_task = asyncio.create_task(producer())
            loop = asyncio.get_running_loop()
            sync_iterator = SyncQueueIterator(queue, loop)
            
            logger.info("Starting blocking DWH export logic in threadpool...")
            run_in_threadpool = get_concurrency_backend()
            await run_in_threadpool(blocking_export_logic, sync_iterator)
            await producer_task

            for r in reporters:
                await r.task_finished("SUCCESS")

        except Exception as e:
            logger.error(f"DwhJoinExportTask failed: {e}", exc_info=True)
            if 'reporters' in locals():
                for r in reporters:
                    await r.task_finished("FAILED", error_message=str(e))
            raise e

        return StatusInfo(
            jobID=task_id,
            status=TaskStatusEnum.COMPLETED, 
            message="Export completed", 
            links=[]
        )
