import logging
import asyncio
from datetime import timedelta
from typing import Optional

# Hard runtime dep — see modules/elasticsearch/module.py for rationale.
# Forces entry-point load to fail on services without ``google-cloud-bigquery``
# (transitively required by the ``dwh`` extra → ``joins``) so the
# CapabilityMap doesn't list this task as claimable on services lacking it.
import google.cloud.bigquery  # noqa: F401

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum
from dynastore.tools.protocol_helpers import get_engine
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import (
    StorageProtocol,
    CloudStorageClientProtocol,
    CloudIdentityProtocol,
)
from dynastore.modules.processes.models import (
    Process,
    ExecuteRequest,
    StatusInfo,
)
from dynastore.tasks.tools import initialize_reporters
from dynastore.tools.async_utils import SyncQueueIterator
from dynastore.tools.file_io import get_features_as_byte_stream
from dynastore.tools.enrichment import enrich_features
from dynastore.modules.gcp.tools.bucket import upload_stream_to_gcs
from dynastore.modules.gcp.tools.signed_urls import generate_gcs_signed_url
from dynastore.modules.tools.features import FeatureStreamConfig, stream_features
from dynastore.modules.tools.field_categories import resolve_category_field_names
from dynastore.modules.storage.hints import Hint
from dynastore.extensions.dwh.dwh import execute_bigquery_async
from dynastore.modules.concurrency import get_concurrency_backend
from dynastore.extensions.tools.formatters import format_map

from .definition import DWH_JOIN_EXPORT_PROCESS_DEFINITION
from .models import DwhJoinExportRequest

logger = logging.getLogger(__name__)

# Signed result URLs are valid for one week (OGC results are retrieved
# out-of-band; a week comfortably covers download + retry windows).
_SIGNED_URL_TTL = timedelta(days=7)


async def _resolve_output_uri(catalog_id: str, process_id: str, job_id: str, filename: str) -> str:
    """Server-derived GCS URI for a process artifact.

    Per OGC API - Processes the server owns result storage, so the output
    lands in the catalog's own bucket under a deterministic, per-job key:
    ``processes/outputs/{process_id}/{job_id}/{filename}``.
    """
    storage = get_protocol(StorageProtocol)
    if storage is None:
        raise RuntimeError(
            "StorageProtocol is not available; cannot resolve the output bucket "
            "for the DWH join export."
        )
    bucket = await storage.get_storage_identifier(catalog_id)
    if not bucket:
        raise RuntimeError(
            f"No storage bucket is provisioned for catalog '{catalog_id}'; "
            "cannot write the DWH join export."
        )
    return f"gs://{bucket}/processes/outputs/{process_id}/{job_id}/{filename}"


async def _sign_output_uri(gs_uri: str, content_type: Optional[str]) -> str:
    """Best-effort 7-day GET signed URL for ``gs_uri``.

    Reuses the shared ``generate_gcs_signed_url`` helper (V4, IAM signing on
    Cloud Run via ``CloudIdentityProtocol``). Falls back to the raw ``gs://``
    URI if signing is unavailable so the job still reports a usable location
    rather than failing after a successful upload.
    """
    client_provider = get_protocol(CloudStorageClientProtocol)
    if client_provider is None:
        logger.warning("CloudStorageClientProtocol unavailable; returning gs:// URI unsigned.")
        return gs_uri
    try:
        signed = await generate_gcs_signed_url(
            gs_uri,
            method="GET",
            expiration=_SIGNED_URL_TTL,
            client_provider=client_provider,
            identity_provider=get_protocol(CloudIdentityProtocol),
            content_type=content_type,
            check_exists=True,
        )
    except Exception as e:  # signing must never sink a successful export
        logger.warning("Failed to sign output URI %s: %s; returning gs:// URI.", gs_uri, e)
        return gs_uri
    return signed or gs_uri


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

            # Server-owned result storage (OGC API - Processes): resolve the
            # catalog bucket and a deterministic per-job key. The output is
            # never client-addressed.
            formatter = format_map.get(request.output_format)
            if formatter is None:
                raise RuntimeError(f"No formatter registered for {request.output_format}")
            content_type = formatter["media_type"]
            extension = formatter.get("extension") or request.output_format.value
            filename = f"{request.collection}.{extension}"
            output_uri = await _resolve_output_uri(
                request.catalog,
                DWH_JOIN_EXPORT_PROCESS_DEFINITION.id,
                str(task_id),
                filename,
            )

            reporters = initialize_reporters(
                engine=engine,
                task_id=str(task_id),
                task_request=request,
                reporting_config=request.reporting
            )

            for r in reporters:
                await r.task_started(str(task_id), request.collection, request.catalog, output_uri)

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

                    # Resolve field names from the three storage-aware categories
                    try:
                        field_names = await resolve_category_field_names(
                            request.catalog,
                            request.collection,
                            properties=request.properties,
                            stats=request.stats,
                            system=request.system,
                            join_column=request.join_column,
                        )
                    except ValueError as e:
                        raise RuntimeError(
                            f"Invalid field selection for DWH export "
                            f"{request.catalog}/{request.collection}: {e}"
                        ) from e

                    # Configure base feature stream
                    stream_config = FeatureStreamConfig(
                        catalog=request.catalog,
                        collection=request.collection,
                        cql_filter=request.where,
                        property_names=field_names or None,
                        limit=request.limit,
                        offset=request.offset,
                        include_geometry=request.with_geometry,
                        target_srid=request.destination_crs
                    )
                    
                    # Stream features and join with DWH data via the shared
                    # ``enrich_features`` helper — the exact streaming O(1) merge
                    # the synchronous /dwh/join endpoint uses (dwh.py). The base
                    # stream yields ``Feature`` objects, not dicts, so the merge
                    # must read ``feature.properties`` rather than ``feature.get``;
                    # enrich_features merges DWH columns into ``feature.properties``
                    # and, with ``inner_join=True``, drops features lacking a DWH
                    # match (the inner-join semantics this task expects).
                    #
                    # Hint.JOIN forces the full-precision PG read path (ES carries
                    # only simplified geometry and cannot project ST_Transform),
                    # matching the synchronous endpoint.
                    feature_stream = stream_features(
                        stream_config, engine, hints=frozenset({Hint.JOIN})
                    )
                    async for feature in enrich_features(
                        feature_stream,
                        join_values,
                        join_column=request.join_column,
                        inner_join=True,
                    ):
                        await queue.put(feature)

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

                # 2. Upload to the server-derived result location.
                upload_stream_to_gcs(
                    byte_stream=byte_stream,
                    destination_uri=output_uri,
                    content_type=content_type,
                )

            # Start tasks
            producer_task = asyncio.create_task(producer())
            loop = asyncio.get_running_loop()
            sync_iterator = SyncQueueIterator(queue, loop)

            logger.info("Starting blocking DWH export logic in threadpool...")
            run_in_threadpool = get_concurrency_backend()
            await run_in_threadpool(blocking_export_logic, sync_iterator)
            await producer_task

            # Surface the artifact as a time-limited signed URL (the standard
            # OGC way of returning a file output by reference).
            result_url = await _sign_output_uri(output_uri, content_type)

            for r in reporters:
                await r.task_finished(TaskStatusEnum.COMPLETED.value)

        except Exception as e:
            logger.error(f"DwhJoinExportTask failed: {e}", exc_info=True)
            if 'reporters' in locals():
                for r in reporters:
                    await r.task_finished(TaskStatusEnum.FAILED.value, error_message=str(e))
            raise e

        return StatusInfo(
            jobID=task_id,
            status=TaskStatusEnum.COMPLETED,
            message=result_url,
            links=[],
        )
