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

import asyncio
import logging
from typing import Any, Dict, Optional

# Hard runtime dep — forces entry-point load to fail on services without
# ``google-cloud-bigquery`` (required by the BigQuery secondary path) so the
# CapabilityMap doesn't list this task as claimable on services lacking it.
import google.cloud.bigquery  # noqa: F401

from dynastore.extensions.tools.formatters import format_map
from dynastore.models.shared_models import OutputFormatEnum
from dynastore.modules.concurrency import get_concurrency_backend
from dynastore.modules.gcp.tools.bucket import upload_stream_to_gcs
from dynastore.modules.joins.bq_secondary import stream_bigquery_secondary
from dynastore.modules.joins.executor import index_secondary, run_join
from dynastore.modules.joins.models import (
    BigQuerySecondarySpec,
    NamedSecondarySpec,
)
from dynastore.modules.processes.models import ExecuteRequest, Process
from dynastore.modules.storage.hints import Hint
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum
from dynastore.modules.tools.features import FeatureStreamConfig, stream_features
from dynastore.tasks import result_message
from dynastore.tasks.protocols import TaskProtocol
from dynastore.tasks.tools import initialize_reporters
from dynastore.tools.async_utils import SyncQueueIterator
from dynastore.tools.file_io import get_features_as_byte_stream
from dynastore.tools.protocol_helpers import get_engine

from .definition import JOINS_EXPORT_PROCESS_DEFINITION
from .models import JoinsExportRequest

logger = logging.getLogger(__name__)

# ``OutputSpec.format`` is a human-facing string Literal; ``format_map`` is
# keyed by OutputFormatEnum (whose values differ, e.g. "geopackage" -> "gpkg").
_FORMAT_TO_ENUM = {
    "geojson": OutputFormatEnum.GEOJSON,
    "json": OutputFormatEnum.JSON,
    "csv": OutputFormatEnum.CSV,
    "geopackage": OutputFormatEnum.GEOPACKAGE,
    "parquet": OutputFormatEnum.PARQUET,
}


class JoinsExportTask(
    TaskProtocol[Process, TaskPayload[ExecuteRequest], Optional[Dict[str, Any]]]
):
    priority: int = 100

    @staticmethod
    def get_definition() -> Process:
        return JOINS_EXPORT_PROCESS_DEFINITION

    def __init__(self, app_state: object):
        self.app_state = app_state

    async def run(
        self, payload: TaskPayload[ExecuteRequest]
    ) -> Optional[Dict[str, Any]]:
        task_id = payload.task_id
        engine = get_engine()
        if engine is None:
            raise RuntimeError("No database engine available.")

        try:
            inputs = payload.inputs.inputs
            request = JoinsExportRequest(**inputs)

            logger.info(
                f"Starting JoinsExportTask {task_id} for "
                f"{request.catalog}:{request.collection}"
            )

            # Server-owned result storage (OGC API - Processes): resolve the
            # catalog bucket and a deterministic per-job key. The output is
            # never client-addressed.
            output_format = _FORMAT_TO_ENUM.get(request.output.format)
            if output_format is None:
                raise RuntimeError(
                    f"Unsupported output format {request.output.format!r}"
                )
            formatter = format_map.get(output_format)
            if formatter is None:
                raise RuntimeError(f"No formatter registered for {output_format}")
            content_type = formatter["media_type"]
            extension = formatter.get("extension") or output_format.value
            filename = f"{request.collection}.{extension}"
            output_uri = await result_message.server_output_uri(
                request.catalog,
                JOINS_EXPORT_PROCESS_DEFINITION.id,
                str(task_id),
                filename,
            )

            reporters = initialize_reporters(
                engine=engine,
                task_id=str(task_id),
                task_request=request,
                reporting_config=request.reporting,
            )
            for r in reporters:
                await r.task_started(
                    str(task_id), request.collection, request.catalog, output_uri
                )

            # --- Producer: build the secondary index, then stream the joined
            # primary features. Mirrors the synchronous /join executor but with
            # no paging (the full joined set is materialized) and the
            # full-precision PG read path (Hint.JOIN) for dense geometries.
            queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

            async def producer():
                try:
                    secondary = request.secondary
                    if isinstance(secondary, BigQuerySecondarySpec):
                        secondary_index = await index_secondary(
                            stream_bigquery_secondary(
                                secondary,
                                secondary_column=request.join.secondary_column,
                            ),
                            secondary_column=request.join.secondary_column,
                        )
                    elif isinstance(secondary, NamedSecondarySpec):
                        secondary_index = await index_secondary(
                            stream_features(
                                # Every field is passed explicitly (matching
                                # the FeatureStreamConfig.__init__ default for
                                # each) — see the primary_stream construction
                                # below for why: pyright doesn't resolve the
                                # `Field(None, ...)` defaults on this model.
                                FeatureStreamConfig(
                                    catalog=request.catalog,
                                    collection=secondary.ref,
                                    cql_filter=None,
                                    property_names=None,
                                    limit=None,
                                    offset=None,
                                    include_geometry=True,
                                    target_srid=None,
                                ),
                                engine,
                            ),
                            secondary_column=request.join.secondary_column,
                        )
                    else:
                        raise RuntimeError(
                            f"Unsupported secondary spec: "
                            f"{type(secondary).__name__}"
                        )

                    # Always carry the join key on the primary stream so
                    # run_join can resolve a match even when the caller
                    # projected a narrow attribute subset.
                    property_names = None
                    if request.projection.attributes:
                        property_names = list(
                            dict.fromkeys(
                                [
                                    *request.projection.attributes,
                                    request.join.primary_column,
                                ]
                            )
                        )

                    primary_stream = stream_features(
                        FeatureStreamConfig(
                            catalog=request.catalog,
                            collection=request.collection,
                            cql_filter=(
                                request.primary_filter.cql
                                if request.primary_filter
                                else None
                            ),
                            property_names=property_names,
                            # paging is None on JoinsExportRequest — run_join
                            # yields every matched feature (no offset/limit
                            # bound); passed explicitly since pyright doesn't
                            # resolve this model's `Field(None, ...)` defaults.
                            limit=None,
                            offset=None,
                            include_geometry=request.projection.with_geometry,
                            target_srid=request.projection.destination_crs,
                        ),
                        engine,
                        hints=frozenset({Hint.JOIN}),
                    )

                    # paging is None on JoinsExportRequest → run_join yields
                    # every matched feature (no offset/limit bound).
                    async for feature in run_join(
                        request,
                        primary_stream=primary_stream,
                        secondary_index=secondary_index,
                    ):
                        await queue.put(feature)

                except Exception as e:
                    logger.error(f"Producer failed: {e}", exc_info=True)
                    raise e
                finally:
                    await queue.put(None)

            # --- Consumer: sync file writing and upload.
            def blocking_export_logic(iterator):
                byte_stream = get_features_as_byte_stream(
                    features=iterator,
                    output_format=output_format,
                    target_srid=request.projection.destination_crs,
                    encoding=request.output.encoding,
                )
                upload_stream_to_gcs(
                    byte_stream=byte_stream,
                    destination_uri=output_uri,
                    content_type=content_type,
                )

            producer_task = asyncio.create_task(producer())
            loop = asyncio.get_running_loop()
            sync_iterator = SyncQueueIterator(queue, loop)

            logger.info("Starting blocking joins export logic in threadpool...")
            run_in_threadpool = get_concurrency_backend()
            await run_in_threadpool(blocking_export_logic, sync_iterator)
            await producer_task

            # Surface the artifact as a time-limited signed URL (the standard
            # OGC way of returning a file output by reference).
            result_url = await result_message.signed_result_url(output_uri)

            for r in reporters:
                await r.task_finished(TaskStatusEnum.COMPLETED.value)

        except Exception as e:
            logger.error(f"JoinsExportTask failed: {e}", exc_info=True)
            if "reporters" in locals():
                for r in reporters:
                    await r.task_finished(
                        TaskStatusEnum.FAILED.value, error_message=str(e)
                    )
            raise e

        # Return the artifact as the declared ``result`` output, by reference:
        # a Link-shaped qualified value ({href, type}) so GET /jobs/{id}/results
        # is a conformant OGC API - Processes results document (Part 1 §7.13).
        # The signed URL is also carried as the status ``message``.
        return result_message.reference_result(
            "result",
            result_url,
            media_type=content_type,
        )
