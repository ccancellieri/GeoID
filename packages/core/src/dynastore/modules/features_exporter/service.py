#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Core feature-exporter pipeline: stream → format → upload.

Single source of truth used by:
  - ``tasks.export_features`` (async background + Cloud Run Job wrapper)
  - sync REST adapters (when total estimated size is small enough to stream
    back to the caller inline)
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional, Sequence

from dynastore.extensions.tools.formatters import format_map
from dynastore.modules.concurrency import get_concurrency_backend
from dynastore.modules.gcp.tools.bucket import upload_stream_to_gcs
from dynastore.modules.tools.features import FeatureStreamConfig, stream_features
from dynastore.tools.async_utils import SyncQueueIterator
from dynastore.tools.file_io import get_features_as_byte_stream

from .models import ExportFeaturesRequest

logger = logging.getLogger(__name__)


# A reporter is any object implementing ``task_started`` / ``task_finished``.
# Kept structural to avoid a hard dependency on the tasks subsystem when the
# export runs synchronously from a REST route.
Reporter = Any


async def export_features(
    engine: Any,
    request: ExportFeaturesRequest,
    *,
    reporters: Optional[Sequence[Reporter]] = None,
    task_id: Optional[str] = None,
    on_started: Optional[Callable[[], Awaitable[None]]] = None,
) -> None:
    """Stream ``request.catalog/collection`` features to ``request.destination_uri``.

    ``reporters`` is optional — callers that don't track task state (e.g. a
    sync REST route) simply omit it. ``task_id`` is propagated to reporters
    when present.
    """
    reporters = reporters or ()

    logger.info(
        f"Exporting features {request.catalog}:{request.collection} "
        f"→ {request.destination_uri}"
    )

    for r in reporters:
        await r.task_started(
            task_id or "",
            request.collection,
            request.catalog,
            request.destination_uri,
        )
    if on_started is not None:
        await on_started()

    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

    async def producer() -> None:
        try:
            stream_config = FeatureStreamConfig(
                catalog=request.catalog,
                collection=request.collection,
                cql_filter=request.cql_filter,
                property_names=request.property_names,
                limit=request.limit,
                offset=request.offset,
                include_geometry=True,
                target_srid=request.target_srid,
            )
            async for feature in stream_features(stream_config, engine):
                await queue.put(feature)
        except Exception:
            logger.exception("Feature export producer failed")
            raise
        finally:
            await queue.put(None)

    def consume(iterator: Any) -> None:
        byte_stream = get_features_as_byte_stream(
            features=iterator,
            output_format=request.output_format,
            target_srid=request.target_srid or 4326,
            encoding=request.encoding,
        )
        formatter = format_map.get(request.output_format)
        if formatter is None:
            raise RuntimeError(f"No formatter registered for {request.output_format}")
        upload_stream_to_gcs(
            byte_stream=byte_stream,
            destination_uri=request.destination_uri,
            content_type=formatter["media_type"],
        )

    producer_task = asyncio.create_task(producer())
    loop = asyncio.get_running_loop()
    sync_iterator = SyncQueueIterator(queue, loop)

    try:
        run_in_threadpool = get_concurrency_backend()
        await run_in_threadpool(consume, sync_iterator)
        await producer_task
    except Exception as e:
        for r in reporters:
            await r.task_finished("FAILED", error_message=str(e))
        raise

    for r in reporters:
        await r.task_finished("SUCCESS")
