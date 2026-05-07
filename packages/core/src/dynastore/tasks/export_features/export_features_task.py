#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Thin task wrapper around ``modules.features_exporter.service.export_features``.

The heavy lifting (feature streaming, byte-stream formatting, GCS upload)
lives in ``dynastore.modules.features_exporter`` so the same pipeline can be
called from a sync REST endpoint, an async in-process background task, or a
Cloud Run Job worker — only the request envelope and reporter plumbing differ.
"""

import logging
from typing import Optional

# Hard runtime dep — see modules/elasticsearch/module.py for rationale.
# Forces entry-point load to fail on services without ``shapely`` (transitively
# required by the ``geospatial_core`` extra in worker_task_export_features) so
# the CapabilityMap doesn't list this task as claimable on services lacking it.
import shapely  # noqa: F401

from dynastore.modules.features_exporter import export_features
from dynastore.modules.processes.models import ExecuteRequest, Process, StatusInfo
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum
from dynastore.tasks.protocols import TaskProtocol
from dynastore.tasks.tools import initialize_reporters
from dynastore.tools.protocol_helpers import get_engine

from .definition import EXPORT_FEATURES_PROCESS_DEFINITION
from .models import ExportFeaturesRequest

logger = logging.getLogger(__name__)


class ExportFeaturesTask(TaskProtocol[Process, TaskPayload[ExecuteRequest], Optional[StatusInfo]]):
    priority: int = 100

    @staticmethod
    def get_definition() -> Process:
        return EXPORT_FEATURES_PROCESS_DEFINITION

    def __init__(self, app_state: object):
        self.app_state = app_state

    async def run(self, payload: TaskPayload[ExecuteRequest]) -> Optional[StatusInfo]:
        task_id = payload.task_id
        engine = get_engine()
        if engine is None:
            raise RuntimeError("No database engine available.")

        inputs = dict(payload.inputs.inputs)
        # OGC dispatcher injects `catalog_id` / `collection_id` from the URL
        # path, but ExportFeaturesRequest's field names are `catalog` /
        # `collection` (kept compatible with sync REST callers that already
        # use the short names). Bridge the convention here so the model
        # validates without breaking either caller.
        if "catalog" not in inputs and "catalog_id" in inputs:
            inputs["catalog"] = inputs.pop("catalog_id")
        if "collection" not in inputs and "collection_id" in inputs:
            inputs["collection"] = inputs.pop("collection_id")
        request = ExportFeaturesRequest(**inputs)

        reporters = initialize_reporters(
            engine=engine,
            task_id=str(task_id),
            task_request=request,
            reporting_config=request.reporting,
        )

        await export_features(
            engine,
            request,
            reporters=reporters,
            task_id=str(task_id),
        )

        return StatusInfo(
            jobID=task_id,
            status=TaskStatusEnum.COMPLETED,
            message="Export completed",
            links=[],
        )
