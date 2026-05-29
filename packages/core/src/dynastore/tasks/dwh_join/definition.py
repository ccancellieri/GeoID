#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Lightweight OGC Process definition for the DWH join export task.

Lives separately from ``dwh_join_export_task.py`` (which imports BigQuery and
other heavy SDKs) so services that only dispatch the work can still expose the
Process via ``/processes``.
"""

from dynastore.modules.processes.models import (
    JobControlOptions,
    Process,
    ProcessOutput,
    ProcessScope,
    TransmissionMode,
)
from dynastore.modules.processes.schema_gen import pydantic_to_process_inputs

from .models import DwhJoinExportRequest

DWH_JOIN_EXPORT_PROCESS_DEFINITION = Process(
    id="dwh_join",
    version="1.0.0",
    title="DWH Join Export",
    description=(
        "Joins catalog features with DWH query results and exports the result to "
        "the catalog's Cloud Storage bucket. The output is returned by reference "
        "as a time-limited signed URL (the job's message / results document); the "
        "server owns the storage location."
    ),
    scopes=[ProcessScope.COLLECTION],
    inputs=pydantic_to_process_inputs(DwhJoinExportRequest),
    outputs={
        "result": ProcessOutput.model_validate(
            {
                "title": "Result",
                "description": (
                    "Time-limited (7-day) signed GET URL to the exported file in "
                    "Cloud Storage."
                ),
                "schema": {"type": "string", "format": "uri"},
            }
        )
    },
    jobControlOptions=[JobControlOptions.ASYNC_EXECUTE],
    outputTransmission=[TransmissionMode.REFERENCE],
)
