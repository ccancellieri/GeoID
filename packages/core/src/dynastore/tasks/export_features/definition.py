#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Lightweight OGC Process definition for the export_features task.

Kept free of heavy imports (GCS clients, feature streaming utilities) so that
catalog services can surface the Process in ``/processes`` without installing
worker-side dependencies.
"""

from dynastore.modules.processes.models import (
    JobControlOptions,
    Process,
    ProcessOutput,
    ProcessScope,
    TransmissionMode,
)
from dynastore.modules.processes.schema_gen import pydantic_to_process_inputs

from dynastore.modules.features_exporter.models import ExportFeaturesRequest

EXPORT_FEATURES_PROCESS_DEFINITION = Process(
    id="export_features",
    version="1.0.0",
    title="Export Features",
    description=(
        "Exports features from a collection to a file in Cloud Storage with "
        "optional CQL filtering and property projection."
    ),
    scopes=[ProcessScope.COLLECTION],
    inputs=pydantic_to_process_inputs(ExportFeaturesRequest),
    outputs={
        "result": ProcessOutput.model_validate(
            {"title": "Result", "schema": {"type": "object"}}
        )
    },
    jobControlOptions=[JobControlOptions.ASYNC_EXECUTE],
    outputTransmission=[TransmissionMode.VALUE],
)
