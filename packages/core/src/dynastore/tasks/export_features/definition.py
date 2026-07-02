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

# title/description are plain str; Process.title/.description are typed
# CoercibleLocalizedText (accepts str at runtime via a BeforeValidator, but not
# statically as a `Process(title=...)` kwarg). model_validate() runs the same
# validators against an untyped mapping, same as the ProcessOutput below.
EXPORT_FEATURES_PROCESS_DEFINITION = Process.model_validate({
    "id": "export_features",
    "version": "1.0.0",
    "title": "Export Features",
    "description": (
        "Exports features from a collection to a file in server-owned Cloud "
        "Storage with optional CQL filtering and property projection. The "
        "artifact is returned as a time-limited signed URL in the job message."
    ),
    "scopes": [ProcessScope.COLLECTION],
    "inputs": pydantic_to_process_inputs(ExportFeaturesRequest),
    "outputs": {
        "result": ProcessOutput.model_validate(
            {"title": "Result", "schema": {"type": "string", "format": "uri"}}
        )
    },
    "jobControlOptions": [JobControlOptions.ASYNC_EXECUTE],
    "outputTransmission": [TransmissionMode.REFERENCE],
})
