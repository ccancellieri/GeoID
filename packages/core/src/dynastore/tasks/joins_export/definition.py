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

"""Lightweight OGC Process definition for the OGC API - Joins export task.

Lives separately from ``joins_export_task.py`` (which imports BigQuery and
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

from .models import JoinsExportRequest

# title/description are plain str; Process.title/.description are typed
# CoercibleLocalizedText (accepts str at runtime via a BeforeValidator, but not
# statically as a `Process(title=...)` kwarg). model_validate() runs the same
# validators against an untyped mapping, same as the ProcessOutput below.
JOINS_EXPORT_PROCESS_DEFINITION = Process.model_validate({
    "id": "joins_export",
    "version": "1.0.0",
    "title": "OGC API - Joins Export",
    "description": (
        "Heavy, non-paginated counterpart to the synchronous /join endpoint. "
        "Joins a primary collection with a secondary source (a registered "
        "collection or an inline BigQuery target) over its full extent and "
        "exports the joined FeatureCollection to the catalog's Cloud Storage "
        "bucket. The artifact is returned by reference as a time-limited "
        "signed URL: the 'result' output of the job's results document (a "
        "link), and also the job's status message. The server owns the storage "
        "location. Use this for dense-geometry or whole-collection joins that a "
        "single synchronous page cannot carry."
    ),
    "scopes": [ProcessScope.COLLECTION],
    "inputs": pydantic_to_process_inputs(JoinsExportRequest),
    "outputs": {
        "result": ProcessOutput.model_validate(
            {
                "title": "Result",
                "description": (
                    "Time-limited (7-day) signed GET URL to the exported file "
                    "in Cloud Storage."
                ),
                "schema": {"type": "string", "format": "uri"},
            }
        )
    },
    "jobControlOptions": [JobControlOptions.ASYNC_EXECUTE],
    "outputTransmission": [TransmissionMode.REFERENCE],
})
