#    Copyright 2025 FAO
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

from dynastore.tools.process_factory import create_process_definition
from dynastore.modules.processes.models import JobControlOptions, ProcessScope, TransmissionMode
from .models import TilesExportRequest

TILES_EXPORT_PROCESS_DEFINITION = create_process_definition(
    id="tiles_export",
    title="Tiles PMTiles Export",
    description=(
        "Exports vector tiles for a collection as a single PMTiles v3 archive. "
        "The archive is stored per-catalog and retrievable via the export ID "
        "returned in the job outputs."
    ),
    version="1.0.0",
    input_model=TilesExportRequest,
    scopes=[ProcessScope.COLLECTION],
    job_control_options=[JobControlOptions.ASYNC_EXECUTE],
    output_transmission=[TransmissionMode.REFERENCE],
)
