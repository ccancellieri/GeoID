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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

from dynastore.tools.process_factory import create_process_definition
from dynastore.tasks.ingestion.ingestion_models import IngestionProcessRequest
from dynastore.modules.processes.models import JobControlOptions, ProcessScope, TransmissionMode

# Define the "ingestion" process according to the OGC standard
INGESTION_PROCESS_DEFINITION = create_process_definition(
    id="ingestion",
    title="Bulk Data Ingestion",
    description="Ingests a geospatial file into a physical collection. This process creates a new versioned layer or appends to an existing one.",
    version="1.0.0",
    input_model=IngestionProcessRequest, # Use the combined request model
    scopes=[ProcessScope.COLLECTION],
    job_control_options=[JobControlOptions.ASYNC_EXECUTE, JobControlOptions.SYNC_EXECUTE],
    output_transmission=[TransmissionMode.REFERENCE]
)
