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

from dynastore.modules.processes.models import (
    JobControlOptions,
    ProcessScope,
    TransmissionMode,
)
from dynastore.tools.process_factory import create_process_definition

from .models import DimensionsMaterializeRequest

DIMENSIONS_MATERIALIZE_PROCESS_DEFINITION = create_process_definition(
    id="dimensions_materialize",
    title="Materialise OGC Dimensions as Records",
    description=(
        "Populate the `_dimensions_` catalog's RECORDS collections from the "
        "dimension providers registered by the dimensions extension. "
        "Idempotent per-dimension via a cube:dimensions equality check — "
        "unchanged dimensions are skipped. Intended to be invoked once per "
        "deploy (manually or via a post-deploy hook), replacing the "
        "lifespan-time materialisation that previously ran on every pod boot."
    ),
    version="1.0.0",
    input_model=DimensionsMaterializeRequest,
    scopes=[ProcessScope.PLATFORM],
    job_control_options=[JobControlOptions.ASYNC_EXECUTE],
    output_transmission=[TransmissionMode.VALUE],
)
