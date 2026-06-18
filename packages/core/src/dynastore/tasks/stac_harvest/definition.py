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

"""OGC Process definition for the ``stac_harvest`` process."""

from dynastore.tools.process_factory import create_process_definition
from dynastore.tasks.stac_harvest.models import StacHarvestRequest
from dynastore.modules.processes.models import (
    JobControlOptions,
    ProcessScope,
    TransmissionMode,
)

STAC_HARVEST_PROCESS_DEFINITION = create_process_definition(
    id="stac_harvest",
    title="STAC Catalog Harvest",
    description=(
        "Walks a remote STAC catalog (collections + items via rel=next cursor "
        "pagination), maps each collection to a local dynastore collection, "
        "and bulk-upserts items.  Upserts are idempotent — re-running the "
        "process updates items in place.  Optionally registers item asset "
        "hrefs as virtual assets."
    ),
    version="1.0.0",
    input_model=StacHarvestRequest,
    scopes=[ProcessScope.CATALOG, ProcessScope.COLLECTION],
    job_control_options=[
        JobControlOptions.ASYNC_EXECUTE,
        JobControlOptions.SYNC_EXECUTE,
    ],
    output_transmission=[TransmissionMode.REFERENCE],
)
