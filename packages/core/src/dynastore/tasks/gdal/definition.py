#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Lightweight OGC Process definition for the GDAL info task.

Lives separately from ``gdalinfo_task.py`` so services that don't have GDAL
installed (e.g. the catalog when it only dispatches GDAL work to a Cloud Run
worker) can still surface the Process in ``/processes`` via the tasks'
definition-only fallback loader.
"""

from dynastore.modules.gdal.models import RasterInfo, VectorInfo
from dynastore.modules.processes.models import (
    JobControlOptions,
    Process,
    ProcessOutput,
    ProcessScope,
)

GDALINFO_PROCESS_DEFINITION = Process(
    id="gdal",
    title="GDAL Info Task",
    description=(
        "Calculates GDAL/OGR information for an asset and enriches its metadata."
    ),
    version="1.0.0",
    scopes=[ProcessScope.ASSET],
    jobControlOptions=[
        JobControlOptions.SYNC_EXECUTE,
        JobControlOptions.ASYNC_EXECUTE,
    ],
    inputs={},
    outputs={
        "info": ProcessOutput(
            title="Result Info",
            description="The calculated GDAL/OGR information.",
            schema={
                "oneOf": [
                    RasterInfo.model_json_schema(),
                    VectorInfo.model_json_schema(),
                ]
            },
        )
    },
)
