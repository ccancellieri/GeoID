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
    ProcessInput,
    ProcessOutput,
    ProcessScope,
)

GDALINFO_PROCESS_DEFINITION = Process(
    id="gdal",
    title="GDAL Info Task",
    description=(
        "Calculates GDAL/OGR information for an asset and enriches its metadata. "
        "Targets a single asset identified by ``asset_id`` in ``inputs``. Run it "
        "at the catalog mount for a catalog-level asset, or the collection mount "
        "for a collection-level asset; ``catalog_id`` (and ``collection_id`` at "
        "the collection mount) are taken from the URL path. The task resolves the "
        "asset's URI itself — callers do not supply it."
    ),
    version="1.0.0",
    scopes=[ProcessScope.CATALOG, ProcessScope.COLLECTION],
    # Async-first: with no client preference, gdal offloads to a Cloud Run job
    # (the default everywhere). Sync execution is opt-in via ``Prefer:
    # respond-sync`` and is only honoured on a service that carries a sync
    # runner able to handle gdal in-process (e.g. maps, which ships osgeo).
    # Order matters: _resolve_execution_mode picks the first declared mode with
    # a capable runner when the caller expresses no preference.
    jobControlOptions=[
        JobControlOptions.ASYNC_EXECUTE,
        JobControlOptions.SYNC_EXECUTE,
    ],
    inputs={
        "asset_id": ProcessInput(
            title="Asset ID",
            description="Identifier of the asset to inspect (required).",
            schema={"type": "string"},
        ),
        "catalog_id": ProcessInput(
            title="Catalog ID",
            description=(
                "Catalog owning the asset (required). Taken from the URL path."
            ),
            schema={"type": "string"},
        ),
        "collection_id": ProcessInput(
            title="Collection ID",
            description=(
                "Collection owning the asset, when collection-level. Taken from "
                "the URL path at the collection mount."
            ),
            schema={"type": ["string", "null"]},
        ),
        "asset_metadata": ProcessInput(
            title="Asset Metadata Override",
            description=(
                "Optional metadata patch merged onto the asset alongside the "
                "computed ``gdalinfo`` block."
            ),
            schema={"type": "object", "additionalProperties": True},
        ),
    },
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
