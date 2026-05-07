"""``gdal`` asset process — wraps the GDAL OGC Process for asset-scoped invocation.

Implements ``AssetProcessProtocol`` (duck-typed) so that the asset router at
``POST /assets/catalogs/{c}/[collections/{c}/]assets/{a}/processes/gdal/execution``
can find and execute it. Without this wrapper the underlying ``GdalInfoTask``
exists in the OGC Processes registry but the asset router returns
``Asset process 'gdal' is not registered.``

Delegates to ``processes_module.execute_process`` so the standard OGC pipeline
(input validation, mode selection, ExecutionEngine routing) is reused — no
duplicate dispatch code here.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import HTTPException, status

from dynastore.models.protocols.asset_process import (
    AssetProcessDescriptor,
    AssetProcessOutput,
    HTTPMethod,
)
from dynastore.modules.catalog.asset_service import Asset, AssetTypeEnum

logger = logging.getLogger(__name__)


class GdalAssetProcess:
    """Asset-scoped invocation surface for the GDAL info task.

    Applicable to RASTER and VECTORIAL assets when GDAL is available in the
    runtime. The execute path delegates to the OGC Processes layer so a single
    canonical execution code path runs the GdalInfoTask — both via this asset
    surface and via direct OGC ``/processes/gdal/execution`` calls.
    """

    process_id: str = "gdal"
    http_method: HTTPMethod = "POST"
    title: str = "GDAL Info"
    description: str = (
        "Calculates GDAL/OGR information (driver, bands, projection, extent) "
        "for a raster or vector asset and enriches its metadata."
    )

    async def describe(self, asset: Asset) -> AssetProcessDescriptor:
        from dynastore.modules.gdal import service as gdal_module

        applicable = (
            gdal_module.GDAL_AVAILABLE
            and asset.asset_type in (AssetTypeEnum.RASTER, AssetTypeEnum.VECTORIAL)
        )
        if not gdal_module.GDAL_AVAILABLE:
            reason = "GDAL/OGR runtime not available in this service."
        elif not applicable:
            reason = (
                f"GDAL info applies only to RASTER/VECTORIAL assets; "
                f"this asset is {asset.asset_type.value}."
            )
        else:
            reason = None
        return AssetProcessDescriptor(
            process_id=self.process_id,
            title=self.title,
            description=self.description,
            http_method=self.http_method,
            applicable=applicable,
            reason=reason,
            parameters_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": True,
                "description": (
                    "GDAL info takes no required parameters; the asset's URI "
                    "and metadata are derived from the asset row itself."
                ),
            },
        )

    async def execute(
        self, asset: Asset, params: Dict[str, Any]
    ) -> AssetProcessOutput:
        from dynastore.modules.gdal import service as gdal_module

        if not gdal_module.GDAL_AVAILABLE:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="GDAL/OGR runtime not available in this service.",
            )
        if asset.asset_type not in (AssetTypeEnum.RASTER, AssetTypeEnum.VECTORIAL):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"GDAL info not applicable to asset_type={asset.asset_type.value}."
                ),
            )

        # Delegate to the OGC Processes layer. ExecutionEngine picks the right
        # runner (in-process BackgroundRunner for sync, GcpJobRunner for the
        # Cloud Run dynastore-gdal-job). The GdalInfoTask itself uses the
        # AssetTasksSPI to inject the asset metadata into inputs, so we only
        # need to supply caller-provided extras here.
        from dynastore.modules.processes import processes_module
        from dynastore.modules.processes.models import (
            ExecuteRequest,
            JobControlOptions,
        )
        from dynastore.tools.protocol_helpers import get_engine

        engine = get_engine()
        if engine is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No database engine available for GDAL job dispatch.",
            )

        execution_request = ExecuteRequest(
            inputs={
                **params,
                "asset_id": asset.asset_id,
                "asset_uri": str(asset.uri) if asset.uri else None,
                "catalog_id": asset.catalog_id,
                "collection_id": asset.collection_id,
                "asset_type": asset.asset_type.value,
            },
            outputs={},
        )

        try:
            # ASYNC_EXECUTE so the dispatcher routes to GcpJobRunner →
            # spawns the deployed dynastore-gdal-job Cloud Run Job (which has
            # GDAL/OGR installed). SYNC_EXECUTE would route to BackgroundRunner
            # in-process; the catalog API service does NOT carry the
            # `module_gdal` SCOPE (osgeo only ships in worker_task_gdal), so
            # in-process execution returns 503 "GDAL/OGR runtime not available".
            # Confirmed against review env image :857 — POST returned 503 even
            # though the gdal Cloud Run Job was deployed and capable.
            result = await processes_module.execute_process(
                process_id="gdal",
                execution_request=execution_request,
                engine=engine,
                preferred_mode=JobControlOptions.ASYNC_EXECUTE,
                catalog_id=asset.catalog_id,
                collection_id=asset.collection_id,
            )
        except Exception as exc:
            logger.exception(
                "GdalAssetProcess.execute: dispatch failed for "
                "asset=%s/%s/%s: %s",
                asset.catalog_id,
                asset.collection_id,
                asset.asset_id,
                exc,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"GDAL info dispatch failed: {exc}",
            ) from exc

        # Async dispatch returns a job handle (StatusInfo); expose as `job`
        # so the asset-process REST surface advertises the polling shape
        # rather than pretending the result is inline.
        if hasattr(result, "jobID") or (isinstance(result, dict) and result.get("jobID")):
            job_id = (
                getattr(result, "jobID", None)
                or (result.get("jobID") if isinstance(result, dict) else None)
            )
            return AssetProcessOutput(type="job", job_id=str(job_id))

        # Sync fallback (defensive): keep the inline shape for cases where
        # ExecutionEngine completed synchronously despite the async preference.
        # `result` is `Any` from execute_process(); use getattr-then-call so
        # Pyright doesn't carry a stale dict-narrowing from the if-branch
        # above into this otherwise-unrelated fallback.
        model_dump = getattr(result, "model_dump", None)
        return AssetProcessOutput(
            type="inline",
            data=model_dump(mode="json") if callable(model_dump) else result,
        )
