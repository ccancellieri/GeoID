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

import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, Tuple, Union

from fastapi import (
    APIRouter, Depends, HTTPException, Query, Request, Response, status, FastAPI
)

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.models.protocols import CloudStorageClientProtocol
from dynastore.modules import get_protocol
from dynastore.extensions.httpx.httpx_service import get_client
from dynastore.modules.gdal.models import RasterInfo, VectorInfo
from dynastore.modules.gdal.gdal_module import GdalModule
from dynastore.modules.gdal.service import FileType, get_raster_info, get_vector_info

logger = logging.getLogger(__name__)
class GdalService(ExtensionProtocol):
    priority: int = 100
    router:APIRouter = APIRouter(prefix="/gdal", tags=["Gdal Utilities"])

    def __init__(self, app: FastAPI):
        """
        Initializes the GdalService extension.
        """
        pass

    @staticmethod
    async def _get_file_type(file_url: str, mimetype: Optional[str] = None) -> FileType:
        """
        Determines the file type (raster/vector) by inspecting MIME type and extension.
        """
        # Priority 1: Use user-provided mimetype.
        if mimetype:
            ftype = GdalModule.get_file_type_by_mime(mimetype)
            if ftype != FileType.UNKNOWN:
                return ftype

        # Priority 2: Fetch MIME type from the source.
        # Lazy import: keeps the gdal extension entry-point load-clean when SCOPE
        # excludes module_gcp (the discovery contract — see tools/discovery.py).
        from dynastore.modules.gcp.tools import bucket as bucket_tool
        from dynastore.modules.gcp.tools.bucket import FileSystem
        parsed_url = bucket_tool.parse_url(file_url)
        fetched_mime_type: Optional[str] = None
        if parsed_url.scheme == FileSystem.gs and parsed_url.bucket:
            storage_client_provider = get_protocol(CloudStorageClientProtocol)
            if storage_client_provider:
                try:
                    storage_client = storage_client_provider.get_storage_client()
                    blob = await asyncio.to_thread(storage_client.bucket(parsed_url.bucket).get_blob, parsed_url.path)
                    fetched_mime_type = blob.content_type if blob else None
                except Exception as e:
                    logger.warning(f"Could not fetch GCS metadata for {file_url}: {e}")
        elif parsed_url.scheme in [FileSystem.https, FileSystem.http]:
            from dynastore.models.protocols import HttpxProtocol
            httpx_protocol = get_protocol(HttpxProtocol)
            if httpx_protocol:
                try:
                    async with httpx_protocol.get_httpx_client() as client:
                        resp = await client.head(file_url, follow_redirects=True)
                        resp.raise_for_status()
                        fetched_mime_type = resp.headers.get('content-type')
                except Exception as e:
                    logger.warning(f"Could not fetch HEAD for {file_url}: {e}")
        
        if fetched_mime_type:
            ftype = GdalModule.get_file_type_by_mime(fetched_mime_type)
            if ftype != FileType.UNKNOWN:
                return ftype

        # Priority 3: Fallback to file extension.
        return GdalModule.get_file_type_by_ext(file_url)

    @staticmethod
    async def _get_gdal_path_and_type(file_url: str, gdal_uri: Optional[str] = None, mimetype: Optional[str] = None) -> Tuple[str, FileType]:
        """
        Intelligently determines the file type (raster/vector) and constructs the
        appropriate GDAL virtual filesystem path.
        """
        file_type = await GdalService._get_file_type(file_url, mimetype)
        if gdal_uri:
            return gdal_uri, file_type

        from dynastore.modules.gcp.tools import bucket as bucket_tool
        gdal_path = bucket_tool.get_gdal_path(file_url)
        return gdal_path, file_type

    @staticmethod
    def _execute_info_call(func, file_path: str):
        try:
            return func(file_path)
        except RuntimeError as e:
            error_str = str(e).lower()
            if "http response code: 403" in error_str:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Access denied: {e}")
            if "http response code: 404" in error_str or "does not exist" in error_str:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Not found: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
        except Exception as e:
            logger.error(f"Error processing '{file_path}': {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    @router.get("/raster/info", response_model=RasterInfo, summary="Get metadata for a raster file.")
    async def raster_info(
        file_url: str = Query(..., description="URL of the raster file (gs:// or https://)."),  # type: ignore[reportGeneralTypeIssues]
        gdal_uri: Optional[str] = Query(None, description="Direct GDAL-compatible URI."),
        mimetype: Optional[str] = Query(None, description="MIME type of the file.")
    ):
        gdal_path, file_type = await GdalService._get_gdal_path_and_type(file_url, gdal_uri, mimetype)
        if file_type == FileType.VECTOR:
            raise HTTPException(status_code=400, detail="File is VECTOR, expected RASTER.")

        info_dict = await asyncio.to_thread(GdalService._execute_info_call, get_raster_info, gdal_path)
        return RasterInfo.model_validate(info_dict)

    @router.get("/vector/info", response_model=VectorInfo, summary="Get metadata for a vector file.")
    async def vector_info(
        file_url: str = Query(..., description="URL of the vector file (gs:// or https://)."),  # type: ignore[reportGeneralTypeIssues]
        gdal_uri: Optional[str] = Query(None, description="Direct GDAL-compatible URI."),
        mimetype: Optional[str] = Query(None, description="MIME type of the file.")
    ):
        gdal_path, file_type = await GdalService._get_gdal_path_and_type(file_url, gdal_uri, mimetype)
        if file_type == FileType.RASTER:
            raise HTTPException(status_code=400, detail="File is RASTER, expected VECTOR.")

        info_dict = await asyncio.to_thread(GdalService._execute_info_call, get_vector_info, gdal_path)
        return VectorInfo.model_validate(info_dict)

    @router.get("/info", response_model=Union[RasterInfo, VectorInfo], summary="Auto-detect and get metadata.")
    async def info(
        file_url: str = Query(..., description="URL of the geospatial file (gs:// or https://)."),  # type: ignore[reportGeneralTypeIssues]
        gdal_uri: Optional[str] = Query(None, description="Direct GDAL-compatible URI."),
        mimetype: Optional[str] = Query(None, description="MIME type of the file.")
    ):
        gdal_path, file_type = await GdalService._get_gdal_path_and_type(file_url, gdal_uri, mimetype)

        if file_type == FileType.RASTER:
            info_dict = await asyncio.to_thread(GdalService._execute_info_call, get_raster_info, gdal_path)
            return RasterInfo.model_validate(info_dict)
        elif file_type == FileType.VECTOR:
            info_dict = await asyncio.to_thread(GdalService._execute_info_call, get_vector_info, gdal_path)
            return VectorInfo.model_validate(info_dict)
        else:
            try:
                info_dict = await asyncio.to_thread(GdalService._execute_info_call, get_raster_info, gdal_path)
                return RasterInfo.model_validate(info_dict)
            except HTTPException:
                info_dict = await asyncio.to_thread(GdalService._execute_info_call, get_vector_info, gdal_path)
                return VectorInfo.model_validate(info_dict)
