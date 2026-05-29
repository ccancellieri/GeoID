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
from contextlib import asynccontextmanager
from typing import AsyncGenerator, ClassVar, Set
from dynastore.modules import ModuleProtocol
from .service import GDAL_AVAILABLE, STATIC_RASTER_MIME_TYPES, STATIC_VECTOR_MIME_TYPES, FileType

try:
    from osgeo import gdal
except ImportError:
    gdal = None

logger = logging.getLogger(__name__)
class GdalModule(ModuleProtocol):
    priority: int = 100
    """
    Module providing GDAL/OGR utilities and format discovery.

    The dynamic GDAL driver format maps are built once in ``lifespan`` rather
    than in the legacy ``initialize()`` classmethod, making the lifecycle
    explicit and consistent with the ProtocolPlugin model.
    """

    RASTER_EXTENSIONS: ClassVar[Set[str]] = set()
    VECTOR_EXTENSIONS: ClassVar[Set[str]] = set()
    RASTER_MIME_TYPES: ClassVar[Set[str]] = set()
    VECTOR_MIME_TYPES: ClassVar[Set[str]] = set()

    @classmethod
    def _build_format_maps(cls):
        """Builds dynamic GDAL/OGR driver format maps (idempotent)."""
        if cls.RASTER_EXTENSIONS or not GDAL_AVAILABLE or not gdal:
            return

        logger.info("Building dynamic GDAL/OGR driver format maps...")
        raster_ext, vector_ext = set(), set()
        raster_mime, vector_mime = set(), set()

        for i in range(gdal.GetDriverCount()):
            driver = gdal.GetDriver(i)
            is_raster = driver.GetMetadataItem(gdal.DCAP_RASTER) == 'YES'
            is_vector = driver.GetMetadataItem(gdal.DCAP_VECTOR) == 'YES'

            ext_str = driver.GetMetadataItem(gdal.DMD_EXTENSIONS)
            extensions = {f".{ext.lower()}" for ext in ext_str.split()} if ext_str else set()
            mimetype = driver.GetMetadataItem(gdal.DMD_MIMETYPE)

            if is_raster:
                raster_ext.update(extensions)
                if mimetype: raster_mime.add(mimetype)

            if is_vector:
                vector_ext.update(extensions)
                if mimetype: vector_mime.add(mimetype)

        cls.RASTER_EXTENSIONS = raster_ext
        cls.VECTOR_EXTENSIONS = vector_ext
        cls.RASTER_MIME_TYPES = raster_mime.union(STATIC_RASTER_MIME_TYPES)
        cls.VECTOR_MIME_TYPES = vector_mime.union(STATIC_VECTOR_MIME_TYPES)

        logger.info(
            f"GDAL Module initialized: {len(cls.RASTER_EXTENSIONS)} raster, "
            f"{len(cls.VECTOR_EXTENSIONS)} vector extensions."
        )

    @classmethod
    def get_file_type_by_mime(cls, mimetype: str) -> FileType:
        cls._build_format_maps()
        if mimetype in cls.RASTER_MIME_TYPES:
            return FileType.RASTER
        if mimetype in cls.VECTOR_MIME_TYPES:
            return FileType.VECTOR
        return FileType.UNKNOWN

    @classmethod
    def get_file_type_by_ext(cls, file_url: str) -> FileType:
        cls._build_format_maps()
        lower_url = file_url.lower()
        if any(lower_url.endswith(ext) for ext in cls.RASTER_EXTENSIONS):
            return FileType.RASTER
        if any(lower_url.endswith(ext) for ext in cls.VECTOR_EXTENSIONS) or lower_url.endswith('.zip'):
            return FileType.VECTOR
        return FileType.UNKNOWN

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        if GDAL_AVAILABLE:
            self._build_format_maps()
        else:
            logger.warning(
                "GDAL/OGR is NOT available in this environment. GdalModule will be limited."
            )

        # ``gdal`` is exposed as a standard OGC process via the task's
        # definition.py and executed at the catalog/collection mount in OGC API
        # - Processes (``/catalogs/{c}[/collections/{col}]/processes/gdal/
        # execution`` with ``asset_id`` in the body). No bespoke asset-process
        # wrapper to register here.
        yield
