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

# dynastore/extensions/tools/formatters.py

import logging
import re
from enum import Enum
from typing import Iterable, Dict, Any, Optional, cast

# File: src/dynastore/extensions/formatters.py

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Literal

from dynastore.modules.catalog.models import (
    Collection as CoreCollection,
    Catalog as CoreCatalog,
)
from fastapi import Request
from fastapi.responses import StreamingResponse
import orjson
from datetime import datetime, timezone
from typing import AsyncIterator, Union, Generator, Iterator
from dynastore.tools.json import orjson_default
# Use the real file writers instead of placeholders
from dynastore.tools.file_io import write_csv, write_geopackage, write_geoparquet, write_parquet, write_shapefile, write_geojson


logger = logging.getLogger(__name__)

from dynastore.models.shared_models import OutputFormatEnum, Link

logger = logging.getLogger(__name__)

class OGCResponseMetadata(BaseModel):
    """Metadata for OGC API responses."""
    numberMatched: Optional[int] = None
    numberReturned: Optional[int] = None
    timeStamp: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))
    links: Optional[List[Link]] = None

# class OutputFormatEnum(str, Enum): ... REMOVED

def _parse_srid_from_srs_name(srs_name: Optional[str]) -> Optional[int]:
    """
    Parses an SRID from a URN-style srsName (e.g., 'urn:ogc:def:crs:EPSG::4326').
    """
    if not srs_name:
        return None
    match = re.search(r'::(\d+)$', srs_name)
    return int(match.group(1)) if match else None

# --- Format Dispatcher ---

format_map = {
    OutputFormatEnum.CSV: {
        "writer": write_csv,
        "media_type": "text/csv",
        "extension": "csv"
    },
    OutputFormatEnum.GEOPACKAGE: {
        "writer": write_geopackage,
        "media_type": "application/geopackage+sqlite3",
        "extension": "gpkg"
    },
    OutputFormatEnum.SHAPEFILE: {
        "writer": write_shapefile,
        "media_type": "application/zip",
        "extension": "zip"
    },
    OutputFormatEnum.PARQUET: {
        "writer": write_parquet,
        "media_type": "application/octet-stream",
        "extension": "parquet"
    },
    OutputFormatEnum.GEOPARQUET: {
        "writer": write_geoparquet,
        "media_type": "application/geoparquet",
        "extension": "parquet"
    },
    OutputFormatEnum.GEOJSON: {
        "writer": write_geojson,
        "media_type": "application/geo+json",
        "extension": "geojson"
    },
    OutputFormatEnum.JSON: {
        "writer": write_geojson,
        "media_type": "application/json",
        "extension": "json"
    },
    OutputFormatEnum.GML: {
        "media_type": "application/gml+xml",
        "extension": "gml"
    }
}

async def _stream_ogc_json(
    items: AsyncIterator[Any],
    metadata: OGCResponseMetadata,
    output_format: OutputFormatEnum = OutputFormatEnum.GEOJSON,
) -> AsyncIterator[bytes]:
    """Unified generator for streaming OGC GeoJSON or plain JSON."""
    is_geojson = output_format == OutputFormatEnum.GEOJSON
    
    yield b'{"type":"FeatureCollection"' if is_geojson else b'{"items":['
    
    if metadata.links:
        links_data = [l.model_dump(exclude_none=True) if hasattr(l, "model_dump") else l for l in metadata.links]
        yield b',"links":' + orjson.dumps(links_data)
        
    if is_geojson:
        yield b',"features":['
    
    is_first = True
    returned_count = 0
    async for item in items:
        if not is_first:
            yield b","
        
        if hasattr(item, "model_dump"):
            feat_dict = item.model_dump(exclude_none=True, by_alias=True)
        else:
            feat_dict = item
            
        yield orjson.dumps(feat_dict, default=orjson_default)
        is_first = False
        returned_count += 1
        
    yield b']' # Close features or items array
    
    if metadata.numberMatched is not None:
        yield b',"numberMatched":' + str(metadata.numberMatched).encode()
    
    yield b',"numberReturned":' + str(returned_count).encode()
    
    ts = metadata.timeStamp or datetime.now(timezone.utc)
    yield b',"timeStamp":"' + ts.isoformat().replace("+00:00", "Z").encode() + b'"}'

def format_response(
    features: Union[Iterable[Any], AsyncIterator[Any]],
    output_format: OutputFormatEnum,
    request: Optional[Request] = None,
    collection_id: str = "layer",
    target_srid: int = 4326,
    encoding: str = "utf-8",
    metadata: Optional[OGCResponseMetadata] = None,
) -> StreamingResponse:
    """
    Dynamically formats a stream of features into the specified output format.
    """
    formatter = format_map.get(output_format)
    if not formatter:
        raise ValueError(f"Unsupported format: {output_format}")

    # 1. Handle JSON/GeoJSON streaming (Preferred async path)
    if output_format in (OutputFormatEnum.GEOJSON, OutputFormatEnum.JSON):
        # Ensure it's an AsyncIterator
        async def _as_async_iter(it):
            if hasattr(it, '__aiter__'):
                async for x in it: yield x
            else:
                for x in it: yield x
        
        return StreamingResponse(
            _stream_ogc_json(
                _as_async_iter(features), 
                metadata or OGCResponseMetadata(),
                output_format=output_format
            ),
            media_type=formatter["media_type"]
        )

    # 2. Handle GML (special case for WFS)
    if output_format == OutputFormatEnum.GML:
        # GML usually comes pre-formatted or handled by a specific generator
        # For now, if it's already a generator of bytes, just return it.
        async def _byte_streamer():
            src = cast(Any, features)
            if hasattr(features, '__aiter__'):
                async for chunk in src: yield chunk
            else:
                for chunk in src: yield chunk
        
        return StreamingResponse(content=_byte_streamer(), media_type=formatter["media_type"])

    # 3. Handle Other Formats (CSV, GPKG, etc.) - BRIDGE SYNC WRITERS
    # These writers currently expect a sync Generator[Feature, None, None]
    def _get_sync_gen():
        src = cast(Any, features)
        if hasattr(features, '__aiter__'):
            # This is slow but necessary if the upstream is async and we need sync
            import asyncio
            loop = asyncio.new_event_loop()
            async def _consume():
                res = []
                async for f in src: res.append(f)
                return res
            items = loop.run_until_complete(_consume())
            loop.close()
            for x in items: yield x
        else:
            for x in src: yield x

    feature_dicts = (f.model_dump(by_alias=True) if hasattr(f, 'model_dump') else f for f in _get_sync_gen())

    content_stream = formatter["writer"](feature_dicts, target_srid, encoding=encoding)
    
    headers = {
        "Content-Disposition": f'attachment; filename="{collection_id}.{formatter["extension"]}"'
    }

    return StreamingResponse(content=content_stream, media_type=formatter["media_type"], headers=headers)
