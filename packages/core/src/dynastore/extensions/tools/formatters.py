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

# dynastore/extensions/tools/formatters.py

import asyncio
import logging
import re
from typing import Iterable, Any, Optional, cast

# File: src/dynastore/extensions/formatters.py

from pydantic import BaseModel, Field
from typing import List

from fastapi import Request
from fastapi.responses import StreamingResponse
import orjson
from datetime import datetime, timezone
from typing import AsyncIterator, Union
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
    max_response_bytes: Optional[int] = None,
    request: Optional[Request] = None,
    offset: Optional[int] = None,
) -> AsyncIterator[bytes]:
    """Unified generator for streaming OGC GeoJSON or plain JSON.

    ``max_response_bytes`` (#2681) bounds process memory on collections with
    very large geometries: once the serialized ``features`` bytes cross this
    budget, the loop stops pulling more items instead of streaming the rest
    of the page — a page-size ceiling ``limit``/``max_limit`` alone cannot
    provide, since a handful of huge multipolygons can already exceed it.
    ``request``/``offset`` (both required together) let this cut-short page
    still carry a correct ``next`` link: the precomputed ``next`` link in
    ``metadata.links`` assumes the full ``limit`` was served, so it is
    replaced with one built from the ACTUAL number of items returned
    (:func:`build_next_link`). Callers that omit ``request``/``offset``
    (e.g. WFS, which does not pass a byte budget) keep the legacy
    precomputed links untouched.

    The ``links`` member is emitted after the ``features`` array (JSON
    member order carries no meaning per RFC 8259) so the corrected ``next``
    link can be computed from the count actually streamed.
    """
    is_geojson = output_format == OutputFormatEnum.GEOJSON

    yield b'{"type":"FeatureCollection"' if is_geojson else b'{"items":['

    if is_geojson:
        yield b',"features":['

    is_first = True
    returned_count = 0
    bytes_emitted = 0
    async for item in items:
        if hasattr(item, "model_dump"):
            feat_dict = item.model_dump(exclude_none=True, by_alias=True)
            # RFC 7946: a GeoJSON Feature MUST carry a ``geometry`` member,
            # even when it is ``null`` (an "unlocated Feature"). ``exclude_none``
            # would drop a ``None`` geometry from the dump; restore it here so
            # the wire shape stays spec-compliant and ``skipGeometry=true``
            # emits the documented ``"geometry": null``.
            if (
                is_geojson
                and feat_dict.get("type") == "Feature"
                and "geometry" not in feat_dict
            ):
                feat_dict["geometry"] = None
        else:
            feat_dict = item

        chunk = orjson.dumps(feat_dict, default=orjson_default)
        if not is_first:
            yield b","
            bytes_emitted += 1
        yield chunk
        bytes_emitted += len(chunk)
        is_first = False
        returned_count += 1

        if max_response_bytes is not None and bytes_emitted >= max_response_bytes:
            # Byte budget reached: stop pulling more items rather than
            # materialising/serialising the rest of the page (#2681). Close
            # the source iterator so the underlying DB stream/transaction is
            # released promptly instead of waiting on garbage collection.
            aclose = getattr(items, "aclose", None)
            if aclose is not None:
                try:
                    await aclose()
                except Exception:
                    logger.debug(
                        "byte-budget cutoff: source aclose() failed", exc_info=True
                    )
            break

    yield b']'  # Close features or items array

    links_data = None
    if metadata.links:
        links_data = cast(
            "List[dict]",
            [
                l.model_dump(exclude_none=True) if hasattr(l, "model_dump") else l
                for l in metadata.links
            ],
        )
        if request is not None and offset is not None:
            from dynastore.extensions.tools.pagination import build_next_link

            links_data = [l for l in links_data if l.get("rel") != "next"]
            next_link = build_next_link(
                request, offset, returned_count, metadata.numberMatched,
            )
            if next_link is not None:
                links_data.append(next_link.model_dump(exclude_none=True))
    if links_data:
        yield b',"links":' + orjson.dumps(links_data)

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
    max_response_bytes: Optional[int] = None,
    offset: Optional[int] = None,
) -> StreamingResponse:
    """
    Dynamically formats a stream of features into the specified output format.

    ``max_response_bytes``/``offset`` (#2681) are forwarded to
    :func:`_stream_ogc_json` for the GeoJSON/JSON streaming path only — see
    its docstring for the byte-budget and corrected-``next``-link contract.
    """
    formatter = format_map.get(output_format)
    if not formatter:
        raise ValueError(f"Unsupported format: {output_format}")

    # 1. Handle JSON/GeoJSON streaming (Preferred async path)
    if output_format in (OutputFormatEnum.GEOJSON, OutputFormatEnum.JSON):
        # Ensure it's an AsyncIterator. ``finally`` propagates an early
        # close (the byte-budget cutoff in ``_stream_ogc_json``, #2681)
        # down to ``it`` — ``async for`` alone does not close the iterable
        # it wraps when this generator itself is closed early.
        async def _as_async_iter(it):
            if hasattr(it, '__aiter__'):
                try:
                    async for x in it: yield x
                finally:
                    aclose = getattr(it, "aclose", None)
                    if aclose is not None:
                        await aclose()
            else:
                for x in it: yield x

        return StreamingResponse(
            _stream_ogc_json(
                _as_async_iter(features),
                metadata or OGCResponseMetadata(),
                output_format=output_format,
                max_response_bytes=max_response_bytes,
                request=request,
                offset=offset,
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
    # These writers consume a *sync* Generator[Feature, None, None]. When the
    # upstream is an async iterator (the asyncpg/Elasticsearch item stream) it is
    # bound to the event loop that is running *now* — the request loop. The sync
    # writer below is driven later by Starlette inside a worker thread, where no
    # loop is running. Spinning a brand-new loop there and re-driving the async
    # source on it re-uses asyncpg/aiohttp objects across loops and raises
    # "got Future attached to a different loop" /
    # "Timeout context manager should be used inside a task". So capture the
    # request loop now and marshal the drain back onto it with
    # run_coroutine_threadsafe — the same idiom as
    # tools.async_utils.SyncQueueIterator.
    request_loop: Optional[asyncio.AbstractEventLoop] = None
    if hasattr(features, "__aiter__"):
        try:
            request_loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop (a purely synchronous caller handing us an async
            # source with nothing loop-bound) — a throwaway loop is safe.
            request_loop = None

    def _get_sync_gen():
        src = cast(Any, features)
        if hasattr(features, "__aiter__"):
            async def _consume():
                return [f async for f in src]

            if request_loop is not None and request_loop.is_running():
                # Drain on the loop that owns the async resources, blocking this
                # worker thread until the full batch has been collected.
                items = asyncio.run_coroutine_threadsafe(
                    _consume(), request_loop
                ).result()
            else:
                loop = asyncio.new_event_loop()
                try:
                    items = loop.run_until_complete(_consume())
                finally:
                    loop.close()
            for x in items:
                yield x
        else:
            for x in src:
                yield x

    feature_dicts = (f.model_dump(by_alias=True) if hasattr(f, 'model_dump') else f for f in _get_sync_gen())

    content_stream = formatter["writer"](feature_dicts, target_srid, encoding=encoding)
    
    headers = {
        "Content-Disposition": f'attachment; filename="{collection_id}.{formatter["extension"]}"'
    }

    return StreamingResponse(content=content_stream, media_type=formatter["media_type"], headers=headers)
