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

"""Pyogrio-backed reader — preferred for the formats it declares.

Sits strictly ahead of :class:`GdalOsgeoReader` (``priority=100``) for
the extensions listed below, reading them in vectorized/Arrow-backed
pages instead of the row-by-row OGR iteration ``GdalOsgeoReader`` uses
— on an 8.4M-row GeoPackage that row-by-row path ran at ~27 rows/sec
(GeoID #2964).  ``priority=10`` gives this reader first crack at its
declared extensions; for any format outside that list ``can_read()``
returns False and the registry falls through to ``GdalOsgeoReader``,
which still covers the ~78 GDAL driver formats (Parquet, FlatGeobuf,
OpenFileGDB, …) pyogrio's PyPI wheel doesn't support.

Reads are paginated via ``pyogrio.read_dataframe(..., skip_features=,
max_features=)`` — ``read_dataframe`` has no ``chunksize`` parameter,
so passing one silently forwards as an unrecognised GDAL open option
and returns a single, non-chunked ``GeoDataFrame``.
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any, ClassVar, Iterable, Iterator, Tuple

# Hard-import gates registration: when geospatial_io isn't installed the
# ImportError prevents the ``register_reader`` call below from running and
# the registry stays narrower (same wrong-SCOPE-soft-skip pattern as the
# rest of the codebase).
import pyogrio  # noqa: F401

from .base import SourceReaderProtocol, _to_vsigs, register_reader

logger = logging.getLogger(__name__)


class PyogrioReader(SourceReaderProtocol):
    """Preferred reader (chunked/vectorized) for the formats it declares;
    backed by pyogrio's bundled GDAL."""

    reader_id: ClassVar[str] = "pyogrio"
    priority: ClassVar[int] = 10
    extensions: ClassVar[Tuple[str, ...]] = (
        # Formats pyogrio reads in vectorized/Arrow-backed pages —
        # dramatically faster than GdalOsgeoReader's row-by-row OGR
        # iteration (GeoID #2964).  Anything outside this list falls
        # through to GdalOsgeoReader's broader driver coverage.
        ".geojson", ".json", ".gpkg", ".shp", ".csv",
    )

    @contextlib.contextmanager
    def open(
        self,
        uri: str,
        *,
        encoding: str = "utf-8",
        content_type: str | None = None,  # noqa: ARG002 — forwarded by registry, unused here
        **opts: Any,
    ) -> Iterator[Iterable[dict]]:
        path = _to_vsigs(uri)
        chunk_size: int = opts.get("read_batch_size", 1000)  # type: ignore[assignment]

        def _iter_chunks() -> Iterator[dict]:
            # Stream in bounded chunks so the full source file is never
            # materialised in memory at once.  On a 300+ MB admin-boundary
            # dataset loading the entire GeoDataFrame in one call exhausts
            # the container; paginating via skip_features/max_features
            # keeps peak RSS proportional to chunk_size, not source-file
            # size.  (``read_dataframe`` has no ``chunksize`` kwarg — it's
            # not a chunked-generator API — so pagination is done here.)
            offset = 0
            while True:
                chunk = pyogrio.read_dataframe(
                    path, encoding=encoding,
                    skip_features=offset, max_features=chunk_size,
                )
                n = len(chunk)
                if n == 0:
                    break
                yield from chunk.iterfeatures()
                if n < chunk_size:
                    break
                offset += chunk_size

        logger.info(
            "PyogrioReader: opened %r (chunked, chunk_size=%d)", path, chunk_size,
        )
        yield _iter_chunks()

    def feature_count(
        self, uri: str, *, content_type: str | None = None,  # noqa: ARG002
    ) -> int | None:
        try:
            info = pyogrio.read_info(_to_vsigs(uri))
            count = info.get("features")
            return int(count) if count is not None else None
        except Exception:  # noqa: BLE001
            return None


register_reader(PyogrioReader)
