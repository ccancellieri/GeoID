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

"""Pyogrio-backed reader, registered as a fallback.

Sits strictly behind :class:`GdalOsgeoReader` (``priority=10``), which
binds the **system** libgdal via ``osgeo.ogr``.  This reader hard-imports
``pyogrio`` instead — pyogrio ships its own GDAL build in the PyPI wheel,
so it provides a vector-read path in scopes that pull in ``geospatial_io``
but not the ``module_gdal`` osgeo bindings.  ``priority=100`` keeps it a
tail candidate.
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
    """Fallback reader backed by pyogrio's bundled GDAL."""

    reader_id: ClassVar[str] = "pyogrio"
    priority: ClassVar[int] = 100
    extensions: ClassVar[Tuple[str, ...]] = (
        # Keep the set small and overlapping with osgeo intentionally so an
        # explicit ``hint=pyogrio`` (future) can pin it; osgeo's lower
        # priority still wins for these by default.
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
            # the container; chunked iteration keeps peak RSS proportional
            # to chunk_size, not source-file size.
            for chunk in pyogrio.read_dataframe(path, encoding=encoding, chunksize=chunk_size):
                yield from chunk.iterfeatures()

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
