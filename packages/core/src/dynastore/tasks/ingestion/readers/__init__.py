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

"""Pluggable source-readers for ingestion.

Solves the historical fragility where ``fiona.open(asset.uri)`` was the
single entry-point — fiona's PyPI wheel ships a bundled libgdal that
omits the Arrow/Parquet driver, so reading a GeoParquet file from
``/vsigs/`` failed with the cryptic ``CPLE_OpenFailedError: not
recognized as being in a supported file format``.

Pattern mirrors :mod:`dynastore.modules.storage.drivers.pg_sidecars`:

- :class:`SourceReaderProtocol` — capability contract.
- :class:`ReaderRegistry` — lazy registry; each reader try-imports its
  heavy deps and only registers if the import succeeds.
- :func:`resolve_reader` — pick the highest-priority reader whose
  ``can_read(uri)`` matches.

Every concrete reader is responsible for its own URI translation
(``gs://`` → ``/vsigs/`` for GDAL-backed; ``gs://`` → ``gcsfs.GCSFileSystem``
for pyarrow; etc.) — ``main_ingestion.py`` no longer cares.
"""

from .base import SourceReaderProtocol, register_reader, resolve_reader
from .registry import ReaderRegistry

# Side-effect import so default readers self-register at module load.
# Each reader hard-imports its heavy dep (osgeo, pyogrio) at module top —
# when the dep isn't installed, the import fails and the reader is
# silently skipped from the registry.  Mirrors the wrong-SCOPE-soft-skip
# contract used everywhere else in the codebase.
import logging as _logging  # noqa: E402
_logger = _logging.getLogger(__name__)

for _mod in ("osgeo_reader", "pyogrio_reader", "duckdb_reader"):
    try:
        __import__(f"{__name__}.{_mod}")
    except ImportError as _exc:  # noqa: PERF203
        _logger.info(
            "ingestion.readers: skipping %s — heavy dep missing (%s)",
            _mod, _exc,
        )

__all__ = (
    "SourceReaderProtocol",
    "register_reader",
    "resolve_reader",
    "ReaderRegistry",
)
