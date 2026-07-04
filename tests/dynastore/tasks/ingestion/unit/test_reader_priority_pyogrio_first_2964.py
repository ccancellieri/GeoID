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

"""Regression coverage for GeoID #2964: ``ReaderRegistry.resolve()`` must
prefer :class:`PyogrioReader` over :class:`GdalOsgeoReader` for the formats
pyogrio declares.

Before this fix ``GdalOsgeoReader`` was registered at ``priority=10`` with
an overridden ``can_read()`` matching a broad ``KNOWN_EXT`` tuple (including
``.gpkg``), while ``PyogrioReader`` sat behind it at ``priority=100`` — so
the registry always resolved to the row-by-row OGR reader even though
pyogrio declared the same extension and reads it in vectorized chunks.
On an 8.4M-row GeoPackage that row-by-row path ran at ~27 rows/sec.

Requires both real readers (GDAL + pyogrio) so the test exercises the
actual registered priority ordering, not a stand-in.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyogrio")

from dynastore.tasks.ingestion.readers.base import resolve_reader  # noqa: E402
from dynastore.tasks.ingestion.readers.pyogrio_reader import PyogrioReader  # noqa: E402
from dynastore.tasks.ingestion.readers.registry import ReaderRegistry  # noqa: E402


@pytest.fixture
def isolated_registry_with_real_pyogrio():
    """Swap in a clean registry containing the REAL PyogrioReader plus a
    stand-in that mirrors GdalOsgeoReader's ``priority``/``KNOWN_EXT``
    ``can_read`` override — without requiring ``osgeo`` to be installed.
    The end-to-end resolve() behaviour with the actual GdalOsgeoReader
    class is covered by ``test_resolve_prefers_pyogrio_for_shared_extensions``
    below, which is gated on ``osgeo`` (CI's GDAL image)."""
    from typing import ClassVar, Tuple

    from dynastore.tasks.ingestion.readers.base import SourceReaderProtocol

    class _GdalLikeStandIn(SourceReaderProtocol):
        reader_id: ClassVar[str] = "gdal_osgeo_standin"
        priority: ClassVar[int] = 100
        extensions: ClassVar[Tuple[str, ...]] = ()
        KNOWN_EXT: ClassVar[Tuple[str, ...]] = (
            ".parquet", ".fgb", ".geojson", ".json", ".gpkg",
            ".shp", ".zip", ".csv", ".kml", ".gml",
        )

        @classmethod
        def can_read(cls, uri: str, *, content_type: str | None = None) -> bool:
            return uri.lower().endswith(cls.KNOWN_EXT)

    saved = list(ReaderRegistry._registered)
    ReaderRegistry.clear()
    ReaderRegistry.register(_GdalLikeStandIn)
    ReaderRegistry.register(PyogrioReader)
    yield _GdalLikeStandIn
    ReaderRegistry.clear()
    for cls in saved:
        ReaderRegistry.register(cls)


@pytest.mark.parametrize(
    "uri",
    [
        "gs://bucket/data.gpkg",
        "gs://bucket/data.geojson",
        "gs://bucket/data.shp",
        "gs://bucket/data.csv",
    ],
)
def test_resolve_prefers_pyogrio_over_gdal_standin_for_shared_extensions(
    isolated_registry_with_real_pyogrio, uri,
):
    assert resolve_reader(uri) is PyogrioReader


def test_resolve_falls_back_to_gdal_standin_for_gdal_only_format(
    isolated_registry_with_real_pyogrio,
):
    # ``.parquet`` is in the gdal-like stand-in's KNOWN_EXT but not in
    # PyogrioReader.extensions — resolve() must still reach the fallback.
    gdal_standin = isolated_registry_with_real_pyogrio
    assert resolve_reader("gs://bucket/data.parquet") is gdal_standin


# ---------------------------------------------------------------------------
# End-to-end with the REAL GdalOsgeoReader — requires osgeo (CI's GDAL image;
# skipped in local dev environments without module_gdal, same convention as
# test_osgeo_reader_fid.py).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "uri",
    [
        "gs://bucket/data.gpkg",
        "gs://bucket/data.geojson",
        "gs://bucket/data.shp",
        "gs://bucket/data.csv",
    ],
)
def test_resolve_prefers_pyogrio_for_shared_extensions(uri):
    pytest.importorskip("osgeo")
    from dynastore.tasks.ingestion.readers.osgeo_reader import GdalOsgeoReader

    assert resolve_reader(uri) is PyogrioReader
    assert GdalOsgeoReader.priority > PyogrioReader.priority


@pytest.mark.parametrize(
    "uri",
    [
        # Formats GdalOsgeoReader's KNOWN_EXT covers but PyogrioReader's
        # ``extensions`` tuple does not declare — the fallback path must
        # still resolve to gdal_osgeo.
        "gs://bucket/data.parquet",
        "gs://bucket/data.fgb",
        "gs://bucket/data.kml",
    ],
)
def test_resolve_falls_back_to_gdal_osgeo_for_gdal_only_formats(uri):
    pytest.importorskip("osgeo")
    from dynastore.tasks.ingestion.readers.osgeo_reader import GdalOsgeoReader

    assert resolve_reader(uri) is GdalOsgeoReader
