#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO

"""GdalOsgeoReader — primary reader, uses system libgdal via ``osgeo.ogr``.

Why this exists: ``fiona`` (PyPI wheel) ships a bundled libgdal that
omits the Arrow/Parquet driver.  ``from osgeo import ogr`` binds to
the **system** libgdal (the one that comes with the
``ghcr.io/osgeo/gdal:ubuntu-full-3.12.3`` base image), which DOES
include Parquet, FlatGeobuf, OpenFileGDB, …  Same osgeo binding the
maps service uses successfully.

Hard-imports ``osgeo`` at module load — when SCOPE excludes
``module_gdal`` the import fails and :class:`ReaderRegistry` skips
this reader (same wrong-SCOPE-soft-skip pattern as the rest of the
codebase).
"""

from __future__ import annotations

import contextlib
import json
import logging
from typing import Any, ClassVar, Iterable, Iterator, Tuple

# Hard-import gates registration.  When module_gdal isn't installed
# (most worker scopes), the ImportError prevents this module's
# ``register_reader(GdalOsgeoReader)`` line from running — the registry
# stays narrower and resolve() falls through to the next candidate
# (FionaReader / future PyArrow reader).
from osgeo import ogr, gdal  # noqa: F401

from .base import SourceReaderProtocol, _to_vsigs, register_reader

logger = logging.getLogger(__name__)


# Initialize once.  Idempotent — safe to call repeatedly.
ogr.UseExceptions()
gdal.UseExceptions()


class GdalOsgeoReader(SourceReaderProtocol):
    """Universal reader backed by system libgdal.

    Handles every vector format the base image's GDAL build supports
    (~78 drivers in ubuntu-full): Parquet, FlatGeobuf, GeoJSON,
    GeoPackage, ESRI Shapefile (incl. zipped via ``/vsizip/``), CSV,
    KML, GML, MapInfo, OpenFileGDB …
    """

    reader_id: ClassVar[str] = "gdal_osgeo"
    priority: ClassVar[int] = 10

    # Empty extension tuple means "match anything" — see can_read override.
    extensions: ClassVar[Tuple[str, ...]] = ()

    # Drivers we explicitly know GDAL can open from /vsigs/ + a small
    # extension hint set so the registry's *priority* ordering still
    # picks us over the legacy fiona reader for common formats.
    KNOWN_EXT: ClassVar[Tuple[str, ...]] = (
        ".parquet", ".geoparquet",
        ".fgb",
        ".geojson", ".json",
        ".gpkg",
        ".shp", ".zip",
        ".csv", ".tsv",
        ".kml", ".kmz",
        ".gml",
        ".tab", ".mif",
        ".gdb",
    )

    @classmethod
    def can_read(cls, uri: str, *, content_type: str | None = None) -> bool:
        u = uri.lower()
        if any(u.endswith(ext) for ext in cls.KNOWN_EXT):
            return True
        # MIME fallback only when the URI has no suffix at all — see the
        # base ``SourceReaderProtocol.can_read`` rationale.  Legacy
        # bare-URI assets (e.g. uploaded with filename ``aoi_oasis``)
        # can still be resolved via the ingestion-task's content_type.
        from .base import _uri_has_recognisable_suffix
        if _uri_has_recognisable_suffix(uri):
            return False
        from dynastore.tools.mime import ext_from_content_type
        derived = ext_from_content_type(content_type)
        if derived and derived.lower() in cls.KNOWN_EXT:
            return True
        return False

    @classmethod
    def describe(cls) -> str:
        return f"{cls.reader_id}(known_extensions={cls.KNOWN_EXT})"

    # ------------------------------------------------------------------
    # URI prep
    # ------------------------------------------------------------------

    @staticmethod
    def _to_gdal_uri(uri: str, *, is_zip: bool | None = None) -> str:
        """Normalize the URI for GDAL.  Translates ``gs://`` and wraps
        zipped shapefiles with ``/vsizip/`` when needed.

        *is_zip* lets the caller force the ``/vsizip/`` wrap when the
        URI itself lacks the ``.zip`` suffix (e.g. an asset uploaded
        with a bare filename, where the caller knows the content_type
        is ``application/zip``).

        When the underlying object's path lacks a recognised archive
        extension we use GDAL's curly-brace notation
        ``/vsizip/{<archive-path>}/`` which tells the driver explicitly
        where the archive ends — no extension-based autodetection.
        See https://gdal.org/user/virtual_file_systems.html#vsizip-zip-archives
        """
        out = _to_vsigs(uri)
        if is_zip is None:
            is_zip = out.lower().endswith(".zip")
        if not is_zip:
            return out
        # GDAL autodetects the archive boundary on these extensions.
        if out.lower().endswith((".zip", ".kmz", ".ods", ".xlsx")):
            return "/vsizip/" + out
        # Bare-filename ZIP (e.g. ``/vsigs/<bucket>/.../aoi_oasis``):
        # use curly-brace form so GDAL doesn't try to autodetect.
        # Trailing slash leaves the inner path empty so the driver
        # discovers .shp / .gpkg etc. itself.
        return "/vsizip/{" + out + "}/"

    # ------------------------------------------------------------------
    # Open / iterate
    # ------------------------------------------------------------------

    def feature_count(self, uri: str, *, content_type: str | None = None) -> int | None:
        from dynastore.tools.mime import ext_from_content_type
        is_zip = (ext_from_content_type(content_type) or "").lower() == ".zip"
        path = self._to_gdal_uri(uri, is_zip=is_zip or None)
        ds = ogr.Open(path)
        if ds is None:
            return None
        try:
            total = 0
            for i in range(ds.GetLayerCount()):
                layer = ds.GetLayer(i)
                if layer is not None:
                    total += layer.GetFeatureCount()
            return total
        finally:
            ds = None  # noqa: F841 — release

    @contextlib.contextmanager
    def open(
        self,
        uri: str,
        *,
        encoding: str = "utf-8",
        content_type: str | None = None,
        **opts: Any,
    ) -> Iterator[Iterable[dict]]:
        from dynastore.tools.mime import ext_from_content_type
        is_zip = (ext_from_content_type(content_type) or "").lower() == ".zip"
        path = self._to_gdal_uri(uri, is_zip=is_zip or None)
        # OGR doesn't honour `encoding=` directly — set via config option.
        # Most modern drivers (Parquet, FGB, GeoJSON, GPKG) are UTF-8 by
        # spec; this only matters for shapefile dbf / CSV.
        prev_enc = gdal.GetConfigOption("SHAPE_ENCODING")
        gdal.SetConfigOption("SHAPE_ENCODING", encoding.upper())
        ds = ogr.Open(path)
        if ds is None:
            raise RuntimeError(
                f"GdalOsgeoReader: ogr.Open({path!r}) returned None — "
                f"no driver matched OR auth/access failed.  "
                f"Drivers available: {[gdal.GetDriver(i).ShortName for i in range(min(gdal.GetDriverCount(), 5))]}…"
            )
        try:
            logger.info(
                "GdalOsgeoReader: opened %r via driver=%s, layers=%d",
                path, ds.GetDriver().GetName(), ds.GetLayerCount(),
            )
            yield self._iter_features(ds)
        finally:
            ds = None  # noqa: F841 — release the dataset / file handle
            if prev_enc is None:
                gdal.SetConfigOption("SHAPE_ENCODING", "")
            else:
                gdal.SetConfigOption("SHAPE_ENCODING", prev_enc)

    @staticmethod
    def _iter_features(ds: Any) -> Iterator[dict]:
        for li in range(ds.GetLayerCount()):
            layer = ds.GetLayer(li)
            if layer is None:
                continue
            layer.ResetReading()
            field_names = [
                layer.GetLayerDefn().GetFieldDefn(i).GetName()
                for i in range(layer.GetLayerDefn().GetFieldCount())
            ]
            for feat in layer:
                if feat is None:
                    continue
                props: dict = {}
                for fname in field_names:
                    try:
                        props[fname] = feat.GetField(fname)
                    except Exception:  # noqa: BLE001
                        props[fname] = None
                geom = feat.GetGeometryRef()
                geom_geojson = None
                geom_wkb = None
                if geom is not None:
                    try:
                        geom_geojson = json.loads(geom.ExportToJson())
                    except Exception:  # noqa: BLE001
                        pass
                    try:
                        geom_wkb = bytes(geom.ExportToWkb())
                    except Exception:  # noqa: BLE001
                        pass
                # Mirror fiona's record shape (`{"properties": …,
                # "geometry": …}`) so call sites that already deconstruct
                # fiona records don't need to branch.  Add ``geometry_wkb``
                # as a convenience so column_mapping=geometry_wkb just
                # works for STAC items.
                yield {
                    "type": "Feature",
                    "properties": props,
                    "geometry": geom_geojson,
                    "geometry_wkb": geom_wkb,
                }


register_reader(GdalOsgeoReader)
