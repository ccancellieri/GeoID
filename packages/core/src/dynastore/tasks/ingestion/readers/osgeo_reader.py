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

"""GdalOsgeoReader — broad-coverage fallback reader, uses system libgdal
via ``osgeo.ogr``.

Why this exists: PyPI GDAL wheels (``pyogrio``, like ``fiona`` before
it) ship a bundled libgdal that omits the Arrow/Parquet driver.
``from osgeo import ogr`` binds to the **system** libgdal (the one that
comes with the
``ghcr.io/osgeo/gdal:ubuntu-full-3.13.0`` base image), which DOES
include Parquet, FlatGeobuf, OpenFileGDB, …  Same osgeo binding the
maps service uses successfully.

Registered at ``priority=100`` — behind :class:`PyogrioReader`
(``priority=10``) — because this reader iterates one OGR feature at a
time via the Python API, which is far slower than pyogrio's vectorized
chunked reads on the formats pyogrio also supports (GeoID #2964). It
remains the reader of record for every format pyogrio's PyPI wheel
doesn't declare (Parquet, FlatGeobuf, OpenFileGDB, KML, GML, MapInfo, …).

Hard-imports ``osgeo`` at module load — when SCOPE excludes
``module_gdal`` the import fails and :class:`ReaderRegistry` skips
this reader (same wrong-SCOPE-soft-skip pattern as the rest of the
codebase).
"""

from __future__ import annotations

import contextlib
import glob
import json
import logging
import os
import shutil
import zipfile
from typing import Any, ClassVar, Generator, Iterable, Iterator, Tuple

# Hard-import gates registration.  When module_gdal isn't installed
# (most worker scopes), the ImportError prevents this module's
# ``register_reader(GdalOsgeoReader)`` line from running — the registry
# stays narrower and resolve() falls through to the next candidate
# (PyogrioReader / future PyArrow reader).
from osgeo import ogr, gdal  # noqa: F401

from .base import SourceReaderProtocol, _to_vsigs, register_reader

logger = logging.getLogger(__name__)


# Initialize once.  Idempotent — safe to call repeatedly.
ogr.UseExceptions()
gdal.UseExceptions()


def _resolve_temp_dir():
    """Return the registered ``TempDirProtocol`` implementation.

    Falls back to ``DefaultTempDir`` when no implementation is registered so
    the reader works out of the box on plain local disk and on-premise deployments.
    """
    try:
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.temp_dir import TempDirProtocol
        return resolve(TempDirProtocol)
    except Exception:  # noqa: BLE001 — protocol is optional
        from dynastore.models.protocols.temp_dir import DefaultTempDir
        return DefaultTempDir()


class GdalOsgeoReader(SourceReaderProtocol):
    """Universal reader backed by system libgdal.

    Handles every vector format the base image's GDAL build supports
    (~78 drivers in ubuntu-full): Parquet, FlatGeobuf, GeoJSON,
    GeoPackage, ESRI Shapefile (incl. zipped via ``/vsizip/``), CSV,
    KML, GML, MapInfo, OpenFileGDB …
    """

    reader_id: ClassVar[str] = "gdal_osgeo"
    priority: ClassVar[int] = 100

    # Empty extension tuple means "match anything" — see can_read override.
    extensions: ClassVar[Tuple[str, ...]] = ()

    # Drivers we explicitly know GDAL can open from /vsigs/. For any
    # extension PyogrioReader also declares (priority=10), pyogrio wins
    # and this reader is only reached as its fallback; for everything
    # else in this list (Parquet, FlatGeobuf, KML, GML, MapInfo, …) this
    # is the sole/first match.
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
    def _to_gdal_uri(
        uri: str, *, is_zip: bool | None = None, use_vsicache: bool = False,
    ) -> str:
        """Normalize the URI for GDAL.  Translates ``gs://`` and wraps
        zipped shapefiles with ``/vsizip/`` when needed.

        *is_zip* lets the caller force the ``/vsizip/`` wrap when the
        URI itself lacks the ``.zip`` suffix (e.g. an asset uploaded
        with a bare filename, where the caller knows the content_type
        is ``application/zip``).

        *use_vsicache* wraps the non-zip path with GDAL's ``/vsicached/``
        local block-cache VSI (see ``_to_vsigs``) — not applied to the zip
        branch since archives are already staged to local disk before
        feature iteration (see ``_extract_archive_to_local``).

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
            return _to_vsigs(uri, use_vsicache=use_vsicache) if use_vsicache else out
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
    ) -> Generator[Iterable[dict], None, None]:
        from dynastore.tools.mime import ext_from_content_type
        use_vsicache = bool(opts.get("use_vsicache", False))
        is_zip = (ext_from_content_type(content_type) or "").lower() == ".zip"
        if is_zip is None or not is_zip:
            is_zip = _to_vsigs(uri).lower().endswith(".zip")

        # OGR doesn't honour `encoding=` directly — set via config option.
        # Most modern drivers (Parquet, FGB, GeoJSON, GPKG) are UTF-8 by
        # spec; this only matters for shapefile dbf / CSV.
        prev_enc = gdal.GetConfigOption("SHAPE_ENCODING")
        gdal.SetConfigOption("SHAPE_ENCODING", encoding.upper())
        try:
            # A zipped shapefile read in-place over ``/vsizip//vsigs/`` forces
            # GDAL to decompress the archive member into memory and keep it
            # resident for the random access the .shx index drives — so reading
            # a large layer grows RSS with the read progress and OOMs the worker
            # mid-stream. Instead extract the archive ONCE to the local temp
            # disk, so feature iteration does bounded range reads.
            if is_zip:
                task_id: str | None = opts.get("task_id")
                task_schema: str | None = opts.get("task_schema")
                with self._extract_archive_to_local(
                    uri, task_id=task_id, task_schema=task_schema
                ) as local_dir:
                    path = self._find_local_dataset(local_dir)
                    yield from self._open_path_and_iter(path)
            else:
                path = self._to_gdal_uri(uri, is_zip=False, use_vsicache=use_vsicache)
                yield from self._open_path_and_iter(path)
        finally:
            if prev_enc is None:
                gdal.SetConfigOption("SHAPE_ENCODING", "")
            else:
                gdal.SetConfigOption("SHAPE_ENCODING", prev_enc)

    def _open_path_and_iter(self, path: str) -> Iterator[Iterable[dict]]:
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

    @contextlib.contextmanager
    def _extract_archive_to_local(
        self,
        uri: str,
        *,
        task_id: str | None = None,
        task_schema: str | None = None,
    ) -> Generator[str, None, None]:
        """Stream a (possibly remote) zip archive to the local temp disk and
        extract it, yielding the extraction directory.

        The scratch directory is allocated via ``TempDirProtocol.mkdtemp()``
        so the root and the naming convention are controlled by the deployment
        (GCSFuse mount, NFS share, or plain local disk on-premise).  The
        protocol's ``TASK_DIR_PREFIX`` ensures the reaper can glob all task
        scratch dirs regardless of which task type created them.

        A ``.owner`` JSON sidecar is written immediately after the directory
        is created so a liveness-aware reaper can decide whether to reclaim
        an abandoned directory.  The directory is always cleaned up on exit.
        """
        src = _to_vsigs(uri)
        tmp_provider = _resolve_temp_dir()
        work_dir = tmp_provider.mkdtemp(task_id=task_id, task_schema=task_schema)
        try:
            # Write the owner sidecar before any heavy I/O so an OOM mid-copy
            # still leaves an attributable directory for the reaper.
            try:
                owner_path = os.path.join(work_dir, ".owner")
                with open(owner_path, "w") as _f:
                    json.dump({"task_id": task_id, "schema": task_schema}, _f)
            except Exception:  # noqa: BLE001
                pass  # sidecar is best-effort; extraction must not fail here

            local_zip = os.path.join(work_dir, "source.zip")
            self._vsi_copy(src, local_zip)
            extract_dir = os.path.join(work_dir, "extracted")
            os.makedirs(extract_dir, exist_ok=True)
            with zipfile.ZipFile(local_zip) as zf:
                self._safe_extractall(zf, extract_dir)
            # The archive copy is no longer needed once expanded — drop it so it
            # does not occupy the temp volume during the (long) read.
            try:
                os.remove(local_zip)
            except OSError:
                pass
            logger.info(
                "GdalOsgeoReader: extracted %r → %s (in-memory /vsizip avoided)",
                src, extract_dir,
            )
            yield extract_dir
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    @staticmethod
    def _safe_extractall(zf: zipfile.ZipFile, dest: str) -> None:
        """Extract every member of *zf* into *dest*, rejecting any entry whose
        path would escape *dest* (Zip-Slip / path traversal). Source archives
        are operator/uploaded content, so a crafted ``../`` member must not be
        allowed to write outside the extraction directory.
        """
        dest_abs = os.path.abspath(dest)
        for member in zf.infolist():
            target = os.path.abspath(os.path.join(dest, member.filename))
            if target != dest_abs and not target.startswith(dest_abs + os.sep):
                raise RuntimeError(
                    "GdalOsgeoReader: refusing archive entry that escapes the "
                    f"extraction directory (possible Zip-Slip): {member.filename!r}"
                )
        zf.extractall(dest)

    @staticmethod
    def _vsi_copy(src_vsi: str, dst_path: str, chunk: int = 8 * 1024 * 1024) -> None:
        """Copy a GDAL-VSI-readable source to a local path in bounded chunks."""
        fh = gdal.VSIFOpenL(src_vsi, "rb")
        if fh is None:
            raise RuntimeError(
                f"GdalOsgeoReader: cannot open source {src_vsi!r} to stage locally."
            )
        try:
            with open(dst_path, "wb") as out:
                while True:
                    buf = gdal.VSIFReadL(1, chunk, fh)
                    if not buf:
                        break
                    out.write(buf)
        finally:
            gdal.VSIFCloseL(fh)

    @staticmethod
    def _find_local_dataset(extract_dir: str) -> str:
        """Locate the openable vector dataset inside an extracted archive.

        Prefers an explicit geospatial file (shapefile first — the common
        archived format), searching nested directories. Falls back to the
        directory itself so OGR's shapefile driver can introspect it.
        """
        for pat in (
            "*.shp", "*.gpkg", "*.geojson", "*.json",
            "*.fgb", "*.gml", "*.tab", "*.gdb",
        ):
            hits = sorted(
                glob.glob(os.path.join(extract_dir, "**", pat), recursive=True)
            )
            if hits:
                return hits[0]
        return extract_dir

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
                    except Exception:  # noqa: BLE001 — geometry_wkb is the fallback path
                        pass
                    try:
                        geom_wkb = bytes(geom.ExportToWkb())
                    except Exception:  # noqa: BLE001 — feature still yields; caller decides
                        pass
                # Stable per-row identity fallback (#2709): the OGR feature id
                # is deterministic across re-reads of the SAME unmodified
                # source (row order on disk), so surfacing it as the GeoJSON
                # top-level "id" lets a re-run converge instead of appending a
                # duplicate copy when no column_mapping.external_id is
                # configured. -1 means "no FID" (some drivers never assign
                # one) — left out entirely so downstream identity resolution
                # falls through to its next fallback rather than colliding
                # every FID-less feature onto the same id.
                fid = feat.GetFID()
                # Emit the GeoJSON Feature record shape (`{"properties": …,
                # "geometry": …}`) so call sites that deconstruct reader
                # records don't need to branch per reader.  Add ``geometry_wkb``
                # as a convenience so column_mapping=geometry_wkb just
                # works for STAC items.
                record: dict = {
                    "type": "Feature",
                    "properties": props,
                    "geometry": geom_geojson,
                    "geometry_wkb": geom_wkb,
                }
                if fid is not None and fid >= 0:
                    record["id"] = fid
                yield record


register_reader(GdalOsgeoReader)
