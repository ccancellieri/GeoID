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

"""DuckDB-backed reader — an alternate (Geo)Parquet reader for A/B testing
against :class:`GdalOsgeoReader`'s ``ST_Read``/OGR path (GeoID #2981).

Registered at ``priority=200`` — strictly behind both existing readers —
so it never wins ``ReaderRegistry.resolve()``'s auto priority/extension
scan.  It's reachable only via an explicit ``reader="duckdb"`` override on
``TaskIngestionRequest``, which is the point: a cheap way to try a third
Parquet code path live without touching the default resolution order.

Reads via DuckDB's native ``read_parquet`` (not ``ST_Read`` — empirically,
the ``spatial`` extension's bundled GDAL has no Arrow/Parquet driver here,
the same PyPI-wheel gap that motivated ``GdalOsgeoReader`` in the first
place; ``ST_Read`` on a local Parquet file raises "Could not open GDAL
dataset").  When ``spatial`` loads, DuckDB 1.x auto-decodes spec-compliant
GeoParquet metadata so a native ``geometry`` column arrives as
DuckDB's ``GEOMETRY`` type; legacy exporters (e.g. GeoPandas ``to_parquet``)
instead store raw WKB as ``BLOB`` — both are decoded to GeoJSON here via
``ST_AsGeoJSON``, one of the two branches also used in
``modules/storage/drivers/duckdb.py``'s ``_source_expr``. Any other stored
type (e.g. WKT text) is passed through undecoded; ``column_mapping`` can
still map it directly.

Current limitation: local filesystem paths only. Verified empirically
that DuckDB's ``read_parquet`` does NOT understand the ``/vsigs/`` VSI
prefix the other readers use for ``gs://`` sources — it treats it as a
literal (non-matching) glob pattern, not a GDAL virtual filesystem, since
``read_parquet`` is DuckDB's own multi-file reader, not GDAL-backed. DuckDB
does have native ``gcs://`` support via its ``httpfs`` extension, but that
requires an HMAC key pair configured as a DuckDB ``SECRET`` — this
codebase has no established credential-wiring convention for that (see
``modules/storage/drivers/_duckdb_helpers.py``), so ``gs://`` sources
raise a clear, documented error here rather than guessing at IAM/secret
setup that may not exist in a given deployment. Follow-up if this reader
proves useful for the live corruption bug: wire a DuckDB GCS secret from
the same service-account credentials ``modules/gcp`` already provisions.
"""

from __future__ import annotations

import contextlib
import json
import logging
from typing import Any, ClassVar, Dict, Iterable, Iterator, Optional, Tuple

# Hard-import gates registration: when duckdb isn't installed the ImportError
# prevents the ``register_reader`` call below from running (same
# wrong-SCOPE-soft-skip pattern as the rest of the codebase).
import duckdb  # noqa: F401

from .base import SourceReaderProtocol, _to_vsigs, register_reader

logger = logging.getLogger(__name__)


def _load_spatial(con: "duckdb.DuckDBPyConnection") -> bool:
    """Best-effort load of the ``spatial`` extension; geometry decode is
    skipped (raw column passthrough) when it's unavailable rather than
    failing the whole read."""
    try:
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        return True
    except Exception as exc:  # noqa: BLE001 — geometry decode is best-effort
        logger.warning(
            "DuckDbReader: 'spatial' extension unavailable (%s) — geometry "
            "columns will be passed through undecoded.", exc,
        )
        return False


def _probe_geometry_column(
    con: "duckdb.DuckDBPyConnection", path: str,
) -> Tuple[Optional[str], Optional[str]]:
    """Return ``(column_name, duckdb_type)`` for the GeoParquet spec's
    canonical ``geometry`` column, or ``(None, None)`` if absent/unreadable.

    Only the conventional column name is recognised — GeoParquet's own
    "geo" metadata key can name an arbitrary primary geometry column, but
    matching that generically isn't needed for this A/B-test reader.
    """
    try:
        cols = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?) LIMIT 0", [path],
        ).fetchall()
    except Exception as exc:  # noqa: BLE001 — surfaced by the caller's open()
        logger.warning("DuckDbReader: could not describe %r: %s", path, exc)
        return None, None
    for name, duck_type, *_ in cols:
        if name.lower() == "geometry":
            return name, duck_type.upper()
    return None, None


def _select_expr(geom_col: Optional[str], geom_type: Optional[str], spatial_loaded: bool) -> str:
    """Build the ``SELECT`` list, replacing the geometry column with a
    GeoJSON-text expression when its stored type is decodable."""
    if not geom_col or not spatial_loaded:
        return "*"
    if geom_type and geom_type.startswith("GEOMETRY"):
        # DuckDB 1.x + spatial already decoded spec-compliant GeoParquet.
        return f'* REPLACE (ST_AsGeoJSON("{geom_col}")::VARCHAR AS "{geom_col}")'
    if geom_type == "BLOB":
        # Legacy exporters (e.g. GeoPandas) store raw WKB bytes.
        return f'* REPLACE (ST_AsGeoJSON(ST_GeomFromWKB("{geom_col}"))::VARCHAR AS "{geom_col}")'
    # Unknown stored type (e.g. VARCHAR/WKT) — leave untouched.
    return "*"


def _row_to_record(row: Dict[str, Any], geom_col: Optional[str]) -> dict:
    """Shape a DuckDB row dict into the same GeoJSON-Feature record the
    other readers emit, so ``prepare_record_for_upsert`` doesn't need to
    branch per reader."""
    row = dict(row)
    geometry = None
    if geom_col and geom_col in row:
        raw_geom = row.pop(geom_col)
        if isinstance(raw_geom, str):
            try:
                geometry = json.loads(raw_geom)
            except (TypeError, ValueError):
                geometry = None
        elif raw_geom is not None:
            geometry = raw_geom
    return {"type": "Feature", "properties": row, "geometry": geometry}


class DuckDbReader(SourceReaderProtocol):
    """Alternate (Geo)Parquet reader for live A/B testing (GeoID #2981);
    never auto-selected — see module docstring."""

    reader_id: ClassVar[str] = "duckdb"
    priority: ClassVar[int] = 200
    extensions: ClassVar[Tuple[str, ...]] = (".parquet", ".geoparquet")

    @staticmethod
    def _resolve_local_path(uri: str) -> str:
        path = _to_vsigs(uri)
        if path.startswith("/vsigs/"):
            raise NotImplementedError(
                f"DuckDbReader: {uri!r} is a gs:// source — not supported "
                "yet (verified empirically: DuckDB's read_parquet() has no "
                "/vsigs/ VSI awareness, and DuckDB's own gcs:// support "
                "needs an HMAC secret this codebase doesn't provision; see "
                "the module docstring). Stage the file to local disk before "
                "using reader='duckdb'."
            )
        return path

    @contextlib.contextmanager
    def open(
        self,
        uri: str,
        *,
        encoding: str = "utf-8",  # noqa: ARG002 — Parquet is UTF-8 by spec
        content_type: Optional[str] = None,  # noqa: ARG002 — forwarded by registry, unused here
        **opts: Any,
    ) -> Iterator[Iterable[dict]]:
        path = self._resolve_local_path(uri)
        chunk_size = int(opts.get("read_batch_size") or 1000)

        con = duckdb.connect(":memory:")
        try:
            spatial_loaded = _load_spatial(con)
            geom_col, geom_type = _probe_geometry_column(con, path)
            select_expr = _select_expr(geom_col, geom_type, spatial_loaded)
            geom_col_for_records = geom_col if (geom_col and spatial_loaded and select_expr != "*") else None

            rel = con.sql(f"SELECT {select_expr} FROM read_parquet(?)", params=[path])  # noqa: S608 — select_expr is built from our own probe, not user input
            reader = rel.to_arrow_reader(batch_size=chunk_size)

            def _iter_records() -> Iterator[dict]:
                for batch in reader:
                    for row in batch.to_pylist():
                        yield _row_to_record(row, geom_col_for_records)

            logger.info(
                "DuckDbReader: opened %r (chunk_size=%d, geometry_column=%r, "
                "spatial_loaded=%s)", path, chunk_size, geom_col, spatial_loaded,
            )
            yield _iter_records()
        finally:
            con.close()

    def feature_count(
        self, uri: str, *, content_type: Optional[str] = None,  # noqa: ARG002
    ) -> Optional[int]:
        try:
            path = self._resolve_local_path(uri)
        except NotImplementedError:
            return None
        try:
            con = duckdb.connect(":memory:")
            try:
                row = con.execute(
                    "SELECT count(*) FROM read_parquet(?)", [path],
                ).fetchone()
                return int(row[0]) if row is not None else None
            finally:
                con.close()
        except Exception:  # noqa: BLE001 — best-effort per base contract
            return None


register_reader(DuckDbReader)
