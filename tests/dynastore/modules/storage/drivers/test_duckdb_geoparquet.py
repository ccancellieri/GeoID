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

"""Unit and integration tests for GeoParquet format support in the DuckDB driver.

Four test layers:

1. SQL-generation unit tests — verify ``_source_expr`` emits the correct SQL
   for each stored-type branch (BLOB→ST_GeomFromWKB, GEOMETRY→passthrough,
   VARCHAR→ST_GeomFromText) without touching DuckDB at all.

2. Hermetic WKB-blob decode test — creates a tiny parquet fixture using
   ``ST_AsWKB(ST_Point(...))`` (stored as BLOB), reads it back through the
   geoparquet path, and asserts the geometry comes back as a usable GeoJSON
   point.  Exercises the BLOB→ST_GeomFromWKB branch.

3. Hermetic native-GeoParquet test — creates a parquet fixture using
   ``COPY ... (FORMAT PARQUET)`` with a real ST_Point geometry column.
   DuckDB 1.x + spatial writes it with GeoParquet metadata so ``read_parquet``
   returns a native GEOMETRY column.  Exercises the GEOMETRY passthrough branch.

4. Live test — fetches the OGC GeoParquet example file over HTTPS;
   skipped gracefully if the network is unavailable.  Uses
   ``_probe_geom_col_stored_type`` to determine the correct decode branch at
   runtime (the OGC file stores GEOMETRY natively in DuckDB 1.x + spatial).
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _driver_cls():
    from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
    return ItemsDuckdbDriver


def _try_import_duckdb():
    """Return the duckdb module or None if unavailable."""
    try:
        import duckdb
        return duckdb
    except ImportError:
        return None


def _spatial_available() -> bool:
    """Return True when DuckDB + the spatial extension can both load."""
    duckdb = _try_import_duckdb()
    if duckdb is None:
        return False
    try:
        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        conn.close()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Layer 1: SQL-generation unit tests (no DuckDB required)
# ---------------------------------------------------------------------------

class TestSourceExprGeoparquet:
    """_source_expr generates the correct SQL for each format and stored type."""

    # -- BLOB (raw WKB bytes) branch — default, exercises ST_GeomFromWKB -------

    def test_geoparquet_format_uses_st_geom_from_wkb(self):
        """Default geom_col_stored_type='BLOB' emits the WKB decode subquery."""
        D = _driver_cls()
        sql = D._source_expr("geoparquet", "/data/places.parquet")
        assert "ST_GeomFromWKB" in sql
        assert "read_parquet" in sql
        assert "/data/places.parquet" in sql
        # Default geometry column
        assert '"geometry"' in sql

    def test_gpq_alias_behaves_like_geoparquet(self):
        D = _driver_cls()
        sql = D._source_expr("gpq", "/data/x.parquet")
        assert "ST_GeomFromWKB" in sql
        assert "read_parquet" in sql

    def test_geoparquet_geometry_column_override_blob(self):
        """geometry_column override in BLOB path redirects the decode target."""
        D = _driver_cls()
        sql = D._source_expr("geoparquet", "/data/x.parquet", geometry_column="geom")
        assert '"geom"' in sql
        assert "ST_GeomFromWKB" in sql
        assert 'ST_GeomFromWKB("geom")' in sql

    # Keep original test name for back-compat.
    test_geoparquet_geometry_column_override = test_geoparquet_geometry_column_override_blob

    # -- GEOMETRY (native) branch — passthrough, no decode needed --------------

    def test_geoparquet_geometry_stored_type_passthrough(self):
        """When stored type is GEOMETRY the expression is a plain read_parquet subquery."""
        D = _driver_cls()
        sql = D._source_expr(
            "geoparquet", "/data/native.parquet", geom_col_stored_type="GEOMETRY"
        )
        assert "ST_GeomFromWKB" not in sql
        assert "ST_GeomFromText" not in sql
        assert "read_parquet('/data/native.parquet')" in sql
        # Still a parenthesised subquery.
        assert sql.startswith("(")
        assert sql.endswith(")")

    def test_geoparquet_geometry_stored_type_passthrough_with_column_override(self):
        """Identifier validation still applies in the passthrough path (no decode)."""
        D = _driver_cls()
        # Valid override — must not raise.
        sql = D._source_expr(
            "geoparquet", "/data/native.parquet",
            geometry_column="geom",
            geom_col_stored_type="GEOMETRY",
        )
        assert "ST_GeomFromWKB" not in sql
        assert "read_parquet('/data/native.parquet')" in sql

    # -- VARCHAR (WKT text) branch — ST_GeomFromText ---------------------------

    def test_geoparquet_varchar_stored_type_uses_st_geom_from_text(self):
        """When stored type is VARCHAR the expression wraps with ST_GeomFromText."""
        D = _driver_cls()
        sql = D._source_expr(
            "geoparquet", "/data/wkt.parquet", geom_col_stored_type="VARCHAR"
        )
        assert "ST_GeomFromText" in sql
        assert "ST_GeomFromWKB" not in sql
        assert "read_parquet('/data/wkt.parquet')" in sql
        assert sql.startswith("(")
        assert sql.endswith(")")

    # -- Security: identifier validation applies across all branches -----------

    @pytest.mark.parametrize(
        "malicious",
        [
            'geometry" AS x, (SELECT 1)) --',  # break out of the quoted identifier
            "geom; DROP TABLE t",
            "geom)",
            'a" "b',
            "1geom",  # starts with a digit
            "geom col",  # whitespace
        ],
    )
    def test_geoparquet_geometry_column_rejects_sql_injection(self, malicious):
        """A non-identifier geometry_column must raise, never reach the SQL string."""
        D = _driver_cls()
        with pytest.raises(ValueError):
            D._source_expr("geoparquet", "/data/x.parquet", geometry_column=malicious)

    @pytest.mark.parametrize(
        "malicious",
        [
            'geometry" AS x, (SELECT 1)) --',
            "geom; DROP TABLE t",
            "1geom",
        ],
    )
    @pytest.mark.parametrize(
        "stored_type", ["BLOB", "VARCHAR"]
    )
    def test_injection_rejected_in_interpolating_branches(self, malicious, stored_type):
        """Identifier validation fires for the branches that interpolate the
        column name (BLOB→ST_GeomFromWKB, VARCHAR→ST_GeomFromText)."""
        D = _driver_cls()
        with pytest.raises(ValueError):
            D._source_expr(
                "geoparquet", "/data/x.parquet",
                geometry_column=malicious,
                geom_col_stored_type=stored_type,
            )

    @pytest.mark.parametrize(
        "odd_name",
        ['geometry" AS x, (SELECT 1)) --', "geom; DROP TABLE t", "geom col", "名前"],
    )
    def test_geometry_passthrough_ignores_column_name(self, odd_name):
        """GEOMETRY passthrough never interpolates the column name, so even an
        odd/non-identifier name is accepted (the name is irrelevant there)."""
        D = _driver_cls()
        sql = D._source_expr(
            "geoparquet", "/data/native.parquet",
            geometry_column=odd_name,
            geom_col_stored_type="GEOMETRY",
        )
        assert sql == "(SELECT * FROM read_parquet('/data/native.parquet'))"
        assert odd_name not in sql

    @pytest.mark.parametrize(
        "bad_path",
        ["/data/x.parquet' UNION SELECT 1 --", "a\nb.parquet", "x\x00.parquet"],
    )
    def test_driver_config_rejects_sql_breakout_path(self, bad_path):
        """ItemsDuckdbDriverConfig rejects paths with quote/newline/null bytes."""
        from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
        with pytest.raises(ValueError):
            ItemsDuckdbDriverConfig(path=bad_path, format="geoparquet")

    def test_driver_config_accepts_legitimate_cloud_paths(self):
        from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
        for ok in (
            "/data/x.parquet",
            "s3://bucket/prefix/*.parquet",
            "https://github.com/opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet",
        ):
            ItemsDuckdbDriverConfig(path=ok, format="geoparquet")  # must not raise

    def test_empty_geometry_column_falls_back_to_default(self):
        """An empty geometry_column must use the default 'geometry', not raise."""
        D = _driver_cls()
        # None → default "geometry"
        sql_none = D._source_expr("geoparquet", "/data/x.parquet", geometry_column=None)
        assert '"geometry"' in sql_none
        # empty string → also falls back to default via ``or``
        sql_empty = D._source_expr("geoparquet", "/data/x.parquet", geometry_column="")
        assert '"geometry"' in sql_empty

    def test_geoparquet_decode_subquery_is_parenthesised(self):
        """The returned expression must be a parenthesised subquery for use in FROM."""
        D = _driver_cls()
        sql = D._source_expr("geoparquet", "s3://bucket/data.parquet")
        assert sql.startswith("(")
        assert sql.endswith(")")

    def test_geoparquet_remote_path_passes_through_to_read_parquet(self):
        """Cloud paths are embedded inside read_parquet — DuckDB httpfs handles them."""
        D = _driver_cls()
        for scheme in ("gs://", "s3://", "https://", "http://"):
            path = f"{scheme}bucket/data.parquet"
            sql = D._source_expr("geoparquet", path)
            assert path in sql
            assert "read_parquet" in sql

    def test_plain_parquet_no_decode(self):
        """Plain ``parquet`` format must NOT emit ST_GeomFromWKB."""
        D = _driver_cls()
        sql = D._source_expr("parquet", "/data/table.parquet")
        assert "ST_GeomFromWKB" not in sql
        assert sql == "read_parquet('/data/table.parquet')"

    def test_gpkg_still_uses_st_read(self):
        """Vector formats must still go through ST_Read, not ST_GeomFromWKB."""
        D = _driver_cls()
        sql = D._source_expr("gpkg", "/data/layer.gpkg")
        assert sql == "ST_Read('/data/layer.gpkg')"
        assert "ST_GeomFromWKB" not in sql

    def test_is_geoparquet_format_recognises_aliases(self):
        D = _driver_cls()
        assert D._is_geoparquet_format("geoparquet") is True
        assert D._is_geoparquet_format("gpq") is True
        assert D._is_geoparquet_format("GEOPARQUET") is True
        assert D._is_geoparquet_format("GPQ") is True
        assert D._is_geoparquet_format("parquet") is False
        assert D._is_geoparquet_format("gpkg") is False
        assert D._is_geoparquet_format(None) is False

    def test_geoparquet_not_classified_as_vector(self):
        """geoparquet must NOT fall into the ST_Read path."""
        D = _driver_cls()
        assert D._is_vector_format("geoparquet") is False
        assert D._is_vector_format("gpq") is False


# ---------------------------------------------------------------------------
# Layer 2: Hermetic WKB-blob decode test (requires duckdb + spatial)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not _spatial_available(),
    reason="duckdb spatial extension not available in this environment",
)
class TestGeoparquetHermeticDecode:
    """End-to-end decode test using a locally generated parquet with WKB BLOB geometry.

    This exercises the BLOB → ST_GeomFromWKB branch: the fixture is written
    using ``ST_AsWKB(ST_Point(...))`` so the stored column type is BLOB, not
    a native GeoParquet GEOMETRY.
    """

    @pytest.fixture(scope="class")
    def geoparquet_fixture(self, tmp_path_factory) -> Path:
        """Create a minimal parquet file with WKB geometry (BLOB column)."""
        import duckdb

        fixture_dir = tmp_path_factory.mktemp("geoparquet_fixtures")
        fixture_path = fixture_dir / "test_points.parquet"

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        conn.execute(
            f"""
            COPY (
                SELECT
                    1 AS id,
                    'Rome' AS name,
                    ST_AsWKB(ST_Point(12.5, 41.9)) AS geometry
                UNION ALL
                SELECT
                    2 AS id,
                    'Paris' AS name,
                    ST_AsWKB(ST_Point(2.35, 48.85)) AS geometry
            ) TO '{fixture_path}' (FORMAT PARQUET)
            """
        )
        conn.close()
        assert fixture_path.exists(), "GeoParquet fixture was not created"
        return fixture_path

    def test_raw_parquet_returns_bytes(self, geoparquet_fixture: Path):
        """Sanity check: plain read_parquet returns the WKB geometry as bytes, not GEOMETRY."""
        import duckdb

        conn = duckdb.connect(":memory:")
        row = conn.execute(
            f"SELECT geometry FROM read_parquet('{geoparquet_fixture}') LIMIT 1"
        ).fetchone()
        conn.close()
        # Raw WKB should come back as bytes (BLOB)
        assert row is not None
        assert isinstance(row[0], (bytes, bytearray)), (
            f"Expected bytes from plain read_parquet of WKB fixture, got {type(row[0])}"
        )

    def test_probe_reports_blob_for_wkb_fixture(self, geoparquet_fixture: Path):
        """_probe_geom_col_stored_type returns 'BLOB' for a WKB-stored fixture."""
        import duckdb

        D = _driver_cls()
        conn = duckdb.connect(":memory:")
        stored = D._probe_geom_col_stored_type(conn, str(geoparquet_fixture), "geometry")
        conn.close()
        assert stored == "BLOB", f"Expected 'BLOB', got {stored!r}"

    def test_geoparquet_path_decodes_geometry(self, geoparquet_fixture: Path):
        """Reading via the geoparquet _source_expr (BLOB path) yields a GEOMETRY column."""
        import duckdb

        D = _driver_cls()
        # BLOB default → ST_GeomFromWKB decode
        source = D._source_expr(
            "geoparquet", str(geoparquet_fixture), geom_col_stored_type="BLOB"
        )

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        schema = conn.execute(
            f"DESCRIBE SELECT * FROM {source} LIMIT 0"
        ).fetchall()
        geom_col_type = {row[0]: row[1] for row in schema}.get("geometry", "")
        assert "GEOMETRY" in str(geom_col_type).upper(), (
            f"Expected GEOMETRY column after WKB decode, got: {geom_col_type}"
        )
        conn.close()

    def test_geoparquet_st_x_works_after_decode(self, geoparquet_fixture: Path):
        """After WKB decode, ST_X / ST_Y work on the geometry column."""
        import duckdb

        D = _driver_cls()
        source = D._source_expr(
            "geoparquet", str(geoparquet_fixture), geom_col_stored_type="BLOB"
        )

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        row = conn.execute(
            f"SELECT ST_X(geometry), ST_Y(geometry) FROM {source} WHERE id = 1"
        ).fetchone()
        conn.close()

        assert row is not None, "No rows returned from GeoParquet decode"
        lon, lat = row
        assert abs(lon - 12.5) < 0.01, f"Unexpected longitude: {lon}"
        assert abs(lat - 41.9) < 0.01, f"Unexpected latitude: {lat}"

    def test_geoparquet_geojson_serialisation(self, geoparquet_fixture: Path):
        """ST_AsGeoJSON round-trip produces a valid Point GeoJSON."""
        import duckdb

        D = _driver_cls()
        source = D._source_expr(
            "geoparquet", str(geoparquet_fixture), geom_col_stored_type="BLOB"
        )

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        row = conn.execute(
            f"SELECT ST_AsGeoJSON(geometry) FROM {source} WHERE id = 2"
        ).fetchone()
        conn.close()

        assert row is not None
        geom_json = json.loads(row[0])
        assert geom_json["type"] == "Point"
        coords = geom_json["coordinates"]
        assert abs(coords[0] - 2.35) < 0.01
        assert abs(coords[1] - 48.85) < 0.01

    def test_geometry_column_override(self, tmp_path):
        """geometry_column override redirects the WKB decode to a non-default column."""
        import duckdb

        fixture_path = tmp_path / "custom_geom_col.parquet"
        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        conn.execute(
            f"""
            COPY (
                SELECT
                    99 AS id,
                    ST_AsWKB(ST_Point(0.0, 51.5)) AS geom
            ) TO '{fixture_path}' (FORMAT PARQUET)
            """
        )

        D = _driver_cls()
        source = D._source_expr(
            "geoparquet", str(fixture_path),
            geometry_column="geom",
            geom_col_stored_type="BLOB",
        )

        assert '"geom"' in source
        row = conn.execute(
            f"SELECT ST_X(geom), ST_Y(geom) FROM {source} LIMIT 1"
        ).fetchone()
        conn.close()

        assert row is not None
        assert abs(row[0] - 0.0) < 0.01
        assert abs(row[1] - 51.5) < 0.01


# ---------------------------------------------------------------------------
# Layer 3: Hermetic native-GeoParquet test (GEOMETRY passthrough branch)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not _spatial_available(),
    reason="duckdb spatial extension not available in this environment",
)
class TestGeoparquetHermeticNative:
    """End-to-end test for parquet files where DuckDB/spatial stores a native GEOMETRY.

    DuckDB 1.x + spatial writes GeoParquet metadata when using
    ``COPY ... (FORMAT PARQUET)`` with a geometry column produced by spatial
    functions (e.g. ``ST_Point``).  ``read_parquet`` then decodes the column
    automatically and returns it as a GEOMETRY, not BLOB.

    This exercises the GEOMETRY passthrough branch: the probe returns
    ``"GEOMETRY"`` and ``_source_expr`` emits a plain subquery with no
    ST_GeomFromWKB wrapper.
    """

    @pytest.fixture(scope="class")
    def native_geoparquet_fixture(self, tmp_path_factory) -> Path:
        """Create a parquet file using DuckDB's native spatial GEOMETRY column."""
        import duckdb

        fixture_dir = tmp_path_factory.mktemp("native_geoparquet_fixtures")
        fixture_path = fixture_dir / "native_points.parquet"

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        # ST_Point produces a GEOMETRY; COPY writes GeoParquet metadata so
        # read_parquet automatically returns a native GEOMETRY column.
        conn.execute(
            f"""
            COPY (
                SELECT
                    1 AS id,
                    'Tokyo' AS name,
                    ST_Point(139.69, 35.68) AS geometry
                UNION ALL
                SELECT
                    2 AS id,
                    'Nairobi' AS name,
                    ST_Point(36.82, -1.29) AS geometry
            ) TO '{fixture_path}' (FORMAT PARQUET)
            """
        )
        conn.close()
        assert fixture_path.exists(), "Native GeoParquet fixture was not created"
        return fixture_path

    def test_raw_parquet_returns_geometry_not_bytes(self, native_geoparquet_fixture: Path):
        """Sanity: plain read_parquet returns GEOMETRY for a native-spatial parquet."""
        import duckdb

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        schema = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{native_geoparquet_fixture}') LIMIT 0"
        ).fetchall()
        col_types = {row[0]: row[1] for row in schema}
        geom_type = str(col_types.get("geometry", "")).upper()
        conn.close()
        assert "GEOMETRY" in geom_type, (
            f"Expected GEOMETRY type for native parquet, got: {geom_type!r}"
        )

    def test_probe_reports_geometry_for_native_fixture(self, native_geoparquet_fixture: Path):
        """_probe_geom_col_stored_type returns 'GEOMETRY' for a native spatial parquet."""
        import duckdb

        D = _driver_cls()
        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        stored = D._probe_geom_col_stored_type(
            conn, str(native_geoparquet_fixture), "geometry"
        )
        conn.close()
        assert stored == "GEOMETRY", f"Expected 'GEOMETRY', got {stored!r}"

    def test_passthrough_source_expr_has_no_wkb_wrap(self, native_geoparquet_fixture: Path):
        """GEOMETRY passthrough branch emits no ST_GeomFromWKB."""
        D = _driver_cls()
        source = D._source_expr(
            "geoparquet", str(native_geoparquet_fixture),
            geom_col_stored_type="GEOMETRY",
        )
        assert "ST_GeomFromWKB" not in source
        assert "ST_GeomFromText" not in source
        assert f"read_parquet('{native_geoparquet_fixture}')" in source

    def test_native_geoparquet_st_x_works_via_passthrough(self, native_geoparquet_fixture: Path):
        """ST_X / ST_Y work after the GEOMETRY passthrough (no decode needed)."""
        import duckdb

        D = _driver_cls()
        source = D._source_expr(
            "geoparquet", str(native_geoparquet_fixture),
            geom_col_stored_type="GEOMETRY",
        )

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        row = conn.execute(
            f"SELECT ST_X(geometry), ST_Y(geometry) FROM {source} WHERE id = 1"
        ).fetchone()
        conn.close()

        assert row is not None, "No rows returned from native GeoParquet passthrough"
        lon, lat = row
        assert abs(lon - 139.69) < 0.01, f"Unexpected longitude: {lon}"
        assert abs(lat - 35.68) < 0.01, f"Unexpected latitude: {lat}"

    def test_native_geoparquet_geojson_via_passthrough(self, native_geoparquet_fixture: Path):
        """ST_AsGeoJSON works on the native GEOMETRY column after passthrough."""
        import duckdb

        D = _driver_cls()
        source = D._source_expr(
            "geoparquet", str(native_geoparquet_fixture),
            geom_col_stored_type="GEOMETRY",
        )

        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        row = conn.execute(
            f"SELECT ST_AsGeoJSON(geometry) FROM {source} WHERE id = 2"
        ).fetchone()
        conn.close()

        assert row is not None
        geom_json = json.loads(row[0])
        assert geom_json["type"] == "Point"
        coords = geom_json["coordinates"]
        assert abs(coords[0] - 36.82) < 0.01
        assert abs(coords[1] - -1.29) < 0.01

    def test_probe_then_passthrough_round_trip(self, native_geoparquet_fixture: Path):
        """Probe → source_expr → query: full round-trip for the GEOMETRY branch."""
        import duckdb

        D = _driver_cls()
        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        stored_type = D._probe_geom_col_stored_type(
            conn, str(native_geoparquet_fixture), "geometry"
        )
        assert stored_type == "GEOMETRY"

        source = D._source_expr(
            "geoparquet", str(native_geoparquet_fixture),
            geom_col_stored_type=stored_type,
        )
        count = conn.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
        conn.close()
        assert count == 2


# ---------------------------------------------------------------------------
# Layer 4: Optional live test — OGC example.parquet from HTTPS
# ---------------------------------------------------------------------------

_OGC_EXAMPLE_URL = (
    "https://github.com/opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"
)


def _fetch_url_to_tmp(url: str) -> Optional[Path]:
    """Fetch *url* via curl into a temp file; return the path or None on failure."""
    try:
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp.close()
        result = subprocess.run(
            ["curl", "-fsSL", "--max-time", "20", url, "-o", tmp.name],
            capture_output=True,
            timeout=25,
        )
        if result.returncode == 0 and Path(tmp.name).stat().st_size > 0:
            return Path(tmp.name)
        os.unlink(tmp.name)
        return None
    except Exception:
        return None


@pytest.mark.skipif(
    not _spatial_available(),
    reason="duckdb spatial extension not available",
)
class TestGeoparquetLiveOgcExample:
    """Live test against the OGC reference GeoParquet file.

    The OGC example.parquet is a spec-compliant GeoParquet file: DuckDB 1.x +
    spatial returns its geometry column as a native GEOMETRY, not BLOB.  These
    tests use ``_probe_geom_col_stored_type`` to determine the correct decode
    branch at runtime, then pass the result into ``_source_expr``.

    Skipped when the file cannot be downloaded (offline / CI without egress).
    """

    @pytest.fixture(scope="class")
    def ogc_fixture(self) -> Path:
        path = _fetch_url_to_tmp(_OGC_EXAMPLE_URL)
        if path is None:
            pytest.skip("OGC GeoParquet example not reachable; skipping live test")
        return path

    def test_probe_returns_geometry_for_ogc_file(self, ogc_fixture: Path):
        """The OGC file's geometry column probes as GEOMETRY (not BLOB) in DuckDB 1.x."""
        import duckdb

        D = _driver_cls()
        conn = duckdb.connect(":memory:")
        stored = D._probe_geom_col_stored_type(conn, str(ogc_fixture), "geometry")
        conn.close()
        assert stored == "GEOMETRY", (
            f"Expected OGC example to probe as GEOMETRY, got {stored!r}. "
            "If DuckDB version changed the auto-decode behaviour, update this assertion."
        )

    def test_live_file_has_features(self, ogc_fixture: Path):
        """The live OGC file returns > 0 features using the probed decode branch."""
        import duckdb

        D = _driver_cls()
        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        stored_type = D._probe_geom_col_stored_type(conn, str(ogc_fixture), "geometry")
        if stored_type == "UNKNOWN":
            stored_type = "BLOB"
        source = D._source_expr("geoparquet", str(ogc_fixture), geom_col_stored_type=stored_type)

        count = conn.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
        conn.close()

        assert count > 0, "OGC example GeoParquet returned 0 features"

    def test_live_file_geometry_is_usable(self, ogc_fixture: Path):
        """All rows produce a non-null ST_AsGeoJSON via the probed decode branch."""
        import duckdb

        D = _driver_cls()
        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        stored_type = D._probe_geom_col_stored_type(conn, str(ogc_fixture), "geometry")
        if stored_type == "UNKNOWN":
            stored_type = "BLOB"
        source = D._source_expr("geoparquet", str(ogc_fixture), geom_col_stored_type=stored_type)

        null_count = conn.execute(
            f"SELECT COUNT(*) FROM {source} WHERE ST_AsGeoJSON(geometry) IS NULL"
        ).fetchone()[0]
        total = conn.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]

        # Sample one geometry to verify it's a real Point/Polygon/etc.
        sample_row = conn.execute(
            f"SELECT ST_AsGeoJSON(geometry) FROM {source} LIMIT 1"
        ).fetchone()
        conn.close()

        assert null_count == 0, (
            f"{null_count}/{total} rows have null geometry after decode "
            f"(stored_type={stored_type!r})"
        )
        assert sample_row is not None
        sample_geom = json.loads(sample_row[0])
        assert "type" in sample_geom, f"Sample geometry is not valid GeoJSON: {sample_geom}"

    def test_live_file_sample_st_x_works(self, ogc_fixture: Path):
        """ST_X returns a finite coordinate value on a sample geometry."""
        import duckdb
        import math

        D = _driver_cls()
        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")

        stored_type = D._probe_geom_col_stored_type(conn, str(ogc_fixture), "geometry")
        if stored_type == "UNKNOWN":
            stored_type = "BLOB"
        source = D._source_expr("geoparquet", str(ogc_fixture), geom_col_stored_type=stored_type)

        # Compute the centroid of each geometry and verify ST_X is finite.
        row = conn.execute(
            f"SELECT ST_X(ST_Centroid(geometry)) FROM {source} LIMIT 1"
        ).fetchone()
        conn.close()

        assert row is not None and row[0] is not None
        assert math.isfinite(row[0]), f"ST_X(centroid) is not finite: {row[0]}"

    def test_cleanup(self, ogc_fixture: Path):
        """Remove the downloaded fixture after the live tests."""
        try:
            ogc_fixture.unlink(missing_ok=True)
        except Exception:
            pass
