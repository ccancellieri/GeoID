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

"""``DuckDbReader`` — the GeoID #2981 A/B-test Parquet reader.

Gated on ``duckdb`` + the ``spatial`` extension being loadable, same
convention ``test_duckdb_geoparquet.py`` uses for the storage driver.
"""

from __future__ import annotations

import pytest

pytest.importorskip("duckdb")

from dynastore.tasks.ingestion.readers.duckdb_reader import DuckDbReader  # noqa: E402


def _spatial_available() -> bool:
    """Return True when DuckDB + the spatial extension can both load."""
    import duckdb

    try:
        conn = duckdb.connect(":memory:")
        conn.install_extension("spatial")
        conn.load_extension("spatial")
        conn.close()
        return True
    except Exception:
        return False


requires_spatial = pytest.mark.skipif(
    not _spatial_available(), reason="duckdb 'spatial' extension unavailable",
)


@pytest.fixture()
def native_geometry_parquet(tmp_path):
    """A spec-compliant GeoParquet fixture: ST_Point written natively, so
    ``read_parquet`` (with ``spatial`` loaded) auto-decodes it to DuckDB's
    ``GEOMETRY`` type."""
    import duckdb

    path = tmp_path / "native.parquet"
    con = duckdb.connect(":memory:")
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    con.execute(
        "COPY (SELECT i AS id, 'feat_' || i AS name, "
        "ST_Point(i::DOUBLE, i::DOUBLE) AS geometry FROM range(0, 7) t(i)) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    return str(path)


@pytest.fixture()
def blob_wkb_parquet(tmp_path):
    """A legacy-exporter-style fixture: raw WKB bytes stored as BLOB
    (e.g. what GeoPandas' ``to_parquet`` produces) instead of DuckDB's
    native GEOMETRY type."""
    import duckdb

    path = tmp_path / "blob.parquet"
    con = duckdb.connect(":memory:")
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    con.execute(
        "COPY (SELECT i AS id, 'feat_' || i AS name, "
        "ST_AsWKB(ST_Point(i::DOUBLE, i::DOUBLE)) AS geometry FROM range(0, 4) t(i)) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    return str(path)


@pytest.fixture()
def no_geometry_parquet(tmp_path):
    """A plain tabular Parquet with no ``geometry`` column at all."""
    import duckdb

    path = tmp_path / "plain.parquet"
    con = duckdb.connect(":memory:")
    con.execute(
        "COPY (SELECT i AS id, 'row_' || i AS label FROM range(0, 3) t(i)) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    return str(path)


# ---------------------------------------------------------------------------
# Registration / priority — never auto-wins
# ---------------------------------------------------------------------------


def test_priority_never_wins_auto_scan():
    """priority=200 must sit strictly behind both existing readers so this
    reader is reachable ONLY via an explicit reader='duckdb' override."""
    assert DuckDbReader.priority == 200
    assert DuckDbReader.reader_id == "duckdb"


def test_can_read_matches_parquet_extensions():
    assert DuckDbReader.can_read("gs://b/x/file.parquet")
    assert DuckDbReader.can_read("gs://b/x/file.geoparquet")
    assert not DuckDbReader.can_read("gs://b/x/file.gpkg")


# ---------------------------------------------------------------------------
# Reading — native GEOMETRY branch
# ---------------------------------------------------------------------------


@requires_spatial
def test_open_decodes_native_geometry_to_geojson(native_geometry_parquet):
    reader = DuckDbReader()
    with reader.open(native_geometry_parquet) as records:
        feats = list(records)
    assert len(feats) == 7
    names = {f["properties"]["name"] for f in feats}
    assert names == {f"feat_{i}" for i in range(7)}
    for f in feats:
        assert f["type"] == "Feature"
        assert f["geometry"]["type"] == "Point"
        assert "geometry" not in f["properties"]  # popped out, not duplicated


# ---------------------------------------------------------------------------
# Reading — BLOB/WKB branch
# ---------------------------------------------------------------------------


@requires_spatial
def test_open_decodes_blob_wkb_to_geojson(blob_wkb_parquet):
    reader = DuckDbReader()
    with reader.open(blob_wkb_parquet) as records:
        feats = list(records)
    assert len(feats) == 4
    for f in feats:
        assert f["geometry"]["type"] == "Point"


# ---------------------------------------------------------------------------
# Reading — no geometry column at all: plain passthrough, no crash
# ---------------------------------------------------------------------------


def test_open_no_geometry_column_passes_through(no_geometry_parquet):
    reader = DuckDbReader()
    with reader.open(no_geometry_parquet) as records:
        feats = list(records)
    assert len(feats) == 3
    for f in feats:
        assert f["geometry"] is None
        assert "label" in f["properties"]


# ---------------------------------------------------------------------------
# Chunking — never materialises the whole result set at once
# ---------------------------------------------------------------------------


@requires_spatial
def test_open_chunked_yields_all_features_regardless_of_batch_size(native_geometry_parquet):
    reader = DuckDbReader()
    with reader.open(native_geometry_parquet, read_batch_size=2) as records:
        feats = list(records)
    assert len(feats) == 7
    ids = {f["properties"]["id"] for f in feats}
    assert ids == set(range(7))


# ---------------------------------------------------------------------------
# feature_count
# ---------------------------------------------------------------------------


def test_feature_count(no_geometry_parquet):
    assert DuckDbReader().feature_count(no_geometry_parquet) == 3


def test_feature_count_bad_path_returns_none():
    assert DuckDbReader().feature_count("/nonexistent/path/nope.parquet") is None


# ---------------------------------------------------------------------------
# Documented gs:// limitation — no invented GCS credential plumbing
# ---------------------------------------------------------------------------


def test_open_gs_uri_raises_documented_not_implemented():
    reader = DuckDbReader()
    with pytest.raises(NotImplementedError, match="gs://"):
        with reader.open("gs://bucket/collections/x/data.parquet"):
            pass


def test_feature_count_gs_uri_returns_none_not_raise():
    """feature_count's base-class contract is best-effort/None-on-failure,
    not an exception — the gs:// limitation must respect that."""
    assert DuckDbReader().feature_count("gs://bucket/x/data.parquet") is None
