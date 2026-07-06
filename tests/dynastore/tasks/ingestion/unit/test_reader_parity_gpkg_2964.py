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

"""Parity coverage for GeoID #2964: switching a GeoPackage's default reader
from the row-by-row ``GdalOsgeoReader`` to the vectorized/chunked
``PyogrioReader`` must not change what gets ingested — same rows, same
geometries, and (critically) the same per-row identity.

Builds a real on-disk GeoPackage with the GDAL writer itself (not a
hand-rolled byte fixture) so both readers open the actual GPKG/SQLite
format the demo's 8.4M-feature source uses, then reads it with each
reader and asserts the canonical output is identical.

Requires both GDAL (``osgeo``) and ``pyogrio`` — skipped where either is
not installed (local dev without ``module_gdal``/``geospatial_io`` SCOPE);
runs in CI's combined image, same convention as ``test_osgeo_reader_fid.py``
and ``test_reader_priority_pyogrio_first_2964.py``.
"""

from __future__ import annotations

import pytest

pytest.importorskip("osgeo")
pytest.importorskip("pyogrio")

from osgeo import ogr  # noqa: E402

from dynastore.tasks.ingestion.readers.osgeo_reader import GdalOsgeoReader  # noqa: E402
from dynastore.tasks.ingestion.readers.pyogrio_reader import PyogrioReader  # noqa: E402

ogr.UseExceptions()

_NUM_FEATURES = 23  # deliberately not a multiple of the small chunk size below


@pytest.fixture()
def gpkg_path(tmp_path) -> str:
    """A real on-disk GeoPackage, written via GDAL's own GPKG driver.

    Mixed field types (string, int, float) including a zero and a null
    value — falsy-zero/None coercion is a recurring bug class in this
    codebase (#1820) — plus Point geometries with distinct coordinates so
    row order/identity is independently checkable.
    """
    path = str(tmp_path / "parity.gpkg")
    drv = ogr.GetDriverByName("GPKG")
    ds = drv.CreateDataSource(path)
    layer = ds.CreateLayer("parity_layer", geom_type=ogr.wkbPoint)
    layer.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("val", ogr.OFTInteger))
    layer.CreateField(ogr.FieldDefn("ratio", ogr.OFTReal))
    for i in range(_NUM_FEATURES):
        feat = ogr.Feature(layer.GetLayerDefn())
        feat.SetField("name", f"feature-{i}")
        feat.SetField("val", i)  # i == 0 exercises the falsy-zero case
        if i % 5 != 0:
            feat.SetField("ratio", i / 2.0)
        # else: leave "ratio" unset (NULL) for every 5th row
        feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({i} {-i})"))
        layer.CreateFeature(feat)
        feat = None
    ds = None  # noqa: F841 — flush to disk
    return path


def _listify(value):
    """Recursively turn tuples into lists (e.g. GeoJSON ``coordinates``).

    GdalOsgeoReader emits geometry via ``json.loads(geom.ExportToJson())``
    (plain lists); PyogrioReader's GeoDataFrame-backed ``iterfeatures()``
    emits shapely's ``__geo_interface__`` (tuples). Both encode to the same
    JSON on the wire — this is a pre-existing, benign Python-level
    representation difference between the two readers, unrelated to this
    test's identity/row-parity claim, so it is normalised away here.
    """
    if isinstance(value, (list, tuple)):
        return [_listify(v) for v in value]
    if isinstance(value, dict):
        return {k: _listify(v) for k, v in value.items()}
    return value


def _canonical(records: list[dict]) -> list[tuple]:
    """Reduce a reader's output to (id, properties, geometry) tuples, sorted
    by id, so the two readers' outputs can be compared independent of the
    concrete dict/key ordering or wrapper type each one returns.

    ``id`` is coerced to ``int`` — GdalOsgeoReader emits the OGR FID as a
    Python int (``feat.GetFID()``), while PyogrioReader's GeoDataFrame-backed
    ``iterfeatures()`` stringifies it; that surface difference is a
    pre-existing reader quirk unrelated to this test's identity/row-parity
    claim, so it is normalised away here rather than asserted on.
    """
    out = []
    for r in records:
        props = {k: v for k, v in r["properties"].items()}
        out.append((int(r["id"]), props, _listify(r["geometry"])))
    return sorted(out, key=lambda t: t[0])


def test_pyogrio_matches_gdal_osgeo_single_read(gpkg_path):
    """Whole-file read (chunk_size >= feature count): sanity baseline before
    the multi-chunk case below is trusted."""
    with GdalOsgeoReader().open(gpkg_path) as records:
        gdal_rows = _canonical(list(records))
    with PyogrioReader().open(gpkg_path, read_batch_size=1000) as records:
        pyogrio_rows = _canonical(list(records))

    assert len(gdal_rows) == _NUM_FEATURES
    assert gdal_rows == pyogrio_rows


def test_pyogrio_matches_gdal_osgeo_multi_chunk(gpkg_path):
    """The real-world case: a chunk size far smaller than the feature count,
    the same shape as the demo's 8.4M-row ingest reading at
    ``read_batch_size=1000``. This is the case the fid_as_index regression
    only shows up in — a chunk_size >= feature count read never exercises a
    second ``pyogrio.read_dataframe`` call."""
    with GdalOsgeoReader().open(gpkg_path) as records:
        gdal_rows = _canonical(list(records))
    with PyogrioReader().open(gpkg_path, read_batch_size=7) as records:
        pyogrio_rows = _canonical(list(records))

    assert len(pyogrio_rows) == _NUM_FEATURES
    assert gdal_rows == pyogrio_rows


def test_pyogrio_ids_are_globally_unique_across_chunks(gpkg_path):
    """Regression guard for the identity-collision bug this test file was
    written to catch: without ``fid_as_index=True`` on the paginated
    ``pyogrio.read_dataframe`` calls, every chunk after the first re-emits
    ids starting from the chunk-local index 0, colliding with earlier
    chunks. ``prepare_record_for_upsert`` (main_ingestion.py) falls back to
    this reader-surfaced "id" as the upsert identity whenever no
    ``column_mapping.external_id`` is configured (GeoID #2709 tier 2) — a
    collision here silently overwrites distinct source rows on ingest
    instead of inserting all of them."""
    with PyogrioReader().open(gpkg_path, read_batch_size=7) as records:
        ids = [int(r["id"]) for r in records]
    assert len(ids) == len(set(ids)), (
        f"PyogrioReader emitted duplicate ids across chunk pages: {ids}"
    )


def test_pyogrio_ids_match_gdal_osgeo_fids(gpkg_path):
    """The two readers must agree on a row's identity, not just its content
    — GeoPackage's OGR FID is 1-based, and PyogrioReader's ``fid_as_index``
    must surface that exact value rather than inventing its own
    (0-based or per-chunk-local) numbering."""
    with GdalOsgeoReader().open(gpkg_path) as records:
        gdal_ids_by_name = {r["properties"]["name"]: int(r["id"]) for r in records}
    with PyogrioReader().open(gpkg_path, read_batch_size=7) as records:
        pyogrio_ids_by_name = {r["properties"]["name"]: int(r["id"]) for r in records}

    assert pyogrio_ids_by_name == gdal_ids_by_name
