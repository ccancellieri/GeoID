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

"""Regression coverage: a zipped shapefile must be EXTRACTED to local disk and
read from there, not opened in place over ``/vsizip//vsigs/``.

Reading a zipped shapefile in place forces GDAL to decompress the archive member
into memory and keep it resident for the random access the .shx index drives, so
RSS grows with read progress and OOMs the ingestion worker mid-stream on a large
layer. ``GdalOsgeoReader.open`` extracts the archive once to the temp disk (a
mounted bucket volume in the deployment) and opens the extracted dataset, so
feature iteration does bounded range reads.
"""

from __future__ import annotations

import inspect
import os

import pytest

# The reader hard-imports ``osgeo`` at module load to gate its registration, so
# the whole module (helpers included) is unimportable without GDAL. Skip the
# file where GDAL is absent (local dev); CI's GDAL image runs it.
pytest.importorskip("osgeo")

from dynastore.tasks.ingestion.readers.osgeo_reader import GdalOsgeoReader  # noqa: E402


# ---------------------------------------------------------------------------
# Pure helpers (no GDAL) — run everywhere
# ---------------------------------------------------------------------------


def test_find_local_dataset_prefers_shapefile(tmp_path):
    (tmp_path / "data.dbf").write_bytes(b"x")
    (tmp_path / "data.shp").write_bytes(b"x")
    (tmp_path / "readme.txt").write_text("hi")
    found = GdalOsgeoReader._find_local_dataset(str(tmp_path))
    assert found.endswith("data.shp")


def test_find_local_dataset_searches_nested(tmp_path):
    nested = tmp_path / "inner"
    nested.mkdir()
    (nested / "layer.gpkg").write_bytes(b"x")
    found = GdalOsgeoReader._find_local_dataset(str(tmp_path))
    assert found.endswith(os.path.join("inner", "layer.gpkg"))


def test_find_local_dataset_falls_back_to_dir(tmp_path):
    (tmp_path / "notes.txt").write_text("nothing geospatial")
    found = GdalOsgeoReader._find_local_dataset(str(tmp_path))
    assert found == str(tmp_path)


def test_safe_extractall_rejects_zip_slip(tmp_path):
    """A crafted ``../`` member must be refused, not written outside dest."""
    import zipfile

    evil = tmp_path / "evil.zip"
    with zipfile.ZipFile(evil, "w") as zf:
        zf.writestr("../escaped.txt", "pwned")
    dest = tmp_path / "extract"
    dest.mkdir()
    with zipfile.ZipFile(evil) as zf:
        with pytest.raises(RuntimeError, match="Zip-Slip|escape"):
            GdalOsgeoReader._safe_extractall(zf, str(dest))
    assert not (tmp_path / "escaped.txt").exists()


def test_safe_extractall_allows_normal_members(tmp_path):
    import zipfile

    ok = tmp_path / "ok.zip"
    with zipfile.ZipFile(ok, "w") as zf:
        zf.writestr("a.shp", "x")
        zf.writestr("nested/b.dbf", "y")
    dest = tmp_path / "extract"
    dest.mkdir()
    with zipfile.ZipFile(ok) as zf:
        GdalOsgeoReader._safe_extractall(zf, str(dest))
    assert (dest / "a.shp").exists()
    assert (dest / "nested" / "b.dbf").exists()


def test_open_source_extracts_for_zip_not_vsizip():
    """The zip branch must route through extraction, never the in-memory
    ``/vsizip/`` wrap — guards against a refactor reintroducing the OOM."""
    src = inspect.getsource(GdalOsgeoReader.open)
    assert "_extract_archive_to_local" in src, (
        "GdalOsgeoReader.open no longer extracts zip archives to local disk — a "
        "large zipped shapefile read in place over /vsizip will decompress into "
        "memory and OOM the worker. Restore the extract-to-disk path."
    )
    # The extract path opens the extracted dataset directly; /vsizip must NOT be
    # how a zip is read anymore.
    assert "/vsizip/" not in src


# ---------------------------------------------------------------------------
# End-to-end via real GDAL (skipped where osgeo is absent, runs in CI)
# ---------------------------------------------------------------------------


def _make_shapefile_zip(tmp_path) -> str:
    import zipfile

    from osgeo import ogr, osr

    shp_dir = tmp_path / "shp"
    shp_dir.mkdir()
    drv = ogr.GetDriverByName("ESRI Shapefile")
    ds = drv.CreateDataSource(str(shp_dir / "pts.shp"))
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    layer = ds.CreateLayer("pts", srs, ogr.wkbPoint)
    layer.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    for i, nm in enumerate(["a", "b", "c"]):
        feat = ogr.Feature(layer.GetLayerDefn())
        feat.SetField("name", nm)
        g = ogr.Geometry(ogr.wkbPoint)
        g.AddPoint(float(i), float(i))
        feat.SetGeometry(g)
        layer.CreateFeature(feat)
        feat = None
    ds = None

    zpath = tmp_path / "pts.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        for fn in os.listdir(shp_dir):
            zf.write(str(shp_dir / fn), fn)
    return str(zpath)


def test_open_zip_extracts_and_reads_features(tmp_path):
    pytest.importorskip("osgeo")
    zip_path = _make_shapefile_zip(tmp_path)
    reader = GdalOsgeoReader()
    with reader.open(zip_path, content_type="application/zip") as features:
        records = list(features)
    assert len(records) == 3
    assert sorted(r["properties"]["name"] for r in records) == ["a", "b", "c"]
    assert all(r["geometry"]["type"] == "Point" for r in records)


def test_open_zip_cleans_up_temp_dir(tmp_path, monkeypatch):
    pytest.importorskip("osgeo")

    zip_path = _make_shapefile_zip(tmp_path)
    created = []

    from dynastore.models.protocols.temp_dir import DefaultTempDir
    real_mkdtemp = DefaultTempDir.mkdtemp

    def _spy_mkdtemp(self, **k):
        d = real_mkdtemp(self, **k)
        created.append(d)
        return d

    monkeypatch.setattr(DefaultTempDir, "mkdtemp", _spy_mkdtemp)
    reader = GdalOsgeoReader()
    with reader.open(zip_path, content_type="application/zip") as features:
        list(features)
    assert created, "extraction should allocate a temp dir"
    for d in created:
        assert not os.path.exists(d), "temp extraction dir must be removed on exit"
