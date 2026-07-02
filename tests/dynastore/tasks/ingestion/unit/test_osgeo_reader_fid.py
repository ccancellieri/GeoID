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

"""Regression coverage for GeoID #2709: ``GdalOsgeoReader`` must surface the
OGR feature id (FID) as the yielded record's top-level ``"id"`` so that
re-ingesting the SAME unmodified source converges on the same identity
instead of minting a fresh one every run.

Requires GDAL (``osgeo``) — skipped where it is not installed (local dev
without ``module_gdal`` SCOPE); runs in CI's GDAL image, same convention as
``test_osgeo_zip_extract.py``.
"""
from __future__ import annotations

import pytest

pytest.importorskip("osgeo")

from osgeo import ogr  # noqa: E402

from dynastore.tasks.ingestion.readers.osgeo_reader import GdalOsgeoReader  # noqa: E402


def _make_memory_datasource(num_features: int = 3):
    """Build an in-memory OGR datasource with sequential FIDs 0..N-1 and one
    string attribute, mirroring what a shapefile/GeoPackage layer provides."""
    drv = ogr.GetDriverByName("Memory")
    ds = drv.CreateDataSource("mem")
    layer = ds.CreateLayer("test_layer")
    layer.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    for i in range(num_features):
        feat = ogr.Feature(layer.GetLayerDefn())
        feat.SetField("name", f"feature-{i}")
        wkt = f"POINT ({i} {i})"
        feat.SetGeometry(ogr.CreateGeometryFromWkt(wkt))
        feat.SetFID(i)
        layer.CreateFeature(feat)
        feat = None
    return ds


def test_iter_features_surfaces_ogr_fid_as_id():
    ds = _make_memory_datasource(3)
    records = list(GdalOsgeoReader._iter_features(ds))
    assert [r["id"] for r in records] == [0, 1, 2]


def test_iter_features_fid_zero_is_present_not_dropped():
    """FID 0 (the first feature) must appear, not be silently omitted —
    regression guard mirroring the identity-layer falsy-zero fix."""
    ds = _make_memory_datasource(1)
    records = list(GdalOsgeoReader._iter_features(ds))
    assert records[0]["id"] == 0


def test_iter_features_ids_stable_across_repeated_reads():
    """Re-opening/re-reading the SAME (unmodified) datasource must yield the
    identical FID sequence both times — the property that makes a re-run of
    the ingestion task converge instead of duplicating (#2709)."""
    ds = _make_memory_datasource(5)
    first_pass = [r["id"] for r in GdalOsgeoReader._iter_features(ds)]
    ds.GetLayer(0).ResetReading()
    second_pass = [r["id"] for r in GdalOsgeoReader._iter_features(ds)]
    assert first_pass == second_pass == [0, 1, 2, 3, 4]
