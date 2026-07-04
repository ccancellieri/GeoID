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

"""Coverage for GeoID #2958: ``GdalOsgeoReader`` offset-resume must use a
native OGR seek where the driver supports random access, and a
heartbeat-logged discard loop otherwise — never a silent per-row skip.

Requires GDAL (``osgeo``) — skipped where it is not installed (local dev
without ``module_gdal`` SCOPE); runs in CI's GDAL image, same convention as
``test_osgeo_reader_fid.py``.
"""
from __future__ import annotations

import logging

import pytest

pytest.importorskip("osgeo")

from osgeo import ogr  # noqa: E402

import dynastore.tasks.ingestion.readers.osgeo_reader as osgeo_reader_mod  # noqa: E402
from dynastore.tasks.ingestion.readers.osgeo_reader import GdalOsgeoReader  # noqa: E402


def _make_memory_datasource(num_features: int = 10, layer_name: str = "test_layer"):
    """In-memory OGR datasource — ``Memory`` driver reports
    ``OLCFastSetNextByIndex`` True, so this exercises the native-seek path."""
    drv = ogr.GetDriverByName("Memory")
    ds = drv.CreateDataSource("mem")
    layer = ds.CreateLayer(layer_name)
    layer.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    for i in range(num_features):
        feat = ogr.Feature(layer.GetLayerDefn())
        feat.SetField("name", f"feature-{i}")
        feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({i} {i})"))
        feat.SetFID(i)
        layer.CreateFeature(feat)
        feat = None
    return ds


def _make_csv_datasource(tmp_path, num_features: int = 10):
    """On-disk CSV datasource — the CSV driver reports
    ``OLCFastSetNextByIndex`` False, so this exercises the heartbeat
    discard-iterate fallback path."""
    path = str(tmp_path / "offset_source.csv")
    drv = ogr.GetDriverByName("CSV")
    ds = drv.CreateDataSource(path)
    layer = ds.CreateLayer("offset_source", geom_type=ogr.wkbNone)
    layer.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    for i in range(num_features):
        feat = ogr.Feature(layer.GetLayerDefn())
        feat.SetField("name", f"feature-{i}")
        layer.CreateFeature(feat)
        feat = None
    ds = None  # noqa: F841 — flush to disk
    return ogr.Open(path)


def test_capability_assumption_memory_is_fast_csv_is_not():
    """Guard the test fixtures' premise: Memory reports the fast capability,
    CSV does not. If GDAL ever changes this, the two tests below would
    silently stop exercising the branch they claim to."""
    mem_ds = _make_memory_datasource(1)
    assert mem_ds.GetLayer(0).TestCapability(ogr.OLCFastSetNextByIndex) is True


def test_offset_zero_yields_all_records():
    ds = _make_memory_datasource(5)
    records = list(GdalOsgeoReader._iter_features(ds, offset=0))
    assert [r["id"] for r in records] == [0, 1, 2, 3, 4]


def test_offset_skips_correct_number_of_records():
    ds = _make_memory_datasource(10)
    records = list(GdalOsgeoReader._iter_features(ds, offset=4))
    assert [r["id"] for r in records] == [4, 5, 6, 7, 8, 9]


def test_offset_beyond_end_yields_nothing():
    ds = _make_memory_datasource(3)
    records = list(GdalOsgeoReader._iter_features(ds, offset=100))
    assert records == []


def test_offset_native_seek_used_for_fast_capability_driver(caplog):
    """Memory reports OLCFastSetNextByIndex — the skip must go through
    ``SetNextByIndex``, not a Python discard loop."""
    ds = _make_memory_datasource(10)
    with caplog.at_level(logging.INFO, logger=osgeo_reader_mod.__name__):
        records = list(GdalOsgeoReader._iter_features(ds, offset=6))
    assert [r["id"] for r in records] == [6, 7, 8, 9]
    assert any(
        "native seek" in rec.message and "OLCFastSetNextByIndex" in rec.message
        for rec in caplog.records
    ), f"expected a native-seek log line, got: {[r.message for r in caplog.records]}"


def test_offset_heartbeat_logged_when_no_fast_seek(tmp_path, caplog, monkeypatch):
    """CSV has no fast ``SetNextByIndex`` — the skip must fall back to the
    discard-iterate loop AND emit a heartbeat log line so a slow skip is
    still observable (the actual bug reported in #2958)."""
    monkeypatch.setattr(osgeo_reader_mod, "_OFFSET_SKIP_HEARTBEAT_INTERVAL", 2)
    ds = _make_csv_datasource(tmp_path, num_features=10)
    assert ds.GetLayer(0).TestCapability(ogr.OLCFastSetNextByIndex) is False

    with caplog.at_level(logging.INFO, logger=osgeo_reader_mod.__name__):
        records = list(GdalOsgeoReader._iter_features(ds, offset=6))

    assert [r["properties"]["name"] for r in records] == [
        "feature-6", "feature-7", "feature-8", "feature-9",
    ]
    heartbeat_lines = [
        rec.message for rec in caplog.records
        if "no fast native seek" in rec.message
    ]
    assert heartbeat_lines, "expected at least one heartbeat log line during the skip"


def test_offset_spans_multiple_layers():
    """A multi-layer dataset concatenates features layer-by-layer (existing
    behaviour); an offset larger than the first layer's feature count must
    skip that whole layer and carry the remainder into the next one."""
    drv = ogr.GetDriverByName("Memory")
    ds = drv.CreateDataSource("mem_multi")
    for layer_idx, prefix in enumerate(("a", "b")):
        layer = ds.CreateLayer(f"layer_{prefix}")
        layer.CreateField(ogr.FieldDefn("name", ogr.OFTString))
        for i in range(5):
            feat = ogr.Feature(layer.GetLayerDefn())
            feat.SetField("name", f"{prefix}-{i}")
            feat.SetFID(i)
            layer.CreateFeature(feat)
            feat = None

    # First layer has 5 features; offset=7 skips it whole and lands 2 rows
    # into the second layer.
    records = list(GdalOsgeoReader._iter_features(ds, offset=7))
    assert [r["properties"]["name"] for r in records] == ["b-2", "b-3", "b-4"]


def test_supports_offset_seek_flag_is_true():
    assert GdalOsgeoReader.supports_offset_seek is True
