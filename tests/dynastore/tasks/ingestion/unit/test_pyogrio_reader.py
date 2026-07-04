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

"""PyogrioReader — the pyogrio-backed fallback reader (replaces FionaReader).

Verifies the reader yields GeoJSON-shaped records and reports a feature
count, using a tiny on-disk GeoJSON so the test needs only pyogrio (no
system osgeo bindings).
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("pyogrio")
pytest.importorskip("geopandas")

from dynastore.tasks.ingestion.readers.pyogrio_reader import PyogrioReader


_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"name": "a", "val": 1},
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        },
        {
            "type": "Feature",
            "properties": {"name": "b", "val": 2},
            "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
        },
    ],
}


@pytest.fixture()
def geojson_path(tmp_path):
    p = tmp_path / "sample.geojson"
    p.write_text(json.dumps(_GEOJSON))
    return str(p)


def test_open_yields_geojson_records(geojson_path):
    reader = PyogrioReader()
    with reader.open(geojson_path) as records:
        feats = list(records)
    assert len(feats) == 2
    names = {f["properties"]["name"] for f in feats}
    assert names == {"a", "b"}
    for f in feats:
        assert f["type"] == "Feature"
        assert f["geometry"]["type"] == "Point"


def test_feature_count(geojson_path):
    assert PyogrioReader().feature_count(geojson_path) == 2


def test_feature_count_bad_uri_returns_none():
    assert PyogrioReader().feature_count("/nonexistent/path/nope.geojson") is None


def test_can_read_matches_extensions():
    assert PyogrioReader.can_read("gs://b/x/file.geojson")
    assert PyogrioReader.can_read("gs://b/x/file.gpkg")
    assert not PyogrioReader.can_read("gs://b/x/file.parquet")


def test_priority_is_ahead_of_gdal_osgeo():
    # Strictly ahead of GdalOsgeoReader (priority=100); lower number = earlier.
    assert PyogrioReader.priority == 10
    assert PyogrioReader.reader_id == "pyogrio"


# ---------------------------------------------------------------------------
# Chunked streaming: never materialises the full GeoDataFrame at once
# ---------------------------------------------------------------------------


_GEOJSON_LARGE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"name": f"feat_{i}", "val": i},
            "geometry": {"type": "Point", "coordinates": [float(i), float(i)]},
        }
        for i in range(10)
    ],
}


@pytest.fixture()
def geojson_large_path(tmp_path):
    p = tmp_path / "large.geojson"
    p.write_text(__import__("json").dumps(_GEOJSON_LARGE))
    return str(p)


def test_open_chunked_yields_all_features(geojson_large_path):
    """Chunked read (read_batch_size=3) must yield all 10 features, regardless
    of how many pyogrio read_dataframe calls are made internally."""
    reader = PyogrioReader()
    with reader.open(geojson_large_path, read_batch_size=3) as records:
        feats = list(records)
    assert len(feats) == 10
    vals = {f["properties"]["val"] for f in feats}
    assert vals == set(range(10))


def test_open_chunk_size_forwarded_to_pyogrio(geojson_large_path):
    """read_batch_size opt must page through pyogrio.read_dataframe via
    skip_features/max_features — read_dataframe has no chunksize kwarg,
    it's not a chunked-generator API (#2964 follow-up: the previous
    ``chunksize=`` call silently no-op'd as an unrecognised GDAL open
    option and iterated the returned GeoDataFrame's column names)."""
    import pyogrio
    from unittest.mock import patch

    # Build a realistic stand-in: two pages of GeoDataFrames
    import geopandas as gpd
    all_feats = _GEOJSON_LARGE
    gdf = gpd.GeoDataFrame.from_features(all_feats["features"])

    page1 = gdf.iloc[:5].copy()
    page2 = gdf.iloc[5:].copy()
    empty = gdf.iloc[:0].copy()

    with patch.object(
        pyogrio, "read_dataframe", side_effect=[page1, page2, empty],
    ) as mock_rdf:
        reader = PyogrioReader()
        with reader.open(geojson_large_path, read_batch_size=5) as records:
            feats = list(records)

    assert mock_rdf.call_count == 3
    first_kwargs = mock_rdf.call_args_list[0].kwargs
    assert first_kwargs.get("skip_features") == 0
    assert first_kwargs.get("max_features") == 5
    second_kwargs = mock_rdf.call_args_list[1].kwargs
    assert second_kwargs.get("skip_features") == 5
    assert second_kwargs.get("max_features") == 5
    assert len(feats) == 10


def test_open_does_not_use_gpd_read_file():
    """The reader must not call gpd.read_file — that materialises the entire
    dataset in memory. Chunked pyogrio.read_dataframe is the required path."""
    import inspect
    from dynastore.tasks.ingestion.readers.pyogrio_reader import PyogrioReader as _R
    src = inspect.getsource(_R.open)
    assert "gpd.read_file" not in src, (
        "PyogrioReader.open still calls gpd.read_file, which loads the whole "
        "GeoDataFrame into memory. Replace with paginated pyogrio.read_dataframe calls."
    )
    assert "pyogrio.read_dataframe" in src, (
        "PyogrioReader.open must use pyogrio.read_dataframe, paginated via "
        "skip_features/max_features, to avoid materialising the full source in memory."
    )
