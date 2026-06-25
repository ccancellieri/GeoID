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

"""Unit tests for modules/moving_features/db.py row helpers."""

import json
import uuid
from datetime import datetime, timezone

from dynastore.modules.moving_features.db import _mf_from_row, _tg_from_row, _compute_bbox_wkt


# ---------------------------------------------------------------------------
# _compute_bbox_wkt
# ---------------------------------------------------------------------------

def test_compute_bbox_wkt_empty():
    assert _compute_bbox_wkt([]) is None


def test_compute_bbox_wkt_single_point():
    wkt = _compute_bbox_wkt([[10.0, 52.0]])
    assert wkt == "POLYGON((10.0 52.0,10.0 52.0,10.0 52.0,10.0 52.0,10.0 52.0))"


def test_compute_bbox_wkt_multiple_points():
    wkt = _compute_bbox_wkt([[10.0, 52.0], [11.0, 53.0], [10.5, 52.5]])
    assert wkt is not None
    assert "POLYGON" in wkt
    assert "10.0" in wkt
    assert "11.0" in wkt
    assert "52.0" in wkt
    assert "53.0" in wkt


def test_compute_bbox_wkt_3d_coordinates():
    wkt = _compute_bbox_wkt([[10.0, 52.0, 100.0], [11.0, 53.0, 200.0]])
    assert wkt is not None
    assert "POLYGON" in wkt


# ---------------------------------------------------------------------------
# _mf_from_row
# ---------------------------------------------------------------------------

def test_mf_from_row_none_returns_none():
    assert _mf_from_row({}) is None
    assert _mf_from_row(None) is None


def test_mf_from_row_valid():
    row = {
        "id": str(uuid.uuid4()),
        "catalog_id": "cat1",
        "collection_id": "fleet",
        "feature_type": "Feature",
        "properties": json.dumps({"name": "truck"}),
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }
    mf = _mf_from_row(row)
    assert mf is not None
    assert mf.catalog_id == "cat1"
    assert mf.properties["name"] == "truck"


def test_mf_from_row_properties_already_dict():
    row = {
        "id": str(uuid.uuid4()),
        "catalog_id": "cat1",
        "collection_id": "fleet",
        "feature_type": "Feature",
        "properties": {"speed": 100},
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }
    mf = _mf_from_row(row)
    assert mf is not None
    assert mf.properties["speed"] == 100


# ---------------------------------------------------------------------------
# _tg_from_row
# ---------------------------------------------------------------------------

def test_tg_from_row_none_returns_none():
    assert _tg_from_row({}) is None
    assert _tg_from_row(None) is None


def test_tg_from_row_valid():
    mf_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    row = {
        "id": str(uuid.uuid4()),
        "mf_id": str(mf_id),
        "catalog_id": "cat1",
        "datetimes": [now],
        "coordinates": json.dumps([[10.0, 52.0]]),
        "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian",
        "interpolation": "Linear",
        "properties": json.dumps({"speed": 80}),
        "created_at": now,
    }
    tg = _tg_from_row(row)
    assert tg is not None
    assert tg.coordinates == [[10.0, 52.0]]
    assert tg.properties["speed"] == 80
    assert len(tg.datetimes) == 1


def test_tg_from_row_coordinates_already_list():
    now = datetime.now(timezone.utc)
    row = {
        "id": str(uuid.uuid4()),
        "mf_id": str(uuid.uuid4()),
        "catalog_id": "cat1",
        "datetimes": [now],
        "coordinates": [[10.0, 52.0], [11.0, 53.0]],
        "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian",
        "interpolation": "Linear",
        "properties": None,
        "created_at": now,
    }
    tg = _tg_from_row(row)
    assert tg is not None
    assert len(tg.coordinates) == 2
