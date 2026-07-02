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

"""Unit coverage for the deterministic per-feature identity precedence
(GeoID #2709): re-running the same vector ingest must CONVERGE instead of
duplicating every feature.

These tests pin the two pure helpers ``_resolve_raw_identity`` (tiers 1/2:
configured field / natural id) and ``_content_hash_feature_id`` (tier 3:
content-hash fallback) directly. Full ``prepare_record_for_upsert`` wiring
(the closure inside ``run_ingestion_task``) is exercised by the integration
suite; the reader-level OGR FID capture is covered separately in
``test_osgeo_reader_fid.py`` (GDAL-gated).
"""
from __future__ import annotations

from dynastore.tasks.ingestion.main_ingestion import (
    _content_hash_feature_id,
    _resolve_raw_identity,
)


# ---------------------------------------------------------------------------
# _resolve_raw_identity — tiers 1 & 2
# ---------------------------------------------------------------------------


def test_top_level_key_wins():
    raw = {"GAUL1_CODE": 42, "properties": {"GAUL1_CODE": 99}}
    assert _resolve_raw_identity(raw, "GAUL1_CODE") == 42


def test_falls_back_to_properties():
    raw = {"type": "Feature", "properties": {"GAUL1_CODE": "G-01"}}
    assert _resolve_raw_identity(raw, "GAUL1_CODE") == "G-01"


def test_missing_field_returns_none():
    raw = {"properties": {"other": "y"}}
    assert _resolve_raw_identity(raw, "GAUL1_CODE") is None


def test_empty_field_name_returns_none():
    raw = {"id": "x"}
    assert _resolve_raw_identity(raw, "") is None


def test_fid_zero_is_not_dropped():
    """A legitimate OGR FID of 0 (the first feature in a source) must
    resolve, not be treated as falsy — regression guard for the
    ``if ext_id:`` bug that silently dropped the first feature's identity."""
    raw = {"id": 0, "properties": {}}
    assert _resolve_raw_identity(raw, "id") == 0


def test_reader_surfaced_fid_resolves_via_id_field():
    """GdalOsgeoReader now surfaces the OGR FID as the record's top-level
    "id" (tier 2). When no column_mapping.external_id is configured the
    default lookup field is "id", so the FID resolves without any extra
    config — this is what makes re-ingesting the SAME shapefile converge."""
    raw = {"id": 17, "properties": {"GAUL1_CODE": 555}, "geometry": None}
    assert _resolve_raw_identity(raw, "id") == 17


# ---------------------------------------------------------------------------
# _content_hash_feature_id — tier 3
# ---------------------------------------------------------------------------


def test_content_hash_is_deterministic():
    geom = {"type": "Point", "coordinates": [1.0, 2.0]}
    props = {"name": "Rome", "value": 100}
    first = _content_hash_feature_id(geom, props)
    second = _content_hash_feature_id(geom, props)
    assert first == second


def test_content_hash_differs_for_different_content():
    geom = {"type": "Point", "coordinates": [1.0, 2.0]}
    a = _content_hash_feature_id(geom, {"name": "Rome"})
    b = _content_hash_feature_id(geom, {"name": "Milan"})
    assert a != b


def test_content_hash_ignores_key_order():
    """Canonical (sorted-key) JSON means dict insertion order never changes
    the resulting hash — a re-run that rebuilds the same properties dict in
    a different order must still converge."""
    geom = {"type": "Point", "coordinates": [1.0, 2.0]}
    props_a = {"name": "Rome", "value": 100}
    props_b = {"value": 100, "name": "Rome"}
    assert _content_hash_feature_id(geom, props_a) == _content_hash_feature_id(geom, props_b)

def test_content_hash_is_string_with_prefix():
    result = _content_hash_feature_id(None, {})
    assert isinstance(result, str)
    assert result.startswith("sha256:")
