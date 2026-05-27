#    Copyright 2025 FAO
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

"""GcsDetailedReporter record filtering (the DWH ingestion report).

Regression: the per-record ``record`` handed to the reporter by the ingestion
task is a GeoJSON Feature (keys ``geometry`` / ``properties``), but the filter
was written against a legacy flat DWH dict (``geom`` / ``attributes`` /
``bbox_coords``). As a result ``include_geometry=False`` and
``reported_attributes`` silently did nothing — geometry leaked into the report
and the attribute allow-list was ignored.

``_filter_result_for_reporting`` only reads ``self.config``, so we drive it on a
bare instance to avoid the GCP storage-client dependency in ``__init__``.
"""

from geojson_pydantic import Feature

from dynastore.modules.gcp.bucket_reporter import (
    GcsDetailedReporter,
    GcsDetailedReporterConfig,
)


def _reporter(**config_kwargs) -> GcsDetailedReporter:
    reporter = GcsDetailedReporter.__new__(GcsDetailedReporter)
    reporter.config = GcsDetailedReporterConfig(**config_kwargs)
    return reporter


def _feature_record() -> dict:
    return {
        "type": "Feature",
        "id": "1621",
        "geometry": {"type": "Point", "coordinates": [13.3, 45.6]},
        "properties": {"CODE": "1621", "NAME": "Friuli", "area": 7845.0},
    }


def test_geometry_excluded_from_geojson_record() -> None:
    out = _reporter(include_geometry=False)._filter_result_for_reporting(
        {"status": "SUCCESS", "record": _feature_record()}
    )
    assert "geometry" not in out["record"]


def test_geometry_kept_when_include_geometry_true() -> None:
    out = _reporter(include_geometry=True)._filter_result_for_reporting(
        {"status": "SUCCESS", "record": _feature_record()}
    )
    assert out["record"]["geometry"]["type"] == "Point"


def test_reported_attributes_filters_geojson_properties() -> None:
    out = _reporter(
        include_geometry=False, reported_attributes=["NAME", "area"]
    )._filter_result_for_reporting({"status": "SUCCESS", "record": _feature_record()})
    assert out["record"]["properties"] == {"NAME": "Friuli", "area": 7845.0}


def test_pydantic_feature_record_is_normalized_and_geometry_excluded() -> None:
    feature = Feature.model_validate(_feature_record())
    out = _reporter(include_geometry=False)._filter_result_for_reporting(
        {"status": "SUCCESS", "record": feature}
    )
    assert isinstance(out["record"], dict)
    assert "geometry" not in out["record"]
    assert out["record"]["properties"]["NAME"] == "Friuli"


def test_legacy_flat_record_geom_still_excluded() -> None:
    legacy = {
        "system": {"asset_id": "ITAL1_01"},
        "geom": "0101...",
        "attributes": {"CODE": "x"},
    }
    out = _reporter(include_geometry=False)._filter_result_for_reporting(
        {"status": "SUCCESS", "record": legacy}
    )
    assert "geom" not in out["record"]
    assert "asset_code" not in out["record"]
    assert out["record"]["system"]["asset_id"] == "ITAL1_01"


def _feature_record_with_top_level_echo() -> dict:
    """A read-assembled Feature: attributes live under ``properties`` *and* are
    echoed at the top level via ``feature.__pydantic_extra__`` (the bridge that
    feeds STAC extension generators). In a report that echo is duplication."""
    return {
        "type": "Feature",
        "id": "1621",
        "geometry": {"type": "Point", "coordinates": [13.3, 45.6]},
        "properties": {"CODE": "1621", "NAME": "Friuli", "area": 7845.0},
        # duplicated at the top level by the read assembly:
        "CODE": "1621",
        "NAME": "Friuli",
        "area": 7845.0,
    }


def test_top_level_attribute_echo_is_deduplicated() -> None:
    out = _reporter(include_geometry=False)._filter_result_for_reporting(
        {"status": "SUCCESS", "record": _feature_record_with_top_level_echo()}
    )
    rec = out["record"]
    # attributes survive exactly once, under ``properties``
    assert rec["properties"] == {"CODE": "1621", "NAME": "Friuli", "area": 7845.0}
    # ...and are no longer echoed at the Feature top level
    for attr in ("CODE", "NAME", "area"):
        assert attr not in rec, f"{attr!r} still duplicated at top level"
    # canonical GeoJSON structural keys are preserved
    assert rec["type"] == "Feature"
    assert rec["id"] == "1621"


def test_dedup_keeps_system_envelope_drops_top_level_echo() -> None:
    """Top-level dedup strips the attribute echo while preserving the
    ``system`` envelope sibling (asset_id lives there, not at the top)."""
    record = _feature_record_with_top_level_echo()
    record["system"] = {"asset_id": "ITAL1_01"}
    out = _reporter(include_geometry=False)._filter_result_for_reporting(
        {"status": "SUCCESS", "record": record}
    )
    assert out["record"]["system"] == {"asset_id": "ITAL1_01"}
    assert "asset_code" not in out["record"]
    assert "CODE" not in out["record"]


def _enriched_record() -> dict:
    """A report record after the D1 envelope split: user attributes live alone
    in ``properties``, platform statistics under ``stats``, identity under
    ``system``."""
    return {
        "type": "Feature",
        "id": "geoid-1621",
        "geometry": {"type": "Point", "coordinates": [13.3, 45.6]},
        "properties": {"NAME": "Friuli"},
        "stats": {"area": 7845.0, "perimeter": 412.3},
        "system": {
            "geoid": "geoid-1621",
            "external_id": "1621",
            "asset_id": "ITAL1_01",
        },
    }


def test_system_envelope_is_the_only_identity_carrier() -> None:
    out = _reporter(include_geometry=False)._filter_result_for_reporting(
        {"status": "SUCCESS", "record": _enriched_record()}
    )
    rec = out["record"]
    assert rec["system"] == {
        "geoid": "geoid-1621",
        "external_id": "1621",
        "asset_id": "ITAL1_01",
    }
    assert "asset_code" not in rec
    for key in ("geoid", "external_id", "asset_id"):
        assert key not in rec


def test_user_properties_and_platform_stats_are_separate_siblings() -> None:
    out = _reporter(include_geometry=False)._filter_result_for_reporting(
        {"status": "SUCCESS", "record": _enriched_record()}
    )
    assert out["record"]["properties"] == {"NAME": "Friuli"}
    assert out["record"]["stats"] == {"area": 7845.0, "perimeter": 412.3}
    # ``area`` (a geometry stat) must NOT leak back into ``properties``.
    assert "area" not in out["record"]["properties"]


def test_include_geoid_false_suppresses_geoid_in_system() -> None:
    out = _reporter(
        include_geometry=False, include_geoid=False
    )._filter_result_for_reporting({"status": "SUCCESS", "record": _enriched_record()})
    rec = out["record"]
    assert "geoid" not in rec["system"]
    assert rec["system"]["external_id"] == "1621"  # others unaffected


def test_include_external_id_false_suppresses_external_id_in_system() -> None:
    out = _reporter(
        include_geometry=False, include_external_id=False
    )._filter_result_for_reporting({"status": "SUCCESS", "record": _enriched_record()})
    rec = out["record"]
    assert "external_id" not in rec["system"]
    assert rec["system"]["geoid"] == "geoid-1621"


def test_include_asset_id_false_suppresses_asset_id() -> None:
    out = _reporter(
        include_geometry=False, include_asset_id=False
    )._filter_result_for_reporting({"status": "SUCCESS", "record": _enriched_record()})
    rec = out["record"]
    assert "asset_id" not in rec["system"]
    assert "asset_code" not in rec
    assert rec["system"]["geoid"] == "geoid-1621"


def test_reported_attributes_allow_list_does_not_touch_stats() -> None:
    """The user-attribute allow-list governs ``properties`` only — platform
    statistics under ``stats`` are a separate concern and ride through whole."""
    out = _reporter(
        include_geometry=False, reported_attributes=["NAME"]
    )._filter_result_for_reporting({"status": "SUCCESS", "record": _enriched_record()})
    assert out["record"]["properties"] == {"NAME": "Friuli"}
    assert out["record"]["stats"] == {"area": 7845.0, "perimeter": 412.3}
