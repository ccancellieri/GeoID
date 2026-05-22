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
    legacy = {"asset_id": "ITAL1_01", "geom": "0101...", "attributes": {"CODE": "x"}}
    out = _reporter(include_geometry=False)._filter_result_for_reporting(
        {"status": "SUCCESS", "record": legacy}
    )
    assert "geom" not in out["record"]
    assert out["record"]["asset_code"] == "ITAL1_01"
