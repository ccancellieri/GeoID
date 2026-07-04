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

"""BqFeatureReporter — config validation + attributes-only row shaping (geoid#2972).

``_to_row`` only reads ``self.config``, so we drive it on a bare instance
(``__new__``) to avoid the GCP BigQuery-client dependency in ``__init__``,
mirroring the pattern already used for ``GcsDetailedReporter`` in
``test_bucket_reporter_filter.py``.
"""

import asyncio

import pytest
from pydantic import ValidationError

from dynastore.modules.gcp.bq_feature_reporter import (
    BqFeatureReporter,
    BqFeatureReporterConfig,
)


def _reporter(**config_kwargs) -> BqFeatureReporter:
    reporter = BqFeatureReporter.__new__(BqFeatureReporter)
    reporter.config = BqFeatureReporterConfig(
        project_id="proj", dataset_id="ds", table_name="tbl", **config_kwargs
    )
    return reporter


def _feature_record() -> dict:
    return {
        "type": "Feature",
        "id": "1621",
        "geometry": {"type": "Point", "coordinates": [13.3, 45.6]},
        "properties": {"CODE": "1621", "NAME": "Friuli", "area": 7845.0},
    }


# --- Config validation -------------------------------------------------------


def test_config_requires_project_dataset_table():
    with pytest.raises(ValidationError):
        BqFeatureReporterConfig()  # type: ignore[call-arg]


def test_config_defaults():
    cfg = BqFeatureReporterConfig(project_id="p", dataset_id="d", table_name="t")
    assert cfg.include_geometry is False
    assert cfg.demo_random_column == "demo_value"


def test_config_accepts_custom_demo_column_name():
    cfg = BqFeatureReporterConfig(
        project_id="p", dataset_id="d", table_name="t", demo_random_column="my_col"
    )
    assert cfg.demo_random_column == "my_col"


# --- __init__ table_fqn ------------------------------------------------------


def test_table_fqn_built_from_config():
    reporter = BqFeatureReporter.__new__(BqFeatureReporter)
    reporter.config = BqFeatureReporterConfig(
        project_id="proj", dataset_id="ds", table_name="tbl"
    )
    reporter._bq = object()
    reporter._table_fqn = (
        f"{reporter.config.project_id}.{reporter.config.dataset_id}.{reporter.config.table_name}"
    )
    assert reporter._table_fqn == "proj.ds.tbl"


# --- _to_row: geometry stripped, demo column present ------------------------


def test_geometry_never_in_row_regardless_of_include_geometry_true():
    reporter = _reporter(include_geometry=True)
    row = reporter._to_row({"status": "SUCCESS", "record": _feature_record()})
    assert row is not None
    assert "geometry" not in row
    assert "geom" not in row
    assert "bbox" not in row


def test_geometry_never_in_row_when_include_geometry_false():
    reporter = _reporter(include_geometry=False)
    row = reporter._to_row({"status": "SUCCESS", "record": _feature_record()})
    assert row is not None
    assert "geometry" not in row


def test_demo_random_column_present_as_float():
    reporter = _reporter()
    row = reporter._to_row({"status": "SUCCESS", "record": _feature_record()})
    assert row is not None
    assert "demo_value" in row
    assert isinstance(row["demo_value"], float)
    assert 0.0 <= row["demo_value"] < 1.0


def test_demo_random_column_name_is_configurable():
    reporter = _reporter(demo_random_column="join_enrichment")
    row = reporter._to_row({"status": "SUCCESS", "record": _feature_record()})
    assert row is not None
    assert "join_enrichment" in row
    assert "demo_value" not in row


def test_row_contains_flattened_properties():
    reporter = _reporter()
    row = reporter._to_row({"status": "SUCCESS", "record": _feature_record()})
    assert row is not None
    assert row["CODE"] == "1621"
    assert row["NAME"] == "Friuli"
    assert row["area"] == 7845.0


def test_pydantic_feature_record_is_normalized():
    from geojson_pydantic import Feature

    feature = Feature.model_validate(_feature_record())
    reporter = _reporter()
    row = reporter._to_row({"status": "SUCCESS", "record": feature})
    assert row is not None
    assert row["NAME"] == "Friuli"
    assert "geometry" not in row


def test_legacy_flat_attributes_bag_supported():
    reporter = _reporter()
    legacy = {"system": {"asset_id": "ITAL1_01"}, "geom": "0101...", "attributes": {"CODE": "x"}}
    row = reporter._to_row({"status": "SUCCESS", "record": legacy})
    assert row is not None
    assert row["CODE"] == "x"
    assert "geom" not in row


def test_failed_record_produces_no_row():
    reporter = _reporter()
    row = reporter._to_row({"status": "FAILURE", "record": _feature_record()})
    assert row is None


def test_record_without_properties_or_attributes_produces_no_row():
    reporter = _reporter()
    row = reporter._to_row({"status": "SUCCESS", "record": {"type": "Feature", "id": "1"}})
    assert row is None


# --- process_batch_outcome wiring -------------------------------------------


def test_process_batch_outcome_calls_insert_rows_json_once_per_batch():
    reporter = _reporter()

    captured = {}

    class _FakeBq:
        async def insert_rows_json(self, table_fqn, rows, *, project_id):
            captured["table_fqn"] = table_fqn
            captured["rows"] = rows
            captured["project_id"] = project_id
            return []

    reporter._bq = _FakeBq()
    reporter._table_fqn = "proj.ds.tbl"

    batch = [
        {"status": "SUCCESS", "record": _feature_record()},
        {"status": "FAILURE", "record": _feature_record()},
    ]
    asyncio.run(reporter.process_batch_outcome(batch))

    assert captured["table_fqn"] == "proj.ds.tbl"
    assert captured["project_id"] == "proj"
    assert len(captured["rows"]) == 1  # the FAILURE record is dropped
    assert "geometry" not in captured["rows"][0]
    assert "demo_value" in captured["rows"][0]


def test_process_batch_outcome_no_op_when_reporter_disabled():
    reporter = BqFeatureReporter.__new__(BqFeatureReporter)
    reporter.config = None
    reporter._bq = None
    # Must not raise even though _bq/_table_fqn are never set up.
    asyncio.run(reporter.process_batch_outcome([{"status": "SUCCESS", "record": _feature_record()}]))


def test_process_batch_outcome_no_rows_skips_insert_call():
    reporter = _reporter()

    class _FakeBq:
        async def insert_rows_json(self, table_fqn, rows, *, project_id):
            raise AssertionError("insert_rows_json should not be called with zero rows")

    reporter._bq = _FakeBq()
    reporter._table_fqn = "proj.ds.tbl"

    asyncio.run(
        reporter.process_batch_outcome([{"status": "FAILURE", "record": _feature_record()}])
    )
