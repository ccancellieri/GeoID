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

"""Regression tests for the DWH-join export task.

Covers three fixes that together make the async ``dwh_join`` OGC Process job
work end-to-end and return a usable result:

1. **Feature-merge** — the producer must merge ``Feature`` objects via the
   shared ``enrich_features`` helper (reading ``feature.properties``), not a
   dict-style ``feature.get(...)`` which raised
   ``'Feature' object has no attribute 'get'`` and killed the job before any
   row was written.
2. **Terminal status** — on success the task must report the valid
   ``TaskStatusEnum.COMPLETED`` to reporters, not the bogus string
   ``"SUCCESS"`` (which failed ``TaskUpdate`` enum validation, marked the job
   failed, and triggered a cold-start retry).
3. **Result delivery** — the output location is server-derived (not a client
   ``destination_uri``) and the artifact is returned as a signed URL in the
   job ``message`` (surfaced from ``outputs`` by ``task_to_status_info``).
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.ogc import Feature

_TASK_MOD = "dynastore.tasks.dwh_join.dwh_join_export_task"


class _RecordingReporter:
    """Captures the lifecycle calls the task makes so we can assert the
    terminal status string the task hands to reporters."""

    def __init__(self):
        self.started = []
        self.finished = []

    async def task_started(self, task_id, collection_id, catalog_id, source):
        self.started.append(source)

    async def task_finished(self, final_status, error_message=None, summary=None):
        self.finished.append(final_status)


def _payload(inputs: dict) -> SimpleNamespace:
    """Minimal stand-in for ``TaskPayload[ExecuteRequest]`` — the task only
    reads ``payload.task_id`` and ``payload.inputs.inputs``."""
    return SimpleNamespace(
        task_id="019e7547-2dba-76ca-a53d-7f5264d05e47",
        inputs=SimpleNamespace(inputs=inputs),
    )


_BASE_INPUTS = {
    "dwh_project_id": "p",
    "dwh_query": "SELECT 1",
    "catalog": "cat",
    "collection": "region",
    "dwh_join_column": "join_col",
    "join_column": "join_col",
    "with_geometry": True,
    "properties": ["*"],
    "stats": [],
    "system": [],
    "output_format": "geojson",
    # NOTE: no destination_uri — the server derives the output location.
}

_RESOLVED_URI = "gs://cat-bucket/processes/outputs/dwh_join/019e7547-2dba-76ca-a53d-7f5264d05e47/region.geojson"
_SIGNED_URL = "https://storage.googleapis.com/signed?token=abc&expires=week"


@pytest.mark.asyncio
async def test_dwh_join_export_signs_server_derived_uri_and_reports_completed():
    """End-to-end: Feature objects merge via enrich_features, the upload goes
    to the server-derived URI, the signed URL is the job message, and the
    terminal status reported is COMPLETED (not 'SUCCESS')."""
    pytest.importorskip("google")  # dwh_join_export_task pulls in BigQuery/GCS
    from dynastore.tasks.dwh_join.dwh_join_export_task import DwhJoinExportTask

    matched = Feature(type="Feature", id="key1", geometry=None,
                      properties={"join_col": "key1", "name": "Abruzzi"})
    unmatched = Feature(type="Feature", id="key2", geometry=None,
                        properties={"join_col": "key2", "name": "NoDwhRow"})

    def fake_stream_features(config, db_resource, hints=frozenset()):
        async def _gen():
            yield matched
            yield unmatched
        return _gen()

    written: list = []

    def fake_byte_stream(features, output_format, target_srid=4326, encoding="utf-8"):
        written.extend(list(features))
        return iter([b"{}"])

    upload_calls: list = []

    def fake_upload(byte_stream, destination_uri, content_type=None):
        list(byte_stream)  # drain
        upload_calls.append(destination_uri)

    reporter = _RecordingReporter()

    with (
        patch(f"{_TASK_MOD}.get_engine", return_value=MagicMock()),
        patch(f"{_TASK_MOD}.initialize_reporters", return_value=[reporter]),
        patch(f"{_TASK_MOD}.execute_bigquery_async", new_callable=AsyncMock,
              return_value={"key1": {"dwh_col": "val1"}}),
        patch(f"{_TASK_MOD}.resolve_category_field_names", new_callable=AsyncMock,
              return_value=["join_col", "name"]),
        patch("dynastore.tasks.result_message.server_output_uri", new_callable=AsyncMock,
              return_value=_RESOLVED_URI),
        patch("dynastore.tasks.result_message.signed_result_url", new_callable=AsyncMock,
              return_value=_SIGNED_URL),
        patch(f"{_TASK_MOD}.stream_features", side_effect=fake_stream_features),
        patch(f"{_TASK_MOD}.get_features_as_byte_stream", side_effect=fake_byte_stream),
        patch(f"{_TASK_MOD}.upload_stream_to_gcs", side_effect=fake_upload),
    ):
        task = DwhJoinExportTask(app_state=MagicMock())
        result = await task.run(_payload(dict(_BASE_INPUTS)))

    # Inner join: only the matched feature, merged via .properties (not .get).
    assert len(written) == 1 and written[0].properties["dwh_col"] == "val1"
    # Upload targeted the server-derived URI (no client destination_uri).
    assert upload_calls == [_RESOLVED_URI]
    # The signed URL is the job message.
    # The artifact is surfaced as the declared ``result`` output, by reference
    # (a Link-shaped {href, type} qualified value) for a conformant OGC results
    # document, and also carried as the status ``message``.
    assert result is not None
    assert result["result"]["href"] == _SIGNED_URL
    assert result["result"]["type"]  # media type populated from the formatter
    assert result["message"] == _SIGNED_URL
    # Terminal status reported to reporters is COMPLETED, never "SUCCESS".
    assert reporter.finished == ["COMPLETED"]
    assert reporter.started == [_RESOLVED_URI]


@pytest.mark.asyncio
async def test_dwh_join_export_empty_join_still_completes():
    """No DWH rows → nothing written, but the task still completes and reports
    COMPLETED (no crash, no spurious failure)."""
    pytest.importorskip("google")  # dwh_join_export_task pulls in BigQuery/GCS
    from dynastore.tasks.dwh_join.dwh_join_export_task import DwhJoinExportTask

    def fake_stream_features(config, db_resource, hints=frozenset()):
        async def _gen():
            yield Feature(type="Feature", id="k", geometry=None, properties={"join_col": "k"})
        return _gen()

    written: list = []
    reporter = _RecordingReporter()

    with (
        patch(f"{_TASK_MOD}.get_engine", return_value=MagicMock()),
        patch(f"{_TASK_MOD}.initialize_reporters", return_value=[reporter]),
        patch(f"{_TASK_MOD}.execute_bigquery_async", new_callable=AsyncMock, return_value={}),
        patch(f"{_TASK_MOD}.resolve_category_field_names", new_callable=AsyncMock,
              return_value=["join_col"]),
        patch("dynastore.tasks.result_message.server_output_uri", new_callable=AsyncMock, return_value=_RESOLVED_URI),
        patch("dynastore.tasks.result_message.signed_result_url", new_callable=AsyncMock, return_value=_SIGNED_URL),
        patch(f"{_TASK_MOD}.stream_features", side_effect=fake_stream_features),
        patch(f"{_TASK_MOD}.get_features_as_byte_stream",
              side_effect=lambda features, output_format, target_srid=4326, encoding="utf-8": (
                  written.extend(list(features)) or iter([b"{}"]))),
        patch(f"{_TASK_MOD}.upload_stream_to_gcs", side_effect=lambda byte_stream, destination_uri, content_type=None: list(byte_stream)),
    ):
        task = DwhJoinExportTask(app_state=MagicMock())
        result = await task.run(_payload(dict(_BASE_INPUTS)))

    assert written == []
    # The artifact is surfaced as the declared ``result`` output, by reference
    # (a Link-shaped {href, type} qualified value) for a conformant OGC results
    # document, and also carried as the status ``message``.
    assert result is not None
    assert result["result"]["href"] == _SIGNED_URL
    assert result["result"]["type"]  # media type populated from the formatter
    assert result["message"] == _SIGNED_URL
    assert reporter.finished == ["COMPLETED"]


def test_task_to_status_info_surfaces_outputs_message():
    """A successful job with a message in its persisted ``outputs`` surfaces
    that message as the job ``message``; failures keep using error_message."""
    from dynastore.modules.processes.models import task_to_status_info
    from dynastore.models.tasks import Task, TaskStatusEnum

    ok = Task(task_type="dwh_join", type="process", status=TaskStatusEnum.COMPLETED,
              error_message=None, outputs={"message": _SIGNED_URL})
    info_ok = task_to_status_info(ok)
    assert info_ok.status == "successful"
    assert info_ok.message == _SIGNED_URL

    failed = Task(task_type="dwh_join", type="process", status=TaskStatusEnum.FAILED,
                  error_message="boom", outputs=None)
    info_failed = task_to_status_info(failed)
    assert info_failed.status == "failed"
    assert info_failed.message == "boom"


# ---------------------------------------------------------------------------
# enrich_features with join_source (#1827)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enrich_features_join_source_system_matches_via_system_section():
    """With join_source='system', enrich_features resolves the join key from
    feature.__pydantic_extra__['system'] rather than flat feature.properties,
    so external_id can drive the join without colliding with a user property."""
    from dynastore.tools.enrichment import enrich_features

    # Feature with external_id in system foreign-member section (as produced by
    # the partial _apply_expose_all_sections path for requested_fields={'external_id'}).
    f = Feature(type="Feature", id="geoid-uuid", geometry=None, properties={"name": "Italy"})
    if f.__pydantic_extra__ is not None:
        f.__pydantic_extra__["system"] = {"external_id": "EXT-001"}

    join_values = {"EXT-001": {"country_code": "IT", "population": 60_000_000}}

    async def _stream():
        yield f

    results = [
        feat async for feat in enrich_features(
            _stream(), join_values, join_column="external_id", join_source="system",
        )
    ]

    assert len(results) == 1
    assert results[0].id == "geoid-uuid"  # feature.id stays the geoid UUID
    assert (results[0].properties or {})["name"] == "Italy"
    assert (results[0].properties or {})["country_code"] == "IT"


@pytest.mark.asyncio
async def test_enrich_features_join_source_system_no_match_dropped_inner_join():
    """With join_source='system' and inner_join=True, a feature whose system
    section has no external_id is dropped (not matched)."""
    from dynastore.tools.enrichment import enrich_features

    f_no_ext = Feature(type="Feature", id="g1", geometry=None,
                       properties={"name": "France"})

    join_values = {"EXT-001": {"data": "x"}}

    async def _stream():
        yield f_no_ext

    results = [
        feat async for feat in enrich_features(
            _stream(), join_values, join_column="external_id", join_source="system",
        )
    ]
    assert results == []


@pytest.mark.asyncio
async def test_enrich_features_default_join_source_properties_back_compat():
    """Default join_source='properties' still joins on flat feature.properties[key],
    preserving back-compat for callers that don't set join_source. (#1827)"""
    from dynastore.tools.enrichment import enrich_features

    f = Feature(type="Feature", id="g1", geometry=None, properties={"CODE": "FR"})
    join_values = {"FR": {"country_name": "France"}}

    async def _stream():
        yield f

    results = [feat async for feat in enrich_features(_stream(), join_values, join_column="CODE")]

    assert len(results) == 1
    assert (results[0].properties or {})["country_name"] == "France"
