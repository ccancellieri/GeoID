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

"""Unit tests for the OGC API - Joins async export task.

The join *merge* logic itself is covered by ``modules/joins/test_executor.py``;
these tests cover the task's orchestration: secondary indexing, the
full-precision primary stream, server-owned output naming, and the conformant
by-reference results document (OGC API - Processes §7.13).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.models.ogc import Feature


def _make_payload(inputs: dict):
    payload = MagicMock()
    payload.task_id = "019f0000-0000-7000-8000-000000000000"
    payload.inputs.inputs = inputs
    return payload


@pytest.mark.asyncio
async def test_joins_export_returns_conformant_reference_result(monkeypatch):
    import dynastore.tasks.joins_export.joins_export_task as mod

    # Engine present (non-None) so the task proceeds.
    monkeypatch.setattr(mod, "get_engine", lambda: object())

    # Secondary: two BQ rows keyed on gaul1_code.
    async def fake_bq(spec, *, secondary_column, **kw):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"gaul1_code": 2346, "demo_stat": 35.0})
        yield Feature(type="Feature", id="bq2", geometry=None,
                      properties={"gaul1_code": 3296, "demo_stat": 29.6})
    monkeypatch.setattr(mod, "stream_bigquery_secondary", fake_bq)

    # Primary: three features; two match the secondary (INNER join → 2 out).
    captured_stream_cfg = {}

    async def fake_stream_features(config, engine, hints=frozenset()):
        captured_stream_cfg["config"] = config
        captured_stream_cfg["hints"] = hints
        for code, name in [(2346, "Utah"), (3296, "Gjirokaster"), (9999, "NoMatch")]:
            yield Feature(type="Feature", id=f"p{code}", geometry=None,
                          properties={"GAUL1_CODE": code, "GAUL1_NAME": name})
    monkeypatch.setattr(mod, "stream_features", fake_stream_features)

    # Server-owned output + signing.
    monkeypatch.setattr(
        mod.result_message, "server_output_uri",
        AsyncMock(return_value="gs://bucket-gaulb/processes/outputs/joins_export/JOB/gaul_level_1.geojson"),
    )
    monkeypatch.setattr(
        mod.result_message, "signed_result_url",
        AsyncMock(return_value="https://signed.example/gaul_level_1.geojson?X-Goog-Expires=604800"),
    )
    monkeypatch.setattr(mod, "initialize_reporters", lambda **kw: [])

    # Capture the joined features the writer sees, and the upload call.
    seen = {}

    def fake_byte_stream(features, output_format, target_srid=4326, encoding="utf-8"):
        seen["features"] = list(features)
        seen["output_format"] = output_format
        return iter([b"GEOJSON-BYTES"])
    monkeypatch.setattr(mod, "get_features_as_byte_stream", fake_byte_stream)

    def fake_upload(byte_stream, destination_uri, content_type):
        seen["uploaded"] = b"".join(byte_stream)
        seen["destination_uri"] = destination_uri
        seen["content_type"] = content_type
    monkeypatch.setattr(mod, "upload_stream_to_gcs", fake_upload)

    task = mod.JoinsExportTask(app_state=object())
    payload = _make_payload({
        "catalog": "gaulb",
        "collection": "gaul_level_1",
        # collection-scoped route injects these; must be tolerated, not used.
        "catalog_id": "gaulb",
        "collection_id": "gaul_level_1",
        "secondary": {"driver": "bigquery", "target": {
            "project_id": "fao-aip-geospatial-review",
            "dataset_id": "demo_data", "table_name": "gaul_join_b"}},
        "join": {"primary_column": "GAUL1_CODE", "secondary_column": "gaul1_code"},
        "projection": {"with_geometry": True, "destination_crs": 4326},
        "output": {"format": "geojson"},
    })

    result = await task.run(payload)

    # INNER join over the full collection → only the two matching features.
    assert len(seen["features"]) == 2
    names = {f.properties["GAUL1_NAME"] for f in seen["features"]}
    assert names == {"Utah", "Gjirokaster"}
    # Secondary columns merged in.
    assert all("demo_stat" in f.properties for f in seen["features"])

    # Full-precision PG read path was requested.
    from dynastore.modules.storage.hints import Hint
    assert Hint.JOIN in captured_stream_cfg["hints"]

    # Server owns the location; geojson media type + extension resolved.
    assert seen["destination_uri"].endswith("/gaul_level_1.geojson")
    assert seen["content_type"] == "application/geo+json"

    # Conformant by-reference results document (OGC API - Processes §7.13):
    # the 'result' output is a {href, type} link, with the signed URL also
    # carried as the status message.
    assert result["result"] == {
        "href": "https://signed.example/gaul_level_1.geojson?X-Goog-Expires=604800",
        "type": "application/geo+json",
    }
    assert result["message"] == result["result"]["href"]


@pytest.mark.asyncio
async def test_joins_export_geopackage_format_maps_to_gpkg_extension(monkeypatch):
    import dynastore.tasks.joins_export.joins_export_task as mod

    monkeypatch.setattr(mod, "get_engine", lambda: object())

    async def fake_bq(spec, *, secondary_column, **kw):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"gaul1_code": 1, "v": 10})
    monkeypatch.setattr(mod, "stream_bigquery_secondary", fake_bq)

    async def fake_stream_features(config, engine, hints=frozenset()):
        yield Feature(type="Feature", id="p1", geometry=None,
                      properties={"GAUL1_CODE": 1})
    monkeypatch.setattr(mod, "stream_features", fake_stream_features)

    captured = {}
    monkeypatch.setattr(
        mod.result_message, "server_output_uri",
        AsyncMock(side_effect=lambda cat, pid, jid, fname: f"gs://b/{fname}"),
    )
    monkeypatch.setattr(
        mod.result_message, "signed_result_url",
        AsyncMock(return_value="https://signed/x"),
    )
    monkeypatch.setattr(mod, "initialize_reporters", lambda **kw: [])

    def fake_byte_stream(features, output_format, target_srid=4326, encoding="utf-8"):
        list(features)  # drain the joined-feature iterator
        return iter([b"BYTES"])
    monkeypatch.setattr(mod, "get_features_as_byte_stream", fake_byte_stream)

    def fake_upload(byte_stream, destination_uri, content_type):
        b"".join(byte_stream)
        captured["destination_uri"] = destination_uri
        captured["content_type"] = content_type
    monkeypatch.setattr(mod, "upload_stream_to_gcs", fake_upload)

    task = mod.JoinsExportTask(app_state=object())
    payload = _make_payload({
        "catalog": "c", "collection": "coll",
        "secondary": {"driver": "bigquery", "target": {
            "project_id": "p", "dataset_id": "d", "table_name": "t"}},
        "join": {"primary_column": "GAUL1_CODE", "secondary_column": "gaul1_code"},
        "output": {"format": "geopackage"},
    })

    await task.run(payload)
    assert captured["destination_uri"].endswith("/coll.gpkg")
    assert captured["content_type"] == "application/geopackage+sqlite3"
