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

"""Round-trip classification and spill/enqueue mechanics for the bulk
sync-write backlog offload (#3253) — ``ogc_bulk_offload.py``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from dynastore.extensions import ogc_bulk_offload as offload_mod
from dynastore.models.driver_context import DriverContext


def _ctx() -> DriverContext:
    # model_construct bypasses DriverContext.db_resource's isinstance
    # validation (Engine/Connection/AsyncEngine/...) — these unit tests
    # fake the whole DB layer, so a bare sentinel object is enough to prove
    # it round-trips into execute_process's ``engine=`` kwarg.
    return DriverContext.model_construct(
        db_resource=object(), processing=None, sidecar_data={}, extensions={},
    )


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


def test_classify_accepts_plain_feature_with_flat_and_nested_scalar_properties():
    item = {
        "type": "Feature",
        "id": "f1",
        "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
        "properties": {"name": "hello", "count": 42, "meta": {"a": 1, "b": "x"}},
    }
    assert offload_mod._classify(item) == item


def test_classify_rejects_stac_shaped_item_with_extra_top_level_keys():
    """STAC items carry assets/links/bbox/stac_extensions/collection —
    none of which the ingestion reader pipeline extracts (verified against
    pyogrio: assets is destructively flattened, bbox is dropped entirely).
    """
    item = {
        "type": "Feature",
        "id": "sentinel-item-1",
        "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
        "properties": {"datetime": "2026-01-01T00:00:00Z"},
        "bbox": [1.0, 2.0, 1.0, 2.0],
        "collection": "sentinel-2-l2a",
        "links": [{"rel": "self", "href": "https://example.com/item1"}],
        "assets": {"thumbnail": {"href": "https://example.com/thumb.png"}},
        "stac_extensions": ["https://stac-extensions.github.io/eo/v1.0.0/schema.json"],
        "stac_version": "1.0.0",
    }
    assert offload_mod._classify(item) is None


def test_classify_rejects_list_valued_property():
    """A list-of-scalars property round-trips as a numpy array through the
    reader pipeline (verified against pyogrio), not a native list."""
    item = {
        "type": "Feature",
        "id": "f1",
        "geometry": None,
        "properties": {"tags": ["a", "b"]},
    }
    assert offload_mod._classify(item) is None


def test_classify_rejects_missing_id():
    item = {"type": "Feature", "id": None, "geometry": None, "properties": {}}
    assert offload_mod._classify(item) is None


def test_classify_rejects_non_dict_geometry():
    item = {"type": "Feature", "id": "f1", "geometry": "POINT(1 2)", "properties": {}}
    assert offload_mod._classify(item) is None


def test_classify_normalizes_pydantic_model_via_model_dump():
    from pydantic import BaseModel

    class _Feature(BaseModel):
        type: str = "Feature"
        id: str
        geometry: Optional[dict] = None
        properties: Dict[str, Any] = {}

    model = _Feature(id="f1", properties={"a": 1})
    result = offload_mod._classify(model)
    assert result == {"type": "Feature", "id": "f1", "properties": {"a": 1}}


# ---------------------------------------------------------------------------
# offload_bulk_remainder — unsupported shape (no I/O attempted, no rejection)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unsupported_shape_is_not_applicable_not_a_rejection():
    """#3253 Finding 2: a STAC-shaped item fails the round-trip classifier,
    but that is 'offload does not apply here', not 'refuse the data' — the
    caller (``_ingest_items``) falls back to writing it inline. No
    rejection is synthesized and no I/O is attempted."""
    items = [
        {
            "type": "Feature",
            "id": "stac-1",
            "geometry": None,
            "properties": {},
            "assets": {"data": {"href": "https://example.com/a.tif"}},
        },
    ]
    outcome = await offload_mod.offload_bulk_remainder(
        "cat", "col", items, ctx=_ctx(), policy_source="/configs/...",
    )
    assert outcome.job_id is None
    assert outcome.count == 0
    assert outcome.rejections == []
    assert outcome.shape_unsupported is True


@pytest.mark.asyncio
async def test_empty_remainder_is_a_no_op():
    outcome = await offload_mod.offload_bulk_remainder(
        "cat", "col", [], ctx=_ctx(), policy_source="/configs/...",
    )
    assert outcome.job_id is None
    assert outcome.count == 0
    assert outcome.rejections == []


# ---------------------------------------------------------------------------
# offload_bulk_remainder — spill/enqueue mechanics
# ---------------------------------------------------------------------------


class _FakeStorage:
    def __init__(self, base_path: str = "gs://bucket/cat/collections/col") -> None:
        self.base_path = base_path
        self.uploads: List[Dict[str, Any]] = []
        self.deleted: List[str] = []

    async def get_collection_storage_path(self, catalog_id: str, collection_id: str) -> str:
        return self.base_path

    async def upload_file_content(self, target_path: str, content: bytes, content_type=None) -> str:
        self.uploads.append({
            "target_path": target_path, "content": content, "content_type": content_type,
        })
        return target_path

    async def delete_file(self, path: str) -> None:
        self.deleted.append(path)


class _FakeJobResult:
    def __init__(self, job_id: str) -> None:
        self.jobID = job_id


@pytest.mark.asyncio
async def test_successful_spill_and_enqueue_yields_accepted_async_only_after_both_succeed(monkeypatch):
    fake_storage = _FakeStorage()
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", lambda proto: fake_storage,
    )

    captured_exec: Dict[str, Any] = {}

    async def _fake_execute_process(
        process_id,
        exec_request,
        *,
        engine,
        caller_id,
        preferred_mode,
        catalog_id,
        collection_id,
        dedup_key=None,
    ):
        captured_exec["process_id"] = process_id
        captured_exec["inputs"] = exec_request.inputs
        captured_exec["catalog_id"] = catalog_id
        captured_exec["collection_id"] = collection_id
        captured_exec["dedup_key"] = dedup_key
        return _FakeJobResult("job-123")

    monkeypatch.setattr(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute_process,
    )

    items = [
        {
            "type": "Feature",
            "id": "abc-007",
            "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
            "properties": {"name": "hello"},
        },
        {
            "type": "Feature",
            "id": "abc-008",
            "geometry": None,
            "properties": {},
        },
    ]
    outcome = await offload_mod.offload_bulk_remainder(
        "cat", "col", items, ctx=_ctx(), policy_source="/configs/...",
    )

    assert outcome.job_id == "job-123"
    assert outcome.monitor_url == "/processes/catalogs/cat/collections/col/jobs/job-123"
    assert outcome.count == 2
    assert outcome.rejections == []

    assert captured_exec["process_id"] == "ingestion"
    assert captured_exec["catalog_id"] == "cat"
    assert captured_exec["collection_id"] == "col"
    ingestion_request = captured_exec["inputs"]["ingestion_request"]
    assert ingestion_request["column_mapping"]["external_id"] == offload_mod._SPILL_ID_PROPERTY

    assert len(fake_storage.uploads) == 1
    import json as _json
    spilled = _json.loads(fake_storage.uploads[0]["content"])
    assert spilled["type"] == "FeatureCollection"
    ids_in_spill = {
        f["properties"][offload_mod._SPILL_ID_PROPERTY] for f in spilled["features"]
    }
    assert ids_in_spill == {"abc-007", "abc-008"}

    # The enqueue must carry a dedup_key, or a client retry spawns a second job.
    assert captured_exec["dedup_key"]
    assert captured_exec["dedup_key"].startswith("sync_ingest_offload:cat:col:")


@pytest.mark.asyncio
async def test_dedup_key_is_content_derived_so_a_retry_cannot_spawn_a_second_job(monkeypatch):
    """A retry of the identical bulk POST must hash to the same dedup_key, so the
    partial unique index on (schema, dedup_key) collapses it into the existing
    job instead of spilling and enqueueing a second one. The key must therefore
    derive from the spilled content — not from the target path, which carries a
    fresh uuid4 on every call and would defeat dedup entirely.
    """
    keys: List[Any] = []

    async def _fake_execute_process(
        process_id, exec_request, *, engine, caller_id, preferred_mode,
        catalog_id, collection_id, dedup_key=None,
    ):
        keys.append(dedup_key)
        return _FakeJobResult("job-123")

    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", lambda proto: _FakeStorage(),
    )
    monkeypatch.setattr(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute_process,
    )

    items = [
        {"type": "Feature", "id": "f1", "geometry": None, "properties": {"a": 1}},
    ]
    other = [
        {"type": "Feature", "id": "f2", "geometry": None, "properties": {"a": 1}},
    ]

    # Same remainder submitted twice — the client retry case.
    await offload_mod.offload_bulk_remainder("cat", "col", items, ctx=_ctx(), policy_source="/configs/...")
    await offload_mod.offload_bulk_remainder("cat", "col", items, ctx=_ctx(), policy_source="/configs/...")
    # A genuinely different remainder must NOT collide with it.
    await offload_mod.offload_bulk_remainder("cat", "col", other, ctx=_ctx(), policy_source="/configs/...")

    assert keys[0] == keys[1], "a retry of the same remainder must reuse the dedup_key"
    assert keys[2] != keys[0], "a different remainder must not dedup into the first job"


@pytest.mark.asyncio
async def test_dedup_hit_reports_no_job_and_cleans_up_the_duplicate_spill_file(monkeypatch):
    """execute_process returns None on a dedup hit (a non-terminal task for
    this dedup_key already exists). This must never be reported as a
    fabricated success — no job_id/monitor_url exists to give the caller —
    and the spill file this call just wrote (a redundant duplicate of
    whatever the in-flight job is already reading) must be removed.
    """
    fake_storage = _FakeStorage()
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", lambda proto: fake_storage,
    )

    async def _fake_execute_process(*args, **kwargs):
        return None  # dedup hit

    monkeypatch.setattr(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute_process,
    )

    items = [
        {"type": "Feature", "id": "f1", "geometry": None, "properties": {}},
    ]
    outcome = await offload_mod.offload_bulk_remainder(
        "cat", "col", items, ctx=_ctx(), policy_source="/configs/...",
    )

    assert outcome.job_id is None
    assert outcome.monitor_url is None
    assert outcome.count == 0
    assert outcome.rejections == []
    assert outcome.dedup_hit is True

    # The spill upload happened, but since a job for the identical content
    # is already in flight, the duplicate file must be cleaned up rather
    # than left orphaned in storage.
    assert len(fake_storage.uploads) == 1
    assert fake_storage.deleted == [fake_storage.uploads[0]["target_path"]]


@pytest.mark.asyncio
async def test_storage_protocol_unavailable_yields_rejections_not_accepted(monkeypatch):
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", lambda proto: None,
    )

    items = [
        {"type": "Feature", "id": "f1", "geometry": None, "properties": {}},
    ]
    outcome = await offload_mod.offload_bulk_remainder(
        "cat", "col", items, ctx=_ctx(), policy_source="/configs/...",
    )

    assert outcome.job_id is None
    assert outcome.count == 0
    assert len(outcome.rejections) == 1
    assert outcome.rejections[0].reason == "async_offload_failed"
    assert outcome.rejections[0].external_id == "f1"


@pytest.mark.asyncio
async def test_execute_process_failure_yields_rejections_not_accepted(monkeypatch):
    fake_storage = _FakeStorage()
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", lambda proto: fake_storage,
    )

    async def _raising_execute_process(*args, **kwargs):
        raise RuntimeError("no runner available")

    monkeypatch.setattr(
        "dynastore.modules.processes.processes_module.execute_process",
        _raising_execute_process,
    )

    items = [
        {"type": "Feature", "id": "f1", "geometry": None, "properties": {}},
        {"type": "Feature", "id": "f2", "geometry": None, "properties": {}},
    ]
    outcome = await offload_mod.offload_bulk_remainder(
        "cat", "col", items, ctx=_ctx(), policy_source="/configs/...",
    )

    # The spill upload happened (durable write succeeded)...
    assert len(fake_storage.uploads) == 1
    # ...but enqueue failed, so nothing is reported accepted-async — every
    # item lands in rejections instead (#2825 acknowledged-set discipline).
    assert outcome.job_id is None
    assert outcome.count == 0
    assert {r.external_id for r in outcome.rejections} == {"f1", "f2"}
    assert all(r.reason == "async_offload_failed" for r in outcome.rejections)
    # execute_process raised — no job could possibly be reading the file
    # it never got to enqueue, so the orphaned spill must be cleaned up.
    assert fake_storage.deleted == [fake_storage.uploads[0]["target_path"]]


@pytest.mark.asyncio
async def test_missing_db_resource_yields_rejections_and_cleans_up_spill(monkeypatch):
    """The upload happens before the ``ctx.db_resource`` check — a missing
    engine fails before enqueue is even attempted, so (like an
    execute_process failure) no job could be reading the spilled file and
    it must be cleaned up rather than orphaned."""
    fake_storage = _FakeStorage()
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", lambda proto: fake_storage,
    )

    ctx = DriverContext.model_construct(
        db_resource=None, processing=None, sidecar_data={}, extensions={},
    )
    items = [
        {"type": "Feature", "id": "f1", "geometry": None, "properties": {}},
    ]
    outcome = await offload_mod.offload_bulk_remainder(
        "cat", "col", items, ctx=ctx, policy_source="/configs/...",
    )

    assert len(fake_storage.uploads) == 1
    assert outcome.job_id is None
    assert {r.external_id for r in outcome.rejections} == {"f1"}
    assert fake_storage.deleted == [fake_storage.uploads[0]["target_path"]]


@pytest.mark.asyncio
async def test_no_job_id_in_result_yields_rejections_but_does_not_delete_spill(monkeypatch):
    """``execute_process`` returning a non-None result already proves a
    runner claimed the work — a task row may genuinely exist and be
    reading ``target_path`` right now even though this function failed to
    recognize a job-id attribute on the result. Unlike the two confirmed-
    no-job cases, this must NOT delete the spill file.
    """
    fake_storage = _FakeStorage()
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", lambda proto: fake_storage,
    )

    class _ResultWithNoRecognizedJobIdAttr:
        pass

    async def _fake_execute_process(*args, **kwargs):
        return _ResultWithNoRecognizedJobIdAttr()

    monkeypatch.setattr(
        "dynastore.modules.processes.processes_module.execute_process",
        _fake_execute_process,
    )

    items = [
        {"type": "Feature", "id": "f1", "geometry": None, "properties": {}},
    ]
    outcome = await offload_mod.offload_bulk_remainder(
        "cat", "col", items, ctx=_ctx(), policy_source="/configs/...",
    )

    assert len(fake_storage.uploads) == 1
    assert outcome.job_id is None
    assert {r.external_id for r in outcome.rejections} == {"f1"}
    assert fake_storage.deleted == [], (
        "a job may already exist for this spill file — must not delete it"
    )
