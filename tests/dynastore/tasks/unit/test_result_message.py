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

"""Unit tests for the shared task-result *message* helpers.

These cover the "general way" every async task sets its OGC job ``message``:

* the success constructors (:func:`completed`, :func:`with_message`),
* the server-owned export delivery (:func:`server_output_uri` +
  :func:`signed_result_url`),
* the in-place resource verify-URL builders (collection / items / asset), and
* the never-null fallback baked into ``task_to_status_info`` so terminal jobs
  always carry a message.
"""

from uuid import UUID

import pytest

from dynastore.models.protocols import (
    CloudIdentityProtocol,
    CloudStorageClientProtocol,
    StorageProtocol,
)
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.tasks import Task, TaskStatusEnum
from dynastore.modules.processes.models import task_to_status_info
from dynastore.tasks import result_message as rm

_JOB = "019e7591-d4ba-7de0-9364-51ad62f54e17"


# --------------------------------------------------------------------------- #
# Pure constructors
# --------------------------------------------------------------------------- #


def test_completed_builds_success_statusinfo():
    si = rm.completed(_JOB, "hello")
    assert si.status == TaskStatusEnum.COMPLETED
    assert si.message == "hello"
    assert si.progress == 100
    assert si.links == []
    assert isinstance(si.jobID, UUID)


def test_with_message_preserves_payload():
    out = rm.with_message({"info": {"x": 1}}, "msg-url")
    assert out == {"info": {"x": 1}, "message": "msg-url"}


def test_results_link_has_ogc_results_rel():
    link = rm.results_link("https://example.org/file.zip")
    assert link.rel.endswith("results")
    assert link.href == "https://example.org/file.zip"


def test_reference_result_is_link_keyed_by_output_id():
    # A by-reference output is a map keyed by the declared output id whose value
    # is a Link-shaped {href, type} qualified value (OGC Processes §7.13).
    out = rm.reference_result(
        "result", "https://example.org/x.geojson", media_type="application/geo+json"
    )
    assert out["result"] == {
        "href": "https://example.org/x.geojson",
        "type": "application/geo+json",
    }
    # The human-facing status message defaults to the artifact href.
    assert out["message"] == "https://example.org/x.geojson"


def test_reference_result_keeps_message_funnel_for_status_doc():
    # The carried ``message`` is what task_to_status_info surfaces on the job
    # *status* document; it is a sibling of the declared output, not nested in it.
    out = rm.reference_result(
        "result", "https://h/x.geojson", media_type="application/geo+json",
        message="custom status",
    )
    assert out["message"] == "custom status"
    t = Task(task_type="dwh_join", type="process", status=TaskStatusEnum.COMPLETED,
             error_message=None, outputs=out)
    assert task_to_status_info(t).message == "custom status"


# --------------------------------------------------------------------------- #
# Server-owned export delivery
# --------------------------------------------------------------------------- #


def _patch_protocols(monkeypatch, mapping):
    monkeypatch.setattr(rm, "get_protocol", lambda proto: mapping.get(proto))


@pytest.mark.asyncio
async def test_server_output_uri_is_per_job_key(monkeypatch):
    class _Storage:
        async def get_storage_identifier(self, catalog_id):
            return f"bucket-{catalog_id}"

    _patch_protocols(monkeypatch, {StorageProtocol: _Storage()})
    uri = await rm.server_output_uri("datamgr10", "dwh_join", _JOB, "region.zip")
    assert uri == f"gs://bucket-datamgr10/processes/outputs/dwh_join/{_JOB}/region.zip"


@pytest.mark.asyncio
async def test_server_output_uri_raises_without_bucket(monkeypatch):
    class _Storage:
        async def get_storage_identifier(self, catalog_id):
            return None

    _patch_protocols(monkeypatch, {StorageProtocol: _Storage()})
    with pytest.raises(RuntimeError, match="No storage bucket"):
        await rm.server_output_uri("cat", "dwh_join", _JOB, "x.zip")


@pytest.mark.asyncio
async def test_signed_result_url_falls_back_when_no_client(monkeypatch):
    _patch_protocols(monkeypatch, {})  # no CloudStorageClientProtocol
    assert await rm.signed_result_url("gs://b/x.zip") == "gs://b/x.zip"


@pytest.mark.asyncio
async def test_signed_result_url_signs_when_available(monkeypatch):
    _patch_protocols(
        monkeypatch,
        {CloudStorageClientProtocol: object(), CloudIdentityProtocol: object()},
    )

    async def _fake_sign(gs_uri, **kw):
        assert kw["method"] == "GET"
        # Download URLs must NOT pin content-type: doing so adds it to
        # X-Goog-SignedHeaders and breaks a plain browser GET with
        # MalformedSecurityHeader.
        assert "content_type" not in kw
        return gs_uri + "?X-Goog-Expires=604800"

    monkeypatch.setattr(rm, "generate_gcs_signed_url", _fake_sign)
    signed = await rm.signed_result_url("gs://b/x.zip")
    assert signed == "gs://b/x.zip?X-Goog-Expires=604800"


@pytest.mark.asyncio
async def test_signed_result_url_never_raises(monkeypatch):
    _patch_protocols(monkeypatch, {CloudStorageClientProtocol: object()})

    async def _boom(*a, **k):
        raise RuntimeError("signing down")

    monkeypatch.setattr(rm, "generate_gcs_signed_url", _boom)
    # A signing failure must not sink a successful export.
    assert await rm.signed_result_url("gs://b/x.zip") == "gs://b/x.zip"


# --------------------------------------------------------------------------- #
# Verify-URL builders
# --------------------------------------------------------------------------- #


def _patch_base(monkeypatch, base):
    class _Configs:
        async def get_config(self, cls, *a, **k):
            return rm.PublicUrlConfig(public_base_url=base)

    _patch_protocols(monkeypatch, {ConfigsProtocol: _Configs()})


@pytest.mark.asyncio
async def test_collection_verify_url_strips_trailing_slash(monkeypatch):
    _patch_base(monkeypatch, "https://h/api/catalog/")
    assert (
        await rm.collection_verify_url("cat", "reg")
        == "https://h/api/catalog/features/catalogs/cat/collections/reg"
    )


@pytest.mark.asyncio
async def test_items_verify_url_appends_asset_filter(monkeypatch):
    _patch_base(monkeypatch, "https://h/api/catalog")
    url = await rm.items_verify_url("cat", "reg", asset_id="a1")
    assert url == (
        "https://h/api/catalog/features/catalogs/cat/collections/reg"
        "/items?filter=asset_id%3D%27a1%27"
    )


@pytest.mark.asyncio
async def test_items_verify_url_without_asset(monkeypatch):
    _patch_base(monkeypatch, "https://h/api/catalog")
    assert (
        await rm.items_verify_url("cat", "reg")
        == "https://h/api/catalog/features/catalogs/cat/collections/reg/items"
    )


@pytest.mark.asyncio
async def test_asset_verify_url_collection_and_catalog_scoped(monkeypatch):
    _patch_base(monkeypatch, "https://h/api/catalog")
    assert (
        await rm.asset_verify_url("cat", "reg", "a1")
        == "https://h/api/catalog/assets/catalogs/cat/collections/reg/assets/a1"
    )
    assert (
        await rm.asset_verify_url("cat", None, "a1")
        == "https://h/api/catalog/assets/catalogs/cat/assets/a1"
    )


@pytest.mark.asyncio
async def test_verify_urls_are_root_relative_when_base_unset(monkeypatch):
    _patch_base(monkeypatch, None)
    assert (
        await rm.collection_verify_url("cat", "reg")
        == "/features/catalogs/cat/collections/reg"
    )


@pytest.mark.asyncio
async def test_verify_urls_relative_when_no_configs_protocol(monkeypatch):
    _patch_protocols(monkeypatch, {})  # no ConfigsProtocol
    assert (
        await rm.items_verify_url("cat", "reg")
        == "/features/catalogs/cat/collections/reg/items"
    )


# --------------------------------------------------------------------------- #
# Framework never-null fallback in task_to_status_info
# --------------------------------------------------------------------------- #


def test_ttsi_surfaces_outputs_message():
    t = Task(task_type="dwh_join", type="process", status=TaskStatusEnum.COMPLETED,
             error_message=None, outputs={"message": "SIGNED_URL"})
    assert task_to_status_info(t).message == "SIGNED_URL"


def test_ttsi_synthesises_completed_default_when_no_message():
    t = Task(task_type="ingestion", type="process", status=TaskStatusEnum.COMPLETED,
             error_message=None, outputs={"some": "data"})
    assert task_to_status_info(t).message == "Job 'ingestion' completed."


def test_ttsi_failed_uses_error_message():
    t = Task(task_type="dwh_join", type="process", status=TaskStatusEnum.FAILED,
             error_message="boom", outputs=None)
    assert task_to_status_info(t).message == "boom"


def test_ttsi_synthesises_failed_default():
    t = Task(task_type="tiles_export", type="process", status=TaskStatusEnum.FAILED,
             error_message=None, outputs=None)
    assert task_to_status_info(t).message == "Job 'tiles_export' failed."


def test_ttsi_dead_letter_treated_as_failed():
    t = Task(task_type="ingestion", type="process", status=TaskStatusEnum.DEAD_LETTER,
             error_message=None, outputs=None)
    assert task_to_status_info(t).message == "Job 'ingestion' failed."


def test_ttsi_running_stays_message_less():
    t = Task(task_type="ingestion", type="process", status=TaskStatusEnum.RUNNING,
             error_message=None, outputs=None)
    assert task_to_status_info(t).message is None


def test_ttsi_dismissed_default():
    t = Task(task_type="ingestion", type="process", status=TaskStatusEnum.DISMISSED,
             error_message=None, outputs=None)
    assert task_to_status_info(t).message == "Job 'ingestion' was dismissed."
