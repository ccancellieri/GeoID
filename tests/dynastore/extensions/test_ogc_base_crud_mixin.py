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

"""Unit tests for the shared catalog + collection CRUD bodies on ``OGCServiceMixin``.

Covers M-2 (catalog CRUD) and M-3 (collection CRUD) extracted in issue #1510.

Tests prove:

* (a) Default hooks fire the Features-style behaviour (no pre-create
  validation, no readiness guard, standard ``.localize()`` path,
  ``DriverContext`` forwarded when a ``db_resource`` is given).
* (b) A STAC-like subclass's overrides are invoked: validation called,
  ``stac_localize`` used, readiness guard called, ``stac_context=True``
  passed to ``create_collection``.
* (c) Delegation passes through arguments and catalogs-service calls correctly.

All collaborators are mocked — no database is touched.
"""

import json as _json_mod
import uuid as _uuid_mod
from typing import Any, Dict, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.extensions.ogc_base import OGCServiceMixin


# ---------------------------------------------------------------------------
# Shared helpers / stub models
# ---------------------------------------------------------------------------


def _make_fake_conn() -> MagicMock:
    """Return a MagicMock that passes ``isinstance(x, AsyncConnection)`` checks.

    ``DriverContext.db_resource`` is typed as ``Optional[DbResource]`` and
    Pydantic validates the type at runtime.  Using ``spec=AsyncConnection``
    makes the mock pass the discriminated-union isinstance check without
    requiring a live database session.
    """
    return MagicMock(spec=AsyncConnection)


class _LocalizableCatalog:
    """Minimal stub that mimics ``LocalizableModelMixin.localize``."""

    def localize(self, language: str) -> Tuple[Dict[str, Any], Any]:
        return {"id": "cat1", "lang": language}, {"en"}


def _make_catalogs_svc(**overrides) -> AsyncMock:
    """Return a mock CatalogsProtocol with standard return values."""
    svc = AsyncMock()
    svc.create_catalog = AsyncMock(return_value=_LocalizableCatalog())
    svc.update_catalog = AsyncMock(return_value=_LocalizableCatalog())
    svc.delete_catalog = AsyncMock(return_value=True)
    svc.create_collection = AsyncMock(return_value=_LocalizableCatalog())
    svc.update_collection = AsyncMock(return_value=_LocalizableCatalog())
    svc.delete_collection = AsyncMock(return_value=True)
    for k, v in overrides.items():
        setattr(svc, k, v)
    return svc


# ---------------------------------------------------------------------------
# Features-style subclass (default hook behaviour)
# ---------------------------------------------------------------------------


class _FeaturesSvc(OGCServiceMixin):
    """Minimal concrete subclass exercising the default (Features) hooks."""


# ---------------------------------------------------------------------------
# STAC-style subclass — records calls to each override
# ---------------------------------------------------------------------------


class _STACSvc(OGCServiceMixin):
    """Subclass with STAC-like hook overrides for verifying the seams."""

    def __init__(self):
        self._validate_catalog_create_called = False
        self._require_catalog_write_ready_calls: list = []
        self._pre_update_collection_validate_calls: list = []

    def _validate_catalog_create(self) -> None:
        self._validate_catalog_create_called = True

    async def _require_catalog_write_ready(
        self, catalog_id: str, catalogs_svc=None
    ) -> None:
        self._require_catalog_write_ready_calls.append(catalog_id)

    def _make_collection_create_kwargs(self) -> Dict[str, Any]:
        return {"stac_context": True}

    def _localize_resource(self, model: Any, language: str) -> Tuple[Dict[str, Any], Any]:
        # Simulate stac_localize — wraps the standard output in a STAC key.
        data, langs = model.localize(language)
        return {"stac": True, **data}, langs

    async def _pre_update_collection_validate(
        self,
        catalog_id: str,
        collection_id: str,
        input_data: Dict[str, Any],
        request=None,
    ) -> None:
        self._pre_update_collection_validate_calls.append(
            (catalog_id, collection_id, input_data, request)
        )


# ---------------------------------------------------------------------------
# M-2: catalog CRUD — Features-style defaults
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ogc_create_catalog_features_no_validation():
    """Default _validate_catalog_create is a no-op — no exception raised."""
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=_make_catalogs_svc())

    resp = await svc._ogc_create_catalog(
        {"id": "cat1"}, {"id": "cat1"}, "en", _make_fake_conn()
    )
    assert resp.status_code == 201


@pytest.mark.asyncio
async def test_ogc_create_catalog_features_passes_ctx():
    """Features passes a DriverContext when db_resource is not None."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    db_conn = _make_fake_conn()
    await svc._ogc_create_catalog({"id": "cat1"}, {"id": "cat1"}, "en", db_conn)

    create_call = catalogs_svc.create_catalog.call_args
    assert "ctx" in create_call.kwargs
    from dynastore.models.driver_context import DriverContext
    assert isinstance(create_call.kwargs["ctx"], DriverContext)
    assert create_call.kwargs["ctx"].db_resource is db_conn


@pytest.mark.asyncio
async def test_ogc_create_catalog_features_no_ctx_when_db_resource_none():
    """When db_resource is None no ctx kwarg is forwarded."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_create_catalog({"id": "cat1"}, {"id": "cat1"}, "en", None)

    create_call = catalogs_svc.create_catalog.call_args
    assert "ctx" not in create_call.kwargs


@pytest.mark.asyncio
async def test_ogc_create_catalog_features_standard_localize():
    """Default _localize_resource uses model.localize()."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_create_catalog({"id": "cat1"}, {"id": "cat1"}, "fr", None)
    import json
    data = json.loads(bytes(resp.body))
    assert data["lang"] == "fr"
    assert "stac" not in data


@pytest.mark.asyncio
async def test_ogc_replace_catalog_features_no_readiness_guard():
    """Default _require_catalog_write_ready is a no-op."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_replace_catalog("cat1", {"id": "cat1"}, "en", _make_fake_conn())
    assert resp.status_code == 200
    catalogs_svc.update_catalog.assert_awaited_once()


@pytest.mark.asyncio
async def test_ogc_replace_catalog_features_404_on_missing():
    """404 is raised when update_catalog returns None."""
    catalogs_svc = _make_catalogs_svc(update_catalog=AsyncMock(return_value=None))
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_replace_catalog("cat1", {"id": "cat1"}, "en", None)
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_ogc_update_catalog_features():
    """PATCH: detect_use_lang applied; update_catalog called with correct lang."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_update_catalog("cat1", {"title": {"en": "Test"}}, "en", None)
    catalogs_svc.update_catalog.assert_awaited_once()


@pytest.mark.asyncio
async def test_ogc_delete_catalog_features_passes_ctx():
    """Features delete passes DriverContext; returns 204 on success."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    db_conn = _make_fake_conn()
    resp = await svc._ogc_delete_catalog("cat1", False, db_conn)
    assert resp.status_code == 204

    delete_call = catalogs_svc.delete_catalog.call_args
    from dynastore.models.driver_context import DriverContext
    assert isinstance(delete_call.kwargs.get("ctx"), DriverContext)
    assert delete_call.kwargs["ctx"].db_resource is db_conn


@pytest.mark.asyncio
async def test_ogc_delete_catalog_features_404():
    """404 raised when delete_catalog returns False."""
    catalogs_svc = _make_catalogs_svc(delete_catalog=AsyncMock(return_value=False))
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_delete_catalog("cat1", False, None)
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# M-2: catalog CRUD — STAC override hooks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ogc_create_catalog_stac_calls_validate_hook():
    """_validate_catalog_create is invoked on STAC create."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_create_catalog({"id": "cat1"}, {"id": "cat1"}, "en", None)
    assert svc._validate_catalog_create_called


@pytest.mark.asyncio
async def test_ogc_create_catalog_stac_uses_stac_localize():
    """STAC _localize_resource override is called; returns STAC-keyed dict."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_create_catalog({"id": "cat1"}, {"id": "cat1"}, "en", None)
    import json
    data = json.loads(bytes(resp.body))
    assert data.get("stac") is True


@pytest.mark.asyncio
async def test_ogc_replace_catalog_stac_calls_readiness_guard():
    """_require_catalog_write_ready is invoked for STAC replace."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_replace_catalog("mycat", {"id": "mycat"}, "en", None)
    assert "mycat" in svc._require_catalog_write_ready_calls


@pytest.mark.asyncio
async def test_ogc_update_catalog_stac_calls_readiness_guard():
    """_require_catalog_write_ready is invoked for STAC update."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_update_catalog("mycat", {"title": "x"}, "en", None)
    assert "mycat" in svc._require_catalog_write_ready_calls


@pytest.mark.asyncio
async def test_ogc_delete_catalog_stac_no_ctx():
    """STAC delete passes None db_resource → no ctx kwarg forwarded."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_delete_catalog("cat1", False, None)
    assert resp.status_code == 204
    delete_call = catalogs_svc.delete_catalog.call_args
    assert "ctx" not in delete_call.kwargs


# ---------------------------------------------------------------------------
# M-3: collection CRUD — Features-style defaults
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ogc_create_collection_features_no_stac_context():
    """Default _make_collection_create_kwargs returns empty dict."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_create_collection("cat1", {"id": "col1"}, "en", _make_fake_conn())
    create_call = catalogs_svc.create_collection.call_args
    assert "stac_context" not in create_call.kwargs


@pytest.mark.asyncio
async def test_ogc_create_collection_features_passes_ctx():
    """Features create_collection includes DriverContext."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    db_conn = _make_fake_conn()
    await svc._ogc_create_collection("cat1", {"id": "col1"}, "en", db_conn)
    create_call = catalogs_svc.create_collection.call_args
    from dynastore.models.driver_context import DriverContext
    assert isinstance(create_call.kwargs.get("ctx"), DriverContext)
    assert create_call.kwargs["ctx"].db_resource is db_conn


@pytest.mark.asyncio
async def test_ogc_replace_collection_features_no_readiness():
    """Default replace does not call readiness guard."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_replace_collection("cat1", "col1", {"id": "col1"}, "en")
    assert resp.status_code == 200
    catalogs_svc.update_collection.assert_awaited_once()


@pytest.mark.asyncio
async def test_ogc_replace_collection_features_404():
    """404 raised when update_collection returns None."""
    catalogs_svc = _make_catalogs_svc(update_collection=AsyncMock(return_value=None))
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_replace_collection("cat1", "col1", {"id": "col1"}, "en")
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_ogc_update_collection_features_no_validate_hook():
    """Default _pre_update_collection_validate is a no-op (no exception)."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_update_collection("cat1", "col1", {"title": "x"}, "en")
    assert resp.status_code == 200
    catalogs_svc.update_collection.assert_awaited_once()


@pytest.mark.asyncio
async def test_ogc_delete_collection_features_passes_ctx():
    """Features delete_collection includes DriverContext; returns 204."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    db_conn = _make_fake_conn()
    resp = await svc._ogc_delete_collection("cat1", "col1", False, db_conn)
    assert resp.status_code == 204

    delete_call = catalogs_svc.delete_collection.call_args
    from dynastore.models.driver_context import DriverContext
    ctx_val = delete_call.kwargs.get("ctx")
    assert isinstance(ctx_val, DriverContext)
    assert ctx_val.db_resource is db_conn


@pytest.mark.asyncio
async def test_ogc_delete_collection_features_404():
    """404 raised when delete_collection returns False."""
    catalogs_svc = _make_catalogs_svc(delete_collection=AsyncMock(return_value=False))
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_delete_collection("cat1", "col1", False, None)
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# M-3: collection CRUD — STAC override hooks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ogc_create_collection_stac_passes_stac_context():
    """STAC _make_collection_create_kwargs injects stac_context=True."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_create_collection("cat1", {"id": "col1"}, "en", None)
    create_call = catalogs_svc.create_collection.call_args
    assert create_call.kwargs.get("stac_context") is True


@pytest.mark.asyncio
async def test_ogc_create_collection_stac_calls_readiness():
    """STAC create calls _require_catalog_write_ready."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_create_collection("mycat", {"id": "col1"}, "en", None)
    assert "mycat" in svc._require_catalog_write_ready_calls


@pytest.mark.asyncio
async def test_ogc_create_collection_stac_uses_stac_localize():
    """STAC _localize_resource wraps the returned model."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_create_collection("cat1", {"id": "col1"}, "en", None)
    import json
    data = json.loads(bytes(resp.body))
    assert data.get("stac") is True


@pytest.mark.asyncio
async def test_ogc_replace_collection_stac_calls_readiness():
    """STAC replace calls readiness guard."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_replace_collection("mycat", "col1", {"id": "col1"}, "en")
    assert "mycat" in svc._require_catalog_write_ready_calls


@pytest.mark.asyncio
async def test_ogc_update_collection_stac_calls_pre_validate_hook():
    """STAC _pre_update_collection_validate is called with correct args."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    sentinel_request: Any = object()
    patch_data = {"title": "new"}
    await svc._ogc_update_collection("cat1", "col1", patch_data, "en", sentinel_request)

    assert len(svc._pre_update_collection_validate_calls) == 1
    cat_id, col_id, data, req = svc._pre_update_collection_validate_calls[0]
    assert cat_id == "cat1"
    assert col_id == "col1"
    assert data is patch_data
    assert req is sentinel_request


@pytest.mark.asyncio
async def test_ogc_update_collection_stac_calls_readiness():
    """STAC update calls readiness guard."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    await svc._ogc_update_collection("mycat", "col1", {"title": "x"}, "en")
    assert "mycat" in svc._require_catalog_write_ready_calls


@pytest.mark.asyncio
async def test_ogc_delete_collection_stac_no_ctx():
    """STAC delete passes None db_resource → no ctx forwarded."""
    catalogs_svc = _make_catalogs_svc()
    svc = _STACSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_delete_collection("cat1", "col1", False, None)
    assert resp.status_code == 204
    delete_call = catalogs_svc.delete_collection.call_args
    assert "ctx" not in delete_call.kwargs


# ---------------------------------------------------------------------------
# Validate hook raises propagate to the caller
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ogc_create_catalog_validation_failure_propagates():
    """When _validate_catalog_create raises, the error propagates out."""
    class _FailValidateSvc(OGCServiceMixin):
        def _validate_catalog_create(self) -> None:
            raise HTTPException(status_code=422, detail="No STAC driver.")

    catalogs_svc = _make_catalogs_svc()
    svc = _FailValidateSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_create_catalog({"id": "x"}, {"id": "x"}, "en", None)
    assert exc_info.value.status_code == 422
    catalogs_svc.create_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_ogc_create_collection_readiness_failure_propagates():
    """When _require_catalog_write_ready raises, the write is aborted."""
    class _FailReadySvc(OGCServiceMixin):
        async def _require_catalog_write_ready(self, catalog_id, catalogs_svc=None):
            raise HTTPException(status_code=409, detail="Not provisioned.")

    catalogs_svc = _make_catalogs_svc()
    svc = _FailReadySvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_create_collection("cat1", {"id": "col1"}, "en", None)
    assert exc_info.value.status_code == 409
    catalogs_svc.create_collection.assert_not_awaited()


# ---------------------------------------------------------------------------
# M-3b: collection hard delete — async 202 path (force=True)
# ---------------------------------------------------------------------------


def _make_fake_task(task_id: str = "11111111-1111-1111-1111-111111111111", task_status: str = "PENDING"):
    """Return a minimal Task-like object for mocking create_task / get_task."""
    task = MagicMock()
    task.task_id = _uuid_mod.UUID(task_id)
    task.jobID = _uuid_mod.UUID(task_id)
    task.status = task_status
    return task


def _make_fake_request(url_for_result: str = "http://test/tasks/catalogs/cat1/11111111-1111-1111-1111-111111111111"):
    """Return a minimal Request-like mock."""
    req = MagicMock()
    req.url_for.return_value = url_for_result
    req.base_url = "http://test/"
    req.scope = {"root_path": ""}
    return req


@pytest.mark.asyncio
async def test_ogc_delete_collection_hard_returns_202():
    """force=True enqueues a task and returns HTTP 202 with Location + body."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    fake_task = _make_fake_task()
    fake_request = _make_fake_request()

    # The imports inside _ogc_delete_collection are local, so we patch their
    # canonical module locations rather than the ogc_base namespace.
    mock_conn_ctx = AsyncMock()
    mock_conn_ctx.__aenter__ = AsyncMock(return_value=AsyncMock())
    mock_conn_ctx.__aexit__ = AsyncMock(return_value=None)

    with (
        patch(
            "dynastore.tools.protocol_helpers.get_engine",
            return_value=MagicMock(),
        ),
        patch(
            "dynastore.modules.tasks.tasks_module._resolve_catalog_schema",
            new=AsyncMock(return_value="s_abc123"),
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.create_task",
            new=AsyncMock(return_value=fake_task),
        ),
        patch(
            "dynastore.modules.db_config.query_executor.managed_transaction",
            return_value=mock_conn_ctx,
        ),
    ):
        resp = await svc._ogc_delete_collection("cat1", "col1", True, None, fake_request)

    assert resp.status_code == 202
    assert "Location" in resp.headers
    assert resp.headers["Location"] == str(fake_request.url_for.return_value)

    body = _json_mod.loads(resp.body)
    assert body["task_id"] == "11111111-1111-1111-1111-111111111111"
    assert body["collection_id"] == "col1"
    assert len(body["links"]) == 1
    assert body["links"][0]["rel"] == "monitor"


@pytest.mark.asyncio
async def test_ogc_delete_collection_hard_dedup_returns_202():
    """When create_task returns None (dedup hit), the existing task link is returned as 202."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    fake_request = _make_fake_request()

    existing_task_dict = {
        "task_id": "22222222-2222-2222-2222-222222222222",
        "schema_name": "s_abc123",
        "scope": "CATALOG",
        "caller_id": "user@example.com",
        "task_type": "collection_hard_delete",
        "type": "task",
        "execution_mode": "ASYNCHRONOUS",
        "status": "PENDING",
        "progress": 0,
        "inputs": None,
        "outputs": None,
        "error_message": None,
        "dedup_key": "collection_hard_delete:cat1:col1",
        "timestamp": "2026-06-15T00:00:00+00:00",
        "started_at": None,
        "finished_at": None,
        "collection_id": "col1",
        "locked_until": None,
        "last_heartbeat_at": None,
        "owner_id": None,
        "runner_ref": None,
        "retry_count": 0,
        "max_retries": 3,
    }

    mock_conn = AsyncMock()
    mock_conn_ctx_schema = AsyncMock()
    mock_conn_ctx_schema.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn_ctx_schema.__aexit__ = AsyncMock(return_value=None)

    mock_conn_ctx_dedup = AsyncMock()
    mock_conn_ctx_dedup.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn_ctx_dedup.__aexit__ = AsyncMock(return_value=None)

    # managed_transaction is called twice: once for schema resolution, once for
    # the dedup lookup. Return different ctx objects for each call.
    managed_tx_mock = MagicMock(side_effect=[mock_conn_ctx_schema, mock_conn_ctx_dedup])

    with (
        patch(
            "dynastore.tools.protocol_helpers.get_engine",
            return_value=MagicMock(),
        ),
        patch(
            "dynastore.modules.tasks.tasks_module._resolve_catalog_schema",
            new=AsyncMock(return_value="s_abc123"),
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.create_task",
            new=AsyncMock(return_value=None),  # dedup hit
        ),
        patch(
            "dynastore.modules.db_config.query_executor.managed_transaction",
            managed_tx_mock,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.get_task_schema",
            return_value="tasks",
        ),
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
        ) as mock_dql,
    ):
        mock_dql.return_value.execute = AsyncMock(return_value=existing_task_dict)
        resp = await svc._ogc_delete_collection("cat1", "col1", True, None, fake_request)

    assert resp.status_code == 202
    body = _json_mod.loads(resp.body)
    assert body["task_id"] == "22222222-2222-2222-2222-222222222222"


@pytest.mark.asyncio
async def test_ogc_delete_collection_hard_no_engine_503():
    """When no DB engine is available the endpoint returns 503."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with patch("dynastore.tools.protocol_helpers.get_engine", return_value=None):
        with pytest.raises(HTTPException) as exc_info:
            await svc._ogc_delete_collection("cat1", "col1", True, None, None)
    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_ogc_delete_collection_soft_still_204():
    """force=False path is unchanged — synchronous 204."""
    catalogs_svc = _make_catalogs_svc()
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_delete_collection("cat1", "col1", False, None, None)
    assert resp.status_code == 204
    catalogs_svc.delete_collection.assert_awaited_once_with("cat1", "col1", False)


@pytest.mark.asyncio
async def test_ogc_delete_collection_soft_404_still_works():
    """force=False 404 path is unchanged."""
    catalogs_svc = _make_catalogs_svc(delete_collection=AsyncMock(return_value=False))
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_delete_collection("cat1", "col1", False, None, None)
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# Catalog create — always-async 202 path
# ---------------------------------------------------------------------------


class _ProvisioningCatalog:
    """Stub catalog returned when async-create flag is ON.

    Mimics a Catalog that came back with provisioning_status='provisioning'
    and an external_id set.
    """

    provisioning_status = "provisioning"
    external_id = "my-catalog"

    def localize(self, language: str) -> Tuple[Dict[str, Any], Any]:
        return {
            "id": "my-catalog",
            "provisioning_status": "provisioning",
            "provisioning_checklist": {"catalog_core": "pending"},
        }, {"en"}


@pytest.mark.asyncio
async def test_ogc_create_catalog_returns_202_when_provisioning():
    """When create_catalog returns provisioning_status='provisioning', respond 202+Location."""
    catalogs_svc = _make_catalogs_svc(
        create_catalog=AsyncMock(return_value=_ProvisioningCatalog())
    )
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_create_catalog({"id": "my-catalog"}, {"id": "my-catalog"}, "en", None)

    assert resp.status_code == 202
    assert "Location" in resp.headers
    assert resp.headers["Location"] == "/catalog/catalogs/my-catalog"

    import json
    body = json.loads(resp.body)
    assert body["provisioning_status"] == "provisioning"
    assert body["provisioning_checklist"]["catalog_core"] == "pending"


@pytest.mark.asyncio
async def test_ogc_create_catalog_returns_202_when_provisioning_from_ready_stub():
    """Create always returns 202; the OGC layer maps provisioning_status='provisioning' to 202."""
    catalogs_svc = _make_catalogs_svc(
        create_catalog=AsyncMock(return_value=_ProvisioningCatalog())
    )
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_create_catalog({"id": "my-catalog"}, {"id": "my-catalog"}, "en", None)

    assert resp.status_code == 202
    assert "Location" in resp.headers

    import json
    body = json.loads(resp.body)
    assert body["provisioning_status"] == "provisioning"


@pytest.mark.asyncio
async def test_ogc_create_catalog_returns_202_from_provisioning_catalog():
    """The 202 path is exercised end-to-end with the provisioning stub."""
    catalogs_svc = _make_catalogs_svc(
        create_catalog=AsyncMock(return_value=_ProvisioningCatalog())
    )
    svc = _FeaturesSvc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    resp = await svc._ogc_create_catalog({"id": "my-catalog"}, {"id": "my-catalog"}, "en", None)
    assert resp.status_code == 202
    assert resp.headers.get("Location") == "/catalog/catalogs/my-catalog"
