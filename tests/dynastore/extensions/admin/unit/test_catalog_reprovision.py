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

"""Unit tests for POST /admin/catalogs/{catalog_id}/reprovision.

All protocol collaborators are mocked so no DB or GCP is required.

Scenarios:
  - Happy path: catalog exists → task is enqueued, 202 response with task_id.
  - Catalog not found → 404.
  - CatalogsProtocol unavailable → 503.
  - DatabaseProtocol unavailable → 503.
  - Route is covered by admin_access (sysadmin+admin) policy; the test
    verifies the handler is the expected coroutine (policy wiring tested
    separately in test_admin_policies_shape.py).
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from dynastore.extensions.admin.admin_service import AdminService

_handler = AdminService.reprovision_catalog

_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_catalog(catalog_id: str, provisioning_status: str = "ready") -> MagicMock:
    c = MagicMock()
    c.id = catalog_id
    c.provisioning_status = provisioning_status
    return c


def _make_db_mock() -> MagicMock:
    db = MagicMock()
    db.engine = MagicMock()
    return db


def _make_task_result(task_id: uuid.UUID | None = None) -> MagicMock:
    t = MagicMock()
    t.task_id = task_id or uuid.uuid4()
    return t


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reprovision_enqueues_task_and_returns_202() -> None:
    """Existing catalog → task enqueued, response carries task_id + catalog_id."""
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    from dynastore.models.protocols import DatabaseProtocol

    catalog = _make_catalog("cat-x", provisioning_status="provisioning")
    db_mock = _make_db_mock()
    expected_task_id = uuid.uuid4()
    task_result = _make_task_result(task_id=expected_task_id)

    catalogs_svc = MagicMock()
    catalogs_svc.get_catalog_model = AsyncMock(return_value=catalog)

    def _get_proto(cls):
        if cls is CatalogsProtocol:
            return catalogs_svc
        if cls is DatabaseProtocol:
            return db_mock
        return None

    create_task = AsyncMock(return_value=task_result)

    with (
        patch(_GET_PROTOCOL, side_effect=_get_proto),
        patch(
            "dynastore.extensions.admin.admin_service.tasks_module"
            if False  # resolved inside the handler's local import
            else "dynastore.modules.tasks.tasks_module.create_task_for_catalog",
            new=create_task,
        ),
    ):
        # We have to call through the local import path the handler uses.
        # Patch the module the handler imports at call time.
        with patch(
            "dynastore.modules.tasks.tasks_module",
        ) as tasks_mod_mock:
            tasks_mod_mock.create_task_for_catalog = AsyncMock(return_value=task_result)
            result = await _handler(catalog_id="cat-x")

    assert result["task_id"] == str(expected_task_id)
    assert result["catalog_id"] == "cat-x"
    assert result["status"] == "queued"
    assert result["provisioning_status"] == "provisioning"


@pytest.mark.asyncio
async def test_reprovision_404_unknown_catalog() -> None:
    from dynastore.models.protocols.catalogs import CatalogsProtocol

    catalogs_svc = MagicMock()
    catalogs_svc.get_catalog_model = AsyncMock(return_value=None)

    def _get_proto(cls):
        if cls is CatalogsProtocol:
            return catalogs_svc
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto):
        with pytest.raises(HTTPException) as exc_info:
            await _handler(catalog_id="nonexistent")

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_reprovision_503_catalogs_unavailable() -> None:
    def _get_proto(cls):
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto):
        with pytest.raises(HTTPException) as exc_info:
            await _handler(catalog_id="any")

    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_reprovision_503_db_unavailable() -> None:
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    from dynastore.models.protocols import DatabaseProtocol

    catalog = _make_catalog("cat-y")
    catalogs_svc = MagicMock()
    catalogs_svc.get_catalog_model = AsyncMock(return_value=catalog)

    def _get_proto(cls):
        if cls is CatalogsProtocol:
            return catalogs_svc
        if cls is DatabaseProtocol:
            return None  # DB unavailable
        return None

    with patch(_GET_PROTOCOL, side_effect=_get_proto):
        with pytest.raises(HTTPException) as exc_info:
            await _handler(catalog_id="cat-y")

    assert exc_info.value.status_code == 503


# ---------------------------------------------------------------------------
# Policy shape: confirm admin_access covers the reprovision route path
# ---------------------------------------------------------------------------

def test_admin_access_policy_covers_reprovision_path() -> None:
    """admin_access resource pattern '/admin/.*' must match the reprovision path."""
    import re
    from dynastore.extensions.admin.policies import admin_policies

    path = "/admin/catalogs/my-catalog/reprovision"
    policies = {p.id: p for p in admin_policies()}
    admin_access = policies["admin_access"]

    matched = any(
        re.match(r, path)
        for r in admin_access.resources
    )
    assert matched, (
        f"admin_access resources {admin_access.resources!r} do not match "
        f"{path!r} — reprovision route is ungated"
    )
