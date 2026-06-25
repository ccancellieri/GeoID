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

"""Unit tests for the async catalog-create path (#2329).

Verifies:
  - ``_async_catalog_create_enabled`` reads DYNASTORE_ASYNC_CATALOG_CREATE correctly.
  - On the async path: ``provisioning_status='provisioning'`` is set, the
    checklist is seeded with ``catalog_core: pending`` (plus any other active
    provisioners), and a ``catalog_core_init`` task is enqueued in the global
    task schema.
  - ``_run_core_init`` is NOT called on the async path.

All DB I/O is mocked — no live database required.
"""

from __future__ import annotations

import json
import os
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_txn_ctx(fake_conn: Any) -> Any:
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=fake_conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


def _make_catalog_model(ext_id: str = "my-catalog") -> Any:
    """Return a minimal Catalog-like object with only the fields _create_catalog_async touches."""
    m = MagicMock()
    m.id = ext_id
    m.provisioning_status = "ready"
    m.external_id = None
    return m


# ---------------------------------------------------------------------------
# Flag helper
# ---------------------------------------------------------------------------


class TestAsyncCatalogCreateFlag:
    def test_flag_off_by_default(self):
        from dynastore.modules.catalog.catalog_service import _async_catalog_create_enabled

        env = {k: v for k, v in os.environ.items() if k != "DYNASTORE_ASYNC_CATALOG_CREATE"}
        with patch.dict("os.environ", env, clear=True):
            assert _async_catalog_create_enabled() is False

    def test_flag_on_true(self):
        from dynastore.modules.catalog.catalog_service import _async_catalog_create_enabled

        with patch.dict("os.environ", {"DYNASTORE_ASYNC_CATALOG_CREATE": "true"}):
            assert _async_catalog_create_enabled() is True

    def test_flag_on_1(self):
        from dynastore.modules.catalog.catalog_service import _async_catalog_create_enabled

        with patch.dict("os.environ", {"DYNASTORE_ASYNC_CATALOG_CREATE": "1"}):
            assert _async_catalog_create_enabled() is True

    def test_flag_on_yes(self):
        from dynastore.modules.catalog.catalog_service import _async_catalog_create_enabled

        with patch.dict("os.environ", {"DYNASTORE_ASYNC_CATALOG_CREATE": "yes"}):
            assert _async_catalog_create_enabled() is True

    def test_flag_off_explicit_false(self):
        from dynastore.modules.catalog.catalog_service import _async_catalog_create_enabled

        with patch.dict("os.environ", {"DYNASTORE_ASYNC_CATALOG_CREATE": "false"}):
            assert _async_catalog_create_enabled() is False


# ---------------------------------------------------------------------------
# Shared patch context for _create_catalog_async
# ---------------------------------------------------------------------------


def _make_async_create_patches(
    fake_conn: Any,
    committed_id: str = "c_abc123",
    extra_checklist: dict | None = None,
    create_task_side_effect: Any = None,
) -> tuple:
    """Return a tuple of (patch-list, state-dict) for the async create path.

    ``state_dict`` accumulates side-effect captures so assertions can read them
    after the context manager exits.
    """
    state: dict = {"checklist": {}, "tasks": []}
    extra_checklist = extra_checklist or {}

    async def _fake_insert(conn, *, external_id, provisioning_status):
        return committed_id

    async def _fake_set_checklist(conn, *, id, status, checklist):
        state["checklist"] = json.loads(checklist)

    async def _fake_create_task(conn, task_request, schema):
        state["tasks"].append((task_request, schema))
        return MagicMock()

    mock_reg = MagicMock()
    mock_reg.build_checklist = AsyncMock(return_value=extra_checklist)

    from unittest.mock import patch as _patch

    patches = [
        _patch(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            return_value=MagicMock(),
        ),
        _patch(
            "dynastore.modules.catalog.catalog_service.managed_transaction",
            return_value=_make_txn_ctx(fake_conn),
        ),
        _patch("dynastore.modules.catalog.catalog_service.emit_event", new=AsyncMock()),
        _patch("dynastore.modules.catalog.catalog_service.DQLQuery"),  # tombstone query
        _patch(
            "dynastore.modules.catalog.catalog_service._insert_catalog_row_with_pk_retry",
            side_effect=_fake_insert,
        ),
        _patch(
            "dynastore.modules.catalog.catalog_service._set_provisioning_checklist_query"
        ),
        _patch(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache"
        ),
        _patch(
            "dynastore.modules.catalog.catalog_service._invalidate_catalog_external_id_cache"
        ),
    ]

    return patches, state, mock_reg, _fake_set_checklist, _fake_create_task


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAsyncCatalogCreatePath:

    @pytest.mark.asyncio
    async def test_returns_provisioning_status(self):
        """Async path must return catalog_model with provisioning_status='provisioning'."""
        from dynastore.modules.catalog.catalog_service import CatalogService

        svc = object.__new__(CatalogService)
        fake_conn = AsyncMock()
        catalog_model = _make_catalog_model("my-catalog")

        patches, state, mock_reg, fake_set, fake_create = _make_async_create_patches(fake_conn)

        with (
            patches[0], patches[1], patches[2],
            patches[3] as mock_dql_cls,
            patches[4], patches[5] as mock_cl_q,
            patches[6], patches[7],
        ):
            mock_dql_cls.return_value.execute = AsyncMock(return_value=None)
            mock_cl_q.execute = AsyncMock(side_effect=fake_set)

            with (
                patch(
                    "dynastore.modules.catalog.provisioning_registry.provisioning_registry",
                    mock_reg,
                ),
                patch(
                    "dynastore.modules.tasks.tasks_module.create_task",
                    new=AsyncMock(side_effect=fake_create),
                ),
            ):
                result = await svc._create_catalog_async(catalog_model, "my-catalog", None)

        assert result.provisioning_status == "provisioning"

    @pytest.mark.asyncio
    async def test_catalog_core_pending_in_checklist(self):
        """catalog_core: pending must always be in the seeded checklist."""
        from dynastore.modules.catalog.catalog_service import CatalogService

        svc = object.__new__(CatalogService)
        fake_conn = AsyncMock()
        catalog_model = _make_catalog_model("cat2")

        # Simulate one extra active provisioner (e.g. GCP).
        patches, state, mock_reg, fake_set, fake_create = _make_async_create_patches(
            fake_conn, committed_id="c_def456", extra_checklist={"gcp_bucket": "pending"}
        )

        with (
            patches[0], patches[1], patches[2],
            patches[3] as mock_dql_cls,
            patches[4], patches[5] as mock_cl_q,
            patches[6], patches[7],
        ):
            mock_dql_cls.return_value.execute = AsyncMock(return_value=None)
            mock_cl_q.execute = AsyncMock(side_effect=fake_set)

            with (
                patch(
                    "dynastore.modules.catalog.provisioning_registry.provisioning_registry",
                    mock_reg,
                ),
                patch(
                    "dynastore.modules.tasks.tasks_module.create_task",
                    new=AsyncMock(side_effect=fake_create),
                ),
            ):
                await svc._create_catalog_async(catalog_model, "cat2", None)

        assert state["checklist"].get("catalog_core") == "pending"
        # Other active provisioners are also kept.
        assert state["checklist"].get("gcp_bucket") == "pending"

    @pytest.mark.asyncio
    async def test_run_core_init_not_called(self):
        """_run_core_init must NOT be called on the async path."""
        from dynastore.modules.catalog.catalog_service import CatalogService

        svc = object.__new__(CatalogService)
        svc._run_core_init = AsyncMock()  # type: ignore[attr-defined]

        fake_conn = AsyncMock()
        catalog_model = _make_catalog_model("cat3")

        patches, state, mock_reg, fake_set, fake_create = _make_async_create_patches(
            fake_conn, committed_id="c_ghi789"
        )

        with (
            patches[0], patches[1], patches[2],
            patches[3] as mock_dql_cls,
            patches[4], patches[5] as mock_cl_q,
            patches[6], patches[7],
        ):
            mock_dql_cls.return_value.execute = AsyncMock(return_value=None)
            mock_cl_q.execute = AsyncMock(side_effect=fake_set)

            with (
                patch(
                    "dynastore.modules.catalog.provisioning_registry.provisioning_registry",
                    mock_reg,
                ),
                patch(
                    "dynastore.modules.tasks.tasks_module.create_task",
                    new=AsyncMock(side_effect=fake_create),
                ),
            ):
                await svc._create_catalog_async(catalog_model, "cat3", None)

        svc._run_core_init.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_task_enqueued_with_correct_type_and_inputs(self):
        """catalog_core_init task must be enqueued with correct type, inputs, and global schema."""
        from dynastore.modules.catalog.catalog_service import CatalogService

        svc = object.__new__(CatalogService)
        fake_conn = AsyncMock()
        committed_id = "c_jkl000"
        catalog_model = _make_catalog_model("my-ext-id")

        patches, state, mock_reg, fake_set, fake_create = _make_async_create_patches(
            fake_conn, committed_id=committed_id
        )

        with (
            patches[0], patches[1], patches[2],
            patches[3] as mock_dql_cls,
            patches[4], patches[5] as mock_cl_q,
            patches[6], patches[7],
        ):
            mock_dql_cls.return_value.execute = AsyncMock(return_value=None)
            mock_cl_q.execute = AsyncMock(side_effect=fake_set)

            with (
                patch(
                    "dynastore.modules.catalog.provisioning_registry.provisioning_registry",
                    mock_reg,
                ),
                patch(
                    "dynastore.modules.tasks.tasks_module.create_task",
                    new=AsyncMock(side_effect=fake_create),
                ),
            ):
                await svc._create_catalog_async(catalog_model, "my-ext-id", None)

        assert len(state["tasks"]) == 1
        task_req, schema = state["tasks"][0]
        assert task_req.task_type == "catalog_core_init"
        assert task_req.inputs["catalog_id"] == committed_id
        assert task_req.inputs["external_id"] == "my-ext-id"
        # Must go into the global task schema, NOT the (non-existent) tenant schema.
        assert schema == "tasks"  # get_task_schema() default
