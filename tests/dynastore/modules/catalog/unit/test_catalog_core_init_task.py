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

"""Unit tests for CatalogCoreInitTask (#2329).

Verifies:
  - task_type and importability (registration contract).
  - Happy path: _run_core_init is called, catalog_core step marked complete.
  - Catalog-not-found path: PermanentTaskFailure, step marked failed.
  - _run_core_init failure: step marked failed, exception propagates.
  - Drain safety: catalog_core_init is in _PROVISIONING_TASK_TYPES.

All DB I/O is mocked — pure unit tests, no live DB required.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_task() -> Any:
    from dynastore.tasks.catalog_core_init.task import CatalogCoreInitTask

    return CatalogCoreInitTask()


def _make_payload(catalog_id: str = "c_testcat", external_id: str = "my-catalog") -> Any:
    from dynastore.tasks.catalog_core_init.task import CatalogCoreInitInputs
    from dynastore.models.tasks import TaskPayload

    inputs = CatalogCoreInitInputs(catalog_id=catalog_id, external_id=external_id)
    return TaskPayload(
        task_id=uuid.uuid4(),
        caller_id="test",
        inputs=inputs,
    )


_SENTINEL_MODEL = object()  # used as default to distinguish None from "not passed"


def _make_mock_catalogs(
    catalog_model: Any = _SENTINEL_MODEL,
    init_side_effect: Any = None,
) -> Any:
    """Build a mock CatalogsProtocol with _run_core_init attached.

    Pass ``catalog_model=None`` to simulate a missing catalog (get_catalog_model
    returns None).  Omit it to get a default ``MagicMock()`` model.
    """
    resolved_model = MagicMock() if catalog_model is _SENTINEL_MODEL else catalog_model
    mock = AsyncMock()
    mock.get_catalog_model = AsyncMock(return_value=resolved_model)
    mock.mark_provisioning_step = AsyncMock()
    run_core_init = AsyncMock()
    if init_side_effect is not None:
        run_core_init.side_effect = init_side_effect
    mock._run_core_init = run_core_init
    return mock


def _make_txn_ctx(fake_conn: Any) -> Any:
    mock_txn = MagicMock()
    mock_txn.__aenter__ = AsyncMock(return_value=fake_conn)
    mock_txn.__aexit__ = AsyncMock(return_value=False)
    return mock_txn


# ---------------------------------------------------------------------------
# Registration / metadata
# ---------------------------------------------------------------------------


class TestCatalogCoreInitTaskRegistration:
    def test_task_type(self):
        from dynastore.tasks.catalog_core_init.task import CatalogCoreInitTask

        assert CatalogCoreInitTask.task_type == "catalog_core_init"

    def test_importable_from_package(self):
        from dynastore.tasks.catalog_core_init import CatalogCoreInitTask  # noqa: F401

        assert CatalogCoreInitTask is not None

    def test_in_provisioning_task_types(self):
        """catalog_core_init must be in _PROVISIONING_TASK_TYPES for drain safety."""
        from dynastore.modules.tasks.execution import _PROVISIONING_TASK_TYPES

        assert "catalog_core_init" in _PROVISIONING_TASK_TYPES


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestCatalogCoreInitTaskHappyPath:
    @pytest.mark.asyncio
    async def test_run_calls_core_init_and_marks_complete(self):
        """Happy path: _run_core_init is awaited and catalog_core step is marked complete."""
        task = _make_task()
        payload = _make_payload(catalog_id="c_abc", external_id="my-cat")

        fake_conn = AsyncMock()
        mock_catalog_model = MagicMock()
        mock_catalogs = _make_mock_catalogs(catalog_model=mock_catalog_model)

        with patch(
            "dynastore.tasks.catalog_core_init.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_core_init.task.managed_transaction",
            return_value=_make_txn_ctx(fake_conn),
        ), patch(
            "dynastore.tasks.catalog_core_init.task.get_catalog_engine",
            return_value=MagicMock(),
        ):
            result = await task.run(payload)

        mock_catalogs.get_catalog_model.assert_awaited_once_with("c_abc")
        mock_catalogs._run_core_init.assert_awaited_once_with(
            fake_conn, mock_catalog_model, "my-cat", "c_abc"
        )
        mock_catalogs.mark_provisioning_step.assert_awaited_once_with(
            "c_abc", "catalog_core", "complete"
        )
        assert result["catalog_id"] == "c_abc"
        assert result["status"] == "complete"

    @pytest.mark.asyncio
    async def test_result_contains_external_id(self):
        """Result dict includes external_id for downstream correlation."""
        task = _make_task()
        payload = _make_payload(catalog_id="c_xyz", external_id="ext-label")

        fake_conn = AsyncMock()
        mock_catalogs = _make_mock_catalogs()

        with patch(
            "dynastore.tasks.catalog_core_init.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_core_init.task.managed_transaction",
            return_value=_make_txn_ctx(fake_conn),
        ), patch(
            "dynastore.tasks.catalog_core_init.task.get_catalog_engine",
            return_value=MagicMock(),
        ):
            result = await task.run(payload)

        assert result["external_id"] == "ext-label"


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestCatalogCoreInitTaskErrorPaths:
    @pytest.mark.asyncio
    async def test_catalog_not_found_marks_failed(self):
        """When get_catalog_model returns None, step is marked failed and PermanentTaskFailure raised."""
        from dynastore.modules.tasks.models import PermanentTaskFailure

        task = _make_task()
        payload = _make_payload(catalog_id="c_missing", external_id="gone")

        mock_catalogs = _make_mock_catalogs(catalog_model=None)

        with patch(
            "dynastore.tasks.catalog_core_init.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ):
            with pytest.raises(PermanentTaskFailure, match="not found"):
                await task.run(payload)

        mock_catalogs.mark_provisioning_step.assert_awaited_once_with(
            "c_missing", "catalog_core", "failed"
        )

    @pytest.mark.asyncio
    async def test_core_init_failure_marks_failed(self):
        """When _run_core_init raises, the step is marked failed and the exception propagates."""
        task = _make_task()
        payload = _make_payload(catalog_id="c_err", external_id="err-cat")

        fake_conn = AsyncMock()
        mock_catalogs = _make_mock_catalogs(init_side_effect=RuntimeError("DDL error"))

        with patch(
            "dynastore.tasks.catalog_core_init.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_core_init.task.managed_transaction",
            return_value=_make_txn_ctx(fake_conn),
        ), patch(
            "dynastore.tasks.catalog_core_init.task.get_catalog_engine",
            return_value=MagicMock(),
        ):
            with pytest.raises(RuntimeError, match="DDL error"):
                await task.run(payload)

        mock_catalogs.mark_provisioning_step.assert_awaited_once_with(
            "c_err", "catalog_core", "failed"
        )

    @pytest.mark.asyncio
    async def test_no_run_core_init_attr_raises_runtime_error(self):
        """When the protocol impl does not expose _run_core_init, RuntimeError is raised."""
        task = _make_task()
        payload = _make_payload(catalog_id="c_bad", external_id="bad-impl")

        # A mock without _run_core_init attribute
        mock_catalogs = AsyncMock(spec=[])
        mock_catalogs.get_catalog_model = AsyncMock(return_value=MagicMock())
        mock_catalogs.mark_provisioning_step = AsyncMock()
        # _run_core_init deliberately absent

        with patch(
            "dynastore.tasks.catalog_core_init.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ):
            with pytest.raises(RuntimeError, match="_run_core_init"):
                await task.run(payload)

        mock_catalogs.mark_provisioning_step.assert_awaited_once_with(
            "c_bad", "catalog_core", "failed"
        )
