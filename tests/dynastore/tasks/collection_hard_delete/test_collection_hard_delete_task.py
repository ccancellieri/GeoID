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

"""Unit tests for CollectionHardDeleteTask."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.tasks.collection_hard_delete.task import (
    CollectionHardDeleteInputs,
    CollectionHardDeleteTask,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_payload(catalog_id: str = "cat1", collection_id: str = "col1") -> Any:
    payload = MagicMock()
    payload.inputs = {"catalog_id": catalog_id, "collection_id": collection_id}
    return payload


def _make_catalogs_svc(delete_result: bool = True) -> AsyncMock:
    svc = AsyncMock()
    svc.delete_collection = AsyncMock(return_value=delete_result)
    return svc


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCollectionHardDeleteInputs:
    def test_valid_inputs_parse(self) -> None:
        inp = CollectionHardDeleteInputs(catalog_id="cat1", collection_id="col1")
        assert inp.catalog_id == "cat1"
        assert inp.collection_id == "col1"

    def test_extra_fields_ignored(self) -> None:
        inp = CollectionHardDeleteInputs.model_validate(
            {"catalog_id": "c", "collection_id": "x", "unknown": "field"}
        )
        assert inp.catalog_id == "c"

    def test_missing_catalog_id_raises(self) -> None:
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            CollectionHardDeleteInputs.model_validate({"collection_id": "x"})

    def test_missing_collection_id_raises(self) -> None:
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            CollectionHardDeleteInputs.model_validate({"catalog_id": "c"})


class TestCollectionHardDeleteTaskMeta:
    def test_task_type(self) -> None:
        assert CollectionHardDeleteTask.task_type == "collection_hard_delete"

    def test_mandatory(self) -> None:
        assert CollectionHardDeleteTask.mandatory is True

    def test_affinity_tier(self) -> None:
        assert CollectionHardDeleteTask.affinity_tier == "catalog"

    def test_payload_model(self) -> None:
        assert CollectionHardDeleteTask.payload_model is CollectionHardDeleteInputs


class TestCollectionHardDeleteTaskRun:
    @pytest.mark.asyncio
    async def test_run_success_calls_delete_collection_force_true(self) -> None:
        """run() calls delete_collection(force=True) and returns success report."""
        catalogs_svc = _make_catalogs_svc(delete_result=True)
        payload = _make_payload("mycat", "mycol")

        task = CollectionHardDeleteTask()

        with patch(
            "dynastore.modules.get_protocol",
            return_value=catalogs_svc,
        ):
            result = await task.run(payload)

        catalogs_svc.delete_collection.assert_awaited_once_with(
            "mycat", "mycol", force=True
        )
        assert result["deleted"] is True
        assert result["catalog_id"] == "mycat"
        assert result["collection_id"] == "mycol"

    @pytest.mark.asyncio
    async def test_run_already_missing_returns_success(self) -> None:
        """When delete_collection returns False (MISSING), run() still succeeds."""
        catalogs_svc = _make_catalogs_svc(delete_result=False)
        payload = _make_payload()

        task = CollectionHardDeleteTask()

        with patch(
            "dynastore.modules.get_protocol",
            return_value=catalogs_svc,
        ):
            result = await task.run(payload)

        assert result["deleted"] is False
        assert result["catalog_id"] == "cat1"

    @pytest.mark.asyncio
    async def test_run_no_protocol_raises_runtime_error(self) -> None:
        """When CatalogsProtocol is not available, run() raises RuntimeError."""
        payload = _make_payload()
        task = CollectionHardDeleteTask()

        with patch(
            "dynastore.modules.get_protocol",
            return_value=None,
        ):
            with pytest.raises(RuntimeError, match="CatalogsProtocol not available"):
                await task.run(payload)

    @pytest.mark.asyncio
    async def test_run_service_exception_raises_runtime_error(self) -> None:
        """Service-layer failure is wrapped in RuntimeError so dispatcher retries."""
        catalogs_svc = AsyncMock()
        catalogs_svc.delete_collection = AsyncMock(side_effect=Exception("DB failure"))
        payload = _make_payload()
        task = CollectionHardDeleteTask()

        with patch(
            "dynastore.modules.get_protocol",
            return_value=catalogs_svc,
        ):
            with pytest.raises(RuntimeError, match="purge failed"):
                await task.run(payload)
