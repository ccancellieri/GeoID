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

"""Unit tests for CatalogModule._on_task_failed provisioning rollback (#2393).

The handler must record a failed provisioning task's reason on the catalog row,
routing by ``task_type`` (the reliably-propagated field) rather than the
never-set ``originating_event`` that left the rollback dead.

Covers:
  - A failed ``catalog_provision`` task re-asserts provisioning_status='failed'
    and records ``provisioning_error`` in extra_metadata.
  - ``gcp_provision_catalog`` is routed the same way.
  - A non-provisioning task type (e.g. ``ingest``) is NOT routed (no catalog write).
  - The handler tolerates the emitter still passing ``originating_event=None``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import AsyncMock, patch

import pytest


def _make_module() -> Any:
    """A stand-in ``self`` — ``_on_task_failed`` uses no instance attributes."""
    return SimpleNamespace()


async def _call(
    *,
    task_type: str,
    catalog_id: Optional[str] = "c_failedcat",
    error_message: str = "gcp_eventing failed: Cannot determine self URL",
    **extra,
) -> Any:
    """Invoke the handler with a mocked CatalogsProtocol; return the mock."""
    from dynastore.modules.catalog.catalog_module import CatalogModule

    catalogs = AsyncMock()
    inputs = {"catalog_id": catalog_id} if catalog_id is not None else {}
    with patch(
        "dynastore.modules.catalog.catalog_module.get_protocol",
        return_value=catalogs,
    ):
        await CatalogModule._on_task_failed(
            _make_module(),
            task_id="t-1",
            task_type=task_type,
            error_message=error_message,
            severity="unrecoverable",
            inputs=inputs,
            **extra,
        )
    return catalogs


@pytest.mark.asyncio
@pytest.mark.parametrize("task_type", ["catalog_provision", "gcp_provision_catalog"])
async def test_provisioning_failure_records_status_and_error(task_type: str) -> None:
    err = "gcp_eventing failed: Cannot determine self URL"
    catalogs = await _call(task_type=task_type, error_message=err)

    catalogs.update_provisioning_status.assert_awaited_once()
    pos = catalogs.update_provisioning_status.await_args
    assert pos.args[0] == "c_failedcat"
    assert pos.args[1] == "failed"

    catalogs.update_catalog.assert_awaited_once()
    upd = catalogs.update_catalog.await_args
    assert upd.args[0] == "c_failedcat"
    assert upd.args[1] == {"extra_metadata": {"provisioning_error": err}}


@pytest.mark.asyncio
async def test_non_provisioning_task_is_not_routed() -> None:
    catalogs = await _call(task_type="ingest")
    catalogs.update_provisioning_status.assert_not_awaited()
    catalogs.update_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_provisioning_failure_without_catalog_id_is_noop() -> None:
    catalogs = await _call(task_type="catalog_provision", catalog_id=None)
    catalogs.update_provisioning_status.assert_not_awaited()
    catalogs.update_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_tolerates_legacy_originating_event_kwarg() -> None:
    # The failure emitter still passes originating_event=None; it must be
    # absorbed by **kwargs and not break routing.
    catalogs = await _call(task_type="catalog_provision", originating_event=None)
    catalogs.update_provisioning_status.assert_awaited_once()
    catalogs.update_catalog.assert_awaited_once()
