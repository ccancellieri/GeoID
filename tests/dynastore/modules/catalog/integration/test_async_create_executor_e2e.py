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

"""End-to-end integration test for the always-async catalog create + executor path (#2329).

Validates, against a real PostgreSQL instance, that:

1. ``CatalogsProtocol.create_catalog`` returns a catalog with
   ``provisioning_status == 'provisioning'`` unconditionally (create is always async).
2. A ``catalog_provision`` task row is present in the tasks table with the right
   inputs (``catalog_id``, ``scope='catalog'``, ``operation='provision'``).
3. Running ``CatalogProvisionTask.run(payload)`` against the committed internal
   catalog_id drives every checklist step to terminal and the catalog reaches
   ``provisioning_status == 'ready'``.

The real ``provisioning_registry`` is used — no ``clean_registry`` fixture — so
the always-active ``catalog_core`` provisioner (registered by CatalogModule at
priority 0) and any other loaded provisioners (e.g. ``gcp_config``) are present.
"""

from __future__ import annotations

import logging
import uuid

import pytest

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.catalog.models import Catalog
from dynastore.modules.catalog.provisioning_registry import (
    STATUS_PROVISIONING,
    STATUS_READY,
    provisioning_registry,
)
from dynastore.tools.discovery import get_protocol
from tests.dynastore.test_utils import generate_test_id

logger = logging.getLogger(__name__)


async def _new_catalog_id(catalogs: CatalogsProtocol, title: str) -> str:
    """Create a fresh external catalog_id and force-clean any prior tombstone."""
    catalog_id = f"test_async_{generate_test_id()}"
    await catalogs.delete_catalog(catalog_id, force=True)
    return catalog_id


def _make_payload(internal_catalog_id: str) -> object:
    """Build a TaskPayload[CatalogProvisionInputs] for direct executor invocation."""
    from dynastore.tasks.catalog_provision.task import CatalogProvisionInputs
    from dynastore.modules.tasks.models import TaskPayload

    inputs = CatalogProvisionInputs(
        catalog_id=internal_catalog_id,
        scope="catalog",
        operation="provision",
    )
    return TaskPayload(task_id=uuid.uuid4(), caller_id="test", inputs=inputs)


@pytest.mark.enable_modules("db_config", "db", "catalog", "tasks")
@pytest.mark.asyncio
async def test_async_create_enqueues_and_executor_drives_ready(
    app_lifespan,
):
    """Full e2e: create always returns provisioning, task row present, executor → ready."""
    # Log which provisioners the real registry has at this point.
    registered_keys = provisioning_registry.keys
    logger.info(
        "test_async_create_executor_e2e: provisioning_registry keys at test start: %s",
        registered_keys,
    )

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None, "CatalogsProtocol not registered — check enable_modules"

    catalog_id = await _new_catalog_id(catalogs, "Async create e2e")
    cat = Catalog(id=catalog_id, title={"en": "Async create e2e test"})

    # ---- Step 1: create_catalog must return provisioning -------------------
    returned_cat = await catalogs.create_catalog(cat.model_dump(), lang="*")

    internal_id: str = returned_cat.id  # the committed c_… internal ID

    try:
        assert returned_cat.provisioning_status == STATUS_PROVISIONING, (
            f"Expected provisioning_status='provisioning' immediately after create; "
            f"got '{returned_cat.provisioning_status}'"
        )

        # The checklist is a DB-only column (not on the Catalog Pydantic model).
        # Query it directly from catalog.catalogs.
        from dynastore.modules.catalog.catalog_service import get_catalog_engine
        from dynastore.modules.db_config.query_executor import (
            DQLQuery,
            ResultHandler,
            managed_transaction,
        )

        engine = get_catalog_engine()
        async with managed_transaction(engine) as conn:
            row = await DQLQuery(
                "SELECT provisioning_checklist FROM catalog.catalogs WHERE id = :id;",
                result_handler=ResultHandler.ONE_DICT,
            ).execute(conn, id=internal_id)

        assert row is not None, (
            f"catalog.catalogs row not found for internal_id='{internal_id}'"
        )
        checklist = row.get("provisioning_checklist") or {}
        assert checklist, (
            f"provisioning_checklist is empty/None for catalog '{internal_id}' — "
            "was the checklist seeded by _create_catalog_async?"
        )
        assert "catalog_core" in checklist, (
            f"'catalog_core' not in checklist {checklist!r}; "
            "catalog_core provisioner must be present (registered by CatalogModule at priority 0)"
        )
        logger.info(
            "test_async_create_executor_e2e: checklist after create: %s", checklist
        )

        # ---- Step 2: catalog_provision task row must exist -----------------
        from dynastore.modules.tasks.tasks_module import get_task_schema

        task_schema = get_task_schema()

        task_row = None
        async with managed_transaction(engine) as conn:
            sql = (
                f'SELECT task_id, catalog_id, inputs, status '
                f'FROM "{task_schema}".tasks '
                f"WHERE task_type = 'catalog_provision' "
                f"  AND inputs->>'catalog_id' = :internal_id "
                f"ORDER BY timestamp DESC LIMIT 1;"
            )
            task_row = await DQLQuery(
                sql, result_handler=ResultHandler.ONE_DICT
            ).execute(conn, internal_id=internal_id)

        assert task_row is not None, (
            f"No catalog_provision task row found in {task_schema}.tasks with "
            f"inputs->>'catalog_id'='{internal_id}' — _create_catalog_async must enqueue it"
        )

        task_inputs = task_row.get("inputs", {}) or {}
        assert task_inputs.get("catalog_id") == internal_id, (
            f"Task inputs.catalog_id mismatch: {task_inputs!r}"
        )
        assert task_inputs.get("scope") == "catalog", (
            f"Task inputs.scope must be 'catalog'; got {task_inputs!r}"
        )
        assert task_inputs.get("operation") == "provision", (
            f"Task inputs.operation must be 'provision'; got {task_inputs!r}"
        )
        logger.info(
            "test_async_create_executor_e2e: task row found: task_id=%s status=%s inputs=%s",
            task_row.get("task_id"),
            task_row.get("status"),
            task_inputs,
        )

        # ---- Step 3: run the executor directly — catalog must reach ready --
        from dynastore.tasks.catalog_provision.task import CatalogProvisionTask

        task = CatalogProvisionTask()
        payload = _make_payload(internal_id)

        result = await task.run(payload)

        logger.info(
            "test_async_create_executor_e2e: executor result: %s", result
        )

        # Fetch the catalog again to check its final status.
        final_cat = await catalogs.get_catalog(catalog_id)
        assert final_cat is not None, (
            f"catalog '{catalog_id}' not found after executor run"
        )

        final_status = final_cat.provisioning_status
        assert final_status == STATUS_READY, (
            f"Expected provisioning_status='ready' after executor run; got '{final_status}'. "
            f"Checklist: {getattr(final_cat, 'provisioning_checklist', 'N/A')!r}"
        )

        active_steps = result.get("steps_completed", 0)
        groups_run = result.get("groups_run", 0)
        logger.info(
            "test_async_create_executor_e2e: PASS — provisioners active=%s, "
            "groups_run=%d, steps_completed=%d, final_status=%s",
            registered_keys,
            groups_run,
            active_steps,
            final_status,
        )

    finally:
        await catalogs.delete_catalog(catalog_id, force=True)
