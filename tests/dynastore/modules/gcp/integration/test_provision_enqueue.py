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

"""Catalog creation must result in a ``gcp_provision_catalog`` task when
provisioning is enabled — without needing live GCP credentials (#1174).

Under the always-async create path, ``create_catalog`` commits the catalog
row and enqueues a ``catalog_provision`` task (the executor).  The
``gcp_provision_catalog`` sub-task is only enqueued later, when the executor's
``gcp_bucket`` provisioner step runs.  This test verifies the two-step sequence:

  Step 1: ``create_catalog`` → ``catalog_provision`` task present (not yet
          ``gcp_provision_catalog`` — the executor has not run yet).
  Step 2: run ``CatalogProvisionTask`` → ``gcp_provision_catalog`` task present.

This pins the bug from #1174: with ``provision_enabled=True`` (the code default),
no provisioning task was produced, so the dispatcher had nothing to claim and no
bucket was ever created.
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.tasks.tasks_module import list_tasks_for_catalog
from dynastore.tools.discovery import get_protocol
from tests.dynastore.test_utils import generate_test_id


async def _tasks_by_type(engine, catalog_id: str, task_type: str):
    async with managed_transaction(engine) as conn:
        tasks = await list_tasks_for_catalog(conn, catalog_id)
    return [t for t in tasks if t.task_type == task_type]


@pytest.mark.asyncio
@pytest.mark.enable_modules(
    "db_config", "db", "catalog", "catalog_postgresql", "tasks", "gcp",
)
async def test_create_catalog_enqueues_catalog_provision_task(app_lifespan):
    """provision_enabled defaults to True → create_catalog must enqueue
    exactly one catalog_provision task (the executor that drives gcp_bucket
    and gcp_eventing provisioner steps)."""
    if not getattr(app_lifespan, "engine", None):
        pytest.skip("app_state.engine not initialized.")

    catalogs = get_protocol(CatalogsProtocol)
    catalog_id = f"it_pq_{generate_test_id(8)}"
    await catalogs.delete_catalog(catalog_id, force=True)

    try:
        await catalogs.create_catalog({"id": catalog_id, "title": {"en": "p"}}, lang="*")

        prov = await _tasks_by_type(app_lifespan.engine, catalog_id, "catalog_provision")
        assert prov, (
            "create_catalog enqueued no catalog_provision task while "
            "provision_enabled=True — the executor will never run (#1174)."
        )
        assert len(prov) == 1, f"expected one catalog_provision task, got {len(prov)}"
        assert prov[0].inputs.get("operation") == "provision"

        # gcp_provision_catalog is enqueued by the gcp_bucket provisioner step
        # INSIDE CatalogProvisionTask — it must NOT be present yet.
        gcp_tasks = await _tasks_by_type(
            app_lifespan.engine, catalog_id, "gcp_provision_catalog"
        )
        assert gcp_tasks == [], (
            "gcp_provision_catalog must not be enqueued inline during create_catalog; "
            f"it is enqueued by the gcp_bucket provisioner step in CatalogProvisionTask. "
            f"Found {len(gcp_tasks)} task(s) — double-provision risk."
        )
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)
        await lifecycle_registry.wait_for_all_tasks()
