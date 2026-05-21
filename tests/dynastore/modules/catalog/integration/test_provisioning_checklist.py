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

"""Integration coverage for the provisioning checklist (#1175) against a live DB.

The pure terminal rule and the registry are unit-tested in
``tests/dynastore/modules/catalog/unit/test_provisioning_registry.py``. Here we
exercise the two DB-bound halves end to end on a real PostgreSQL catalog:

- ``create_catalog`` materialising the checklist from the *active* provisioners
  and setting ``provisioning_status`` accordingly (the barrier);
- ``mark_provisioning_step`` flipping individual steps terminal and
  re-evaluating catalog readiness (complete / skipped / failed / multi-step
  barrier).

Provisioners are registered through the process-wide ``provisioning_registry``
with a fixture that snapshots and restores it, so these tests neither depend on
nor leak into whatever modules (e.g. GCP) happen to be loaded.
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.catalog.models import Catalog
from dynastore.modules.catalog.provisioning_registry import (
    STATUS_FAILED,
    STATUS_PROVISIONING,
    STATUS_READY,
    STEP_COMPLETE,
    STEP_FAILED,
    STEP_SKIPPED,
    provisioning_registry,
)
from dynastore.tools.discovery import get_protocol
from tests.dynastore.test_utils import generate_test_id


@pytest.fixture
def clean_registry():
    """Isolate ``provisioning_registry`` mutations to a single test.

    Snapshots the registered provisioners, clears them so the test starts from a
    known-empty registry, and restores the original set afterwards — so a loaded
    GCP module (or any other provisioner) neither perturbs nor is perturbed by
    these tests.
    """
    saved = dict(provisioning_registry._provisioners)  # noqa: SLF001 — test isolation
    provisioning_registry.clear()
    yield provisioning_registry
    provisioning_registry._provisioners = saved  # noqa: SLF001


def _active(_active_value: bool = True):
    async def _predicate(catalog_id, conn):  # noqa: ANN001
        return _active_value
    return _predicate


async def _new_catalog(catalogs, title: str) -> str:
    catalog_id = f"test_prov_{generate_test_id()}"
    await catalogs.delete_catalog(catalog_id, force=True)
    cat = Catalog(id=catalog_id, title={"en": title})
    await catalogs.create_catalog(cat.model_dump(), lang="*")
    return catalog_id


@pytest.mark.enable_modules("db_config", "db", "catalog")
@pytest.mark.asyncio
async def test_no_provisioner_ready_immediately(app_lifespan, clean_registry):
    """On-prem / no active provisioner → catalog is ``ready`` straight away."""
    catalogs = get_protocol(CatalogsProtocol)
    catalog_id = await _new_catalog(catalogs, "No provisioner")
    try:
        cat = await catalogs.get_catalog(catalog_id)
        assert cat.provisioning_status == STATUS_READY
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


@pytest.mark.enable_modules("db_config", "db", "catalog")
@pytest.mark.asyncio
async def test_inactive_provisioner_ready_immediately(app_lifespan, clean_registry):
    """A loaded-but-inactive provisioner contributes no item → ``ready``."""
    clean_registry.register("gcp_bucket", _active(False))
    catalogs = get_protocol(CatalogsProtocol)
    catalog_id = await _new_catalog(catalogs, "Inactive provisioner")
    try:
        cat = await catalogs.get_catalog(catalog_id)
        assert cat.provisioning_status == STATUS_READY
        # No checklist item was created, so marking it is a no-op.
        assert await catalogs.mark_provisioning_step(catalog_id, "gcp_bucket") is False
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


@pytest.mark.enable_modules("db_config", "db", "catalog")
@pytest.mark.asyncio
async def test_active_provisioner_blocks_then_completes(app_lifespan, clean_registry):
    """Active provisioner → ``provisioning`` until its step completes → ``ready``."""
    clean_registry.register("gcp_bucket", _active(True))
    catalogs = get_protocol(CatalogsProtocol)
    catalog_id = await _new_catalog(catalogs, "Active provisioner")
    try:
        cat = await catalogs.get_catalog(catalog_id)
        assert cat.provisioning_status == STATUS_PROVISIONING

        changed = await catalogs.mark_provisioning_step(
            catalog_id, "gcp_bucket", STEP_COMPLETE
        )
        assert changed is True
        cat = await catalogs.get_catalog(catalog_id)
        assert cat.provisioning_status == STATUS_READY
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


@pytest.mark.enable_modules("db_config", "db", "catalog")
@pytest.mark.asyncio
async def test_skipped_step_makes_ready(app_lifespan, clean_registry):
    """A provisioner that skips (e.g. GCP enabled but no creds) still → ``ready``."""
    clean_registry.register("gcp_bucket", _active(True))
    catalogs = get_protocol(CatalogsProtocol)
    catalog_id = await _new_catalog(catalogs, "Skipped provisioner")
    try:
        assert (await catalogs.get_catalog(catalog_id)).provisioning_status == (
            STATUS_PROVISIONING
        )
        await catalogs.mark_provisioning_step(catalog_id, "gcp_bucket", STEP_SKIPPED)
        cat = await catalogs.get_catalog(catalog_id)
        assert cat.provisioning_status == STATUS_READY
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


@pytest.mark.enable_modules("db_config", "db", "catalog")
@pytest.mark.asyncio
async def test_failed_step_makes_failed(app_lifespan, clean_registry):
    """A genuine provisioning failure surfaces as ``failed``."""
    clean_registry.register("gcp_bucket", _active(True))
    catalogs = get_protocol(CatalogsProtocol)
    catalog_id = await _new_catalog(catalogs, "Failed provisioner")
    try:
        await catalogs.mark_provisioning_step(catalog_id, "gcp_bucket", STEP_FAILED)
        cat = await catalogs.get_catalog(catalog_id)
        assert cat.provisioning_status == STATUS_FAILED
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


@pytest.mark.enable_modules("db_config", "db", "catalog")
@pytest.mark.asyncio
async def test_multi_step_barrier(app_lifespan, clean_registry):
    """Two active provisioners: catalog stays ``provisioning`` until BOTH finish."""
    clean_registry.register("gcp_bucket", _active(True))
    clean_registry.register("other_backend", _active(True))
    catalogs = get_protocol(CatalogsProtocol)
    catalog_id = await _new_catalog(catalogs, "Two provisioners")
    try:
        assert (await catalogs.get_catalog(catalog_id)).provisioning_status == (
            STATUS_PROVISIONING
        )

        # First step done — barrier still holds (second is pending).
        await catalogs.mark_provisioning_step(catalog_id, "gcp_bucket", STEP_COMPLETE)
        assert (await catalogs.get_catalog(catalog_id)).provisioning_status == (
            STATUS_PROVISIONING
        )

        # Second step done — now the catalog flips ready.
        await catalogs.mark_provisioning_step(
            catalog_id, "other_backend", STEP_COMPLETE
        )
        assert (await catalogs.get_catalog(catalog_id)).provisioning_status == (
            STATUS_READY
        )
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)
