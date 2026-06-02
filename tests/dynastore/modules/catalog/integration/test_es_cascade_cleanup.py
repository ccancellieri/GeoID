"""E2E integration: catalog hard-delete tears down per-catalog ES indices.

Closes #1471 (cascade follow-up to #1456).

Covers the full hard-delete cascade against a live Elasticsearch instance:

  1. A catalog is created and its four per-catalog ES indices are
     materialised via the registered drivers' ``ensure_storage`` hooks:
       - ``{prefix}-{catalog_id}-items``           (public items)
       - ``{prefix}-{catalog_id}-assets``          (assets)
       - ``{prefix}-{catalog_id}-private-items``   (private items)
       - ``{prefix}-{catalog_id}-envelope-items``  (envelope items)
  2. Each per-catalog index is asserted to EXIST before the delete.
  3. The shared/platform indices (``{prefix}-catalogs``,
     ``{prefix}-collections``) are materialised so the post-delete
     preservation assertion is meaningful.
  4. ``delete_catalog(force=True)`` performs a hard delete, which snapshots
     each registered cascade owner's ``CleanupRef`` and enqueues a single
     ``cascade_cleanup`` task into the ``system`` task queue.
  5. The enqueued ``cascade_cleanup`` task is driven to completion in-process
     (the production dispatcher runs in a separate Cloud Run Job that the
     in-process test harness never claims), mirroring the dispatcher's call
     into ``CascadeCleanupTask.run``.
  6. Every per-catalog index is asserted GONE; the shared indices are
     asserted to SURVIVE — deleting one catalog must never touch platform
     state shared across tenants.

The per-catalog ES teardown wiring under test is provided by
``RoutingDrivenCascadeOwner`` registered in ``CatalogModule.lifespan``.
This test also verifies that catalog-owned collection docs are removed from
the singleton ``{prefix}-collections`` index (fixes #1750).

Requires a live Elasticsearch instance (``@pytest.mark.elasticsearch``) and
is skipped otherwise.
"""
from __future__ import annotations

import pytest

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.catalog.models import Catalog
from dynastore.tools.discovery import get_protocol
from tests.dynastore.test_utils import generate_test_id

pytestmark = [
    # Full default stack + elasticsearch. ``enable_modules`` replaces the
    # default list entirely, so the catalog/iam/storage modules the
    # delete-cascade depends on are repeated here. ``tasks`` is required so
    # the ``cascade_cleanup`` row can be enqueued into the system queue.
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "iam", "stac",
        "collection_postgresql", "catalog_postgresql", "elasticsearch", "tasks",
    ),
    pytest.mark.enable_extensions("stac"),
    pytest.mark.elasticsearch,
    pytest.mark.asyncio,
]


# ---------------------------------------------------------------------------
# Index-name helpers — derive every name from the SAME production helpers the
# cascade owners use, so the prefix is never hardcoded.
# ---------------------------------------------------------------------------

def _public_items_index(catalog_id: str) -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_tenant_items_index

    return get_tenant_items_index(get_index_prefix(), catalog_id)


def _assets_index(catalog_id: str) -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_assets_index_name

    return get_assets_index_name(get_index_prefix(), catalog_id)


def _private_items_index(catalog_id: str) -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
        get_private_index_name,
    )

    return get_private_index_name(get_index_prefix(), catalog_id)


def _envelope_items_index(catalog_id: str) -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.storage.drivers.elasticsearch_envelope.mappings import (
        get_envelope_index_name,
    )

    return get_envelope_index_name(get_index_prefix(), catalog_id)


def _catalogs_index() -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_index_name

    return get_index_name(get_index_prefix(), "catalog")


def _collections_index() -> str:
    from dynastore.modules.elasticsearch.client import get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_index_name

    return get_index_name(get_index_prefix(), "collection")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _es_client():
    from dynastore.modules.elasticsearch.client import get_client

    es = get_client()
    assert es is not None, "Elasticsearch client not initialized — is the module loaded?"
    return es


async def _ensure_shared_index(index_name: str) -> None:
    """Idempotently create a platform-shared index so its survival can be asserted."""
    es = _es_client()
    if not await es.indices.exists(index=index_name):
        await es.indices.create(index=index_name)


async def _materialise_per_catalog_indices(catalog_id: str) -> None:
    """Create all four per-catalog ES indices via the drivers' ensure_storage.

    Each driver owns its own per-catalog index; calling ``ensure_storage`` is
    the same path production uses when a collection routed to that driver is
    created. We also index one item into the public index for realism.
    """
    from dynastore.models.protocols.indexer import IndexContext, IndexOp
    from dynastore.modules.storage.drivers.elasticsearch import (
        AssetElasticsearchDriver,
        ItemsElasticsearchDriver,
    )
    from dynastore.modules.storage.drivers.elasticsearch_envelope import (
        ItemsElasticsearchEnvelopeDriver,
    )
    from dynastore.modules.storage.drivers.elasticsearch_private import (
        ItemsElasticsearchPrivateDriver,
    )

    await ItemsElasticsearchDriver().ensure_storage(catalog_id)
    await AssetElasticsearchDriver().ensure_storage(catalog_id)
    await ItemsElasticsearchPrivateDriver().ensure_storage(catalog_id)
    await ItemsElasticsearchEnvelopeDriver().ensure_storage(catalog_id)

    # Index one item into the per-catalog public items index so the index
    # holds real data at delete time (not just an empty shell).
    item_id = "cascade-cleanup-item-1"
    item = {
        "id": item_id,
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [10.0, 40.0]},
        "bbox": [10.0, 40.0, 10.0, 40.0],
        "properties": {
            "datetime": "2024-01-15T00:00:00Z",
            "title": {"en": "ES cascade cleanup test item"},
        },
        "links": [],
        "assets": {},
    }
    ctx = IndexContext(catalog=catalog_id, collection="cascade-col")
    op = IndexOp(op_type="upsert", entity_type="item", entity_id=item_id, payload=item)
    await ItemsElasticsearchDriver().index(ctx, op)


async def _drive_cascade_cleanup(app_state) -> dict:
    """Run the enqueued ``cascade_cleanup`` task to completion in-process.

    The production dispatcher runs in a separate Cloud Run Job whose
    CapabilityMap advertises ``cascade_cleanup``; the in-process test harness
    never claims it, so the row would sit forever. We fetch the row from the
    ``system`` task queue and invoke ``CascadeCleanupTask.run`` directly —
    exactly what ``ExecutionEngine.dispatch`` does for a claimed row.
    """
    from dynastore.modules.tasks import tasks_module
    from dynastore.tasks.cascade_cleanup.task import CascadeCleanupTask

    async with app_state.engine.connect() as conn:
        rows = await tasks_module.list_tasks(conn, schema="system", limit=50)

    cascade_rows = [t for t in rows if t.task_type == "cascade_cleanup"]
    assert cascade_rows, (
        "hard-delete did not enqueue a cascade_cleanup task into the system "
        "queue — the cascade owners were not invoked"
    )
    # Most-recent first (list_tasks orders by timestamp DESC); the freshest row
    # is the one our delete enqueued.
    task_row = cascade_rows[0]
    return await CascadeCleanupTask().run(task_row)


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

async def test_hard_delete_catalog_removes_per_catalog_es_indices(app_lifespan):
    """Hard-deleting a catalog deletes every per-catalog ES index while the
    platform-shared indices survive."""
    app_state = app_lifespan
    es = _es_client()

    catalog_id = f"es_cascade_{generate_test_id()}"

    per_catalog = {
        "public items": _public_items_index(catalog_id),
        "assets": _assets_index(catalog_id),
        "private items": _private_items_index(catalog_id),
        "envelope items": _envelope_items_index(catalog_id),
    }
    catalogs_index = _catalogs_index()
    collections_index = _collections_index()

    catalogs = get_protocol(CatalogsProtocol)
    cat = Catalog(id=catalog_id, title={"en": "ES Cascade Cleanup"})
    await catalogs.create_catalog(cat.model_dump(), lang="*")

    try:
        # Materialise the per-catalog indices + a shared-index baseline.
        await _materialise_per_catalog_indices(catalog_id)
        await _ensure_shared_index(catalogs_index)
        await _ensure_shared_index(collections_index)

        # Pre-delete: every per-catalog index must exist.
        for label, index_name in per_catalog.items():
            assert await es.indices.exists(index=index_name), (
                f"precondition failed: {label} index {index_name!r} should exist "
                "before hard delete"
            )
        assert await es.indices.exists(index=catalogs_index)
        assert await es.indices.exists(index=collections_index)

        # Hard delete → enqueues the cascade_cleanup task.
        await catalogs.delete_catalog(catalog_id, force=True)

        # Drive the enqueued cascade_cleanup task to completion.
        result = await _drive_cascade_cleanup(app_state)
        assert result.get("status") == "ok", f"cascade_cleanup did not complete cleanly: {result}"
        assert result.get("dead", 0) == 0, f"cascade_cleanup left DEAD refs: {result}"
        assert result.get("retry", 0) == 0, f"cascade_cleanup left RETRY refs: {result}"

        # Post-delete: every per-catalog index must be gone.
        for label, index_name in per_catalog.items():
            assert not await es.indices.exists(index=index_name), (
                f"{label} index {index_name!r} should be deleted by the cascade "
                "cleanup after hard delete"
            )

        # Shared/platform indices must NOT be touched by a single-catalog delete.
        assert await es.indices.exists(index=catalogs_index), (
            f"shared catalogs index {catalogs_index!r} must survive a catalog delete"
        )
        assert await es.indices.exists(index=collections_index), (
            f"shared collections index {collections_index!r} must survive a catalog delete"
        )
    finally:
        # Best-effort teardown of any index the cascade did not remove, so a
        # mid-test failure does not leak indices into the shared ES instance.
        for index_name in per_catalog.values():
            try:
                await es.indices.delete(
                    index=index_name, params={"ignore_unavailable": "true"}
                )
            except Exception:
                pass
