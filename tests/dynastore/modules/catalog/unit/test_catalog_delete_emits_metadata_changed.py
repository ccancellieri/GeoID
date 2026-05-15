"""Regression coverage for #825: catalog_service.delete_catalog must
emit ``CATALOG_METADATA_CHANGED`` so ReindexWorker fans the delete out
to ``CatalogRoutingConfig.operations[INDEX]`` (the canonical ES cleanup
path that replaces the retired ``ElasticsearchModule._on_catalog_delete``
listener).

Asserts at source-shape level: both the soft- and hard-delete branches
of ``catalog_service.delete_catalog`` contain a
``CatalogEventType.CATALOG_METADATA_CHANGED`` emit with the expected
operation payload. Source-shape rather than runtime because a true
runtime assertion requires standing up the full
``managed_transaction``/SQL stack — overkill for pinning that the line
stays present.
"""

from __future__ import annotations

import inspect

from dynastore.modules.catalog import catalog_service


def _delete_catalog_source() -> str:
    return inspect.getsource(catalog_service.CatalogService.delete_catalog)


def test_soft_delete_emits_metadata_changed_with_soft_delete_operation() -> None:
    src = _delete_catalog_source()
    assert 'CatalogEventType.CATALOG_METADATA_CHANGED' in src, (
        "delete_catalog no longer emits CATALOG_METADATA_CHANGED — the #825 "
        "canonical-path extension was reverted. Re-add the inline "
        "emit_event(CATALOG_METADATA_CHANGED, operation='soft_delete'|'delete') "
        "calls at the soft-delete branch (return True path) AND after the "
        "CATALOG_HARD_DELETION emit."
    )
    assert '"operation": "soft_delete"' in src, (
        "soft-delete branch must emit CATALOG_METADATA_CHANGED with "
        "operation='soft_delete' so ReindexWorker._dispatch_one routes to "
        "driver.delete_catalog_metadata(soft=True)."
    )


def test_hard_delete_emits_metadata_changed_with_delete_operation() -> None:
    src = _delete_catalog_source()
    assert '"operation": "delete"' in src, (
        "hard-delete branch must emit CATALOG_METADATA_CHANGED with "
        "operation='delete' so ReindexWorker._dispatch_one routes to "
        "driver.delete_catalog_metadata(soft=False)."
    )
