"""Regression coverage for the #825 listener-path retirement.

Pins the contract that catalog/collection INDEX propagation has left
the legacy listener path forever. Before #825, ``ElasticsearchModule``
registered async-event listeners on ``CATALOG_*``/``COLLECTION_*``
lifecycle events that enqueued ``ElasticsearchIndexTask`` /
``ElasticsearchDeleteTask`` rows — running in parallel to the canonical
routing-config rails (#820 items / #732 collection / ReindexWorker
catalog) and producing the misleading "Indexing collection …" log line
that originally surfaced #810.

This test catches a regression where someone re-introduces the listener
methods, the dispatch helper imports, or the dead task type.
"""

from __future__ import annotations

import importlib

import pytest

from dynastore.modules.elasticsearch.module import ElasticsearchModule


def test_no_lifecycle_listener_methods() -> None:
    """The four lifecycle-listener methods must not exist on the module."""
    for attr in (
        "_on_catalog_upsert",
        "_on_catalog_delete",
        "_on_collection_upsert",
        "_on_collection_delete",
    ):
        assert not hasattr(ElasticsearchModule, attr), (
            f"ElasticsearchModule.{attr} was reintroduced — this is the "
            "legacy listener path that #825 retired. Catalog INDEX now "
            "flows via catalog_router → CATALOG_METADATA_CHANGED → "
            "ReindexWorker → CatalogRoutingConfig.operations[INDEX]; "
            "collection INDEX via collection_router._dispatch_collection_index "
            "→ IndexDispatcher.fan_out_bulk."
        )


def test_no_indexer_facade_methods() -> None:
    """index_document/delete_document had zero non-test callers and were
    a tail on the listener path — they go with #825."""
    for attr in ("index_document", "delete_document"):
        assert not hasattr(ElasticsearchModule, attr), (
            f"ElasticsearchModule.{attr} was reintroduced — the facade "
            "wrapped the legacy elasticsearch_index/elasticsearch_delete "
            "task types retired in #825."
        )


def test_legacy_task_module_gone() -> None:
    """``dynastore.tasks.elasticsearch.tasks`` housed
    ``ElasticsearchIndexTask`` / ``ElasticsearchDeleteTask`` — only the
    retired listener path imported it. Module must no longer be importable."""
    with pytest.raises(ImportError):
        importlib.import_module("dynastore.tasks.elasticsearch.tasks")


def test_bulk_reindex_still_present() -> None:
    """The Cloud Run Job bulk-reindex entry point survives the retirement."""
    assert hasattr(ElasticsearchModule, "bulk_reindex")
    assert hasattr(ElasticsearchModule, "ensure_index")
