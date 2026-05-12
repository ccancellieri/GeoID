"""Cycle E.2.b — smoke tests for ``CollectionElasticsearchPrivateDriver``.

Pins the structural invariants of the new driver class without
exercising the live ES client (those tests live under
``tests/dynastore/modules/elasticsearch/`` and run against an
in-cluster fixture):

- Distinct ``indexer_id`` so the dispatcher routes to the right driver.
- ``auto_register_for_routing`` is empty — operators must explicitly
  pin this driver in :class:`CollectionRoutingConfig`; auto-default
  augmentation must NEVER reach for it.
- Per-tenant index name resolution via
  ``get_tenant_collections_private_index`` matches the convention.
- ``location`` returns the per-tenant index path (not the shared
  ``{prefix}-collections`` of the public driver).
- ``upsert_metadata`` lazily ensures the per-tenant index exists
  before writing (defensive against the not-yet-wired catalog
  ensure_storage lifecycle hook + against ES clusters with
  auto_create disabled).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.elasticsearch.collection_es_driver import (
    CollectionElasticsearchDriver,
)
from dynastore.modules.elasticsearch.mappings import (
    get_tenant_collections_private_index,
)
from dynastore.modules.storage.drivers.elasticsearch_private import (
    CollectionElasticsearchPrivateDriver,
)


def test_indexer_id_resolves_via_class_name_snake_case():
    """Indexer identity is ``_to_snake(type(impl).__name__)`` per the
    convention pinned in ``IndexDispatcher._make_default_indexer_registry``
    (no separate ``indexer_id`` ClassVar — that attribute was removed
    during the indexer refactor). This test pins that the private
    driver's class name resolves to a *distinct* id from the public
    driver so dispatcher routing can target either independently.
    """
    from dynastore.tools.typed_store.base import _to_snake

    private_id = _to_snake(CollectionElasticsearchPrivateDriver.__name__)
    public_id = _to_snake(CollectionElasticsearchDriver.__name__)
    assert private_id == "collection_elasticsearch_private_driver"
    assert public_id == "collection_elasticsearch_driver"
    assert private_id != public_id


def test_auto_register_for_routing_is_empty():
    """Operators must explicitly pin the private driver — never
    auto-injected, even into ``CollectionRoutingConfig.operations[INDEX]``."""
    assert CollectionElasticsearchPrivateDriver.auto_register_for_routing == frozenset()


def test_subclasses_public_driver():
    """The private driver inherits the public ``CollectionStore`` surface
    so we get capabilities, doc enrichment helpers, and the
    is_collection_indexer marker for free."""
    assert issubclass(
        CollectionElasticsearchPrivateDriver, CollectionElasticsearchDriver,
    )


def test_per_tenant_index_name():
    """Index name follows the per-catalog convention."""
    expected = get_tenant_collections_private_index("dynastore", "cat-a")
    assert expected == "dynastore-cat-a-collections-private"


def test_index_name_resolution_uses_per_tenant_helper():
    driver = CollectionElasticsearchPrivateDriver()
    with patch.object(driver, "_get_prefix", return_value="dynastore"):
        assert driver._private_index("cat-a") == "dynastore-cat-a-collections-private"


def test_doc_id_is_bare_collection_id():
    """Tenant-scoped index → no catalog prefix needed in doc-id."""
    driver = CollectionElasticsearchPrivateDriver()
    assert driver._doc_id("col-a") == "col-a"


def test_location_returns_per_tenant_path():
    driver = CollectionElasticsearchPrivateDriver()
    with patch.object(driver, "_get_prefix", return_value="dynastore"):
        loc = driver.location("cat-a", "col-a")
    assert loc.backend == "elasticsearch_private"
    assert loc.canonical_uri == "es://dynastore-cat-a-collections-private"
    assert loc.identifiers["index"] == "dynastore-cat-a-collections-private"
    assert loc.identifiers["catalog_id"] == "cat-a"
    # No "routing" identifier — per-tenant index doesn't need shard routing.
    assert "routing" not in loc.identifiers


@pytest.mark.asyncio
async def test_upsert_metadata_lazily_ensures_index():
    """Cycle E.2.c gap: until the catalog ensure_storage lifecycle hook
    is wired, the first write to a per-tenant private collection index
    must self-create the index — otherwise auto_create-disabled clusters
    would reject the write OR auto-create the index without
    COLLECTION_MAPPING (geo_shape on bbox_shape would be missing,
    breaking spatial search)."""
    driver = CollectionElasticsearchPrivateDriver()
    es_client = MagicMock()
    es_client.indices = MagicMock()
    es_client.indices.exists = AsyncMock(return_value=False)
    es_client.indices.create = AsyncMock(return_value=None)
    es_client.index = AsyncMock(return_value={"result": "created"})
    with patch.object(driver, "_get_client", return_value=es_client), \
         patch.object(driver, "_get_prefix", return_value="dynastore"):
        await driver.upsert_metadata(
            "cat-a", "col-a", {"id": "col-a", "extent": {}},
        )
    # ensure_storage should have been called via the lazy path.
    es_client.indices.exists.assert_awaited_once()
    es_client.indices.create.assert_awaited_once()
    # And then the actual upsert.
    es_client.index.assert_awaited_once()
    create_kwargs = es_client.indices.create.await_args.kwargs
    assert create_kwargs["index"] == "dynastore-cat-a-collections-private"
    assert "mappings" in create_kwargs["body"]


@pytest.mark.asyncio
async def test_upsert_metadata_skips_create_when_index_exists():
    """ensure_storage short-circuits when the index already exists —
    avoids racing concurrent upserts on the create path."""
    driver = CollectionElasticsearchPrivateDriver()
    es_client = MagicMock()
    es_client.indices = MagicMock()
    es_client.indices.exists = AsyncMock(return_value=True)
    es_client.indices.create = AsyncMock(return_value=None)
    es_client.index = AsyncMock(return_value={"result": "created"})
    with patch.object(driver, "_get_client", return_value=es_client), \
         patch.object(driver, "_get_prefix", return_value="dynastore"):
        await driver.upsert_metadata("cat-a", "col-a", {"id": "col-a"})
    es_client.indices.exists.assert_awaited_once()
    es_client.indices.create.assert_not_awaited()
    es_client.index.assert_awaited_once()
