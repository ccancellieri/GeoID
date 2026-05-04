"""Driver-shape tests for ``CatalogElasticsearchDriver``.

Mirrors ``test_collection_es_driver.py`` for the catalog tier:
capability set, marker opt-in, Protocol-signature regression guards
(``context`` kwarg), and a no-live-ES smoke that the read path no-ops
cleanly when the index doesn't exist.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, patch

from dynastore.models.protocols.indexer import (
    AssetIndexer,
    CatalogIndexer,
    CollectionIndexer,
    ItemAssetIndexer,
    ItemIndexer,
    PlatformAssetIndexer,
)
from dynastore.models.protocols.entity_store import EntityStoreCapability
from dynastore.modules.elasticsearch.catalog_es_driver import (
    CatalogElasticsearchDriver,
    CatalogElasticsearchDriverConfig,
)


# ---------------------------------------------------------------------------
# Capability + marker shape
# ---------------------------------------------------------------------------


def test_capabilities_set_matches_protocol():
    """Catalog ES driver advertises read/write/search/aggregation +
    soft-delete + physical-addressing.  No SPATIAL_FILTER (catalogs
    carry no spatial extent).
    """
    expected = frozenset({
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SOFT_DELETE,
        EntityStoreCapability.SEARCH,
        EntityStoreCapability.AGGREGATION,
        EntityStoreCapability.PHYSICAL_ADDRESSING,
    })
    assert CatalogElasticsearchDriver.capabilities == expected
    # SPATIAL_FILTER must NOT be advertised — would mislead the router.
    assert EntityStoreCapability.SPATIAL_FILTER not in CatalogElasticsearchDriver.capabilities


def test_marker_opt_in_catalog_only():
    """Driver opts into :class:`CatalogIndexer` only.  Pin against
    accidental future tier opt-ins (would surface routing fan-out
    that nothing supports yet).
    """
    obj = CatalogElasticsearchDriver()
    assert isinstance(obj, CatalogIndexer)
    assert not isinstance(obj, CollectionIndexer)
    assert not isinstance(obj, AssetIndexer)
    assert not isinstance(obj, ItemIndexer)
    assert not isinstance(obj, ItemAssetIndexer)
    assert not isinstance(obj, PlatformAssetIndexer)


def test_typed_driver_bind_resolves():
    """``TypedDriver[CatalogElasticsearchDriverConfig]`` round-trips."""
    assert CatalogElasticsearchDriver.config_cls() is CatalogElasticsearchDriverConfig
    # And class_key derives from the driver class (auto-rename safe).
    assert CatalogElasticsearchDriverConfig.class_key() == "catalog_elasticsearch_driver"


# ---------------------------------------------------------------------------
# Protocol-signature regression — same `context` drift guard as collection
# ---------------------------------------------------------------------------


def test_get_catalog_metadata_accepts_context_kwarg():
    params = inspect.signature(
        CatalogElasticsearchDriver.get_catalog_metadata,
    ).parameters
    assert "context" in params, (
        "get_catalog_metadata must accept `context` per CatalogStore protocol"
    )


# ---------------------------------------------------------------------------
# No-live-ES smoke — read path no-ops cleanly when index doesn't exist
# ---------------------------------------------------------------------------


async def test_get_catalog_metadata_no_index_returns_none():
    driver = CatalogElasticsearchDriver()
    mock_client = AsyncMock()
    mock_client.indices.exists = AsyncMock(return_value=False)
    with patch.object(driver, "_get_client", return_value=mock_client), \
         patch.object(driver, "_get_prefix", return_value="meta"):
        result = await driver.get_catalog_metadata("cat", context={"user": "x"})
    assert result is None
    mock_client.indices.exists.assert_awaited_once()


async def test_delete_catalog_metadata_swallows_es_errors():
    """Hard-delete on a missing/transient index logs and continues
    (matches the collection driver's tombstone semantics)."""
    driver = CatalogElasticsearchDriver()
    mock_client = AsyncMock()
    mock_client.delete = AsyncMock(side_effect=RuntimeError("boom"))
    with patch.object(driver, "_get_client", return_value=mock_client), \
         patch.object(driver, "_get_prefix", return_value="meta"):
        # Must not raise.
        await driver.delete_catalog_metadata("cat")
    mock_client.delete.assert_awaited_once()


# ---------------------------------------------------------------------------
# Index naming — single shared index across all catalogs
# ---------------------------------------------------------------------------


def test_index_name_is_shared_across_catalogs():
    """Catalog cardinality is low — single index, NOT per-catalog like
    the collection driver.  Any drift back to per-catalog would
    silently re-introduce the per-tenant shard overhead.
    """
    driver = CatalogElasticsearchDriver()
    with patch.object(driver, "_get_prefix", return_value="meta"):
        n_a = driver._index_name()
        n_b = driver._index_name()
    # Same prefix → same index regardless of which catalog we ask about.
    assert n_a == n_b
    # Naming follows the existing mappings.get_index_name convention.
    assert n_a == "meta-catalogs"
