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

"""Driver-shape tests for ``CatalogElasticsearchDriver``.

Mirrors ``test_collection_es_driver.py`` for the catalog tier:
capability set, marker opt-in, Protocol-signature regression guards
(``context`` kwarg), and a no-live-ES smoke that the read path no-ops
cleanly when the index doesn't exist.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, patch

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
    """Driver claims the ``catalog`` INDEX tier only.  Pin against
    accidental future tier opt-ins (would surface routing fan-out
    that nothing supports yet).
    """
    assert CatalogElasticsearchDriver.index_tiers == frozenset({"catalog"})


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
# Storage-lifecycle contract: drop_storage delegates to delete_catalog_metadata
# ---------------------------------------------------------------------------


async def test_drop_storage_delegates_to_delete_catalog_metadata_hard():
    """Catalog ``drop_storage`` removes the catalog's record from the shared
    index by delegating to ``delete_catalog_metadata`` (hard by default)."""
    driver = CatalogElasticsearchDriver()
    with patch.object(
        driver, "delete_catalog_metadata", new=AsyncMock()
    ) as mock_del:
        await driver.drop_storage("cat-a")
    mock_del.assert_awaited_once_with("cat-a", soft=False)


async def test_drop_storage_passes_soft_through():
    """``soft=True`` is forwarded so the record is tombstoned, not removed."""
    driver = CatalogElasticsearchDriver()
    with patch.object(
        driver, "delete_catalog_metadata", new=AsyncMock()
    ) as mock_del:
        await driver.drop_storage("cat-a", soft=True)
    mock_del.assert_awaited_once_with("cat-a", soft=True)


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
