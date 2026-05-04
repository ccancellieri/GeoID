"""M7 regression guard: Protocol-based driver grouping.

Invariants:
1. No ``CollectionStorageDriverProtocol``, ``AssetDriverProtocol``, or
   ``CollectionMetadataDriverProtocol`` names survive anywhere in source or
   tests — they were renamed in M7.
2. The three new Protocol classes (``CollectionItemsStore``, ``AssetStore``,
   ``CollectionStore``) exist and are importable.
3. ``DriverRegistry`` builds correct indices keyed by the new names.
4. Every driver class that implements a Protocol is discoverable via the
   Protocol's qualname (structural subtype check via ``@runtime_checkable``).
"""
import subprocess
from pathlib import Path
from typing import Optional

import pytest

_REPO_ROOT = Path(__file__).parents[6]  # geoid/


# ---------------------------------------------------------------------------
# 1. No old Protocol names remain
# ---------------------------------------------------------------------------


class TestOldProtocolNamesAbsent:
    _OLD_NAMES = [
        "CollectionStorageDriverProtocol",
        "AssetDriverProtocol",
        "CollectionMetadataDriverProtocol",
    ]

    def _grep(self, name: str) -> list[str]:
        result = subprocess.run(
            ["grep", "-r", "--include=*.py", "-l", name,
             "src/dynastore", "tests/dynastore"],
            capture_output=True, text=True, cwd=_REPO_ROOT,
        )
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    def test_no_collection_storage_driver_protocol(self):
        hits = self._grep("CollectionStorageDriverProtocol")
        assert hits == [], f"Old name still present in: {hits}"

    def test_no_asset_driver_protocol(self):
        hits = self._grep("AssetDriverProtocol")
        assert hits == [], f"Old name still present in: {hits}"

    def test_no_collection_metadata_driver_protocol(self):
        hits = self._grep("CollectionMetadataDriverProtocol")
        assert hits == [], f"Old name still present in: {hits}"


# ---------------------------------------------------------------------------
# 2. New Protocol classes are importable and correctly named
# ---------------------------------------------------------------------------


class TestNewProtocolsImportable:
    def test_collection_items_store_importable(self):
        from dynastore.models.protocols.storage_driver import CollectionItemsStore
        assert CollectionItemsStore.__name__ == "CollectionItemsStore"

    def test_asset_store_importable(self):
        from dynastore.models.protocols.asset_driver import AssetStore
        assert AssetStore.__name__ == "AssetStore"

    def test_collection_metadata_store_importable(self):
        from dynastore.models.protocols.entity_store import CollectionStore
        assert CollectionStore.__name__ == "CollectionStore"

    def test_protocols_re_exported_from_package(self):
        from dynastore.models.protocols import (
            CollectionItemsStore,
            AssetStore,
            CollectionStore,
        )
        assert CollectionItemsStore.__name__ == "CollectionItemsStore"
        assert AssetStore.__name__ == "AssetStore"
        assert CollectionStore.__name__ == "CollectionStore"

    def test_collection_items_store_re_exported_from_storage(self):
        from dynastore.modules.storage import CollectionItemsStore
        assert CollectionItemsStore.__name__ == "CollectionItemsStore"


# ---------------------------------------------------------------------------
# 3. DriverRegistry uses the new Protocol names for discovery
# ---------------------------------------------------------------------------


class TestDriverRegistryProtocolBinding:
    def test_registry_build_collection_uses_collection_items_store(self):
        from dynastore.modules.storage.driver_registry import DriverRegistry
        from dynastore.models.protocols.storage_driver import CollectionItemsStore
        # The _build_collection method imports CollectionItemsStore
        import inspect
        src = inspect.getsource(DriverRegistry._build_collection)
        assert "CollectionItemsStore" in src

    def test_registry_build_asset_uses_asset_store(self):
        from dynastore.modules.storage.driver_registry import DriverRegistry
        import inspect
        src = inspect.getsource(DriverRegistry._build_asset)
        assert "AssetStore" in src


# ---------------------------------------------------------------------------
# 4. Structural subtyping — driver classes satisfy Protocol contracts
# ---------------------------------------------------------------------------


class TestDriverStructuralSubtyping:
    """Verify driver classes structurally implement the new Protocol names."""

    def test_postgresql_driver_satisfies_collection_items_store(self):
        from dynastore.models.protocols.storage_driver import CollectionItemsStore
        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver
        # @runtime_checkable allows isinstance with Protocol
        assert isinstance(ItemsPostgresqlDriver(), CollectionItemsStore)

    def test_pg_asset_driver_satisfies_asset_store(self):
        from dynastore.models.protocols.asset_driver import AssetStore
        # Import PG asset driver
        from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver
        driver = AssetPostgresqlDriver.__new__(AssetPostgresqlDriver)
        assert isinstance(driver, AssetStore)
