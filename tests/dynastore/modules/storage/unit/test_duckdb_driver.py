import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from dynastore.models.protocols.storage_driver import Capability
from dynastore.modules.storage.errors import (
    ReadOnlyDriverError,
    SoftDeleteNotSupportedError,
)
from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig


class TestDuckDBDriverMeta:
    def test_driver_class_name(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        assert type(driver).__name__ == "ItemsDuckdbDriver"

    def test_priority(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        assert driver.priority == 30

    def test_capabilities(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        assert Capability.READ in driver.capabilities
        assert Capability.STREAMING in driver.capabilities
        assert Capability.EXPORT in driver.capabilities

    def test_read_flavour_hints(self):
        """Read-flavour capabilities moved to ``Hint`` in PR #3b."""
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        from dynastore.modules.storage.hints import Hint
        driver = ItemsDuckdbDriver()
        assert Hint.SPATIAL_FILTER in driver.supported_hints
        assert Hint.SORT in driver.supported_hints
        assert Hint.GROUP_BY in driver.supported_hints

    def test_is_available_without_duckdb(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        with patch("dynastore.modules.storage.drivers.duckdb._duckdb_available", return_value=False):
            driver = ItemsDuckdbDriver()
            assert driver.is_available() is False


class TestDuckDBFormatReaders:
    def test_reader_func_parquet(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        assert driver._reader_func("parquet") == "read_parquet"

    def test_reader_func_csv(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        assert driver._reader_func("csv") == "read_csv_auto"

    def test_reader_func_json(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        assert driver._reader_func("json") == "read_json_auto"

    def test_reader_func_unknown_defaults_to_parquet(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        assert driver._reader_func("unknown") == "read_parquet"


class TestDuckDBWritability:
    def test_is_writable_false_by_default(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        loc = ItemsDuckdbDriverConfig(format="parquet", path="/data/f.parquet")
        assert driver._is_writable(loc) is False

    def test_is_writable_true_with_write_path(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        loc = ItemsDuckdbDriverConfig(
            format="parquet",
            path="/data/f.parquet",
            write_path="/data/w.db",
            write_format="sqlite",
        )
        assert driver._is_writable(loc) is True


class TestDuckDBWriteEntities:
    @pytest.mark.asyncio
    async def test_write_raises_read_only_without_write_path(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        loc = ItemsDuckdbDriverConfig(format="parquet", path="/data/f.parquet")
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            with pytest.raises(ReadOnlyDriverError):
                await driver.write_entities("cat1", "col1", {"id": "1"})

    @pytest.mark.asyncio
    async def test_write_raises_read_only_without_location(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=None):
            with pytest.raises(ReadOnlyDriverError):
                await driver.write_entities("cat1", "col1", {"id": "1"})


class TestDuckDBDeleteEntities:
    @pytest.mark.asyncio
    async def test_delete_raises_read_only_without_write_path(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        loc = ItemsDuckdbDriverConfig(format="parquet", path="/data/f.parquet")
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            with pytest.raises(ReadOnlyDriverError):
                await driver.delete_entities("cat1", "col1", ["id1"])

    @pytest.mark.asyncio
    async def test_soft_delete_raises(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        loc = ItemsDuckdbDriverConfig(
            format="parquet", path="/data/f.parquet",
            write_path="/data/w.db", write_format="sqlite",
        )
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            with pytest.raises(SoftDeleteNotSupportedError):
                await driver.delete_entities("cat1", "col1", ["id1"], soft=True)


class TestDuckDBDropStorage:
    @pytest.mark.asyncio
    async def test_soft_drop_raises(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        with pytest.raises(SoftDeleteNotSupportedError):
            await driver.drop_storage("cat1", "col1", soft=True)


class TestDuckDBLocation:
    """Modern typed-location API. Replaces deleted resolve_storage_location()
    tests after the StorageLocationResolver Protocol was removed in favour of
    CollectionItemsStore.location() returning a typed StorageLocation."""

    @pytest.mark.asyncio
    async def test_location_returns_typed_storage_location(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        loc = ItemsDuckdbDriverConfig(format="csv", path="/data/f.csv")
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            result = await driver.location("cat1", "col1")
            assert result.backend == "duckdb"
            assert result.identifiers["format"] == "csv"
            assert result.identifiers["path"] == "/data/f.csv"

    @pytest.mark.asyncio
    async def test_location_default_format_when_missing(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=None):
            result = await driver.location("cat1", "col1")
            assert result.backend == "duckdb"
            assert result.identifiers["format"] == "parquet"


class TestDuckDBEnsureStorage:
    @pytest.mark.asyncio
    async def test_ensure_storage_no_location(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=None):
            await driver.ensure_storage("cat1", "col1")  # should not raise

    @pytest.mark.asyncio
    async def test_ensure_storage_file_not_found(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        driver = ItemsDuckdbDriver()
        loc = ItemsDuckdbDriverConfig(format="parquet", path="/nonexistent/file.parquet")
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            # Missing read-only source path is logged as info, not raised —
            # the file will be populated by ETL or the first write_entities() call.
            await driver.ensure_storage("cat1", "col1")  # should not raise
