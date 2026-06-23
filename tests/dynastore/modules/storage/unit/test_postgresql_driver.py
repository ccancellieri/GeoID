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

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver
from dynastore.models.ogc import Feature
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig


class TestItemsPostgresqlDriverMeta:
    def test_driver_class_name(self):
        driver = ItemsPostgresqlDriver()
        assert type(driver).__name__ == "ItemsPostgresqlDriver"

    def test_priority(self):
        driver = ItemsPostgresqlDriver()
        assert driver.priority == 10

    def test_capabilities(self):
        driver = ItemsPostgresqlDriver()
        assert Capability.STREAMING in driver.capabilities
        assert Capability.SOFT_DELETE in driver.capabilities
        assert Capability.EXPORT in driver.capabilities
        assert Capability.READ_ONLY not in driver.capabilities

    def test_read_flavour_hints(self):
        """Read-flavour capabilities moved to ``Hint`` in PR #3b."""
        from dynastore.modules.storage.hints import Hint
        driver = ItemsPostgresqlDriver()
        assert Hint.SPATIAL_FILTER in driver.supported_hints
        assert Hint.AGGREGATION in driver.supported_hints
        assert Hint.GEOMETRY_EXACT in driver.supported_hints

    def test_is_available_with_items_protocol(self):
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_gp.return_value = MagicMock()
            driver = ItemsPostgresqlDriver()
            assert driver.is_available() is True

    def test_is_available_without_items_protocol(self):
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_gp.return_value = None
            driver = ItemsPostgresqlDriver()
            assert driver.is_available() is False


class TestWriteEntities:
    @pytest.mark.asyncio
    async def test_write_single_feature(self):
        driver = ItemsPostgresqlDriver()
        mock_crud = AsyncMock()
        mock_crud.upsert = AsyncMock(return_value=[MagicMock(spec=Feature)])

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            feature = MagicMock(spec=Feature)
            result = await driver.write_entities("cat1", "col1", feature)
            mock_crud.upsert.assert_called_once()
            assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_write_returns_list_from_single(self):
        driver = ItemsPostgresqlDriver()
        mock_crud = AsyncMock()
        single_result = MagicMock(spec=Feature)
        mock_crud.upsert = AsyncMock(return_value=single_result)

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            result = await driver.write_entities("cat1", "col1", MagicMock(spec=Feature))
            assert isinstance(result, list)
            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_write_list_returns_list(self):
        driver = ItemsPostgresqlDriver()
        mock_crud = AsyncMock()
        items = [MagicMock(spec=Feature), MagicMock(spec=Feature)]
        mock_crud.upsert = AsyncMock(return_value=items)

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            result = await driver.write_entities("cat1", "col1", items)
            assert isinstance(result, list)
            assert len(result) == 2


class TestReadEntities:
    @pytest.mark.asyncio
    async def test_read_with_default_query_request(self):
        driver = ItemsPostgresqlDriver()
        mock_query = AsyncMock()
        mock_feature = MagicMock(spec=Feature)

        async def mock_items():
            yield mock_feature

        mock_response = MagicMock()
        mock_response.items = mock_items()
        mock_query.stream_items = AsyncMock(return_value=mock_response)

        with patch.object(driver, "_get_query_protocol", return_value=mock_query):
            results = []
            async for f in driver.read_entities("cat1", "col1"):
                results.append(f)

            assert len(results) == 1
            assert results[0] is mock_feature
            call_args = mock_query.stream_items.call_args
            request_arg = call_args[0][2]
            assert isinstance(request_arg, QueryRequest)
            assert request_arg.limit == 100
            assert request_arg.offset == 0

    @pytest.mark.asyncio
    async def test_read_with_entity_ids(self):
        driver = ItemsPostgresqlDriver()
        mock_query = AsyncMock()

        async def mock_items():
            yield MagicMock(spec=Feature)

        mock_response = MagicMock()
        mock_response.items = mock_items()
        mock_query.stream_items = AsyncMock(return_value=mock_response)

        with patch.object(driver, "_get_query_protocol", return_value=mock_query):
            results = []
            async for f in driver.read_entities(
                "cat1", "col1", entity_ids=["id1", "id2"]
            ):
                results.append(f)

            call_args = mock_query.stream_items.call_args
            request_arg = call_args[0][2]
            assert request_arg.item_ids == ["id1", "id2"]

    @pytest.mark.asyncio
    async def test_read_with_custom_request(self):
        driver = ItemsPostgresqlDriver()
        mock_query = AsyncMock()

        async def mock_items():
            yield MagicMock(spec=Feature)

        mock_response = MagicMock()
        mock_response.items = mock_items()
        mock_query.stream_items = AsyncMock(return_value=mock_response)

        custom_request = QueryRequest(limit=50, offset=10)

        with patch.object(driver, "_get_query_protocol", return_value=mock_query):
            results = []
            async for f in driver.read_entities(
                "cat1", "col1", request=custom_request
            ):
                results.append(f)

            call_args = mock_query.stream_items.call_args
            request_arg = call_args[0][2]
            assert request_arg.limit == 50
            assert request_arg.offset == 10


class TestDeleteEntities:
    @pytest.mark.asyncio
    async def test_delete_entities(self):
        driver = ItemsPostgresqlDriver()
        mock_crud = AsyncMock()
        mock_crud.delete_item = AsyncMock(return_value=1)

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            count = await driver.delete_entities("cat1", "col1", ["id1", "id2", "id3"])
            assert count == 3
            assert mock_crud.delete_item.call_count == 3

    @pytest.mark.asyncio
    async def test_delete_empty_list(self):
        driver = ItemsPostgresqlDriver()
        mock_crud = AsyncMock()

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            count = await driver.delete_entities("cat1", "col1", [])
            assert count == 0

    @pytest.mark.asyncio
    async def test_soft_delete_raises(self):
        driver = ItemsPostgresqlDriver()
        with pytest.raises(SoftDeleteNotSupportedError):
            await driver.delete_entities("cat1", "col1", ["id1"], soft=True)


class TestLifecycleMethods:
    @pytest.mark.asyncio
    async def test_ensure_storage_noop_without_collection(self):
        """ensure_storage with no collection_id is a no-op."""
        driver = ItemsPostgresqlDriver()
        # Should return immediately without touching the DB
        with patch.object(driver, "_resolve_schema", new_callable=AsyncMock) as mock_resolve:
            await driver.ensure_storage("cat1")
            mock_resolve.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_storage_requires_db_resource(self):
        driver = ItemsPostgresqlDriver()
        with pytest.raises(ValueError, match="db_resource"):
            await driver.ensure_storage("cat1", "col1")

    @pytest.mark.asyncio
    async def test_drop_storage_collection(self):
        """Hard drop with a known collection: drops every sidecar table then the hub."""
        import dynastore.modules.db_config.shared_queries as sq

        driver = ItemsPostgresqlDriver()
        mock_conn = AsyncMock()
        mock_execute = AsyncMock()

        with (
            patch.object(
                driver, "resolve_physical_table", new_callable=AsyncMock,
                return_value="items_hub"
            ),
            patch.object(
                driver, "_resolve_schema", new_callable=AsyncMock,
                return_value="cat1_schema"
            ),
            patch(
                "dynastore.modules.storage.drivers.pg_sidecars.registry"
                ".SidecarRegistry.get_available_types",
                return_value=["attributes", "geometries"],
            ),
            patch.object(sq.delete_table_query, "execute", mock_execute),
        ):
            await driver.drop_storage("cat1", "col1", db_resource=mock_conn)

        assert mock_execute.call_count == 3  # 2 sidecars + 1 hub
        called_tables = [
            kw["table"] for _, kw in mock_execute.call_args_list
        ]
        assert "items_hub_attributes" in called_tables
        assert "items_hub_geometries" in called_tables
        assert "items_hub" in called_tables

    @pytest.mark.asyncio
    async def test_drop_storage_catalog(self):
        """Catalog-level call (collection_id=None) is a no-op: no DDL, no service calls."""
        driver = ItemsPostgresqlDriver()
        with (
            patch.object(
                driver, "resolve_physical_table", new_callable=AsyncMock
            ) as mock_rpt,
            patch.object(
                driver, "_resolve_schema", new_callable=AsyncMock
            ) as mock_rs,
        ):
            await driver.drop_storage("cat1")
            mock_rpt.assert_not_called()
            mock_rs.assert_not_called()

    @pytest.mark.asyncio
    async def test_drop_storage_soft(self):
        """soft=True: logs intent and returns without issuing any DDL."""
        driver = ItemsPostgresqlDriver()
        with (
            patch.object(
                driver, "resolve_physical_table", new_callable=AsyncMock
            ) as mock_rpt,
            patch.object(
                driver, "_resolve_schema", new_callable=AsyncMock
            ) as mock_rs,
        ):
            await driver.drop_storage("cat1", "col1", soft=True)
            mock_rpt.assert_not_called()
            mock_rs.assert_not_called()

    @pytest.mark.asyncio
    async def test_export_entities_not_implemented(self):
        driver = ItemsPostgresqlDriver()
        with pytest.raises(NotImplementedError):
            await driver.export_entities("cat1", "col1")


class TestLocation:
    """Modern typed-location API. Replaces deleted resolve_storage_location()
    tests after the StorageLocationResolver Protocol was removed in favour of
    CollectionItemsStore.location() returning a typed StorageLocation."""

    @pytest.mark.asyncio
    async def test_location_with_collection(self):
        driver = ItemsPostgresqlDriver()
        # Pre-populate the confirmed-active cache so resolve_physical_table
        # skips the to_regclass existence probe (the collection is already
        # known to be provisioned in this process).
        from dynastore.modules.catalog.collection_service import (
            _mark_confirmed_active,
            _unmark_confirmed_active,
        )
        _mark_confirmed_active("cat1", "col1")
        try:
            with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
                mock_catalogs = AsyncMock()
                mock_catalogs.resolve_physical_schema = AsyncMock(return_value="my_schema")
                # resolve_physical_id returns the collection's physical_id
                # which is also the physical table name (clean-break: no JSONB pin).
                mock_catalogs.resolve_physical_id = AsyncMock(return_value="c__mytable1")

                def side_effect(proto):
                    name = proto.__name__ if hasattr(proto, "__name__") else str(proto)
                    if "Catalogs" in name:
                        return mock_catalogs
                    return None

                mock_gp.side_effect = side_effect
                loc = await driver.location("cat1", "col1")
                assert loc.backend == "postgresql"
                assert loc.identifiers["schema"] == "my_schema"
                assert loc.identifiers["table"] == "c__mytable1"
        finally:
            _unmark_confirmed_active("cat1", "col1")

    @pytest.mark.asyncio
    async def test_location_raises_when_physical_id_unset(self):
        """When collections.physical_id is NULL (unprovisioned), location()
        must raise rather than silently falling back — the silent fallback
        previously hid lifecycle gaps."""
        driver = ItemsPostgresqlDriver()
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_catalogs = AsyncMock()
            mock_catalogs.resolve_physical_schema = AsyncMock(return_value="my_schema")
            # NULL physical_id means the collection was never provisioned.
            mock_catalogs.resolve_physical_id = AsyncMock(return_value=None)

            def side_effect(proto):
                name = proto.__name__ if hasattr(proto, "__name__") else str(proto)
                if "Catalogs" in name:
                    return mock_catalogs
                return None

            mock_gp.side_effect = side_effect
            with pytest.raises(ValueError, match="No physical_table configured"):
                await driver.location("cat1", "col1")
