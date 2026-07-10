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

from typing import Any, Dict

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver
from dynastore.models.ogc import Feature
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig


class TestItemsPostgresqlDriverMeta:
    """Driver class name / priority / capabilities / read-flavour hints
    are pinned once for all drivers in ``test_driver_meta_contract.py``."""

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

    @pytest.mark.asyncio
    async def test_write_entities_forwards_context_to_item_service(self):
        driver = ItemsPostgresqlDriver()
        mock_crud = AsyncMock()
        mock_crud.upsert = AsyncMock(return_value=[MagicMock(spec=Feature)])

        with patch.object(driver, "_get_crud_protocol", return_value=mock_crud):
            await driver.write_entities(
                "cat1",
                "col1",
                [MagicMock(spec=Feature)],
                context={"write_id": "w-123"},
            )

        assert mock_crud.upsert.await_args.kwargs["processing_context"] == {
            "write_id": "w-123",
        }


class TestWriteIdChunkReads:
    @pytest.mark.asyncio
    async def test_read_active_rows_by_write_id_uses_keyset_paging(self, monkeypatch):
        driver = ItemsPostgresqlDriver()
        captured: list[dict[str, Any]] = []
        returned_rows = [
            {"geoid": "00000000-0000-0000-0000-000000000001", "write_id": "w-123"},
            {"geoid": "00000000-0000-0000-0000-000000000002", "write_id": "w-123"},
            {"geoid": "00000000-0000-0000-0000-000000000003", "write_id": "w-123"},
        ]

        class _FakeResultHandler:
            ALL_DICTS = object()

        class _FakeDQLQuery:
            def __init__(self, sql, *, result_handler):
                captured.append({"sql": sql, "result_handler": result_handler})

            async def execute(self, conn, **params):
                captured[-1]["params"] = params
                return returned_rows

        monkeypatch.setattr(
            "dynastore.modules.db_config.query_executor.DQLQuery", _FakeDQLQuery,
        )
        monkeypatch.setattr(
            "dynastore.modules.db_config.query_executor.ResultHandler",
            _FakeResultHandler,
        )

        with (
            patch.object(
                driver, "_resolve_schema", new_callable=AsyncMock, return_value="cat_schema",
            ),
            patch.object(
                driver, "resolve_physical_table", new_callable=AsyncMock, return_value="items_hub",
            ),
        ):
            rows, next_after_geoid = await driver.read_active_rows_by_write_id(
                "cat1",
                "col1",
                write_id="w-123",
                limit=2,
                after_geoid="00000000-0000-0000-0000-000000000000",
                db_resource=object(),
            )

        assert [row["geoid"] for row in rows] == [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        assert next_after_geoid == "00000000-0000-0000-0000-000000000002"
        assert captured[0]["result_handler"] is _FakeResultHandler.ALL_DICTS
        assert captured[0]["params"] == {
            "write_id": "w-123",
            "after_geoid": "00000000-0000-0000-0000-000000000000",
            "limit": 3,
        }
        assert 'FROM "cat_schema"."items_hub"' in captured[0]["sql"]
        assert '"write_id" = :write_id' in captured[0]["sql"]
        assert '"deleted_at" IS NULL' in captured[0]["sql"]
        assert '"geoid" > CAST(:after_geoid AS uuid)' in captured[0]["sql"]
        assert 'ORDER BY "geoid" ASC' in captured[0]["sql"]

    @pytest.mark.asyncio
    async def test_read_tombstoned_ids_by_write_id_returns_page(self, monkeypatch):
        driver = ItemsPostgresqlDriver()
        captured: list[dict[str, Any]] = []
        returned_rows = [
            {"geoid": "00000000-0000-0000-0000-00000000000a"},
            {"geoid": "00000000-0000-0000-0000-00000000000b"},
        ]

        class _FakeResultHandler:
            ALL_DICTS = object()

        class _FakeDQLQuery:
            def __init__(self, sql, *, result_handler):
                captured.append({"sql": sql, "result_handler": result_handler})

            async def execute(self, conn, **params):
                captured[-1]["params"] = params
                return returned_rows

        monkeypatch.setattr(
            "dynastore.modules.db_config.query_executor.DQLQuery", _FakeDQLQuery,
        )
        monkeypatch.setattr(
            "dynastore.modules.db_config.query_executor.ResultHandler",
            _FakeResultHandler,
        )

        with (
            patch.object(
                driver, "_resolve_schema", new_callable=AsyncMock, return_value="cat_schema",
            ),
            patch.object(
                driver, "resolve_physical_table", new_callable=AsyncMock, return_value="items_hub",
            ),
        ):
            ids, next_after_geoid = await driver.read_tombstoned_ids_by_write_id(
                "cat1",
                "col1",
                write_id="w-del",
                limit=5,
                db_resource=object(),
            )

        assert ids == [
            "00000000-0000-0000-0000-00000000000a",
            "00000000-0000-0000-0000-00000000000b",
        ]
        assert next_after_geoid is None
        assert captured[0]["result_handler"] is _FakeResultHandler.ALL_DICTS
        assert captured[0]["params"] == {"write_id": "w-del", "limit": 6}
        assert '"deleted_at" IS NOT NULL' in captured[0]["sql"]
        assert 'ORDER BY "geoid" ASC' in captured[0]["sql"]


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
    async def test_ensure_storage_hub_ddl_includes_write_id_column_and_index(self):
        driver = ItemsPostgresqlDriver()
        executed_sql: list[str] = []
        write_policy = MagicMock()
        write_policy.find_compute.return_value = None
        write_policy.track_asset_id = False
        write_policy.compute = []

        class _FakeDDLQuery:
            def __init__(self, sql):
                self.sql = sql

            async def execute(self, conn, **kwargs):
                executed_sql.append(self.sql)

        class _FakeManagedTransaction:
            async def __aenter__(self):
                return object()

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _FakeSidecarRegistry:
            @staticmethod
            def get_sidecar(config):
                return None

        with (
            patch.object(
                driver, "_resolve_schema", new_callable=AsyncMock, return_value="cat_schema",
            ),
            patch.object(
                driver, "get_driver_config", new_callable=AsyncMock,
                return_value=ItemsPostgresqlDriverConfig(),
            ),
            patch(
                "dynastore.tools.discovery.get_protocol",
                return_value=None,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.generate_physical_name",
                return_value="items_hub",
            ),
            patch(
                "dynastore.modules.db_config.query_executor.DDLQuery",
                _FakeDDLQuery,
            ),
            patch(
                "dynastore.modules.db_config.query_executor.managed_transaction",
                return_value=_FakeManagedTransaction(),
            ),
            patch(
                "dynastore.modules.storage.drivers.pg_sidecars.registry.SidecarRegistry",
                _FakeSidecarRegistry,
            ),
            patch.object(
                driver, "_resolve_write_policy", new_callable=AsyncMock, return_value=write_policy,
            ),
            patch.object(
                driver, "set_physical_table", new_callable=AsyncMock,
            ),
        ):
            await driver.ensure_storage("cat1", "col1", db_resource=object())

        assert executed_sql, "expected hub DDL to execute"
        assert '"write_id" TEXT' in executed_sql[0]
        # #2687: persisted write-time owner — CREATE-only, nullable, same
        # rollout shape as write_id.
        assert '"access_owner" TEXT' in executed_sql[0]
        assert any(
            'CREATE INDEX IF NOT EXISTS "items_hub_write_id_active_idx"' in sql
            and 'ON "cat_schema"."items_hub" ("write_id", "geoid")' in sql
            and 'WHERE "write_id" IS NOT NULL AND "deleted_at" IS NULL' in sql
            for sql in executed_sql
        )
        assert any(
            'CREATE INDEX IF NOT EXISTS "items_hub_write_id_deleted_idx"' in sql
            and 'ON "cat_schema"."items_hub" ("write_id", "geoid")' in sql
            and 'WHERE "write_id" IS NOT NULL AND "deleted_at" IS NOT NULL' in sql
            for sql in executed_sql
        )
        # #2688 lane 1: the obligation sweep range-scans transaction_time
        # every tick and separately range-scans deleted_at to catch soft
        # deletes (which don't bump transaction_time).
        assert any(
            'CREATE INDEX IF NOT EXISTS "items_hub_transaction_time_idx"' in sql
            and 'ON "cat_schema"."items_hub" ("transaction_time")' in sql
            for sql in executed_sql
        )
        assert any(
            'CREATE INDEX IF NOT EXISTS "items_hub_deleted_at_idx"' in sql
            and 'ON "cat_schema"."items_hub" ("deleted_at")' in sql
            and 'WHERE "deleted_at" IS NOT NULL' in sql
            for sql in executed_sql
        )

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
    async def test_drop_storage_records_collection_tolerates_absent_geometry_table(self):
        """#2655: a RECORDS collection created after this fix never had a
        geometries sidecar table provisioned. drop_storage still issues a
        DROP for the full registered-type superset (IF EXISTS makes the
        absent geometries table a no-op) — deletion must not fail on it."""
        import dynastore.modules.db_config.shared_queries as sq

        driver = ItemsPostgresqlDriver()
        mock_conn = AsyncMock()
        mock_execute = AsyncMock()

        with (
            patch.object(
                driver, "resolve_physical_table", new_callable=AsyncMock,
                return_value="records_hub"
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
            # No exception even though "records_hub_geometries" was never
            # created — the query itself is DROP TABLE IF EXISTS.
            await driver.drop_storage("cat1", "records_col", db_resource=mock_conn)

        called_tables = [kw["table"] for _, kw in mock_execute.call_args_list]
        assert "records_hub_geometries" in called_tables
        assert "records_hub_attributes" in called_tables
        assert "records_hub" in called_tables

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


class TestEnsureStorageCollectionTypeThreading:
    """#2655: ``ensure_storage`` must thread the real ``CollectionInfo.kind``
    (+ ``allow_geometry``) into ``_effective_sidecars``, the same resolution
    ``collection_has_geometry()`` / ``_get_effective_driver_config`` already
    use — so a RECORDS collection no longer provisions an unused geometry
    sidecar table at DDL time, while VECTOR provisioning stays unchanged.

    Each test stops execution right after ``_effective_sidecars`` resolves
    (by raising from a spy that wraps the real resolver) so the DDL /
    managed_transaction machinery never has to be mocked — only the
    collection_type-threading contract under test.
    """

    @staticmethod
    def _install_config_stub(kind, monkeypatch_target="dynastore.tools.discovery.get_protocol"):
        from dynastore.modules.catalog.catalog_config import CollectionInfo

        mock_configs = AsyncMock()

        async def _get_config_side_effect(cls, **kwargs):
            if cls is CollectionInfo:
                return CollectionInfo(kind=kind)
            return None

        mock_configs.get_config = AsyncMock(side_effect=_get_config_side_effect)
        return patch(monkeypatch_target, return_value=mock_configs)

    @staticmethod
    def _install_effective_sidecars_spy():
        """Wrap the real ``_effective_sidecars`` and raise with its result
        so the test can assert on both the resolved sidecar list and the
        kwargs ``ensure_storage`` passed in, without mocking DDL/DB internals.
        """
        from dynastore.modules.storage.drivers.pg_sidecars import (
            _effective_sidecars as _real_effective_sidecars,
        )

        captured: Dict[str, Any] = {}

        class _StopAfterSidecars(Exception):
            pass

        def _spy(*args, **kwargs):
            captured["collection_type"] = kwargs.get("collection_type")
            captured["context"] = kwargs.get("context")
            captured["sidecars"] = _real_effective_sidecars(*args, **kwargs)
            raise _StopAfterSidecars

        return (
            patch(
                "dynastore.modules.storage.drivers.pg_sidecars._effective_sidecars",
                side_effect=_spy,
            ),
            captured,
            _StopAfterSidecars,
        )

    @pytest.mark.asyncio
    async def test_records_collection_skips_geometry_sidecar(self):
        """A RECORDS collection resolves collection_type="RECORDS" into
        _effective_sidecars and the geometries sidecar is omitted.
        """
        from dynastore.modules.catalog.catalog_config import CollectionKind

        driver = ItemsPostgresqlDriver()
        spy_patch, captured, stop_exc = self._install_effective_sidecars_spy()

        with (
            patch.object(driver, "_resolve_schema", new_callable=AsyncMock, return_value="schema1"),
            self._install_config_stub(CollectionKind.RECORDS),
            spy_patch,
        ):
            with pytest.raises(stop_exc):
                await driver.ensure_storage(
                    "cat1", "col1", db_resource=MagicMock(spec=AsyncConnection),
                )

        assert captured["collection_type"] == "RECORDS"
        sidecar_types = [s.sidecar_type for s in captured["sidecars"]]
        assert "geometries" not in sidecar_types
        assert "attributes" in sidecar_types

    @pytest.mark.asyncio
    async def test_vector_collection_ddl_unchanged(self):
        """A VECTOR collection (default kind) still resolves geometries +
        attributes — provisioning DDL for VECTOR stays byte-identical.
        """
        from dynastore.modules.catalog.catalog_config import CollectionKind

        driver = ItemsPostgresqlDriver()
        spy_patch, captured, stop_exc = self._install_effective_sidecars_spy()

        with (
            patch.object(driver, "_resolve_schema", new_callable=AsyncMock, return_value="schema1"),
            self._install_config_stub(CollectionKind.VECTOR),
            spy_patch,
        ):
            with pytest.raises(stop_exc):
                await driver.ensure_storage(
                    "cat1", "col1", db_resource=MagicMock(spec=AsyncConnection),
                )

        assert captured["collection_type"] == "VECTOR"
        sidecar_types = [s.sidecar_type for s in captured["sidecars"]]
        assert "geometries" in sidecar_types
        assert "attributes" in sidecar_types

    @pytest.mark.asyncio
    async def test_no_configs_protocol_defaults_to_vector(self):
        """No ConfigsProtocol registered → CollectionInfo() default (VECTOR),
        matching the pre-#2655 fallback behaviour for that edge case.
        """
        driver = ItemsPostgresqlDriver()
        spy_patch, captured, stop_exc = self._install_effective_sidecars_spy()

        with (
            patch.object(driver, "_resolve_schema", new_callable=AsyncMock, return_value="schema1"),
            patch("dynastore.tools.discovery.get_protocol", return_value=None),
            spy_patch,
        ):
            with pytest.raises(stop_exc):
                await driver.ensure_storage(
                    "cat1", "col1", db_resource=MagicMock(spec=AsyncConnection),
                )

        assert captured["collection_type"] == "VECTOR"


class TestLocation:
    """Modern typed-location API. Replaces deleted resolve_storage_location()
    tests after the StorageLocationResolver Protocol was removed in favour of
    CollectionItemsStore.location() returning a typed StorageLocation."""

    @pytest.mark.asyncio
    async def test_location_with_collection(self):
        driver = ItemsPostgresqlDriver()
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_catalogs = AsyncMock()
            mock_catalogs.resolve_physical_schema = AsyncMock(return_value="my_schema")

            mock_configs = AsyncMock()
            mock_configs.get_config = AsyncMock(
                return_value=ItemsPostgresqlDriverConfig(physical_table="my_table")
            )

            def side_effect(proto):
                name = proto.__name__ if hasattr(proto, "__name__") else str(proto)
                if "Catalogs" in name:
                    return mock_catalogs
                if "Configs" in name:
                    return mock_configs
                return None

            mock_gp.side_effect = side_effect
            loc = await driver.location("cat1", "col1")
            assert loc.backend == "postgresql"
            assert loc.identifiers["schema"] == "my_schema"
            assert loc.identifiers["table"] == "my_table"

    @pytest.mark.asyncio
    async def test_location_raises_when_physical_table_unset(self):
        """Driver config with no physical_table must raise instead of
        silently using collection_id — the silent fallback hid lifecycle
        gaps (collection registered but not activated) until the deeper
        resolver in _apply_query_transformations raised the opaque
        'Could not resolve storage' from a frame far from the cause."""
        driver = ItemsPostgresqlDriver()
        with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
            mock_catalogs = AsyncMock()
            mock_catalogs.resolve_physical_schema = AsyncMock(return_value="my_schema")

            mock_configs = AsyncMock()
            mock_configs.get_config = AsyncMock(
                return_value=ItemsPostgresqlDriverConfig(physical_table=None)
            )

            def side_effect(proto):
                name = proto.__name__ if hasattr(proto, "__name__") else str(proto)
                if "Catalogs" in name:
                    return mock_catalogs
                if "Configs" in name:
                    return mock_configs
                return None

            mock_gp.side_effect = side_effect
            with pytest.raises(ValueError, match="No physical_table configured"):
                await driver.location("cat1", "col1")
