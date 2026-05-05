"""
Integration tests for the Iceberg storage driver.

Uses a real PyIceberg SqlCatalog backed by the test PostgreSQL database.
No MagicMock for catalog or table objects — all OTF operations are exercised
against real Iceberg tables with Parquet data files on a temp filesystem.
"""

import hashlib
import json
import os
import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime, timezone

from dynastore.models.protocols.storage_driver import Capability
from dynastore.modules.storage.driver_config import ItemsIcebergDriverConfig, IcebergConfig
from dynastore.modules.storage.drivers.iceberg import (
    ItemsIcebergDriver,
    _resolve_iceberg_type,
)

pyiceberg = pytest.importorskip("pyiceberg", reason="pyiceberg not installed")
from pyiceberg.types import (
    BooleanType, DateType, DoubleType, FloatType,
    IntegerType, LongType, StringType,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_loc(table_name="test_tbl", namespace="test_ns", **overrides):
    # catalog_name / catalog_uri / catalog_type / warehouse_uri are now env-level
    # (IcebergConfig), not per-collection.  Only table-location fields belong here.
    defaults = dict(namespace=namespace, table_name=table_name)
    defaults.update(overrides)
    return ItemsIcebergDriverConfig(**defaults)


@pytest.fixture
def iceberg_warehouse(tmp_path):
    """Temp directory for Iceberg data files."""
    wh = tmp_path / "iceberg_wh"
    wh.mkdir()
    return str(wh)


@pytest.fixture
def ns_name(request):
    """Unique namespace per test to avoid xdist cross-worker conflicts."""
    return f"ns_{hashlib.md5(request.node.nodeid.encode()).hexdigest()[:12]}"


@pytest.fixture
def iceberg_catalog(db_url, iceberg_warehouse, ns_name):
    """Real PyIceberg SqlCatalog backed by the test PostgreSQL.

    Uses ``IcebergConfig.catalog_name`` so the catalog name matches what the
    driver will use (both read from the same env-level singleton).
    """
    from pyiceberg.catalog.sql import SqlCatalog
    from dynastore.modules.db_config.tools import normalize_db_url

    sync_url = normalize_db_url(db_url, is_async=False)
    if sync_url.startswith("postgresql://"):
        sync_url = sync_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    catalog = SqlCatalog(
        IcebergConfig.catalog_name,
        uri=sync_url,
        warehouse=f"file://{iceberg_warehouse}",
    )
    try:
        catalog.create_namespace(ns_name)
    except Exception:
        pass  # namespace may already exist

    yield catalog

    # Cleanup: drop all tables and namespace
    for table_id in catalog.list_tables(ns_name):
        try:
            catalog.drop_table(table_id)
        except Exception:
            pass
    try:
        catalog.drop_namespace(ns_name)
    except Exception:
        pass


@pytest.fixture
def iceberg_table(iceberg_catalog, ns_name, request):
    """Real Iceberg table with id/name/value/geometry schema — unique per test."""
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import NestedField

    test_id = hashlib.md5(request.node.nodeid.encode()).hexdigest()[:8]
    table_name = f"tbl_{test_id}"

    # Drop if leftover from a previous run
    try:
        iceberg_catalog.drop_table((ns_name, table_name))
    except Exception:
        pass

    schema = IcebergSchema(
        NestedField(1, "id", StringType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "value", LongType(), required=False),
        NestedField(4, "geometry", StringType(), required=False),
    )
    table = iceberg_catalog.create_table((ns_name, table_name), schema=schema)
    yield table

    try:
        iceberg_catalog.drop_table((ns_name, table_name))
    except Exception:
        pass


@pytest.fixture
def test_loc(iceberg_table, ns_name):
    """ItemsIcebergDriverConfig pointing to the test's unique table."""
    return _make_loc(table_name=iceberg_table.name()[-1], namespace=ns_name)


@pytest.fixture
def driver_with_catalog(iceberg_catalog, iceberg_table):
    """ItemsIcebergDriver pre-loaded with the real test catalog.

    Pre-populates ``_catalog_cache`` with the key the driver computes from
    ``IcebergConfig``, bypassing the ``_load_catalog_sync`` call so tests do
    not need a live PG connection in ``_load_catalog_sync``.
    """
    driver = ItemsIcebergDriver()
    loc_key = (
        f"{IcebergConfig.catalog_name}:{IcebergConfig.catalog_uri}"
        f":{IcebergConfig.catalog_type}"
    )
    driver._catalog_cache[loc_key] = iceberg_catalog
    yield driver
    driver._catalog_cache.pop(loc_key, None)


def _write_rows(table, rows):
    """Helper to write rows to a real Iceberg table via PyArrow."""
    import pyarrow as pa
    schema = pa.schema([
        pa.field("id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("value", pa.int64()),
        pa.field("geometry", pa.string()),
    ])
    pa_table = pa.table(
        {
            "id": [r.get("id") for r in rows],
            "name": [r.get("name") for r in rows],
            "value": [r.get("value") for r in rows],
            "geometry": [r.get("geometry") for r in rows],
        },
        schema=schema,
    )
    table.append(pa_table)


# ---------------------------------------------------------------------------
# Meta & Capabilities
# ---------------------------------------------------------------------------


class TestIcebergDriverMeta:
    def test_driver_class_name(self):
        assert type(ItemsIcebergDriver()).__name__ == "ItemsIcebergDriver"

    def test_priority(self):
        assert ItemsIcebergDriver().priority == 20

    def test_capabilities(self):
        caps = ItemsIcebergDriver().capabilities
        assert Capability.STREAMING in caps
        assert Capability.EXPORT in caps
        assert Capability.TIME_TRAVEL in caps
        assert Capability.VERSIONING in caps
        assert Capability.SNAPSHOTS in caps
        assert Capability.SCHEMA_EVOLUTION in caps
        assert Capability.SOFT_DELETE in caps
        assert Capability.READ_ONLY not in caps

    def test_read_flavour_hints(self):
        """Read-flavour capabilities moved to ``Hint`` in PR #3b."""
        from dynastore.modules.storage.hints import Hint
        hints = ItemsIcebergDriver().supported_hints
        assert Hint.SPATIAL_FILTER in hints
        assert Hint.STATISTICS in hints
        assert Hint.SORT in hints

    def test_is_available_true(self):
        assert ItemsIcebergDriver().is_available() is True

    def test_is_available_without_pyiceberg(self):
        with patch(
            "dynastore.modules.storage.drivers.iceberg._pyiceberg_available",
            return_value=False,
        ):
            assert ItemsIcebergDriver().is_available() is False


# ---------------------------------------------------------------------------
# Table Identifier
# ---------------------------------------------------------------------------


class TestIcebergTableIdentifier:
    def test_default_mapping(self):
        driver = ItemsIcebergDriver()
        loc = ItemsIcebergDriverConfig()
        assert driver._table_identifier(loc, "cat", "col") == ("cat", "col")

    def test_explicit_namespace_and_table(self):
        driver = ItemsIcebergDriver()
        loc = _make_loc(namespace="ns", table_name="tbl")
        assert driver._table_identifier(loc, "cat", "col") == ("ns", "tbl")


# ---------------------------------------------------------------------------
# Catalog Configuration
# ---------------------------------------------------------------------------


class TestIcebergCatalogConfig:
    """Connection-level fields now live in IcebergConfig (env-level), not per-collection."""

    def test_iceberg_config_catalog_type_default(self):
        from dynastore.modules.storage.driver_config import IcebergConfig
        assert IcebergConfig.catalog_type == "sql"

    def test_iceberg_config_catalog_name_default(self):
        from dynastore.modules.storage.driver_config import IcebergConfig
        assert IcebergConfig.catalog_name == "default"

    def test_per_collection_config_rejects_catalog_name(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="catalog_name"):
            _make_loc(catalog_name="glue")

    def test_per_collection_config_rejects_catalog_type(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="catalog_type"):
            _make_loc(catalog_type="rest")

    def test_real_catalog_connection(self, iceberg_catalog, ns_name):
        """Verify the real catalog can list namespaces."""
        namespaces = iceberg_catalog.list_namespaces()
        assert (ns_name,) in namespaces


# ---------------------------------------------------------------------------
# Write Entities
# ---------------------------------------------------------------------------


class TestIcebergWriteEntities:
    @pytest.mark.asyncio
    async def test_write_raises_without_location(self):
        driver = ItemsIcebergDriver()
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=None):
            with pytest.raises(RuntimeError, match="no OTF location config"):
                await driver.write_entities("cat1", "col1", {"id": "1"})

    @pytest.mark.asyncio
    async def test_write_empty_returns_empty(self, driver_with_catalog, test_loc):
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            result = await driver_with_catalog.write_entities("cat1", "col1", [])
            assert result == []

    @pytest.mark.asyncio
    async def test_write_single_dict(self, driver_with_catalog, test_loc, iceberg_table):
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            result = await driver_with_catalog.write_entities(
                "cat1", "col1", {"id": "f1", "name": "test"}
            )
            assert len(result) == 1
            assert result[0].id == "f1"

        iceberg_table.refresh()
        rows = iceberg_table.scan().to_arrow().to_pylist()
        assert len(rows) == 1
        assert rows[0]["id"] == "f1"

    @pytest.mark.asyncio
    async def test_write_feature_model(self, driver_with_catalog, test_loc):
        from dynastore.models.ogc import Feature
        feature = Feature(type="Feature", id="f1", geometry=None, properties={"name": "alice", "value": 42})

        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            result = await driver_with_catalog.write_entities("cat1", "col1", feature)
            assert len(result) == 1
            assert result[0].id == "f1"

    @pytest.mark.asyncio
    async def test_write_multiple_dicts(self, driver_with_catalog, test_loc, iceberg_table):
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            result = await driver_with_catalog.write_entities(
                "cat1", "col1",
                [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}],
            )
            assert len(result) == 2

        iceberg_table.refresh()
        rows = iceberg_table.scan().to_arrow().to_pylist()
        assert len(rows) == 2
        ids = {r["id"] for r in rows}
        assert ids == {"a", "b"}


# ---------------------------------------------------------------------------
# Read Entities
# ---------------------------------------------------------------------------


class TestIcebergReadEntities:
    @pytest.mark.asyncio
    async def test_read_no_location_returns_empty(self, driver_with_catalog):
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=None):
            features = [f async for f in driver_with_catalog.read_entities("cat1", "col1")]
            assert features == []

    @pytest.mark.asyncio
    async def test_read_after_write(self, driver_with_catalog, test_loc, iceberg_table):
        _write_rows(iceberg_table, [
            {"id": "r1", "name": "Alice", "value": 10, "geometry": None},
            {"id": "r2", "name": "Bob", "value": 20, "geometry": None},
        ])

        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            features = [f async for f in driver_with_catalog.read_entities("cat1", "col1")]
            assert len(features) == 2
            ids = {f.id for f in features}
            assert ids == {"r1", "r2"}
            assert features[0].type == "Feature"

    @pytest.mark.asyncio
    async def test_read_with_limit(self, driver_with_catalog, test_loc, iceberg_table):
        _write_rows(iceberg_table, [
            {"id": f"r{i}", "name": f"name{i}", "value": i, "geometry": None}
            for i in range(10)
        ])

        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            features = [f async for f in driver_with_catalog.read_entities(
                "cat1", "col1", limit=3
            )]
            assert len(features) == 3

    @pytest.mark.asyncio
    async def test_read_with_entity_ids(self, driver_with_catalog, test_loc, iceberg_table):
        _write_rows(iceberg_table, [
            {"id": "a", "name": "A", "value": 1, "geometry": None},
            {"id": "b", "name": "B", "value": 2, "geometry": None},
            {"id": "c", "name": "C", "value": 3, "geometry": None},
        ])

        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            features = [f async for f in driver_with_catalog.read_entities(
                "cat1", "col1", entity_ids=["a", "c"]
            )]
            ids = {f.id for f in features}
            assert ids == {"a", "c"}


# ---------------------------------------------------------------------------
# Delete Entities
# ---------------------------------------------------------------------------


class TestIcebergDeleteEntities:
    @pytest.mark.asyncio
    async def test_soft_delete(self, driver_with_catalog, test_loc, iceberg_table):
        _write_rows(iceberg_table, [
            {"id": "d1", "name": "A", "value": 1, "geometry": None},
            {"id": "d2", "name": "B", "value": 2, "geometry": None},
        ])

        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            count = await driver_with_catalog.delete_entities(
                "cat1", "col1", ["d1"], soft=True
            )
            assert count == 1

        iceberg_table.refresh()
        rows = iceberg_table.scan().to_arrow().to_pylist()
        ids = {r["id"] for r in rows}
        assert "d1" not in ids
        assert "d2" in ids

    @pytest.mark.asyncio
    async def test_hard_delete(self, driver_with_catalog, test_loc, iceberg_table):
        _write_rows(iceberg_table, [
            {"id": "h1", "name": "A", "value": 1, "geometry": None},
            {"id": "h2", "name": "B", "value": 2, "geometry": None},
        ])

        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            count = await driver_with_catalog.delete_entities(
                "cat1", "col1", ["h1"], soft=False
            )
            assert count == 1

        iceberg_table.refresh()
        rows = iceberg_table.scan().to_arrow().to_pylist()
        ids = {r["id"] for r in rows}
        assert "h1" not in ids
        assert "h2" in ids


# ---------------------------------------------------------------------------
# Drop Storage
# ---------------------------------------------------------------------------


class TestIcebergDropStorage:
    @pytest.mark.asyncio
    async def test_soft_drop_tags_table(self, driver_with_catalog, test_loc, iceberg_table):
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            await driver_with_catalog.drop_storage("cat1", "col1", soft=True)

        # Reload table to see updated properties — get catalog from cache
        loc_key = (
            f"{IcebergConfig.catalog_name}:{IcebergConfig.catalog_uri}"
            f":{IcebergConfig.catalog_type}"
        )
        refreshed = driver_with_catalog._catalog_cache[loc_key].load_table(iceberg_table.name())
        props = refreshed.properties
        assert props.get("dynastore.deleted") == "true"
        assert "dynastore.deleted_at" in props

    @pytest.mark.asyncio
    async def test_hard_drop_removes_table(self, iceberg_catalog, ns_name, request):
        """Create a unique table for hard drop test."""
        from pyiceberg.schema import Schema as IcebergSchema
        from pyiceberg.types import NestedField

        test_id = hashlib.md5(request.node.nodeid.encode()).hexdigest()[:8]
        tbl_name = f"drop_{test_id}"

        # Clean up leftover from previous runs
        try:
            iceberg_catalog.drop_table((ns_name, tbl_name))
        except Exception:
            pass

        schema = IcebergSchema(
            NestedField(1, "id", StringType(), required=False),
        )
        iceberg_catalog.create_table((ns_name, tbl_name), schema=schema)

        driver = ItemsIcebergDriver()
        loc_key = f"{IcebergConfig.catalog_name}:{IcebergConfig.catalog_uri}:{IcebergConfig.catalog_type}"
        driver._catalog_cache[loc_key] = iceberg_catalog

        loc = _make_loc(table_name=tbl_name, namespace=ns_name)
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            await driver.drop_storage("cat1", "col1", soft=False)

        driver._catalog_cache.pop(loc_key, None)
        tables = iceberg_catalog.list_tables(ns_name)
        table_names = [t[1] for t in tables]
        assert tbl_name not in table_names


# ---------------------------------------------------------------------------
# Ensure Storage
# ---------------------------------------------------------------------------


class TestIcebergEnsureStorage:
    @pytest.mark.asyncio
    async def test_ensure_creates_namespace_and_table(self, iceberg_catalog, ns_name, request):
        test_id = hashlib.md5(request.node.nodeid.encode()).hexdigest()[:8]
        tbl_name = f"ensure_{test_id}"

        # Clean up leftover
        try:
            iceberg_catalog.drop_table((ns_name, tbl_name))
        except Exception:
            pass

        driver = ItemsIcebergDriver()
        loc_key = f"{IcebergConfig.catalog_name}:{IcebergConfig.catalog_uri}:{IcebergConfig.catalog_type}"
        driver._catalog_cache[loc_key] = iceberg_catalog

        loc = _make_loc(table_name=tbl_name, namespace=ns_name)
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=loc):
            await driver.ensure_storage("cat1", tbl_name)

        driver._catalog_cache.pop(loc_key, None)
        tables = iceberg_catalog.list_tables(ns_name)
        table_names = [t[1] for t in tables]
        assert tbl_name in table_names

        # Cleanup
        try:
            iceberg_catalog.drop_table((ns_name, tbl_name))
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Export Entities
# ---------------------------------------------------------------------------


class TestIcebergExportEntities:
    @pytest.mark.asyncio
    async def test_export_parquet(self, driver_with_catalog, test_loc, iceberg_table, tmp_path):
        _write_rows(iceberg_table, [
            {"id": "e1", "name": "Export", "value": 99, "geometry": None},
        ])

        target = str(tmp_path / "export.parquet")
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            result = await driver_with_catalog.export_entities(
                "cat1", "col1", format="parquet", target_path=target
            )
            assert result == target
            assert os.path.exists(target)

    @pytest.mark.asyncio
    async def test_export_json(self, driver_with_catalog, test_loc, iceberg_table, tmp_path):
        _write_rows(iceberg_table, [
            {"id": "j1", "name": "Json", "value": 42, "geometry": None},
        ])

        target = str(tmp_path / "export.json")
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            result = await driver_with_catalog.export_entities(
                "cat1", "col1", format="json", target_path=target
            )
            assert result == target
            with open(target) as f:
                data = json.load(f)
            assert len(data) == 1
            assert data[0]["id"] == "j1"


# ---------------------------------------------------------------------------
# Resolve Storage Location
# ---------------------------------------------------------------------------


class TestIcebergLocation:
    """Modern typed-location API. Replaces deleted resolve_storage_location()
    test after the StorageLocationResolver Protocol was removed in favour of
    CollectionItemsStore.location() returning a typed StorageLocation."""

    @pytest.mark.asyncio
    async def test_location_returns_default_when_no_config(self):
        driver = ItemsIcebergDriver()
        with patch.object(driver, "_get_location_async", new_callable=AsyncMock, return_value=None):
            loc = await driver.location("cat1", "col1")
            assert loc.backend == "iceberg"
            # Defaults: namespace == catalog_id, table == collection_id
            assert loc.identifiers["namespace"] == "cat1"
            assert loc.identifiers["table"] == "col1"


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------


class TestIcebergSnapshots:
    @pytest.mark.asyncio
    async def test_list_snapshots_after_write(self, driver_with_catalog, test_loc, iceberg_table):
        _write_rows(iceberg_table, [
            {"id": "s1", "name": "Snap", "value": 1, "geometry": None},
        ])

        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            snapshots = await driver_with_catalog.list_snapshots("cat1", "col1")
            assert len(snapshots) >= 1
            assert snapshots[0].snapshot_id is not None
            assert snapshots[0].timestamp is not None

    @pytest.mark.asyncio
    async def test_multiple_writes_create_multiple_snapshots(self, driver_with_catalog, test_loc, iceberg_table):
        _write_rows(iceberg_table, [{"id": "w1", "name": "A", "value": 1, "geometry": None}])
        _write_rows(iceberg_table, [{"id": "w2", "name": "B", "value": 2, "geometry": None}])

        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            snapshots = await driver_with_catalog.list_snapshots("cat1", "col1")
            assert len(snapshots) >= 2


# ---------------------------------------------------------------------------
# Time Travel
# ---------------------------------------------------------------------------


class TestIcebergTimeTravel:
    @pytest.mark.asyncio
    async def test_read_at_snapshot(self, driver_with_catalog, test_loc, iceberg_table):
        # Write v1
        _write_rows(iceberg_table, [{"id": "t1", "name": "V1", "value": 1, "geometry": None}])
        snap1_id = str(iceberg_table.current_snapshot().snapshot_id)

        # Write v2
        _write_rows(iceberg_table, [{"id": "t2", "name": "V2", "value": 2, "geometry": None}])

        # Read at snapshot v1 — should only see t1
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            features = [f async for f in driver_with_catalog.read_at_snapshot(
                "cat1", "col1", snap1_id
            )]
            ids = {f.id for f in features}
            assert ids == {"t1"}
            assert "t2" not in ids

    @pytest.mark.asyncio
    async def test_read_at_timestamp(self, driver_with_catalog, test_loc, iceberg_table):
        _write_rows(iceberg_table, [{"id": "ts1", "name": "TS", "value": 1, "geometry": None}])

        now = datetime.now(tz=timezone.utc)
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            features = [f async for f in driver_with_catalog.read_at_timestamp(
                "cat1", "col1", now
            )]
            assert len(features) >= 1


# ---------------------------------------------------------------------------
# Schema Evolution
# ---------------------------------------------------------------------------


class TestIcebergSchemaEvolution:
    @pytest.mark.asyncio
    async def test_add_column(self, driver_with_catalog, test_loc):
        from dynastore.models.otf import SchemaEvolution, SchemaField

        changes = SchemaEvolution(
            add_columns=[SchemaField(name="extra", type="string")],
        )
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            result = await driver_with_catalog.evolve_schema("cat1", "col1", changes)
            field_names = [f.name for f in result.fields]
            assert "extra" in field_names

    @pytest.mark.asyncio
    async def test_rename_column(self, driver_with_catalog, test_loc):
        from dynastore.models.otf import SchemaEvolution

        changes = SchemaEvolution(rename_columns={"name": "full_name"})
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            result = await driver_with_catalog.evolve_schema("cat1", "col1", changes)
            field_names = [f.name for f in result.fields]
            assert "full_name" in field_names
            assert "name" not in field_names

    @pytest.mark.asyncio
    async def test_drop_column(self, driver_with_catalog, test_loc):
        from dynastore.models.otf import SchemaEvolution

        changes = SchemaEvolution(drop_columns=["geometry"])
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            result = await driver_with_catalog.evolve_schema("cat1", "col1", changes)
            field_names = [f.name for f in result.fields]
            assert "geometry" not in field_names

    @pytest.mark.asyncio
    async def test_add_typed_column(self, driver_with_catalog, test_loc):
        from dynastore.models.otf import SchemaEvolution

        changes = SchemaEvolution(
            add_columns=[{"name": "score", "type": "float64"}],
        )
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            result = await driver_with_catalog.evolve_schema("cat1", "col1", changes)
            field_names = [f.name for f in result.fields]
            assert "score" in field_names

    @pytest.mark.asyncio
    async def test_schema_history(self, driver_with_catalog, test_loc):
        from dynastore.models.otf import SchemaEvolution, SchemaField

        changes = SchemaEvolution(
            add_columns=[SchemaField(name="tag", type="string")],
        )
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            await driver_with_catalog.evolve_schema("cat1", "col1", changes)
            history = await driver_with_catalog.get_schema_history("cat1", "col1")
            assert len(history) >= 2  # original + evolved

    @pytest.mark.asyncio
    async def test_evolve_schema_with_dict_input(self, driver_with_catalog, test_loc):
        changes_dict = {
            "add_columns": [{"name": "notes", "type": "string"}],
        }
        with patch.object(driver_with_catalog, "_get_location_async", new_callable=AsyncMock, return_value=test_loc):
            result = await driver_with_catalog.evolve_schema("cat1", "col1", changes_dict)
            field_names = [f.name for f in result.fields]
            assert "notes" in field_names


# ---------------------------------------------------------------------------
# Bbox Spatial Filter (pure function — no catalog needed)
# ---------------------------------------------------------------------------


class TestIcebergBboxFilter:
    def test_point_inside_bbox(self):
        geom = {"type": "Point", "coordinates": [10, 20]}
        assert ItemsIcebergDriver._bbox_matches(geom, [0, 0, 50, 50]) is True

    def test_point_outside_bbox(self):
        geom = {"type": "Point", "coordinates": [100, 100]}
        assert ItemsIcebergDriver._bbox_matches(geom, [0, 0, 50, 50]) is False

    def test_polygon_partially_inside(self):
        geom = {"type": "Polygon", "coordinates": [[[10, 10], [60, 10], [60, 60], [10, 60], [10, 10]]]}
        assert ItemsIcebergDriver._bbox_matches(geom, [0, 0, 50, 50]) is True

    def test_none_geometry(self):
        assert ItemsIcebergDriver._bbox_matches(None, [0, 0, 50, 50]) is False

    def test_empty_bbox(self):
        geom = {"type": "Point", "coordinates": [10, 20]}
        assert ItemsIcebergDriver._bbox_matches(geom, []) is False

    def test_json_string_geometry(self):
        geom = json.dumps({"type": "Point", "coordinates": [10, 20]})
        assert ItemsIcebergDriver._bbox_matches(geom, [0, 0, 50, 50]) is True

    def test_linestring(self):
        geom = {"type": "LineString", "coordinates": [[5, 5], [15, 15]]}
        assert ItemsIcebergDriver._bbox_matches(geom, [0, 0, 10, 10]) is True

    def test_multipoint_none_inside(self):
        geom = {"type": "MultiPoint", "coordinates": [[100, 100], [200, 200]]}
        assert ItemsIcebergDriver._bbox_matches(geom, [0, 0, 50, 50]) is False

    def test_point_on_bbox_edge(self):
        geom = {"type": "Point", "coordinates": [50, 50]}
        assert ItemsIcebergDriver._bbox_matches(geom, [0, 0, 50, 50]) is True


# ---------------------------------------------------------------------------
# Type Resolution (pure function — no catalog needed)
# ---------------------------------------------------------------------------


class TestIcebergTypeResolution:
    def test_string(self):
        assert isinstance(_resolve_iceberg_type("string"), StringType)

    def test_int32(self):
        assert isinstance(_resolve_iceberg_type("int32"), IntegerType)

    def test_int64(self):
        assert isinstance(_resolve_iceberg_type("int64"), LongType)

    def test_float32(self):
        assert isinstance(_resolve_iceberg_type("float32"), FloatType)

    def test_float64(self):
        assert isinstance(_resolve_iceberg_type("float64"), DoubleType)

    def test_boolean(self):
        assert isinstance(_resolve_iceberg_type("boolean"), BooleanType)

    def test_date(self):
        assert isinstance(_resolve_iceberg_type("date"), DateType)

    def test_unknown_defaults_to_string(self):
        assert isinstance(_resolve_iceberg_type("unknown_type"), StringType)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


class TestIcebergLifespan:
    @pytest.mark.asyncio
    async def test_lifespan_clears_catalog(self):
        driver = ItemsIcebergDriver()
        driver._catalog_cache["test_key"] = "something"
        async with driver.lifespan(object()):
            pass
        assert driver._catalog_cache == {}
