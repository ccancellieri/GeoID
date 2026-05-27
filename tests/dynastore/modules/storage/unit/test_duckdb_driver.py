import pytest
from unittest.mock import AsyncMock, patch

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


# ---------------------------------------------------------------------------
# #1295 — DuckDB consumes materialize_feature_fields for ensure_storage.
# The projection drives the SQLite write-table column list (instead of the
# historical hardcoded 7-column shape); the canonical→DuckDB type map is the
# cross-driver counterpart of CANONICAL_TO_PG_DDL.
# ---------------------------------------------------------------------------
class TestDuckDBCanonicalTypeMap:
    """Every canonical data_type token maps to a DuckDB native type."""

    def test_string_maps_to_varchar(self):
        from dynastore.modules.storage.drivers.duckdb import _canonical_to_duckdb
        assert _canonical_to_duckdb("string") == "VARCHAR"

    def test_integer_bigint_double_numeric(self):
        from dynastore.modules.storage.drivers.duckdb import _canonical_to_duckdb
        assert _canonical_to_duckdb("integer") == "INTEGER"
        assert _canonical_to_duckdb("bigint") == "BIGINT"
        assert _canonical_to_duckdb("double") == "DOUBLE"
        assert _canonical_to_duckdb("numeric") == "DECIMAL"

    def test_boolean_temporal_binary(self):
        from dynastore.modules.storage.drivers.duckdb import _canonical_to_duckdb
        assert _canonical_to_duckdb("boolean") == "BOOLEAN"
        assert _canonical_to_duckdb("date") == "DATE"
        assert _canonical_to_duckdb("time") == "TIME"
        assert _canonical_to_duckdb("timestamp") == "TIMESTAMP"
        assert _canonical_to_duckdb("binary") == "BLOB"

    def test_jsonb_and_uuid_keep_sqlite_affinity(self):
        """JSONB and UUID degrade to VARCHAR — the SQLite write backend has
        no JSON/UUID affinity, so the canonical token is mapped to the
        affinity SQLite stores it as. The parquet read path doesn't care."""
        from dynastore.modules.storage.drivers.duckdb import _canonical_to_duckdb
        assert _canonical_to_duckdb("jsonb") == "VARCHAR"
        assert _canonical_to_duckdb("uuid") == "VARCHAR"

    def test_geometry_falls_back_to_varchar(self):
        """Geometry is owned by the driver's geometry column, never an
        attribute column; if a projected field somehow names a geometry type,
        it stores the GeoJSON serialization (VARCHAR) the rest of the driver
        already uses."""
        from dynastore.modules.storage.drivers.duckdb import _canonical_to_duckdb
        assert _canonical_to_duckdb("geometry") == "VARCHAR"
        assert _canonical_to_duckdb("geometry(Point,4326)") == "VARCHAR"

    def test_unknown_token_tolerant_fallback(self):
        """Unknown / bypassed canonical token degrades to VARCHAR instead
        of raising mid-DDL (the same posture as the PG bridge)."""
        from dynastore.modules.storage.drivers.duckdb import _canonical_to_duckdb
        assert _canonical_to_duckdb("not-a-real-type") == "VARCHAR"
        assert _canonical_to_duckdb("") == "VARCHAR"


class TestDuckDBProjectionToColumns:
    """The driver builds its SQLite column list from the cross-driver
    ``materialize_feature_fields`` projection — not from a private re-derivation."""

    def test_empty_projection_only_scaffold(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
        cols = ItemsDuckdbDriver._build_table_columns({})
        assert cols == [
            ("id", "VARCHAR PRIMARY KEY"),
            ("geometry", "VARCHAR"),
            ("properties", "VARCHAR"),
        ]

    def test_projection_appends_typed_columns(self):
        from dynastore.models.protocols.field_definition import (
            FieldCapability, FieldDefinition,
        )
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver

        projection = {
            "country_code": FieldDefinition(
                name="country_code",
                data_type="string",
                capabilities=[FieldCapability.FILTERABLE],
            ),
            "year": FieldDefinition(
                name="year",
                data_type="integer",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            ),
            "area": FieldDefinition(
                name="area",
                data_type="double",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            ),
        }
        cols = ItemsDuckdbDriver._build_table_columns(projection)
        # Scaffold first, then projection in iteration order.
        names = [c for c, _ in cols]
        assert names == ["id", "geometry", "properties", "country_code", "year", "area"]
        types = {c: t for c, t in cols}
        assert types["country_code"] == "VARCHAR"
        assert types["year"] == "INTEGER"
        assert types["area"] == "DOUBLE"

    def test_projection_skips_scaffold_collisions(self):
        """Projection entries that name a scaffold column are ignored —
        the scaffold wins so the existing row-shape contract holds."""
        from dynastore.models.protocols.field_definition import FieldDefinition
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver

        projection = {
            "id": FieldDefinition(name="id", data_type="integer"),
            "geometry": FieldDefinition(name="geometry", data_type="string"),
            "properties": FieldDefinition(name="properties", data_type="jsonb"),
            "external_id": FieldDefinition(name="external_id", data_type="string"),
        }
        cols = ItemsDuckdbDriver._build_table_columns(projection)
        assert cols == [
            ("id", "VARCHAR PRIMARY KEY"),
            ("geometry", "VARCHAR"),
            ("properties", "VARCHAR"),
            ("external_id", "VARCHAR"),
        ]


class TestDuckDBEnsureStorageProjection:
    """End-to-end: ``ensure_storage`` resolves schema+policy from the configs
    waterfall, projects the field set, and passes the column list into the
    thread-pool ``_ensure_storage_sync``. The legacy 7-column fallback only
    fires when no schema/policy could be resolved."""

    @pytest.mark.asyncio
    async def test_ensure_storage_passes_projected_columns(self):
        from dynastore.models.protocols.field_definition import (
            FieldCapability, FieldDefinition,
        )
        from dynastore.modules.storage.driver_config import (
            ItemsSchema, ItemsWritePolicy,
        )
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver

        driver = ItemsDuckdbDriver()
        loc = ItemsDuckdbDriverConfig(
            format="parquet", path="/data/f.parquet",
            write_path="/tmp/x.db", write_format="sqlite",
        )
        schema = ItemsSchema(fields={
            "country_code": FieldDefinition(
                name="country_code",
                data_type="string",
                capabilities=[FieldCapability.FILTERABLE],
            ),
            "year": FieldDefinition(
                name="year",
                data_type="integer",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            ),
        })
        policy = ItemsWritePolicy(track_asset_id=True)

        captured = {}

        async def fake_run_in_thread(fn, *args, **kwargs):
            captured["fn"] = fn
            captured["args"] = args
            captured["kwargs"] = kwargs
            return None

        with patch.object(
            driver, "_get_location_async", new_callable=AsyncMock, return_value=loc,
        ), patch.object(
            driver, "_resolve_schema_and_policy", new_callable=AsyncMock,
            return_value=(schema, policy),
        ), patch(
            "dynastore.modules.storage.drivers.duckdb.run_in_thread",
            new=fake_run_in_thread,
        ):
            await driver.ensure_storage("cat1", "col1")

        # _ensure_storage_sync is called with the projection-driven column list.
        args = captured["args"]
        # signature: (loc, catalog_id, collection_id, columns, fast_columns)
        columns = args[3]
        assert columns is not None
        names = [c for c, _ in columns]
        # Scaffold + projection — asset_id appears because policy.track_asset_id.
        assert names[:3] == ["id", "geometry", "properties"]
        assert "country_code" in names
        assert "year" in names
        assert "asset_id" in names
        col_types = {c: t for c, t in columns}
        assert col_types["country_code"] == "VARCHAR"
        assert col_types["year"] == "INTEGER"
        assert col_types["asset_id"] == "VARCHAR"

    @pytest.mark.asyncio
    async def test_ensure_storage_no_schema_no_policy_falls_back_to_legacy(self):
        """No schema, no policy → the projection is empty, so columns
        remains ``None`` and ``_ensure_storage_sync`` honours the legacy
        7-column scaffold (compatibility with catalogs created pre-#1295)."""
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver

        driver = ItemsDuckdbDriver()
        loc = ItemsDuckdbDriverConfig(
            format="parquet", path="/data/f.parquet",
            write_path="/tmp/x.db", write_format="sqlite",
        )

        captured = {}

        async def fake_run_in_thread(fn, *args, **kwargs):
            captured["args"] = args
            return None

        with patch.object(
            driver, "_get_location_async", new_callable=AsyncMock, return_value=loc,
        ), patch.object(
            driver, "_resolve_schema_and_policy", new_callable=AsyncMock,
            return_value=(None, None),
        ), patch(
            "dynastore.modules.storage.drivers.duckdb.run_in_thread",
            new=fake_run_in_thread,
        ):
            await driver.ensure_storage("cat1", "col1")

        # columns argument is None — sync helper picks the legacy scaffold.
        assert captured["args"][3] is None
        assert captured["args"][4] is None  # fast_columns

    @pytest.mark.asyncio
    async def test_ensure_storage_collects_fast_fields(self):
        """``FieldAccess.FAST`` fields are surfaced in ``fast_columns`` so
        the SQLite path can build indexes (and a future parquet writer can
        emit bloom filters / row-group stats)."""
        from dynastore.models.protocols.field_definition import (
            FieldAccess, FieldCapability, FieldDefinition,
        )
        from dynastore.modules.storage.driver_config import ItemsSchema
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver

        driver = ItemsDuckdbDriver()
        loc = ItemsDuckdbDriverConfig(
            format="parquet", path="/data/f.parquet",
            write_path="/tmp/x.db", write_format="sqlite",
        )
        schema = ItemsSchema(fields={
            "fast_field": FieldDefinition(
                name="fast_field",
                data_type="string",
                capabilities=[FieldCapability.FILTERABLE],
                access=FieldAccess.FAST,
            ),
            "auto_field": FieldDefinition(
                name="auto_field",
                data_type="string",
                capabilities=[FieldCapability.FILTERABLE],
            ),
        })

        captured = {}

        async def fake_run_in_thread(fn, *args, **kwargs):
            captured["args"] = args
            return None

        with patch.object(
            driver, "_get_location_async", new_callable=AsyncMock, return_value=loc,
        ), patch.object(
            driver, "_resolve_schema_and_policy", new_callable=AsyncMock,
            return_value=(schema, None),
        ), patch(
            "dynastore.modules.storage.drivers.duckdb.run_in_thread",
            new=fake_run_in_thread,
        ):
            await driver.ensure_storage("cat1", "col1")

        fast_columns = captured["args"][4]
        assert fast_columns is not None
        assert "fast_field" in fast_columns
        assert "auto_field" not in fast_columns
