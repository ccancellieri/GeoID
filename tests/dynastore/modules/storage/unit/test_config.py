import pytest
from pydantic import ValidationError

from dynastore.modules.catalog.catalog_config import (
    CollectionPluginConfig,
    CollectionTypeEnum,
    CompositePartitionConfig,
)
from dynastore.modules.storage.driver_config import (
    ItemsDuckdbDriverConfig,
    ItemsIcebergDriverConfig,
    ItemsPostgresqlDriverConfig,
    DuckDBConfig,
    IcebergConfig,
)
from dynastore.modules.storage.routing_config import (
    ItemsRoutingConfig,
    AssetRoutingConfig,
    CollectionRoutingConfig,
    Operation,
    OperationDriverEntry,
    FailurePolicy,
    WriteMode,
)


# ---------------------------------------------------------------------------
# CollectionPluginConfig (structural only — sidecars/partitioning moved to PG config)
# ---------------------------------------------------------------------------


class TestCollectionPluginConfigDefaults:
    def test_class_key(self):
        assert CollectionPluginConfig.class_key() == "collection_plugin_config"

    def test_extra_fields_allowed(self):
        cfg = CollectionPluginConfig(custom_field="value")
        assert cfg.custom_field == "value"


# ---------------------------------------------------------------------------
# ItemsPostgresqlDriverConfig (sidecars, partitioning, collection_type)
# ---------------------------------------------------------------------------


class TestItemsPostgresqlDriverConfigDefaults:
    def test_class_key(self):
        # Wire key is the bound TypedDriver class name, not the *Config name.
        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver  # noqa: F401
        assert ItemsPostgresqlDriverConfig.class_key() == "items_postgresql_driver"

    def test_default_sidecars_is_empty(self):
        """Default is empty as of M1b.2.

        The old eager ``[geometries, attributes]`` default was the
        anti-pattern the role-based driver refactor evicts: every
        default-body POST /collections used to persist those two
        sidecar entries via ``configs.set_config``.  In M1b.2 the PG
        driver resolves sidecar defaults lazily at DDL / read / write
        time via ``pg_sidecars._effective_sidecars(...)`` — so a
        default-body config persists zero ``collection_configs`` rows
        (plan §Principle — default-fast invariant), and the first
        real DDL/ingest call materialises ``[geometries, attributes]``
        (+ registry injections like ``item_metadata``) on demand.
        """
        cfg = ItemsPostgresqlDriverConfig()
        assert cfg.sidecars == []

    def test_effective_sidecars_vector_default(self):
        """Empty VECTOR config resolves to [geometries, attributes]+injections."""
        from dynastore.modules.storage.drivers.pg_sidecars import (
            _effective_sidecars,
            FeatureAttributeSidecarConfig,
            GeometriesSidecarConfig,
        )
        cfg = ItemsPostgresqlDriverConfig()
        resolved = _effective_sidecars(cfg, catalog_id="cat", collection_id="col")
        types = [s.sidecar_type for s in resolved]
        assert "geometries" in types
        assert "attributes" in types
        assert any(isinstance(s, GeometriesSidecarConfig) for s in resolved)
        assert any(isinstance(s, FeatureAttributeSidecarConfig) for s in resolved)

    def test_effective_sidecars_records_drops_geometry(self):
        """RECORDS collections have no spatial component → geometry omitted.

        Phase 1.6: ``collection_type`` is no longer on
        ``ItemsPostgresqlDriverConfig`` — it lives on the new
        ``CollectionType`` PluginConfig at collection scope.  The
        resolver receives ``collection_type`` as a kw arg from its
        async callers (after they fetch ``CollectionType``).
        """
        from dynastore.modules.storage.drivers.pg_sidecars import _effective_sidecars
        cfg = ItemsPostgresqlDriverConfig()
        resolved = _effective_sidecars(
            cfg, catalog_id="cat", collection_id="col",
            collection_type="RECORDS",
        )
        types = [s.sidecar_type for s in resolved]
        assert "geometries" not in types
        assert "attributes" in types

    def test_effective_sidecars_explicit_config_preserved_and_supplemented(self):
        """Caller-explicit sidecars are preserved; registry fills missing types.

        Matches the pre-M1b.2 inline-injection semantics in
        collection_service.py: the registry is additive over whatever
        the caller supplied.  A caller that passes ``[attributes]`` for
        a VECTOR collection still gets ``geometries`` appended by the
        registry (since VECTOR collections always need it), mirroring
        the old behaviour where the eager default was
        ``[geometries, attributes]``.

        A caller who wants ONLY attributes (no geometry) uses a
        non-VECTOR ``collection_type`` — see
        ``test_effective_sidecars_records_drops_geometry``.
        """
        from dynastore.modules.storage.drivers.pg_sidecars import (
            _effective_sidecars,
            FeatureAttributeSidecarConfig,
        )
        cfg = ItemsPostgresqlDriverConfig(
            sidecars=[FeatureAttributeSidecarConfig()],
        )
        resolved = _effective_sidecars(cfg, catalog_id="cat", collection_id="col")
        types = [s.sidecar_type for s in resolved]
        # attributes is explicit → preserved; geometries is registry-supplemented.
        assert "attributes" in types
        assert "geometries" in types
        # Explicit entry appears before registry-supplemented ones.
        assert types.index("attributes") < types.index("geometries")

    def test_default_partitioning_disabled(self):
        cfg = ItemsPostgresqlDriverConfig()
        assert cfg.partitioning.enabled is False

    def test_collection_type_field_hoisted_off(self):
        """Phase 1.6: ``collection_type`` was hoisted out of the PG driver
        config into a standalone ``CollectionType`` PluginConfig at
        collection scope.  The field is no longer part of this class's
        ``model_fields``.  See ``test_collection_type.py`` for the new
        regression suite.
        """
        assert "collection_type" not in ItemsPostgresqlDriverConfig.model_fields

    def test_column_definitions(self):
        cfg = ItemsPostgresqlDriverConfig()
        cols = cfg.get_column_definitions()
        assert "geoid" in cols
        assert "transaction_time" in cols


class TestSidecarConfigDiscriminatorRoundTrip:
    """Round-trip tests for the M1b.1 discriminator-retention fix.

    Before M1b.1, ``SidecarConfig.sidecar_type`` was declared in the base
    class with no default; concrete subclasses supplied a
    ``Literal[...] = "..."`` default.  Pydantic treated that subclass
    default as not-explicitly-set on a default-constructed instance, so
    ``model_dump(exclude_unset=True)`` dropped the discriminator — making
    a round-trip through the ``Annotated[Union[...], Discriminator(...)]``
    on ``ItemsPostgresqlDriverConfig.sidecars`` fail.

    The fix is a ``@model_validator(mode="after")`` on ``SidecarConfig``
    that adds ``"sidecar_type"`` to ``__pydantic_fields_set__`` on every
    instance, regardless of how it was constructed.
    """

    def test_default_constructed_sidecar_dumps_keep_sidecar_type(self):
        from dynastore.modules.storage.drivers.pg_sidecars import (
            GeometriesSidecarConfig,
            FeatureAttributeSidecarConfig,
            ItemMetadataSidecarConfig,
        )
        for cls, expected in [
            (GeometriesSidecarConfig, "geometries"),
            (FeatureAttributeSidecarConfig, "attributes"),
            (ItemMetadataSidecarConfig, "item_metadata"),
        ]:
            dumped = cls().model_dump(exclude_unset=True)
            assert dumped.get("sidecar_type") == expected, (
                f"{cls.__name__} lost its sidecar_type under exclude_unset=True"
            )

    def test_round_trip_exclude_unset_preserves_discriminator(self):
        """model_dump(exclude_unset=True) → model_validate round-trips cleanly."""
        from dynastore.modules.storage.drivers.pg_sidecars import (
            GeometriesSidecarConfig,
            FeatureAttributeSidecarConfig,
            ItemMetadataSidecarConfig,
        )
        for cls in (GeometriesSidecarConfig, FeatureAttributeSidecarConfig, ItemMetadataSidecarConfig):
            original = cls()
            dumped = original.model_dump(exclude_unset=True)
            reloaded = cls.model_validate(dumped)
            assert reloaded.sidecar_type == original.sidecar_type
            assert type(reloaded) is cls

    def test_discriminated_union_dispatches_to_correct_subclass(self):
        """Explicit-sidecars config dumps + reloads with discriminator-based dispatch.

        Default ``ItemsPostgresqlDriverConfig()`` has empty sidecars
        post-M1b.2 (see ``test_default_sidecars_is_empty``), so we supply
        explicit sidecars here to exercise the
        ``Annotated[Union[...], Discriminator("sidecar_type")]`` dispatch
        on a realistic persisted payload.
        """
        from dynastore.modules.storage.drivers.pg_sidecars import (
            GeometriesSidecarConfig,
            FeatureAttributeSidecarConfig,
            ItemMetadataSidecarConfig,
        )
        cfg = ItemsPostgresqlDriverConfig(
            sidecars=[
                GeometriesSidecarConfig(),
                FeatureAttributeSidecarConfig(),
                ItemMetadataSidecarConfig(),
            ],
        )
        dumped = cfg.model_dump(exclude_unset=False)  # full dump — simulates DB persistence
        reloaded = ItemsPostgresqlDriverConfig.model_validate(dumped)
        types_out = [s.sidecar_type for s in reloaded.sidecars]
        assert "geometries" in types_out
        assert "attributes" in types_out
        assert "item_metadata" in types_out
        # Every reloaded entry is the specialised subclass (Union dispatched on sidecar_type)
        subclass_map = {s.sidecar_type: type(s) for s in reloaded.sidecars}
        assert subclass_map.get("geometries") is GeometriesSidecarConfig
        assert subclass_map.get("attributes") is FeatureAttributeSidecarConfig
        assert subclass_map.get("item_metadata") is ItemMetadataSidecarConfig


class TestCompositePartitionConfig:
    def test_disabled_no_keys(self):
        cfg = CompositePartitionConfig()
        assert cfg.enabled is False
        assert cfg.partition_keys == []

    def test_enabled_requires_keys(self):
        with pytest.raises(ValidationError):
            CompositePartitionConfig(enabled=True, partition_keys=[])

    def test_enabled_with_keys(self):
        cfg = CompositePartitionConfig(enabled=True, partition_keys=["asset_id"])
        assert cfg.partition_keys == ["asset_id"]


# ---------------------------------------------------------------------------
# ItemsRoutingConfig
# ---------------------------------------------------------------------------


class TestCollectionMetadataRouting:
    def test_class_key(self):
        assert ItemsRoutingConfig.class_key() == "items_routing_config"

    def test_defaults(self):
        cfg = ItemsRoutingConfig()
        assert Operation.WRITE in cfg.operations
        assert Operation.READ in cfg.operations
        assert cfg.operations[Operation.WRITE][0].driver_id == "items_postgresql_driver"

    def test_custom_operations(self):
        cfg = ItemsRoutingConfig(operations={
            Operation.WRITE: [OperationDriverEntry(driver_id="items_postgresql_driver")],
            Operation.READ: [OperationDriverEntry(driver_id="items_elasticsearch_driver", hints={"search"})],
            Operation.SEARCH: [OperationDriverEntry(driver_id="items_elasticsearch_driver", hints={"search"})],
        })
        assert len(cfg.operations) == 3
        assert cfg.operations[Operation.SEARCH][0].driver_id == "items_elasticsearch_driver"

    def test_failure_policy(self):
        entry = OperationDriverEntry(driver_id="es", on_failure=FailurePolicy.WARN)
        assert entry.on_failure == FailurePolicy.WARN

    def test_default_failure_policy_is_fatal(self):
        entry = OperationDriverEntry(driver_id="pg")
        assert entry.on_failure == FailurePolicy.FATAL

    def test_pascal_case_driver_id_normalized_to_snake_case(self):
        """PR-1e: OperationDriverEntry.driver_id field_validator coerces every
        input through ``_to_snake``. Legacy PascalCase values from auto-augment
        helpers and persisted configs predating the cutover are read-side
        normalised so dispatcher lookups against ``DriverRegistry`` (also
        snake_case) hit instead of silently missing.
        """
        e = OperationDriverEntry(driver_id="ItemsPostgresqlDriver")
        assert e.driver_id == "items_postgresql_driver"
        # Idempotent on already-snake_case input
        e2 = OperationDriverEntry(driver_id="items_postgresql_driver")
        assert e2.driver_id == "items_postgresql_driver"

    def test_es_primary_in_read_default_with_geometry_hints(self):
        """Default routing puts ES (public) at READ position 0 with the
        ``geometry_simplified`` hint; PG follows with ``geometry_exact``.
        WRITE is PG-only — ES indexing is async via the dispatcher feeding
        ``operations[INDEX]`` (auto-augmented with ES); see
        ``feedback_es_indexing_per_item_async_not_bulk.md``.
        """
        cfg = ItemsRoutingConfig()
        write = cfg.operations[Operation.WRITE]
        read = cfg.operations[Operation.READ]
        assert [e.driver_id for e in write] == ["items_postgresql_driver"]
        assert [e.on_failure for e in write] == [FailurePolicy.FATAL]
        assert [e.driver_id for e in read] == [
            "items_elasticsearch_driver", "items_postgresql_driver",
        ]
        assert read[0].hints == {"geometry_simplified"}
        assert read[1].hints == {"geometry_exact"}


class TestOperationEnum:
    def test_transform_exists(self):
        assert Operation.TRANSFORM == "TRANSFORM"

    def test_all_operations(self):
        ops = {Operation.WRITE, Operation.READ, Operation.SEARCH, Operation.TRANSFORM}
        assert len(ops) == 4


class TestWriteMode:
    def test_composition_modes_exist(self):
        assert WriteMode.FIRST == "first"
        assert WriteMode.FAN_OUT == "fan_out"
        assert WriteMode.CHAIN == "chain"

    def test_execution_modes_still_exist(self):
        assert WriteMode.SYNC == "sync"
        assert WriteMode.ASYNC == "async"


class TestMetadataRoutingConfig:
    def test_default_operations_empty(self):
        cfg = CollectionRoutingConfig()
        # After Phase 2c, the Searcher auto-self-registration validator may
        # seed Operation.SEARCH on construction once a Searcher has registered
        # itself. The invariant that still holds: no WRITE/READ/TRANSFORM
        # entries are seeded by default — those remain operator-driven.
        for op in (Operation.WRITE, Operation.READ, Operation.TRANSFORM):
            assert op not in cfg.operations, (
                f"{op} unexpectedly present in default operations: {cfg.operations}"
            )

    def test_read_operation(self):
        cfg = CollectionRoutingConfig(operations={
            Operation.READ: [OperationDriverEntry(
                driver_id="collection_elasticsearch_driver",
                write_mode=WriteMode.FIRST,
            )],
        })
        assert len(cfg.operations[Operation.READ]) == 1
        assert cfg.operations[Operation.READ][0].driver_id == "collection_elasticsearch_driver"

    def test_transform_operation(self):
        cfg = CollectionRoutingConfig(operations={
            Operation.TRANSFORM: [OperationDriverEntry(
                driver_id="items_iceberg_driver",
                write_mode=WriteMode.CHAIN,
                on_failure=FailurePolicy.WARN,
            )],
        })
        entry = cfg.operations[Operation.TRANSFORM][0]
        assert entry.driver_id == "items_iceberg_driver"
        assert entry.write_mode == WriteMode.CHAIN
        assert entry.on_failure == FailurePolicy.WARN

    def test_read_and_transform_together(self):
        cfg = CollectionRoutingConfig(operations={
            Operation.READ: [OperationDriverEntry(driver_id="collection_postgresql_driver")],
            Operation.TRANSFORM: [OperationDriverEntry(driver_id="items_iceberg_driver")],
        })
        assert Operation.READ in cfg.operations
        assert Operation.TRANSFORM in cfg.operations

class TestAssetRoutingConfig:
    def test_class_key(self):
        assert AssetRoutingConfig.class_key() == "asset_routing_config"

    def test_defaults(self):
        cfg = AssetRoutingConfig()
        assert Operation.WRITE in cfg.operations
        assert cfg.operations[Operation.WRITE][0].driver_id == "asset_postgresql_driver"


# ---------------------------------------------------------------------------
# M4 — env vs per-collection config split
# ---------------------------------------------------------------------------


class TestIcebergConfigEnvLevel:
    """IcebergConfig is an env-level singleton — connection fields live here."""

    def test_has_catalog_name(self):
        assert hasattr(IcebergConfig, "catalog_name")

    def test_has_catalog_type(self):
        assert hasattr(IcebergConfig, "catalog_type")
        assert IcebergConfig.catalog_type == "sql"

    def test_has_warehouse_uri(self):
        assert hasattr(IcebergConfig, "warehouse_uri")

    def test_has_catalog_uri(self):
        assert hasattr(IcebergConfig, "catalog_uri")

    def test_has_warehouse_scheme(self):
        assert hasattr(IcebergConfig, "warehouse_scheme")


class TestItemsIcebergDriverConfigValidator:
    """ItemsIcebergDriverConfig rejects connection-level fields."""

    def test_valid_construction_no_fields(self):
        cfg = ItemsIcebergDriverConfig()
        assert cfg.namespace is None
        assert cfg.table_name is None

    def test_valid_construction_table_fields(self):
        cfg = ItemsIcebergDriverConfig(
            namespace="my_ns",
            table_name="my_table",
            table_properties={"write.format.default": "parquet"},
        )
        assert cfg.namespace == "my_ns"
        assert cfg.table_name == "my_table"
        assert cfg.table_properties == {"write.format.default": "parquet"}

    def test_valid_construction_partition_spec(self):
        spec = [{"name": "year", "transform": "year", "source": "event_date"}]
        cfg = ItemsIcebergDriverConfig(partition_spec=spec)
        assert cfg.partition_spec == spec

    def test_valid_construction_sort_order(self):
        order = [{"name": "id", "direction": "asc", "null_order": "nulls-last"}]
        cfg = ItemsIcebergDriverConfig(sort_order=order)
        assert cfg.sort_order == order

    def test_rejects_catalog_name(self):
        with pytest.raises(ValidationError, match="catalog_name"):
            ItemsIcebergDriverConfig(catalog_name="glue")

    def test_rejects_catalog_uri(self):
        with pytest.raises(ValidationError, match="catalog_uri"):
            ItemsIcebergDriverConfig(catalog_uri="http://catalog:8181")

    def test_rejects_catalog_type(self):
        with pytest.raises(ValidationError, match="catalog_type"):
            ItemsIcebergDriverConfig(catalog_type="rest")

    def test_rejects_catalog_properties(self):
        with pytest.raises(ValidationError, match="catalog_properties"):
            ItemsIcebergDriverConfig(catalog_properties={"key": "value"})

    def test_rejects_warehouse_uri(self):
        with pytest.raises(ValidationError, match="warehouse_uri"):
            ItemsIcebergDriverConfig(warehouse_uri="gs://my-bucket/iceberg/")

    def test_rejects_warehouse_scheme(self):
        with pytest.raises(ValidationError, match="warehouse_scheme"):
            ItemsIcebergDriverConfig(warehouse_scheme="gs")

    def test_rejects_multiple_connection_fields(self):
        with pytest.raises(ValidationError):
            ItemsIcebergDriverConfig(
                catalog_name="glue",
                warehouse_uri="s3://bucket/wh",
            )

    def test_class_key(self):
        from dynastore.modules.storage.drivers.iceberg import ItemsIcebergDriver  # noqa: F401
        assert ItemsIcebergDriverConfig.class_key() == "items_iceberg_driver"


class TestDuckDBConfigEnvLevel:
    """DuckDBConfig has data_root for locating per-collection relative paths."""

    def test_has_data_root(self):
        assert hasattr(DuckDBConfig, "data_root")

    def test_data_root_is_string(self):
        assert isinstance(DuckDBConfig.data_root, str)


class TestItemsDuckdbDriverConfigDefaults:
    def test_class_key(self):
        from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver  # noqa: F401
        assert ItemsDuckdbDriverConfig.class_key() == "items_duckdb_driver"

    def test_default_format(self):
        cfg = ItemsDuckdbDriverConfig()
        assert cfg.format == "parquet"

    def test_path_fields_default_none(self):
        cfg = ItemsDuckdbDriverConfig()
        assert cfg.path is None
        assert cfg.write_path is None
