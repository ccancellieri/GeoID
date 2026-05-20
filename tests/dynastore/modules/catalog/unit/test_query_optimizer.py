import pytest
from unittest.mock import MagicMock, patch
from dynastore.modules.catalog.query_optimizer import (
    QueryOptimizer,
    QueryRequest,
    FieldSelection,
    FilterCondition,
    SortOrder,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import FieldDefinition, FieldCapability
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
    TargetDimension,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
    AttributeStorageMode,
)
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig


# Mock Sidecar Configs
@pytest.fixture
def mock_col_config():
    # Phase 1.6: ``collection_type`` lives on its own ``CollectionInfo``
    # PluginConfig (collection scope) and is no longer accepted on the
    # PG driver config.
    return ItemsPostgresqlDriverConfig(
        sidecars=[
            GeometriesSidecarConfig(
                sidecar_type="geometries",
                target_srid=4326,
                target_dimension=TargetDimension.FORCE_2D,
                partition_strategy=None,
                partition_resolution=0,
                statistics=None,
            ),
            FeatureAttributeSidecarConfig(
                sidecar_type="attributes",
                storage_mode=AttributeStorageMode.JSONB,
                index_external_id=True,
                expose_geoid=False,
                index_asset_id=True,
                enable_validity=True,
                attribute_schema=None,
                jsonb_column_name="attributes",
                use_hot_updates=True,
                partition_strategy=None,
                partition_attribute=None,
            ),
        ],
    )


# Mock Sidecar Registry
@pytest.fixture
def mock_registry():
    with patch("dynastore.modules.storage.drivers.pg_sidecars.registry.SidecarRegistry") as mock:
        yield mock


def test_optimizer_initialization(mock_col_config, mock_registry):
    # Setup mocks
    mock_geom = MagicMock()
    mock_geom.config.sidecar_id = "geometries"
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geom.geom",
            capabilities=[FieldCapability.SPATIAL],
            data_type="geometry",
        )
    }

    mock_attr = MagicMock()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.get_queryable_fields.return_value = {
        "external_id": FieldDefinition(
            name="external_id",
            sql_expression="sc_attr.external_id",
            capabilities=[FieldCapability.FILTERABLE],
            data_type="text",
        )
    }

    # Use list side_effect which relies on iteration order in optimizer
    mock_registry.get_sidecar.side_effect = [mock_geom, mock_attr]

    optimizer = QueryOptimizer(mock_col_config)
    assert "geom" in optimizer.field_index
    assert "external_id" in optimizer.field_index


class _SidecarLike(MagicMock):
    """MagicMock subclass that carries a class-level ``serves_consumers``.

    ``QueryOptimizer.determine_required_sidecars`` resolves
    ``type(sc).serves_consumers()`` to decide whether a sidecar serves the
    active consumer; bare ``MagicMock`` instances have no such classmethod
    on the type, so the lookup raises. Returning ``None`` here means
    "consumer-agnostic" — matches the production default.
    """
    @classmethod
    def serves_consumers(cls):
        return None


def test_determine_required_sidecars(mock_col_config, mock_registry):
    # Setup mocks
    mock_geom = _SidecarLike()
    mock_geom.config.sidecar_id = "geometries"
    mock_geom.sidecar_id = "geometries"
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geom.geom",
            capabilities=[],
            data_type="geometry",
        )
    }
    mock_geom.get_main_geometry_field.return_value = "geom"

    mock_attr = _SidecarLike()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.sidecar_id = "attributes"
    mock_attr.get_queryable_fields.return_value = {
        "external_id": FieldDefinition(
            name="external_id",
            sql_expression="sc_attr.external_id",
            capabilities=[],
            data_type="text",
        )
    }
    mock_attr.get_main_geometry_field.return_value = None

    # Use a lambda so get_sidecar never exhausts — called many times across init + 3 queries
    mock_registry.get_sidecar.side_effect = (
        lambda sc, lenient=True: mock_geom if getattr(sc, "sidecar_type", "") == "geometries" else mock_attr
    )

    optimizer = QueryOptimizer(mock_col_config)

    # Query requesting only geometry
    req_geom = QueryRequest(
        select=[FieldSelection(field="geom")], raw_where=None, include_total_count=False
    )
    required = optimizer.determine_required_sidecars(req_geom)
    assert len(required) == 1
    assert required[0].sidecar_id == "geometries"

    # Query requesting only attribute (disable require_geometry to test pure field selection)
    req_attr = QueryRequest(
        select=[FieldSelection(field="external_id")],
        raw_where=None,
        include_total_count=False,
    )
    required = optimizer.determine_required_sidecars(req_attr, require_geometry=False)
    assert len(required) == 1
    assert required[0].sidecar_id == "attributes"

    # Query requesting *
    req_all = QueryRequest(
        select=[FieldSelection(field="*")], raw_where=None, include_total_count=False
    )
    required = optimizer.determine_required_sidecars(req_all)
    assert len(required) == 2


def test_build_optimized_query(mock_col_config, mock_registry):
    # Setup mocks
    mock_geom = MagicMock()
    mock_geom.config.sidecar_id = "geometries"
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geom.geom",
            capabilities=[FieldCapability.SPATIAL],
            data_type="geometry",
        )
    }
    mock_geom.get_join_clause.return_value = (
        "LEFT JOIN geom_table sc_geom ON h.geoid = sc_geom.geoid"
    )

    mock_attr = MagicMock()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.get_queryable_fields.return_value = {
        "external_id": FieldDefinition(
            name="external_id",
            sql_expression="sc_attr.external_id",
            capabilities=[FieldCapability.FILTERABLE],
            data_type="text",
        )
    }
    mock_geom.get_join_clause.return_value = (
        "LEFT JOIN geom_table sc_geom ON h.geoid = sc_geom.geoid"
    )

    mock_attr = MagicMock()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.get_queryable_fields.return_value = {
        "external_id": FieldDefinition(
            name="external_id",
            sql_expression="sc_attr.external_id",
            capabilities=[FieldCapability.FILTERABLE],
            data_type="text",
        )
    }
    mock_attr.get_join_clause.return_value = (
        "LEFT JOIN attr_table sc_attr ON h.geoid = sc_attr.geoid"
    )
    mock_attr.supports_aggregation.return_value = True  # Default allow for test
    mock_attr.supports_transformation.return_value = True
    mock_geom.supports_aggregation.return_value = True
    mock_geom.supports_transformation.return_value = True

    mock_geom.get_default_sort.return_value = None
    mock_attr.get_default_sort.return_value = None

    # Use a lambda so get_sidecar never exhausts regardless of how many times it is called
    mock_geom.sidecar_id = "geometries"
    mock_attr.sidecar_id = "attributes"
    mock_registry.get_sidecar.side_effect = (
        lambda sc, lenient=True: mock_geom if getattr(sc, "sidecar_type", "") == "geometries" else mock_attr
    )

    optimizer = QueryOptimizer(mock_col_config)

    # Test built query
    req = QueryRequest(
        select=[FieldSelection(field="geom", alias="geometry")],
        filters=[FilterCondition(field="external_id", operator="=", value="123")],
        raw_where=None,
        include_total_count=False,
    )

    # This calls registry.get_sidecar again for required sidecars
    sql, params = optimizer.build_optimized_query(req, "schema", "table")

    assert "sc_geom.geom as geometry" in sql
    assert 'FROM "schema"."table" h' in sql
    assert "LEFT JOIN geom_table sc_geom" in sql  # Needed for select
    assert "LEFT JOIN attr_table sc_attr" in sql  # Needed for filter
    assert "WHERE" in sql
    assert "sc_attr.external_id = :filter_0" in sql
    assert params["filter_0"] == "123"


def test_read_policy_disables_external_id_as_feature_id(mock_col_config, mock_registry):
    """When ItemsReadPolicy.feature_type.external_id_as_feature_id is False,
    QueryOptimizer must NOT alias the sidecar's external_id as ``id`` —
    feature.id reverts to h.geoid regardless of sidecar capability."""
    from dynastore.modules.storage.read_policy import ItemsReadPolicy
    from dynastore.modules.storage.computed_fields import FeatureType

    mock_geom = MagicMock()
    mock_geom.config.sidecar_id = "geometries"
    mock_geom.sidecar_id = "geometries"
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom", sql_expression="sc_geom.geom",
            capabilities=[FieldCapability.SPATIAL], data_type="geometry",
        )
    }
    mock_geom.get_join_clause.return_value = "LEFT JOIN geom_table sc_geom ON h.geoid = sc_geom.geoid"
    mock_geom.supports_aggregation.return_value = True
    mock_geom.supports_transformation.return_value = True
    mock_geom.get_default_sort.return_value = None
    mock_geom.provides_feature_id = False
    mock_geom.get_main_geometry_field.return_value = "geom"

    mock_attr = MagicMock()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.sidecar_id = "attributes"
    mock_attr.get_queryable_fields.return_value = {
        "external_id": FieldDefinition(
            name="external_id", sql_expression="sc_attr.external_id",
            capabilities=[FieldCapability.FILTERABLE], data_type="text",
        )
    }
    mock_attr.get_join_clause.return_value = "LEFT JOIN attr_table sc_attr ON h.geoid = sc_attr.geoid"
    mock_attr.supports_aggregation.return_value = True
    mock_attr.supports_transformation.return_value = True
    mock_attr.get_default_sort.return_value = None
    mock_attr.provides_feature_id = True
    mock_attr.feature_id_field_name = "external_id"
    mock_attr.get_main_geometry_field.return_value = None

    mock_registry.get_sidecar.side_effect = (
        lambda sc, lenient=True: mock_geom if getattr(sc, "sidecar_type", "") == "geometries" else mock_attr
    )

    # Filter on external_id to force the attributes sidecar to be required,
    # so the feature_id_expr resolution path runs.
    req = QueryRequest(
        select=[FieldSelection(field="geom", alias="geometry")],
        filters=[FilterCondition(field="external_id", operator="=", value="X")],
        raw_where=None, include_total_count=False,
    )

    # Default (no policy) — external_id IS aliased as id
    optimizer = QueryOptimizer(mock_col_config)
    sql_default, _ = optimizer.build_optimized_query(req, "schema", "table")
    assert "COALESCE(sc_attributes.external_id, h.geoid::text) AS id" in sql_default

    # Policy disables — external_id is NOT aliased; falls back to h.geoid
    policy = ItemsReadPolicy(
        feature_type=FeatureType(external_id_as_feature_id=False)
    )
    optimizer_off = QueryOptimizer(mock_col_config, read_policy=policy)
    sql_off, _ = optimizer_off.build_optimized_query(req, "schema", "table")
    assert "COALESCE(sc_attributes.external_id" not in sql_off
    assert "h.geoid AS id" in sql_off


def test_attributes_sidecar_config_drops_external_id_as_feature_id() -> None:
    """Sidecar config must no longer accept ``external_id_as_feature_id`` —
    that knob moved to ItemsReadPolicy.feature_type."""
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        FeatureAttributeSidecarConfig,
    )

    c = FeatureAttributeSidecarConfig()
    assert not hasattr(c, "external_id_as_feature_id")
    assert not hasattr(c, "provides_feature_id")
    # The storage-layout property is preserved (column name is unrelated to
    # the wire-shape decision).
    assert c.feature_id_field_name == "external_id"
