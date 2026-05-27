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
                index_asset_id=True,
                validity_column="valid_from",
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
            data_type="string",
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
            data_type="string",
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
            data_type="string",
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
            data_type="string",
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
    """By default (and when external_id_as_feature_id is False) QueryOptimizer
    must NOT alias the sidecar's external_id as ``id`` — feature.id is h.geoid
    regardless of sidecar capability; the external_id alias appears only when a
    collection opts in (#1212)."""
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
            capabilities=[FieldCapability.FILTERABLE], data_type="string",
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

    # Default (no policy) — geoid IS the id; external_id is NOT aliased (#1212)
    optimizer = QueryOptimizer(mock_col_config)
    sql_default, _ = optimizer.build_optimized_query(req, "schema", "table")
    assert "COALESCE(sc_attributes.external_id" not in sql_default
    assert "h.geoid AS id" in sql_default

    # Policy opts in — external_id IS aliased as id
    policy = ItemsReadPolicy(
        feature_type=FeatureType(external_id_as_feature_id=True)
    )
    optimizer_on = QueryOptimizer(mock_col_config, read_policy=policy)
    sql_on, _ = optimizer_on.build_optimized_query(req, "schema", "table")
    assert "COALESCE(sc_attributes.external_id, h.geoid::text) AS id" in sql_on


def test_map_row_to_feature_forwards_read_policy(mock_col_config) -> None:
    """The optimizer must thread its resolved ``read_policy`` into the items
    service so the row mapper can honour ``feature_type.expose`` /
    ``external_id_as_feature_id``. Without this the wire-shape contract is
    silently dropped at read time."""
    from dynastore.modules.storage.read_policy import ItemsReadPolicy
    from dynastore.modules.storage.computed_fields import FeatureType

    policy = ItemsReadPolicy(feature_type=FeatureType(expose=["area"]))
    optimizer = QueryOptimizer(mock_col_config, read_policy=policy)

    captured = {}

    class _FakeItems:
        def map_row_to_feature(self, row, col_config, lang="en", read_policy=None):
            captured["read_policy"] = read_policy
            captured["lang"] = lang
            return MagicMock()

    with patch(
        "dynastore.modules.catalog.query_optimizer.get_protocol",
        return_value=_FakeItems(),
    ):
        optimizer.map_row_to_feature({"geoid": "g1"}, mock_col_config, lang="fr")

    assert captured["read_policy"] is policy
    assert captured["lang"] == "fr"


def test_validate_query_whitelists_geoid_select_when_columnar(mock_col_config, mock_registry):
    """Selecting the hub ``geoid`` field must validate regardless of how the
    attributes sidecar resolves. In COLUMNAR mode the dynamic JSONB fallback
    returns ``None`` for ``geoid`` — without the SELECT-loop hub whitelist that
    surfaced as ``Unknown field: geoid`` and dropped the whole collection from
    the tile (empty MVT). The other validation loops (filter/sort/group_by)
    already whitelist the hub fields; SELECT must mirror them."""
    from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
        FeatureAttributeSidecar,
    )
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeSchemaEntry,
        FeatureAttributeSidecarConfig,
        PostgresType,
    )

    mock_geom = MagicMock()
    mock_geom.config.sidecar_id = "geometries"
    mock_geom.sidecar_id = "geometries"
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geom.geom",
            capabilities=[FieldCapability.SPATIAL],
            data_type="geometry",
        )
    }
    # The geometries sidecar does not resolve ``geoid`` either — make the
    # dynamic fallback honestly return None (a bare MagicMock would return a
    # truthy mock and mask the bug).
    mock_geom.get_dynamic_field_definition.return_value = None

    # Real attributes sidecar in COLUMNAR mode (attribute_schema present), so
    # ``get_dynamic_field_definition('geoid')`` returns None for the dynamic
    # fallback — exactly the runtime condition that triggered the empty tile.
    attr_sidecar = FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(
            attribute_schema=[
                AttributeSchemaEntry(name="area", type=PostgresType.INTEGER),
            ],
        )
    )
    assert (
        attr_sidecar.get_dynamic_field_definition("geoid") is None
    ), "precondition: COLUMNAR dynamic fallback yields None for geoid"

    mock_registry.get_sidecar.side_effect = (
        lambda sc, lenient=True: mock_geom
        if getattr(sc, "sidecar_type", "") == "geometries"
        else attr_sidecar
    )

    optimizer = QueryOptimizer(mock_col_config)

    req = QueryRequest(
        select=[FieldSelection(field="geoid", alias="id")],
        raw_where=None,
        include_total_count=False,
    )
    errors = optimizer.validate_query(req)
    assert "Unknown field: geoid" not in errors


def test_raw_where_resolved_columnar_expr_not_double_qualified(mock_col_config, mock_registry):
    """Regression for #1255: a CQL filter on a COLUMNAR column compiles
    straight to its resolved ``sql_expression`` (the quoted form
    ``sc_attributes."adm2_pcode"``) before reaching ``build_optimized_query``.

    The ``raw_where`` field-mapping loop must NOT re-substitute the bare field
    name *inside* that already-resolved quoted identifier. The leading ``"``
    is neither ``.`` nor a word char, so the ``(?<![.\\w])`` lookbehind failed
    to skip it and the name matched again, producing the invalid
    ``sc_attributes."sc_attributes."adm2_pcode""`` and a Postgres syntax error.
    """
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

    mock_attr = MagicMock()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.sidecar_id = "attributes"
    # COLUMNAR columns resolve to the quoted form ``sc_attributes."<name>"``.
    mock_attr.get_queryable_fields.return_value = {
        "adm2_pcode": FieldDefinition(
            name="adm2_pcode",
            sql_expression='sc_attributes."adm2_pcode"',
            capabilities=[FieldCapability.FILTERABLE],
            data_type="string",
        )
    }
    mock_attr.get_join_clause.return_value = (
        "LEFT JOIN attr_table sc_attributes ON h.geoid = sc_attributes.geoid"
    )
    mock_attr.supports_aggregation.return_value = True
    mock_attr.supports_transformation.return_value = True
    mock_attr.get_default_sort.return_value = None

    mock_registry.get_sidecar.side_effect = (
        lambda sc, lenient=True: mock_geom if getattr(sc, "sidecar_type", "") == "geometries" else mock_attr
    )

    optimizer = QueryOptimizer(mock_col_config)

    # The CQL filter has ALREADY compiled to the resolved expression.
    req = QueryRequest(
        select=[FieldSelection(field="geom", alias="geometry")],
        raw_where='sc_attributes."adm2_pcode" = :cqlp_0',
        raw_params={"cqlp_0": "PK001"},
        include_total_count=False,
    )
    sql, params = optimizer.build_optimized_query(req, "schema", "table")

    assert 'sc_attributes."sc_attributes."adm2_pcode""' not in sql
    assert 'sc_attributes."adm2_pcode" = :cqlp_0' in sql
    assert params["cqlp_0"] == "PK001"


def test_raw_where_bare_columnar_token_still_substituted(mock_col_config, mock_registry):
    """The idempotency guard must not regress the legitimate case: a *bare*
    field token in ``raw_where`` (resolved expression not yet present) must
    still be mapped to its quoted COLUMNAR ``sql_expression``."""
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

    mock_attr = MagicMock()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.sidecar_id = "attributes"
    mock_attr.get_queryable_fields.return_value = {
        "adm2_pcode": FieldDefinition(
            name="adm2_pcode",
            sql_expression='sc_attributes."adm2_pcode"',
            capabilities=[FieldCapability.FILTERABLE],
            data_type="string",
        )
    }
    mock_attr.get_join_clause.return_value = (
        "LEFT JOIN attr_table sc_attributes ON h.geoid = sc_attributes.geoid"
    )
    mock_attr.supports_aggregation.return_value = True
    mock_attr.supports_transformation.return_value = True
    mock_attr.get_default_sort.return_value = None

    mock_registry.get_sidecar.side_effect = (
        lambda sc, lenient=True: mock_geom if getattr(sc, "sidecar_type", "") == "geometries" else mock_attr
    )

    optimizer = QueryOptimizer(mock_col_config)

    req = QueryRequest(
        select=[FieldSelection(field="geom", alias="geometry")],
        raw_where="adm2_pcode = :cqlp_0",
        raw_params={"cqlp_0": "PK001"},
        include_total_count=False,
    )
    sql, _ = optimizer.build_optimized_query(req, "schema", "table")

    assert 'sc_attributes."adm2_pcode" = :cqlp_0' in sql
    assert 'sc_attributes."sc_attributes."adm2_pcode""' not in sql


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


def test_star_expansion_skips_sidecar_field_when_explicit_override_present(
    mock_col_config, mock_registry
):
    """When query.select contains an explicit FieldSelection(field='geom', …) PLUS
    FieldSelection(field='*'), the * expansion must NOT include the sidecar's
    own geom projection — that would produce two ``AS geom`` aliases and a
    PostgreSQL syntax error.

    The optimizer detects the collision by comparing the logical alias extracted
    from the sidecar SQL string against the set of explicitly overridden field
    names.
    """
    # _serves_active_consumer calls type(sc).serves_consumers() — a classmethod
    # that plain MagicMock does not carry.  Create a thin MagicMock subclass
    # that adds the classmethod on the class itself, so type(instance) resolves it.
    class _SidecarMock(MagicMock):
        @classmethod
        def serves_consumers(cls):
            return None  # consumer-agnostic (same as the base SidecarProtocol default)

    mock_geom = _SidecarMock()
    mock_geom.config.sidecar_id = "geometries"
    mock_geom.sidecar_id = "geometries"
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geometries.geom",
            capabilities=[FieldCapability.SPATIAL],
            data_type="geometry(Geometry,4326)",
        )
    }
    mock_geom.get_join_clause.return_value = (
        'LEFT JOIN "schema"."table_geometries" sc_geometries '
        "ON h.geoid = sc_geometries.geoid"
    )
    mock_geom.supports_aggregation.return_value = True
    mock_geom.supports_transformation.return_value = True
    mock_geom.get_default_sort.return_value = None
    mock_geom.get_main_geometry_field.return_value = "geom"
    # Sidecar returns its own SQL string for geom — note the "AS geom" alias.
    mock_geom.get_select_fields.return_value = [
        "ST_AsGeoJSON(sc_geometries.geom)::jsonb as geom"
    ]

    mock_attr = _SidecarMock()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.sidecar_id = "attributes"
    mock_attr.get_queryable_fields.return_value = {}
    mock_attr.get_join_clause.return_value = (
        'LEFT JOIN "schema"."table_attributes" sc_attributes '
        "ON h.geoid = sc_attributes.geoid"
    )
    mock_attr.supports_aggregation.return_value = True
    mock_attr.supports_transformation.return_value = True
    mock_attr.get_default_sort.return_value = None
    mock_attr.get_main_geometry_field.return_value = None
    mock_attr.get_select_fields.return_value = []

    mock_registry.get_sidecar.side_effect = (
        lambda sc, lenient=True: mock_geom
        if getattr(sc, "sidecar_type", "") == "geometries"
        else mock_attr
    )

    optimizer = QueryOptimizer(mock_col_config)

    # Simulate what stream_features emits when target_srid is set:
    # explicit ST_Transform(geom) + wildcard *
    req = QueryRequest(
        select=[
            FieldSelection(
                field="geom",
                transformation="ST_Transform",
                transform_args={"srid": 3857},
            ),
            FieldSelection(field="*"),
        ],
        raw_where=None,
        include_total_count=False,
    )

    sql, _ = optimizer.build_optimized_query(req, "schema", "table")

    # The explicit ST_Transform expression must appear exactly once.
    # The optimizer renders lowercase "as" aliases.
    sql_lower = sql.lower()
    geom_occurrences = sql_lower.count(" as geom")
    assert geom_occurrences == 1, (
        f"Expected exactly 1 'as geom' in SQL, found {geom_occurrences}.\nSQL:\n{sql}"
    )
    # Verify it is the ST_Transform variant, not the raw sidecar projection.
    assert "st_transform" in sql_lower, f"Expected ST_Transform in SQL:\n{sql}"


def test_star_expansion_skips_sidecar_field_when_raw_select_override_present(
    mock_col_config, mock_registry
):
    """MVT-path regression: when the caller injects an MVT geom projection via
    ``query.raw_selects`` (``ST_AsMVTGeom(...) AS geom``) together with
    ``FieldSelection(field="*")``, the * expansion must NOT also emit the
    sidecar's own ``... AS geom`` — that would produce two ``AS geom`` aliases
    in the inner SELECT, and the wrapping ``SELECT "geom" FROM (...)`` (added
    by ``MVTQueryTransform.post_process_sql``) would fail with PostgreSQL
    ``column reference "geom" is ambiguous``.

    This mirrors the explicit-FieldSelection case above but exercises the
    raw_selects branch used by the tile path.
    """
    class _SidecarMock(MagicMock):
        @classmethod
        def serves_consumers(cls):
            return None

    mock_geom = _SidecarMock()
    mock_geom.config.sidecar_id = "geometries"
    mock_geom.sidecar_id = "geometries"
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geometries.geom",
            capabilities=[FieldCapability.SPATIAL],
            data_type="geometry(Geometry,4326)",
        )
    }
    mock_geom.get_join_clause.return_value = (
        'LEFT JOIN "schema"."table_geometries" sc_geometries '
        "ON h.geoid = sc_geometries.geoid"
    )
    mock_geom.supports_aggregation.return_value = True
    mock_geom.supports_transformation.return_value = True
    mock_geom.get_default_sort.return_value = None
    mock_geom.get_main_geometry_field.return_value = "geom"
    mock_geom.get_select_fields.return_value = [
        "ST_AsGeoJSON(sc_geometries.geom)::jsonb as geom"
    ]

    mock_attr = _SidecarMock()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.sidecar_id = "attributes"
    mock_attr.get_queryable_fields.return_value = {}
    mock_attr.get_join_clause.return_value = (
        'LEFT JOIN "schema"."table_attributes" sc_attributes '
        "ON h.geoid = sc_attributes.geoid"
    )
    mock_attr.supports_aggregation.return_value = True
    mock_attr.supports_transformation.return_value = True
    mock_attr.get_default_sort.return_value = None
    mock_attr.get_main_geometry_field.return_value = None
    mock_attr.get_select_fields.return_value = []

    mock_registry.get_sidecar.side_effect = (
        lambda sc, lenient=True: mock_geom
        if getattr(sc, "sidecar_type", "") == "geometries"
        else mock_attr
    )

    optimizer = QueryOptimizer(mock_col_config)

    # Simulate what MVTQueryTransform emits: strip geom from select (leaving
    # only the wildcard) and inject the MVT geometry expression into
    # raw_selects.
    req = QueryRequest(
        select=[FieldSelection(field="*")],
        raw_where=None,
        include_total_count=False,
    )
    req.raw_selects.append(
        "ST_AsMVTGeom(sc_geometries.geom, "
        "ST_SetSRID(ST_GeomFromWKB(:tile_wkb), CAST(:target_srid AS INTEGER)), "
        "4096, 256, true) AS geom"
    )

    sql, _ = optimizer.build_optimized_query(req, "schema", "table")

    sql_lower = sql.lower()
    geom_occurrences = sql_lower.count(" as geom")
    assert geom_occurrences == 1, (
        f"Expected exactly 1 'as geom' in SQL, found {geom_occurrences}.\nSQL:\n{sql}"
    )
    assert "st_asmvtgeom" in sql_lower, f"Expected ST_AsMVTGeom in SQL:\n{sql}"
    # The sidecar's ST_AsGeoJSON(...)::jsonb projection must have been skipped.
    assert "st_asgeojson" not in sql_lower, (
        f"Sidecar ST_AsGeoJSON projection should have been skipped:\n{sql}"
    )
