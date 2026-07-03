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
from unittest.mock import MagicMock, patch
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.models.query_builder import (
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


def test_determine_required_sidecars_scans_raw_selects(mock_col_config, mock_registry):
    """Regression for un-fao/dynastore#339: a raw SELECT projection that
    references a sidecar alias (``sc_attributes.attributes->>'datetime'``) must
    mark that sidecar as required, even when ``raw_where`` does not mention it.

    Post-#974 the attributes sidecar defaults to ``enable_validity=False``; the
    STAC ``/search`` dispatch then sorts by the item's own datetime via a raw
    projection ``(sc_attributes.attributes->>'datetime')::timestamptz AS
    valid_from`` rather than a ``raw_where`` clause. Because the optimizer
    scanned only ``raw_where`` for sidecar aliases, the ``attributes`` sidecar
    was never JOINed and the SELECT referenced a missing FROM-clause table,
    raising asyncpg ``UndefinedTableError: missing FROM-clause entry for table
    "sc_attributes"``.
    """
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

    mock_registry.get_sidecar.side_effect = (
        lambda sc, lenient=True: mock_geom
        if getattr(sc, "sidecar_type", "") == "geometries"
        else mock_attr
    )

    optimizer = QueryOptimizer(mock_col_config)

    # Only a raw SELECT references the attributes sidecar (via its ``sc_attributes``
    # alias); nothing in ``select`` / ``filters`` / ``raw_where`` does. Disable the
    # geometry auto-include so the assertion isolates the raw_selects scan.
    req = QueryRequest(
        select=[FieldSelection(field="geom")],
        raw_selects=[
            "(sc_attributes.attributes->>'datetime')::timestamptz as valid_from"
        ],
        raw_where=None,
        include_total_count=False,
    )

    required_ids = {
        sc.sidecar_id
        for sc in optimizer.determine_required_sidecars(req, require_geometry=False)
    }
    assert "attributes" in required_ids, (
        "raw SELECT referencing sc_attributes must mark the attributes sidecar "
        "as required so it is JOINed"
    )


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

    assert 'sc_geom.geom as "geometry"' in sql
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


def test_item_ids_filter_casts_geoid_uuid(mock_col_config, mock_registry):
    """item_ids filter must not emit a bare ``uuid = text`` comparison.

    When the feature-id expression is the bare ``h.geoid`` uuid column (the
    canonical-index read / GENERIC path with no external-id COALESCE), the
    ``_item_ids`` text[] parameter must be cast to ``uuid[]`` — otherwise
    Postgres raises ``operator does not exist: uuid = text`` and the canonical
    read silently returns nothing, degrading the ES doc to a stats-less,
    geoid-only fallback. The COALESCE (text) branch must stay a plain text
    comparison.
    """
    from dynastore.modules.storage.read_policy import ItemsReadPolicy
    from dynastore.modules.storage.computed_fields import FeatureType

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

    mock_registry.get_sidecar.side_effect = lambda sc, lenient=True: mock_attr

    req = QueryRequest(
        item_ids=["019ecf75-882f-7d74-8e35-fcb165bbff37"],
        filters=[FilterCondition(field="external_id", operator="=", value="X")],
        raw_where=None, include_total_count=False,
    )

    # Default (no policy): feature id is the bare h.geoid uuid → cast required.
    optimizer = QueryOptimizer(mock_col_config)
    sql_default, params_default = optimizer.build_optimized_query(req, "schema", "table")
    assert "h.geoid = ANY(CAST(:_item_ids AS uuid[]))" in sql_default
    assert "(h.geoid) = ANY(:_item_ids)" not in sql_default
    assert params_default["_item_ids"] == ["019ecf75-882f-7d74-8e35-fcb165bbff37"]

    # Policy opts in → COALESCE(...) text expression → plain text comparison.
    policy = ItemsReadPolicy(feature_type=FeatureType(external_id_as_feature_id=True))
    optimizer_on = QueryOptimizer(mock_col_config, read_policy=policy)
    sql_on, _ = optimizer_on.build_optimized_query(req, "schema", "table")
    assert "= ANY(:_item_ids)" in sql_on
    assert "CAST(:_item_ids AS uuid[])" not in sql_on


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
    # The optimizer quotes aliases to preserve case (#719).
    sql_lower = sql.lower()
    # Alias is quoted: as "geom" — search for the quoted form after lowercasing.
    geom_occurrences = sql_lower.count(' as "geom"')
    assert geom_occurrences == 1, (
        f'Expected exactly 1 \'as "geom"\' in SQL, found {geom_occurrences}.\nSQL:\n{sql}'
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


def test_star_expansion_drops_ungrouped_unaggregated_explicit_override(
    mock_col_config, mock_registry
):
    """#2859: the wildcard branch's explicit-override loop must apply the
    same ``group_by`` guard the non-wildcard branch got in #2843 — an
    override field that is neither grouped nor aggregated cannot appear in
    SELECT alongside an explicit GROUP BY (PostgreSQL 42803).

    Repro from #2859: ``select=[FieldSelection(field="*"),
    FieldSelection(field="extra")], group_by=["region"]``.
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
    mock_geom.get_select_fields.return_value = []

    mock_attr = _SidecarMock()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.sidecar_id = "attributes"
    mock_attr.get_queryable_fields.return_value = {
        "region": FieldDefinition(
            name="region",
            sql_expression="sc_attributes.region",
            data_type="string",
            capabilities=[FieldCapability.GROUPABLE],
        ),
        "extra": FieldDefinition(
            name="extra",
            sql_expression="sc_attributes.extra",
            data_type="string",
        ),
    }
    mock_attr.get_join_clause.return_value = (
        'LEFT JOIN "schema"."table_attributes" sc_attributes '
        "ON h.geoid = sc_attributes.geoid"
    )
    mock_attr.supports_aggregation.return_value = True
    mock_attr.supports_transformation.return_value = True
    mock_attr.get_default_sort.return_value = None
    mock_attr.get_main_geometry_field.return_value = None
    # Narrowed "selective mode" projection under group_by — restricted to
    # the grouped field, mirroring the real sidecar's group_by-aware gating
    # (``get_select_fields(..., include_all=not query.group_by)``).
    mock_attr.get_select_fields.return_value = ['sc_attributes.region as "region"']

    mock_registry.get_sidecar.side_effect = (
        lambda sc, lenient=True: mock_geom
        if getattr(sc, "sidecar_type", "") == "geometries"
        else mock_attr
    )

    optimizer = QueryOptimizer(mock_col_config)

    req = QueryRequest(
        select=[FieldSelection(field="*"), FieldSelection(field="extra")],
        group_by=["region"],
    )

    sql, _ = optimizer.build_optimized_query(req, "schema", "table")

    sql_lower = sql.lower()
    assert "extra" not in sql_lower, (
        f"ungrouped, unaggregated override field 'extra' must not appear in "
        f"SELECT alongside GROUP BY:\n{sql}"
    )
    assert "group by" in sql_lower


# ---------------------------------------------------------------------------
# items_schema → field_index enrichment (regression for the empty-MVT case
# where a VECTOR collection's ``ItemsSchema`` declares user fields that live
# in the JSONB attributes blob — ``START_DATE`` / ``END_DATE`` on the
# ``datamgr02/region`` review collection rejected as ``Unknown field`` at
# tile render time).
# ---------------------------------------------------------------------------


def test_jsonb_property_field_casts_via_canonical_data_type():
    """``jsonb_property_field`` must derive the SQL cast from the canonical
    ``data_type`` so a typed comparison parses correctly downstream (a
    text-vs-date compare would otherwise raise at execution)."""
    from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
        FeatureAttributeSidecar,
    )

    sidecar = FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.JSONB)
    )

    fd_date = FieldDefinition(name="START_DATE", data_type="date")
    out = sidecar.jsonb_property_field("START_DATE", fd_date)
    assert out.sql_expression == "(sc_attributes.attributes->>'START_DATE')::DATE"
    assert out.data_type == "date"

    fd_text = FieldDefinition(name="CODE", data_type="string")
    out_text = sidecar.jsonb_property_field("CODE", fd_text)
    assert out_text.sql_expression == "(sc_attributes.attributes->>'CODE')::TEXT"

    fd_jsonb = FieldDefinition(name="BLOB", data_type="jsonb")
    out_jsonb = sidecar.jsonb_property_field("BLOB", fd_jsonb)
    assert out_jsonb.sql_expression == "(sc_attributes.attributes->>'BLOB')::JSONB"


def test_jsonb_property_field_returns_none_in_columnar_mode():
    """COLUMNAR-mode sidecars have no JSONB blob column on disk, so any
    extraction SQL would crash at runtime with ``UndefinedColumnError``.
    The helper must refuse to synthesise one — symmetric with the existing
    guard in :meth:`get_dynamic_field_definition`.

    Regression for v0.17.50: an items_schema field that didn't match the
    sidecar's columnar entries was being aliased to ``(sc_attributes.
    attributes->>'NAME')::TEXT`` and crashed every MVT render on
    ``datamgr02/region`` with ``column sc_attributes.attributes does not
    exist``.
    """
    from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
        FeatureAttributeSidecar,
    )
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeSchemaEntry,
        PostgresType,
    )

    columnar = FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(
            attribute_schema=[
                AttributeSchemaEntry(name="CODE", type=PostgresType.TEXT),
            ],
        ),
    )
    assert columnar.jsonb_property_field(
        "MISSING", FieldDefinition(name="MISSING", data_type="string"),
    ) is None

    explicit_columnar = FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.COLUMNAR),
    )
    assert explicit_columnar.jsonb_property_field(
        "X", FieldDefinition(name="X", data_type="date"),
    ) is None


@pytest.fixture
def _real_sidecar_registry():
    """Yield with the default sidecar registry warmed up, leaving the
    registry populated for any later tests in the same session — clearing
    in teardown would force every subsequent test that touches the
    registry to re-import the defaults.
    """
    from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

    SidecarRegistry._ensure_defaults()
    yield SidecarRegistry


def test_optimizer_enriches_field_index_from_items_schema(_real_sidecar_registry):
    """Reproduces the ``datamgr02/region`` empty-MVT cause: a VECTOR
    collection whose ``ItemsSchema.fields`` declares ``START_DATE`` /
    ``END_DATE`` that the attributes sidecar would not surface from its
    ``attribute_schema`` alone. The optimizer must enrich its index so the
    SELECT projection validates against the schema (the SSOT)."""

    col_config = ItemsPostgresqlDriverConfig(
        sidecars=[
            FeatureAttributeSidecarConfig(
                sidecar_type="attributes",
                storage_mode=AttributeStorageMode.JSONB,
            ),
        ],
    )
    schema_fields = {
        "CODE": FieldDefinition(name="CODE", data_type="string"),
        "START_DATE": FieldDefinition(name="START_DATE", data_type="date"),
        "END_DATE": FieldDefinition(name="END_DATE", data_type="date"),
        # Geometry-typed → must be skipped (owned by the geometry sidecar /
        # driver, never an attribute column).
        "the_geom": FieldDefinition(name="the_geom", data_type="geometry(Polygon,4326)"),
        # ``expose=False`` → must be skipped (declared for write validation
        # only; the read path does not surface it).
        "INTERNAL": FieldDefinition(name="INTERNAL", data_type="string", expose=False),
    }

    optimizer = QueryOptimizer(col_config, schema_fields=schema_fields)

    assert "START_DATE" in optimizer.field_index, (
        "items_schema date field must reach the index for SELECT validation"
    )
    assert "END_DATE" in optimizer.field_index
    assert "CODE" in optimizer.field_index
    assert "the_geom" not in optimizer.field_index, "geometry-typed must be skipped"
    assert "INTERNAL" not in optimizer.field_index, "expose=False must be skipped"

    # Validation passes for a SELECT list built from the items_schema (the
    # exact shape ``project_select_for_feature_type`` produces on the MVT
    # path) — no ``Unknown field`` is emitted.
    req = QueryRequest(
        select=[
            FieldSelection(field="CODE"),
            FieldSelection(field="START_DATE"),
            FieldSelection(field="END_DATE"),
        ],
    )
    errors = optimizer.validate_query(req)
    assert errors == [], errors

    _, fd_start = optimizer.field_index["START_DATE"]
    assert fd_start.sql_expression == "(sc_attributes.attributes->>'START_DATE')::DATE"


def test_optimizer_schema_enrichment_does_not_shadow_native_columns(
    _real_sidecar_registry, caplog,
):
    """A schema field whose name matches an existing native (columnar)
    column must keep the columnar ``sql_expression`` — never shadowed by
    a schema-derived fallback. A sibling schema field that is NOT in the
    columnar ``attribute_schema`` must be skipped + warned, because a
    COLUMNAR-mode sidecar has no JSONB blob column on disk (a JSONB
    extract would crash with ``UndefinedColumnError``).
    """
    import logging

    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeSchemaEntry,
        PostgresType,
    )

    col_config = ItemsPostgresqlDriverConfig(
        sidecars=[
            FeatureAttributeSidecarConfig(
                sidecar_type="attributes",
                attribute_schema=[
                    AttributeSchemaEntry(name="CODE", type=PostgresType.TEXT),
                ],
            ),
        ],
    )
    schema_fields = {
        "CODE": FieldDefinition(name="CODE", data_type="string"),
        "START_DATE": FieldDefinition(name="START_DATE", data_type="date"),
    }

    with caplog.at_level(
        logging.WARNING, logger="dynastore.modules.catalog.query_optimizer",
    ):
        optimizer = QueryOptimizer(col_config, schema_fields=schema_fields)

    _, fd_code = optimizer.field_index["CODE"]
    # Native column expression (case-preserving quoted identifier) — NOT a
    # JSONB extraction — must win over the schema-derived fallback.
    assert fd_code.sql_expression == 'sc_attributes."CODE"'
    # The non-columnar schema field is skipped (no JSONB blob exists on a
    # COLUMNAR-mode sidecar table — synthesising one would crash the query).
    assert "START_DATE" not in optimizer.field_index
    assert any(
        "cannot be enriched" in rec.getMessage() and rec.args and rec.args[0] == "START_DATE"
        for rec in caplog.records
    )


def test_optimizer_skips_columnar_expected_field_when_sidecar_silent(
    _real_sidecar_registry, caplog
):
    """Per-field storage routing: if items_schema says the field MUST be a
    native column (``access=FAST``) but the attributes sidecar didn't
    advertise it, the optimizer must skip + warn — never silently fall back
    to a JSONB extract that would return NULL for every row."""
    import logging

    from dynastore.models.protocols.field_definition import FieldAccess

    col_config = ItemsPostgresqlDriverConfig(
        sidecars=[
            FeatureAttributeSidecarConfig(
                sidecar_type="attributes",
                # Sidecar deliberately empty — simulates an out-of-sync
                # ``attribute_schema`` lagging behind the items_schema.
                attribute_schema=None,
            ),
        ],
    )
    schema_fields = {
        # ``access=FAST`` → ``schema_field_materializes_as_column`` is True.
        "FAST_COL": FieldDefinition(
            name="FAST_COL", data_type="string", access=FieldAccess.FAST,
        ),
        # ``required=True`` is also a hard column-synthesis trigger.
        "REQ_COL": FieldDefinition(
            name="REQ_COL", data_type="string", required=True,
        ),
        # COMPACT → JSONB-stored; enrichment path injects the JSONB extract.
        "JSON_FIELD": FieldDefinition(
            name="JSON_FIELD", data_type="string", access=FieldAccess.COMPACT,
        ),
    }

    with caplog.at_level(logging.WARNING, logger="dynastore.modules.catalog.query_optimizer"):
        optimizer = QueryOptimizer(col_config, schema_fields=schema_fields)

    assert "FAST_COL" not in optimizer.field_index, (
        "columnar-expected field must NOT be silently aliased to a JSONB extract"
    )
    assert "REQ_COL" not in optimizer.field_index
    # JSONB-stored field still enriches normally.
    assert "JSON_FIELD" in optimizer.field_index

    warned = [
        rec for rec in caplog.records
        if "expects a native column" in rec.getMessage()
    ]
    assert {rec.args[0] if rec.args else "" for rec in warned} >= {"FAST_COL", "REQ_COL"}


def test_items_schema_rejects_reserved_root_names():
    """Reserved-name validator fires at config-save when ``ItemsSchema.fields``
    declares a key that collides with the tenant feature mapping root
    (``geoid``, ``geometry``, ``bbox`` …). Fail-fast — never silently shadow
    a system field."""
    import asyncio

    from dynastore.modules.storage.driver_config import (
        ItemsSchema,
        _validate_items_schema_reserved_names,
    )

    schema = ItemsSchema(
        fields={
            "CODE": FieldDefinition(name="CODE", data_type="string"),
            "geometry": FieldDefinition(name="geometry", data_type="string"),
        },
    )
    with pytest.raises(ValueError, match="reserved root name"):
        asyncio.run(_validate_items_schema_reserved_names(schema, None, None, None))


# ---------------------------------------------------------------------------
# Filterable/sortable-by-default (#1628)
#
# The read-side query validator (query_optimizer.validate_query) treats a field
# with an *unspecified* capability set as both filterable and sortable, so a
# config author need not spell out FILTERABLE/SORTABLE on every field. A field
# is rejected only when it declares an explicit capability set that omits the
# relevant capability — the deliberate opt-out. The defaulting lives on the read
# path only (FieldDefinition.is_filterable / is_sortable); the stored
# ``capabilities`` are never mutated, so the write/DDL column-implying rules are
# unaffected.
# ---------------------------------------------------------------------------
def test_field_definition_filterable_sortable_defaults():
    """Pure FieldDefinition semantics — no DB, no optimizer."""

    # Unspecified capabilities → filterable AND sortable by default.
    empty = FieldDefinition(name="f", data_type="string")
    assert empty.is_filterable() is True
    assert empty.is_sortable() is True

    # Explicit FILTERABLE → filterable; (no SORTABLE) → not sortable.
    filt = FieldDefinition(
        name="f", data_type="string", capabilities=[FieldCapability.FILTERABLE]
    )
    assert filt.is_filterable() is True
    assert filt.is_sortable() is False

    # Explicit SORTABLE-only → the deliberate filter opt-out.
    sort_only = FieldDefinition(
        name="f", data_type="string", capabilities=[FieldCapability.SORTABLE]
    )
    assert sort_only.is_filterable() is False
    assert sort_only.is_sortable() is True

    # SPATIAL and FULLTEXT are filtering capabilities (spatial predicate / ES
    # match), so a field carrying only one of them is still filterable.
    spatial = FieldDefinition(
        name="g", data_type="geometry", capabilities=[FieldCapability.SPATIAL]
    )
    assert spatial.is_filterable() is True
    fulltext = FieldDefinition(
        name="t", data_type="string", capabilities=[FieldCapability.FULLTEXT]
    )
    assert fulltext.is_filterable() is True

    # An explicit set with no filtering/sorting capability → opt-out of both.
    agg_only = FieldDefinition(
        name="n", data_type="double", capabilities=[FieldCapability.AGGREGATABLE]
    )
    assert agg_only.is_filterable() is False
    assert agg_only.is_sortable() is False


def _optimizer_with_fields(mock_col_config, mock_registry, fields):
    """Build a QueryOptimizer whose field_index is exactly ``fields``."""
    sidecar = MagicMock()
    sidecar.sidecar_id = "test"
    sidecar.config.sidecar_id = "test"
    sidecar.get_queryable_fields.return_value = fields
    sidecar.get_dynamic_field_definition.return_value = None
    mock_registry.get_sidecar.side_effect = lambda sc, lenient=True: sidecar
    return QueryOptimizer(mock_col_config)


def test_validate_query_filter_allowed_when_capabilities_unspecified(
    mock_col_config, mock_registry
):
    """A filter on a field with no declared capabilities must validate — the
    re-enabled guard (#1628) defaults unspecified fields to filterable."""
    optimizer = _optimizer_with_fields(
        mock_col_config,
        mock_registry,
        {
            "code": FieldDefinition(
                name="code", sql_expression="sc_test.code", data_type="string"
            ),
        },
    )
    req = QueryRequest(filters=[FilterCondition(field="code", operator="=", value="x")])
    errors = optimizer.validate_query(req)
    assert not any("not filterable" in e for e in errors), errors


def test_validate_query_rejects_filter_on_explicit_non_filterable(
    mock_col_config, mock_registry
):
    """A field declared with an explicit capability set that omits the filtering
    capabilities is the deliberate opt-out — filtering on it is rejected."""
    optimizer = _optimizer_with_fields(
        mock_col_config,
        mock_registry,
        {
            "rank": FieldDefinition(
                name="rank",
                sql_expression="sc_test.rank",
                data_type="integer",
                capabilities=[FieldCapability.SORTABLE],
            ),
        },
    )
    req = QueryRequest(filters=[FilterCondition(field="rank", operator="=", value=1)])
    errors = optimizer.validate_query(req)
    assert any("rank" in e and "not filterable" in e for e in errors), errors


def test_validate_query_spatial_filter_on_spatial_field_allowed(
    mock_col_config, mock_registry
):
    """A spatial predicate on a geometry field declared SPATIAL must validate —
    SPATIAL is a filtering capability, so the re-enabled guard does not reject
    it (regression guard against breaking bbox/ST_* queries)."""
    optimizer = _optimizer_with_fields(
        mock_col_config,
        mock_registry,
        {
            "geom": FieldDefinition(
                name="geom",
                sql_expression="sc_test.geom",
                data_type="geometry",
                capabilities=[FieldCapability.SPATIAL],
            ),
        },
    )
    req = QueryRequest(
        filters=[
            FilterCondition(
                field="geom", operator="ST_Intersects", value="POINT(0 0)", spatial_op=True
            )
        ]
    )
    errors = optimizer.validate_query(req)
    assert not any("not filterable" in e for e in errors), errors


def test_validate_query_sort_defaults_and_optout(mock_col_config, mock_registry):
    """Sort mirrors filter: unspecified caps → sortable; an explicit set that
    omits SORTABLE → rejected."""
    optimizer = _optimizer_with_fields(
        mock_col_config,
        mock_registry,
        {
            "code": FieldDefinition(
                name="code", sql_expression="sc_test.code", data_type="string"
            ),
            "blob": FieldDefinition(
                name="blob",
                sql_expression="sc_test.blob",
                data_type="string",
                capabilities=[FieldCapability.FILTERABLE],
            ),
        },
    )
    # Unspecified caps → sortable.
    ok = optimizer.validate_query(
        QueryRequest(sort=[SortOrder(field="code", direction="ASC")])
    )
    assert not any("not sortable" in e for e in ok), ok
    # FILTERABLE-only (no SORTABLE) → sort rejected.
    bad = optimizer.validate_query(
        QueryRequest(sort=[SortOrder(field="blob", direction="ASC")])
    )
    assert any("blob" in e and "not sortable" in e for e in bad), bad


# ---------------------------------------------------------------------------
# #2859 — validate_query diagnostic for a selected, ungrouped/unaggregated
# field alongside an explicit group_by (previously a silent projection drop,
# #2843).
# ---------------------------------------------------------------------------


def _group_by_fields() -> dict:
    return {
        "region": FieldDefinition(
            name="region",
            sql_expression="sc_test.region",
            data_type="string",
            capabilities=[FieldCapability.GROUPABLE],
        ),
        "id": FieldDefinition(
            name="id", sql_expression="sc_test.id", data_type="string",
        ),
        "count": FieldDefinition(
            name="count", sql_expression="sc_test.count", data_type="integer",
        ),
    }


def test_validate_query_rejects_selected_ungrouped_unaggregated_field(
    mock_col_config, mock_registry
):
    """``select=[id, region], group_by=[region]`` — "id" is neither grouped
    nor aggregated. Must be reported by ``validate_query`` rather than
    silently dropped at render time (the #2843 gap)."""
    optimizer = _optimizer_with_fields(mock_col_config, mock_registry, _group_by_fields())
    req = QueryRequest(
        select=[FieldSelection(field="id"), FieldSelection(field="region")],
        group_by=["region"],
    )
    errors = optimizer.validate_query(req)
    assert any(
        "id" in e and "neither grouped nor aggregated" in e for e in errors
    ), errors


def test_validate_query_allows_grouped_and_aggregated_selects(
    mock_col_config, mock_registry
):
    """A field that is grouped, or aggregated, must not trigger the #2859
    diagnostic — only a selected field that is neither must."""
    optimizer = _optimizer_with_fields(mock_col_config, mock_registry, _group_by_fields())
    req = QueryRequest(
        select=[
            FieldSelection(field="region"),
            FieldSelection(field="count", aggregation="sum"),
        ],
        group_by=["region"],
    )
    errors = optimizer.validate_query(req)
    assert not any("neither grouped nor aggregated" in e for e in errors), errors


def test_validate_query_skips_ungrouped_check_for_wildcard_select(
    mock_col_config, mock_registry
):
    """A wildcard select's rendered projection (sidecar expansion + explicit
    overrides) isn't knowable at validate time, so the #2859 diagnostic must
    not fire for it — the render-time guard in
    ``build_optimized_query`` is the only defense there."""
    optimizer = _optimizer_with_fields(mock_col_config, mock_registry, _group_by_fields())
    req = QueryRequest(
        select=[FieldSelection(field="*"), FieldSelection(field="id")],
        group_by=["region"],
    )
    errors = optimizer.validate_query(req)
    assert not any("neither grouped nor aggregated" in e for e in errors), errors


# ---------------------------------------------------------------------------
# #719 — projection alias quoting (regression for MVT mixed/upper-case fields)
# ---------------------------------------------------------------------------


def test_quote_alias_helper():
    """``_quote_alias`` must double-quote bare identifiers and be idempotent."""
    from dynastore.modules.catalog.query_optimizer import _quote_alias

    # Bare lowercase identifier gets quoted.
    assert _quote_alias("code") == '"code"'
    # Bare uppercase identifier gets quoted.
    assert _quote_alias("CODE") == '"CODE"'
    # Mixed-case identifier gets quoted.
    assert _quote_alias("ADM0_Name") == '"ADM0_Name"'
    # Already-quoted alias is returned unchanged (idempotent).
    assert _quote_alias('"CODE"') == '"CODE"'
    # Star is never quoted — wildcard must stay bare.
    assert _quote_alias("*") == "*"
    # Embedded double-quote is escaped.
    assert _quote_alias('a"b') == '"a""b"'
    # Surrounding whitespace is stripped before quoting.
    assert _quote_alias("  CODE  ") == '"CODE"'


def test_build_optimized_query_quotes_upper_case_alias(mock_col_config, mock_registry):
    """Regression for #719: a FieldSelection with an UPPER-case field (e.g. ``CODE``)
    must produce ``as "CODE"`` in the generated SQL so that the alias survives
    Postgres identifier folding.  An unquoted ``as CODE`` is folded to lowercase
    ``code`` by Postgres, breaking the MVT outer wrapper that selects ``"CODE"``
    from the inner subquery.
    """
    mock_geom = MagicMock()
    mock_geom.config.sidecar_id = "geometries"
    mock_geom.sidecar_id = "geometries"
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geometries.geom",
            capabilities=[FieldCapability.SPATIAL],
            data_type="geometry",
        )
    }
    mock_geom.get_join_clause.return_value = (
        "LEFT JOIN geom_table sc_geometries ON h.geoid = sc_geometries.geoid"
    )
    mock_geom.supports_aggregation.return_value = True
    mock_geom.supports_transformation.return_value = True
    mock_geom.get_default_sort.return_value = None

    mock_attr = MagicMock()
    mock_attr.config.sidecar_id = "attributes"
    mock_attr.sidecar_id = "attributes"
    # COLUMNAR mode: CODE is a native column; sql_expression already quoted.
    mock_attr.get_queryable_fields.return_value = {
        "CODE": FieldDefinition(
            name="CODE",
            sql_expression='sc_attributes."CODE"',
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
        lambda sc, lenient=True: mock_geom
        if getattr(sc, "sidecar_type", "") == "geometries"
        else mock_attr
    )

    optimizer = QueryOptimizer(mock_col_config)

    # Non-wildcard path: the optimizer must quote the alias.
    req = QueryRequest(
        select=[FieldSelection(field="CODE")],
        raw_where=None,
        include_total_count=False,
    )
    sql, _ = optimizer.build_optimized_query(req, "schema", "table")

    assert 'as "CODE"' in sql, (
        f'Expected quoted alias as "CODE" in SQL (got: {sql})'
    )
    # The old buggy unquoted form must not appear.
    assert " as CODE" not in sql.replace('"CODE"', ""), (
        f'Unquoted alias "as CODE" found in SQL (got: {sql})'
    )


def test_build_optimized_query_quotes_upper_case_alias_star_path(
    mock_col_config, mock_registry
):
    """Regression for #719: the wildcard (``*``) expansion path must also quote
    the alias for any explicit non-``*`` FieldSelection whose field name is
    mixed/upper case.
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
            data_type="geometry",
        )
    }
    mock_geom.get_join_clause.return_value = (
        "LEFT JOIN geom_table sc_geometries ON h.geoid = sc_geometries.geoid"
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
    mock_attr.get_queryable_fields.return_value = {
        "CODE": FieldDefinition(
            name="CODE",
            sql_expression='sc_attributes."CODE"',
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
    mock_attr.get_main_geometry_field.return_value = None
    mock_attr.get_select_fields.return_value = []

    mock_registry.get_sidecar.side_effect = (
        lambda sc, lenient=True: mock_geom
        if getattr(sc, "sidecar_type", "") == "geometries"
        else mock_attr
    )

    optimizer = QueryOptimizer(mock_col_config)

    # Wildcard path with an explicit UPPER-case FieldSelection alongside it.
    req = QueryRequest(
        select=[
            FieldSelection(field="CODE"),
            FieldSelection(field="*"),
        ],
        raw_where=None,
        include_total_count=False,
    )
    sql, _ = optimizer.build_optimized_query(req, "schema", "table")

    assert 'as "CODE"' in sql, (
        f'Expected quoted alias as "CODE" in SQL on the wildcard path (got: {sql})'
    )


# ---------------------------------------------------------------------------
# Bbox spatial filter — operator does not exist: && record (#2282)
#
# A bbox FilterCondition with operator="&&" and spatial_op=True was rendered
# as ``&&(sc_geometries.geom, :filter_0)`` by build_optimized_query.
# PostgreSQL parses the parenthesised pair as a composite/record type and
# applies ``&&`` to that single record operand, raising:
#   operator does not exist: && record
# The fix renders infix spatial operators as ``expr op ST_GeomFromEWKT(:param)``
# so both sides are typed geometry values.
# ---------------------------------------------------------------------------


def test_build_optimized_query_bbox_filter_emits_infix_not_function_form(
    mock_col_config, mock_registry
):
    """A bbox FilterCondition (operator='&&', spatial_op=True, EWKT value) must
    produce ``sc_geometries.geom && ST_GeomFromEWKT(:filter_0)`` in the WHERE
    clause, not the broken ``&&(sc_geometries.geom, :filter_0)`` form that
    PostgreSQL rejects with 'operator does not exist: && record'."""
    mock_geom = MagicMock()
    mock_geom.config.sidecar_id = "geometries"
    mock_geom.sidecar_id = "geometries"
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geometries.geom",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SPATIAL],
            data_type="geometry",
        )
    }
    mock_geom.get_join_clause.return_value = (
        "LEFT JOIN geom_table sc_geometries ON h.geoid = sc_geometries.geoid"
    )
    mock_geom.supports_aggregation.return_value = True
    mock_geom.supports_transformation.return_value = True
    mock_geom.get_default_sort.return_value = None

    mock_registry.get_sidecar.side_effect = lambda sc, lenient=True: mock_geom

    optimizer = QueryOptimizer(mock_col_config)

    ewkt = "SRID=4326;POLYGON((1.0 2.0, 1.0 4.0, 3.0 4.0, 3.0 2.0, 1.0 2.0))"
    req = QueryRequest(
        select=[FieldSelection(field="geom", alias="geometry")],
        filters=[
            FilterCondition(
                field="geom",
                operator="&&",
                value=ewkt,
                spatial_op=True,
            )
        ],
        raw_where=None,
        include_total_count=False,
    )

    sql, params = optimizer.build_optimized_query(req, "myschema", "mytable")

    # Correct infix form: geometry_col && ST_GeomFromEWKT(:param)
    assert "sc_geometries.geom && ST_GeomFromEWKT(:filter_0)" in sql, (
        f"Expected infix bbox predicate in SQL, got:\n{sql}"
    )
    # Broken record form must NOT appear
    assert "&&(sc_geometries.geom," not in sql, (
        f"Broken record-form &&(expr, :param) must not appear in SQL, got:\n{sql}"
    )
    assert params["filter_0"] == ewkt


def test_build_optimized_query_st_intersects_uses_function_form(
    mock_col_config, mock_registry
):
    """ST_Intersects (and other ST_* spatial operators) must still use the
    function-call form ST_Intersects(expr, :param) — only infix operators
    like && change to the infix form."""
    mock_geom = MagicMock()
    mock_geom.config.sidecar_id = "geometries"
    mock_geom.sidecar_id = "geometries"
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geometries.geom",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SPATIAL],
            data_type="geometry",
        )
    }
    mock_geom.get_join_clause.return_value = (
        "LEFT JOIN geom_table sc_geometries ON h.geoid = sc_geometries.geoid"
    )
    mock_geom.supports_aggregation.return_value = True
    mock_geom.supports_transformation.return_value = True
    mock_geom.get_default_sort.return_value = None

    mock_registry.get_sidecar.side_effect = lambda sc, lenient=True: mock_geom

    optimizer = QueryOptimizer(mock_col_config)

    req = QueryRequest(
        select=[FieldSelection(field="geom", alias="geometry")],
        filters=[
            FilterCondition(
                field="geom",
                operator="ST_Intersects",
                value="SRID=4326;POINT(1 2)",
                spatial_op=True,
            )
        ],
        raw_where=None,
        include_total_count=False,
    )

    sql, params = optimizer.build_optimized_query(req, "myschema", "mytable")

    # ST_* operators use function-call form
    assert "ST_Intersects(sc_geometries.geom, :filter_0)" in sql, (
        f"Expected ST_Intersects(expr, :param) in SQL, got:\n{sql}"
    )
