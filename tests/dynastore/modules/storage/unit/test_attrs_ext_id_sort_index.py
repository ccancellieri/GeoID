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

"""Unit tests for the attributes sidecar sort-index and scoped INNER JOIN.

Verifies that:
- get_ddl() emits idx_{table}_ext_id_sort on the external_id column whenever
  external_id_field is configured, regardless of index_external_id.
- The sort index uses a plain (external_id) prefix, not (geoid, external_id),
  so PostgreSQL can use it for ORDER BY external_id ASC streaming.
- get_ddl() does NOT emit the sort index when external_id_field is None.
- get_join_clause() keeps LEFT as its default (global default unchanged).
- QueryOptimizer passes join_type="INNER" to the attrs sidecar ONLY when the
  default external_id sort path is active (no explicit sort, no aggregation,
  no GROUP BY).  All other query shapes receive LEFT JOIN.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dynastore.models.query_builder import (
    FieldSelection,
    FilterCondition,
    QueryRequest,
    SortOrder,
)
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FieldCapability,
    FieldDefinition,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
    TargetDimension,
)
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _sidecar(
    external_id_field: str | None = "external_id",
    index_external_id: bool = True,
    storage_mode: AttributeStorageMode = AttributeStorageMode.JSONB,
) -> FeatureAttributeSidecar:
    return FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(
            storage_mode=storage_mode,
            external_id_field=external_id_field,
            index_external_id=index_external_id,
        )
    )


class _SidecarLike(MagicMock):
    """MagicMock with a serves_consumers classmethod that returns None
    (consumer-agnostic), matching the production SidecarProtocol default."""
    @classmethod
    def serves_consumers(cls):
        return None


def _make_col_config() -> ItemsPostgresqlDriverConfig:
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
                external_id_field="external_id",
                index_external_id=True,
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Sort-index DDL tests
# ---------------------------------------------------------------------------

class TestExtIdSortIndexDdl:
    def test_sort_index_present_with_default_external_id_field(self):
        """Default config emits idx_..._ext_id_sort on (external_id)."""
        ddl = _sidecar().get_ddl("t_abc123")
        assert 'idx_t_abc123_attributes_ext_id_sort' in ddl
        assert '"idx_t_abc123_attributes_ext_id_sort"' in ddl

    def test_sort_index_is_plain_external_id_only(self):
        """The sort index covers only external_id — NOT (geoid, external_id).

        The unique identity index (idx_..._ext_id) already covers
        (geoid, external_id) with geoid as the leading column.  That index
        cannot be used for ORDER BY external_id because geoid varies freely
        across rows.  The sort index must have external_id as the first
        (and only) column so the planner can use it for index-ordered scans.
        """
        ddl = _sidecar().get_ddl("t_abc123")
        sort_idx_line = next(
            line for line in ddl.splitlines()
            if "ext_id_sort" in line
        )
        # Must not list geoid before external_id.
        assert '"geoid"' not in sort_idx_line
        assert "external_id" in sort_idx_line

    def test_sort_index_present_when_index_external_id_false(self):
        """Sort index is independent of the index_external_id flag.

        index_external_id=False only suppresses the UNIQUE identity index;
        the sort index is always created when external_id_field is set, because
        it serves ORDER BY, not uniqueness.
        """
        ddl = _sidecar(index_external_id=False).get_ddl("t_abc123")
        assert 'idx_t_abc123_attributes_ext_id_sort' in ddl
        # The unique identity index must be absent.
        assert '"idx_t_abc123_attributes_ext_id"' not in ddl

    def test_sort_index_absent_when_external_id_field_is_none(self):
        """No sort index when external_id_field is None (column disabled)."""
        ddl = _sidecar(external_id_field=None).get_ddl("t_abc123")
        assert 'ext_id_sort' not in ddl

    def test_sort_index_uses_configured_column_name(self):
        """If external_id_field is a non-default name, the sort index targets it."""
        ddl = _sidecar(external_id_field="my_ext_id").get_ddl("t_abc123")
        assert 'idx_t_abc123_attributes_ext_id_sort' in ddl
        sort_idx_line = next(
            line for line in ddl.splitlines()
            if "ext_id_sort" in line
        )
        assert "my_ext_id" in sort_idx_line

    def test_sort_index_is_idempotent(self):
        """The sort index uses IF NOT EXISTS so re-runs are safe."""
        ddl = _sidecar().get_ddl("t_abc123")
        sort_idx_line = next(
            line for line in ddl.splitlines()
            if "ext_id_sort" in line
        )
        assert "IF NOT EXISTS" in sort_idx_line

    def test_sort_index_present_in_columnar_mode(self):
        """Sort index is emitted in COLUMNAR mode too (not just JSONB)."""
        ddl = _sidecar(storage_mode=AttributeStorageMode.COLUMNAR).get_ddl("t_abc123")
        assert 'idx_t_abc123_attributes_ext_id_sort' in ddl

    def test_unique_identity_index_still_present(self):
        """The existing unique (geoid, external_id) index must not be removed."""
        ddl = _sidecar().get_ddl("t_abc123")
        assert '"idx_t_abc123_attributes_ext_id"' in ddl
        assert "UNIQUE INDEX" in ddl


# ---------------------------------------------------------------------------
# get_join_clause default (LEFT unchanged globally)
# ---------------------------------------------------------------------------

class TestGetJoinClauseDefault:
    def test_default_join_type_is_left(self):
        """get_join_clause() global default remains LEFT JOIN.

        The INNER JOIN optimisation is applied by the query optimizer only
        on the default-sort path — the sidecar itself stays LEFT by default
        so that all other callers (filters, field projections, explicit-sort
        queries) continue to see LEFT semantics without silent row drops.
        """
        sidecar = _sidecar()
        clause = sidecar.get_join_clause(schema="s_test", hub_table="t_abc123")
        assert "LEFT JOIN" in clause
        assert "INNER JOIN" not in clause

    def test_join_targets_correct_sidecar_table(self):
        """JOIN clause references {hub_table}_attributes."""
        sidecar = _sidecar()
        clause = sidecar.get_join_clause(schema="s_test", hub_table="t_abc123")
        assert "t_abc123_attributes" in clause

    def test_join_condition_is_geoid_equality(self):
        """ON condition is hub.geoid = attrs.geoid."""
        sidecar = _sidecar()
        clause = sidecar.get_join_clause(
            schema="s_test", hub_table="t_abc123", hub_alias="h",
            sidecar_alias="sc_attributes",
        )
        assert "h.geoid = sc_attributes.geoid" in clause

    def test_explicit_inner_join_override(self):
        """Caller can request INNER JOIN explicitly (query optimizer does this
        on the default-sort path)."""
        sidecar = _sidecar()
        clause = sidecar.get_join_clause(
            schema="s_test", hub_table="t_abc123", join_type="INNER"
        )
        assert "INNER JOIN" in clause
        assert "LEFT JOIN" not in clause


# ---------------------------------------------------------------------------
# Scoped INNER JOIN via QueryOptimizer
# ---------------------------------------------------------------------------

class TestQueryOptimizerScopedInnerJoin:
    """QueryOptimizer passes join_type="INNER" to the attrs sidecar ONLY when
    the default external_id sort is the active ORDER BY (no explicit sort, no
    aggregation, no GROUP BY).  All other query shapes keep LEFT JOIN."""

    def _make_optimizer_with_real_attrs(self, mock_registry):
        """Build a QueryOptimizer with a real FeatureAttributeSidecar for attrs
        and a mocked geometry sidecar."""
        real_attrs = _sidecar()

        mock_geom = _SidecarLike()
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
            'LEFT JOIN "s_test"."t_abc_geometries" sc_geometries'
            " ON h.geoid = sc_geometries.geoid"
        )
        mock_geom.get_default_sort.return_value = None
        mock_geom.get_main_geometry_field.return_value = "geom"
        mock_geom.provides_feature_id = False
        mock_geom.feature_id_field_name = None
        mock_geom.supports_aggregation.return_value = True
        mock_geom.supports_transformation.return_value = True

        mock_registry.get_sidecar.side_effect = (
            lambda sc, lenient=True: (
                mock_geom if getattr(sc, "sidecar_type", "") == "geometries"
                else real_attrs
            )
        )

        col_config = _make_col_config()
        return QueryOptimizer(col_config), real_attrs

    @pytest.fixture
    def mock_registry(self):
        with patch(
            "dynastore.modules.storage.drivers.pg_sidecars.registry.SidecarRegistry"
        ) as mock:
            yield mock

    def test_default_sort_query_uses_inner_join_for_attrs(self, mock_registry):
        """No explicit sort → attrs is INNER-joined so the planner can use
        the ext_id_sort index to stream results in external_id order."""
        optimizer, _ = self._make_optimizer_with_real_attrs(mock_registry)

        req = QueryRequest(
            select=[FieldSelection(field="*")],
            raw_where=None,
            include_total_count=False,
        )
        sql, _ = optimizer.build_optimized_query(req, "s_test", "t_abc")

        assert 'INNER JOIN "s_test"."t_abc_attributes"' in sql
        assert "ORDER BY" in sql
        assert "external_id" in sql

    def test_explicit_sort_query_keeps_left_join_for_attrs(self, mock_registry):
        """Explicit user sort (e.g. sortby=geoid) → attrs remains LEFT-joined;
        no performance regression and no silent row drop risk."""
        optimizer, _ = self._make_optimizer_with_real_attrs(mock_registry)

        req = QueryRequest(
            select=[FieldSelection(field="*")],
            sort=[SortOrder(field="geoid", direction="ASC")],
            raw_where=None,
            include_total_count=False,
        )
        sql, _ = optimizer.build_optimized_query(req, "s_test", "t_abc")

        assert 'LEFT JOIN "s_test"."t_abc_attributes"' in sql
        assert 'INNER JOIN "s_test"."t_abc_attributes"' not in sql

    def test_filter_only_query_keeps_left_join_for_attrs(self, mock_registry):
        """A filter-only query (no sort) without a default sort sidecar
        keeps LEFT JOIN.  Here we force attrs.get_default_sort → None to
        simulate a collection without external_id configured."""
        real_attrs_no_sort = FeatureAttributeSidecar(
            FeatureAttributeSidecarConfig(
                storage_mode=AttributeStorageMode.JSONB,
                external_id_field=None,  # no external_id → no default sort
            )
        )

        mock_geom = _SidecarLike()
        mock_geom.config.sidecar_id = "geometries"
        mock_geom.sidecar_id = "geometries"
        mock_geom.get_queryable_fields.return_value = {}
        mock_geom.get_join_clause.return_value = (
            'LEFT JOIN "s_test"."t_abc_geometries" sc_geometries'
            " ON h.geoid = sc_geometries.geoid"
        )
        mock_geom.get_default_sort.return_value = None
        mock_geom.get_main_geometry_field.return_value = None
        mock_geom.provides_feature_id = False
        mock_geom.feature_id_field_name = None
        mock_geom.supports_aggregation.return_value = True
        mock_geom.supports_transformation.return_value = True

        mock_registry.get_sidecar.side_effect = (
            lambda sc, lenient=True: (
                mock_geom if getattr(sc, "sidecar_type", "") == "geometries"
                else real_attrs_no_sort
            )
        )

        col_config = _make_col_config()
        optimizer = QueryOptimizer(col_config)

        req = QueryRequest(
            select=[FieldSelection(field="*")],
            filters=[FilterCondition(field="geoid", operator="=", value="abc")],
            raw_where=None,
            include_total_count=False,
        )
        sql, _ = optimizer.build_optimized_query(req, "s_test", "t_abc")

        assert 'LEFT JOIN "s_test"."t_abc_attributes"' in sql
        assert 'INNER JOIN "s_test"."t_abc_attributes"' not in sql
