"""Regression guard: an MVT tile with feature_type.expose=["area"] (a COLUMNAR
geodesic stat) must not produce two ``area`` output columns in the inner SELECT.

Before the fix, two paths injected the same logical column with different SQL:
  1. The explicit else-branch of build_optimized_query processes
     FieldSelection(field="area") and appends ``sc_geometries.area as area``
     (field_def.sql_expression + " as " + alias).
  2. GeometriesSidecar.apply_query_context calls get_select_fields in selective
     mode, whose COLUMNAR non-CENTROID stat branch appended a bare
     ``sc_geometries.area`` (no alias) — the only named column the method emitted
     unaliased.
  3. The optimizer's dedup is string-exact, so
     "sc_geometries.area as area" != "sc_geometries.area" and both survived; the
     wrapping ``SELECT "area" FROM (inner) _mvt_inner`` then raised PostgreSQL
     ``column reference "area" is ambiguous`` (asyncpg.exceptions.AmbiguousColumnError).

The fix aliases the COLUMNAR stat column at the source so get_select_fields emits
``sc_geometries.area as area`` like every sibling column; the string-exact dedup
then collapses the duplicate. This test asserts ``area`` appears as an output
column EXACTLY ONCE — it failed before the fix and guards against regression.
"""

import pytest

from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.models.query_builder import FieldSelection, QueryRequest
from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    StatisticStorageMode,
)
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
    TargetDimension,
)


# ---------------------------------------------------------------------------
# Real-registry fixture (mirrors test_query_optimizer._real_sidecar_registry)
# ---------------------------------------------------------------------------


@pytest.fixture
def real_registry():
    """Warm up the default sidecar registry and yield it."""
    from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

    SidecarRegistry._ensure_defaults()
    yield SidecarRegistry


# ---------------------------------------------------------------------------
# Collection config: a geometry sidecar with COLUMNAR area stat
# ---------------------------------------------------------------------------


def _col_config_with_columnar_area() -> ItemsPostgresqlDriverConfig:
    """ItemsPostgresqlDriverConfig with a single GeometriesSidecar that
    carries a COLUMNAR AREA ComputedField — the exact shape that triggers
    the duplicate-column bug.
    """
    area_field = ComputedField(
        kind=ComputedKind.AREA,
        storage_mode=StatisticStorageMode.COLUMNAR,
    )
    geom_cfg = GeometriesSidecarConfig(
        sidecar_type="geometries",
        target_srid=4326,
        target_dimension=TargetDimension.FORCE_2D,
        partition_strategy=None,
        partition_resolution=0,
        statistics=None,
        compute_fields_overlay=[area_field],
    )
    return ItemsPostgresqlDriverConfig(sidecars=[geom_cfg])


# ---------------------------------------------------------------------------
# Reproduction test
# ---------------------------------------------------------------------------


def test_columnar_area_expose_no_duplicate_in_select(real_registry):
    """expose=["area"] + a COLUMNAR area stat must yield exactly one ``area``
    output column. Before the fix, ``"sc_geometries.area as area"`` (from the
    explicit FieldSelection else-branch) and the bare ``"sc_geometries.area"``
    (from GeometriesSidecar.apply_query_context -> get_select_fields selective
    mode) both survived the string-exact dedup, and the outer
    ``SELECT "area" FROM (inner) _mvt_inner`` added by
    MVTQueryTransform.post_process_sql failed with PostgreSQL
    ``column reference "area" is ambiguous``.

    With the source-level alias fix the two strings are identical and the dedup
    collapses them. This assertion failed before the fix; it now passes.
    """
    col_config = _col_config_with_columnar_area()
    optimizer = QueryOptimizer(col_config)

    # Simulate what project_select_for_feature_type emits when
    # feature_type.expose=["area"] (no schema fields, only the computed stat).
    req = QueryRequest(
        select=[FieldSelection(field="area")],
        raw_where=None,
        include_total_count=False,
    )

    sql, _ = optimizer.build_optimized_query(req, "test_schema", "test_table")

    # Extract the inner SELECT list (everything between SELECT and FROM).
    # The full SQL is:
    #   SELECT <fields> FROM "test_schema"."test_table" h ...
    # We only need to count how many times `area` appears as an *output* column.
    #
    # Count occurrences of `area` as an alias or bare column in the SELECT list.
    # Both ``... as area`` and the bare ``sc_geometries.area`` (which becomes
    # the output column name `area` after PostgreSQL strips the table qualifier)
    # are bugs we want to catch.
    sql_lower = sql.lower()

    # Isolate the SELECT projection (up to first FROM keyword).
    from_idx = sql_lower.find(" from ")
    assert from_idx != -1, f"No FROM found in SQL:\n{sql}"
    select_clause = sql_lower[:from_idx]

    # Count how many times `area` appears as an output column name.
    # `as area` covers the aliased form; bare `sc_geometries.area` also
    # contributes an implicit `area` output column (PostgreSQL uses the
    # column name after the dot).
    import re

    # Match explicit aliases: `... as area` or `... as "area"` (quoted form, see #719)
    explicit_area = re.findall(r'\bas\s+"?area"?\b', select_clause)
    # Match bare unaliased: `sc_geometries.area` (no following `as`)
    bare_area = re.findall(r"sc_geometries\.area(?!\s+as\b)", select_clause)

    total_area_outputs = len(explicit_area) + len(bare_area)

    # Capture the full SQL for the failure message.
    assert total_area_outputs == 1, (
        f"Expected exactly 1 'area' output column in the inner SELECT, "
        f"found {total_area_outputs} "
        f"(explicit aliases: {len(explicit_area)}, bare columns: {len(bare_area)}).\n"
        f"This is the ambiguous-column bug: both 'sc_geometries.area as area' "
        f"and bare 'sc_geometries.area' survive the string-exact dedup.\n"
        f"Full SQL:\n{sql}"
    )


# ---------------------------------------------------------------------------
# H3/S2 index columns share the EXACT dual-emission shape that caused the
# ``area`` ambiguous-column 500: they are registered in the field index (so the
# explicit FieldSelection path emits ``... as h3_resN``) and re-projected by
# GeometriesSidecar.get_select_fields (which emitted a bare ``sc_geometries.h3_resN``
# before the fix). They were never reported live only because index columns are
# rarely exposed via feature_type. This guards the whole class, not just ``area``.
# ---------------------------------------------------------------------------


def _count_output_columns(sql: str, name: str, table: str = "sc_geometries") -> int:
    """Count occurrences of ``name`` as an output column in the SELECT
    projection — both the explicit ``as {name}`` form and the bare
    ``{table}.{name}`` form (PostgreSQL names the output column after the dot).
    """
    import re

    sql_lower = sql.lower()
    from_idx = sql_lower.find(" from ")
    assert from_idx != -1, f"No FROM found in SQL:\n{sql}"
    select_clause = sql_lower[:from_idx]
    # Accept both quoted (as "name", see #719) and unquoted (as name) forms.
    explicit = re.findall(rf'\bas\s+"?{re.escape(name)}"?\b', select_clause)
    bare = re.findall(
        rf"{re.escape(table)}\.{re.escape(name)}(?!\s+as\b)", select_clause
    )
    return len(explicit) + len(bare)


def _col_config_with_h3(res: int = 5) -> ItemsPostgresqlDriverConfig:
    """Geometry sidecar carrying a single H3 spatial-cell column at ``res``."""
    h3_field = ComputedField(kind=ComputedKind.H3, resolution=res)
    geom_cfg = GeometriesSidecarConfig(
        sidecar_type="geometries",
        target_srid=4326,
        target_dimension=TargetDimension.FORCE_2D,
        partition_strategy=None,
        partition_resolution=0,
        statistics=None,
        spatial_cells_overlay=[h3_field],
    )
    return ItemsPostgresqlDriverConfig(sidecars=[geom_cfg])


def test_h3_index_expose_no_duplicate_in_select(real_registry):
    """expose=["h3_res5"] must yield exactly one ``h3_res5`` output column.

    Same structural trap as ``area``: the explicit FieldSelection path emits
    ``sc_geometries.h3_res5 as h3_res5`` while get_select_fields used to emit a
    bare ``sc_geometries.h3_res5``; both survived the string-exact dedup. The
    fix aliases the index column at the source AND deduplicates by logical
    output name, so only one column reaches the wrapping MVT SELECT.
    """
    col_config = _col_config_with_h3(5)
    optimizer = QueryOptimizer(col_config)
    req = QueryRequest(
        select=[FieldSelection(field="h3_res5")],
        raw_where=None,
        include_total_count=False,
    )
    sql, _ = optimizer.build_optimized_query(req, "test_schema", "test_table")
    count = _count_output_columns(sql, "h3_res5")
    assert count == 1, (
        f"Expected exactly 1 'h3_res5' output column, found {count}.\nFull SQL:\n{sql}"
    )
