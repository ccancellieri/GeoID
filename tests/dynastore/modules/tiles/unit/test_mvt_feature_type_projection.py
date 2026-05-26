"""MVT tile path must respect ``ItemsReadPolicy.feature_type``.

Bug-driven regression suite: the live tile path emitted per-feature
properties that bypassed the collection's read-shape contract — raw
geometry WKB hex (``_geom_source``), lowercase JSONB aliases, ``type``,
``id``/``geoid`` — regardless of ``feature_type.expose`` /
``expose_geoid``. ``ST_AsMVT(mvtgeom.*, …, 'geom')`` emits every column of
the inner SELECT as a tile property, so any projection has to happen at
SELECT build time (Python row-mapping doesn't run on the tile path).

These tests pin two layers:

  1. ``project_select_for_feature_type`` — pure helper, the SSOT shared
     with ``/items`` (both paths read the same ``feature_type``).
  2. The full MVT transform pipeline — empty/non-empty ``expose`` and
     ``expose_geoid`` map to the right SELECT/SQL wrap shape.
"""
from __future__ import annotations

from types import SimpleNamespace

from dynastore.models.query_builder import QueryRequest, FieldSelection
from dynastore.modules.storage.computed_fields import FeatureType
from dynastore.modules.storage.read_policy import (
    project_select_for_feature_type,
)
from dynastore.modules.storage.drivers.pg_sidecars import (
    FeatureAttributeSidecarConfig,
    GeometriesSidecarConfig,
)
from dynastore.modules.tiles.query_transform import MVTQueryTransform


def _col_config() -> SimpleNamespace:
    return SimpleNamespace(
        sidecars=[
            GeometriesSidecarConfig(geom_column="geom"),
            FeatureAttributeSidecarConfig(),
        ]
    )


def _mvt_context(feature_type: FeatureType | None = None) -> dict:
    return {
        "geom_format": "MVT",
        "target_srid": 3857,
        "srid": 4326,
        "tile_wkb": b"\x00" * 8,
        "extent": 4096,
        "buffer": 256,
        "col_config": _col_config(),
        "feature_type": feature_type,
    }


# ---------------------------------------------------------------------------
# project_select_for_feature_type — pure unit
# ---------------------------------------------------------------------------


def test_empty_expose_yields_no_property_selects():
    """``expose=[]`` (the default) → no property columns surfaced.

    With no expose entries, no expose_geoid, no expose_created, the helper
    emits an empty SELECT list — the tile carries geometry only.
    """
    ft = FeatureType()
    assert ft.expose == []
    assert ft.expose_geoid is False
    assert ft.expose_created is False

    selects = project_select_for_feature_type(ft)
    assert selects == []


def test_expose_list_yields_one_select_per_name_no_aliasing():
    """``expose=["CODE","NAME"]`` → exactly those two selects.

    No lowercase aliasing, no schema-name normalisation — the helper passes
    the names through verbatim so JSONB extraction (case-sensitive in
    Postgres) finds the original keys.
    """
    ft = FeatureType(expose=["CODE", "NAME"])
    selects = project_select_for_feature_type(ft)
    assert [s.field for s in selects] == ["CODE", "NAME"]
    assert [s.alias for s in selects] == [None, None]


def test_expose_geoid_adds_geoid_select():
    ft = FeatureType(expose_geoid=True)
    selects = project_select_for_feature_type(ft)
    assert [s.field for s in selects] == ["geoid"]


def test_expose_created_aliases_transaction_time():
    ft = FeatureType(expose_created=True)
    selects = project_select_for_feature_type(ft)
    assert len(selects) == 1
    assert selects[0].field == "transaction_time"
    assert selects[0].alias == "created"


def test_combined_flags_compose_select_order():
    ft = FeatureType(
        expose=["CODE"],
        expose_geoid=True,
        expose_created=True,
    )
    selects = project_select_for_feature_type(ft)
    # geoid first, then created, then expose entries.
    assert [s.field for s in selects] == [
        "geoid",
        "transaction_time",
        "CODE",
    ]


# ---------------------------------------------------------------------------
# MVTQueryTransform.post_process_sql — column-restricting wrap
# ---------------------------------------------------------------------------


def test_post_process_sql_no_op_when_feature_type_absent():
    """Without a feature_type in context the wrap is skipped — keeps the
    legacy (non-policy-aware) callers untouched.
    """
    sql_in = "SELECT h.geoid, geom FROM table"
    sql_out, params_out = MVTQueryTransform().post_process_sql(
        sql_in, {}, _mvt_context(feature_type=None)
    )
    assert sql_out == sql_in
    assert params_out == {}


def test_post_process_sql_empty_expose_keeps_only_geom():
    """``expose=[]`` → outer SELECT projects ``geom`` only.

    ST_AsMVT then emits each feature with zero properties — no `_geom_source`
    leak, no `geoid`, no `type`, no JSONB blob.
    """
    sql_in = (
        "SELECT h.geoid, sc_attributes.attributes, ST_AsMVTGeom(...) AS geom "
        "FROM hub h LEFT JOIN sc_attributes ON ..."
    )
    sql_out, _ = MVTQueryTransform().post_process_sql(
        sql_in, {}, _mvt_context(feature_type=FeatureType())
    )
    # The wrap keeps geom only; geoid and the JSONB blob are projected out.
    assert sql_out.startswith('SELECT "geom" FROM (')
    assert "_mvt_inner" in sql_out
    assert '"geoid"' not in sql_out
    assert '"attributes"' not in sql_out


def test_post_process_sql_expose_list_quotes_mixed_case_keys():
    """JSONB keys are case-sensitive in Postgres; the wrap must quote them
    so ``CODE``/``NAME`` survive the planner's lowercase folding."""
    ft = FeatureType(expose=["CODE", "NAME"])
    sql_in = (
        "SELECT h.geoid, sc_attributes.attributes->>'CODE' AS CODE, "
        "sc_attributes.attributes->>'NAME' AS NAME, ST_AsMVTGeom(...) AS geom FROM ..."
    )
    sql_out, _ = MVTQueryTransform().post_process_sql(sql_in, {}, _mvt_context(ft))
    # geom + exposed columns, in helper-defined order: geom first, then expose.
    assert sql_out.startswith('SELECT "geom", "CODE", "NAME" FROM (')
    # No lowercase alias leak.
    assert '"code"' not in sql_out
    assert '"name"' not in sql_out
    # geoid projected away.
    assert '"geoid"' not in sql_out


def test_post_process_sql_expose_geoid_surfaces_geoid_column():
    ft = FeatureType(expose_geoid=True)
    sql_in = "SELECT h.geoid, ST_AsMVTGeom(...) AS geom FROM ..."
    sql_out, _ = MVTQueryTransform().post_process_sql(sql_in, {}, _mvt_context(ft))
    assert sql_out.startswith('SELECT "geom", "geoid" FROM (')


# ---------------------------------------------------------------------------
# Full MVTQueryTransform.transform_query — verifies the placeholder removal
# ---------------------------------------------------------------------------


def test_transform_query_no_geom_source_leak_in_select():
    """The transform must not insert a ``_geom_source`` placeholder.

    The QueryOptimizer joins the geometry sidecar unconditionally via
    ``require_geometry=True``; a placeholder selection of the raw geometry
    column would surface WKB hex as an MVT property (the symptom on the live
    review tile path).
    """
    req = MVTQueryTransform().transform_query(
        QueryRequest(select=[FieldSelection(field="geoid", alias="id")]),
        _mvt_context(),
    )
    # No FieldSelection refers to the raw geometry column or its alias.
    fields = {s.field for s in req.select}
    aliases = {s.alias for s in req.select if s.alias}
    assert "_geom_source" not in aliases
    assert "geom" not in fields  # the raw geom field name
    # The MVT geometry comes from raw_selects as the named ``geom`` column.
    raw = " ".join(req.raw_selects)
    assert raw.rstrip().endswith("AS geom")
