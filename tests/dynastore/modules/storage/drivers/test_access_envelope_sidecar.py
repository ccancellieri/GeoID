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

"""Unit tests for the AccessEnvelopeSidecar (opt-in ABAC PG sub-table scaffolding).

TC-1  access_filter_to_pg_clause: attr predicate maps to JSONB path via sidecar alias.
TC-2  access_filter_to_pg_clause: deny_all → 'FALSE'.
TC-3  access_filter_to_pg_clause: allow_all with no clauses → None (no restriction).
TC-4  access_filter_to_pg_clause: empty allow set → 'FALSE' (fail-closed).
TC-5  access_filter_to_pg_clause: range predicate translates via sidecar alias.
TC-6  prepare_upsert_payload: context with _access_envelope produces correct row dict.
TC-7  apply_query_context: request with access_filter appends WHERE clause.
TC-8  apply_query_context: request without access_filter appends 'FALSE' (fail-closed).
TC-9  sidecar is NOT auto-injected (get_default_config returns None).
TC-10 config round-trip: AccessEnvelopeSidecarConfig serialises / deserialises via discriminator.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Dict, Optional

from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    FieldPredicate,
    RangePredicate,
)
from dynastore.modules.storage.access_filter_pg import access_filter_to_pg_clause
from dynastore.modules.storage.drivers.pg_sidecars.access_envelope import (
    AccessEnvelopeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.access_envelope_config import (
    AccessEnvelopeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarConfigRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sidecar(
    column_name: str = "access_envelope",
    gin_index: bool = True,
) -> AccessEnvelopeSidecar:
    cfg = AccessEnvelopeSidecarConfig(column_name=column_name, gin_index=gin_index)
    return AccessEnvelopeSidecar(config=cfg)


def _make_request(access_filter: Optional[AccessFilter] = None) -> Any:
    """Build a minimal QueryRequest-like object with an access_filter attribute."""
    return SimpleNamespace(
        access_filter=access_filter,
        select=[SimpleNamespace(field="*")],
        filters=[],
        sort=None,
    )


def _empty_context() -> Dict[str, Any]:
    return {
        "joins": [],
        "params": {},
        "select_fields": [],
        "where_conditions": [],
    }


# ---------------------------------------------------------------------------
# TC-1: attr predicate maps to JSONB path expression via sidecar alias
# ---------------------------------------------------------------------------

def test_tc1_attr_predicate_maps_to_jsonb_path_via_sidecar_alias():
    """FieldPredicate on _attrs.dept maps to ae.access_envelope->'attrs'->>'dept'."""
    af = AccessFilter(
        allow=(AccessClause((FieldPredicate("_attrs.dept", ("finance", "global")),)),)
    )
    clause, params = access_filter_to_pg_clause(af, envelope_col="ae.access_envelope")

    assert clause is not None
    assert "ae.access_envelope" in clause
    assert "'dept'" in clause
    # Should have bind parameters for the dept values.
    assert any("dept" in k for k in params)
    vals = next(v for k, v in params.items() if "dept" in k)
    assert set(vals) == {"finance", "global"}


# ---------------------------------------------------------------------------
# TC-2: deny_all → 'FALSE'
# ---------------------------------------------------------------------------

def test_tc2_deny_all_returns_false_literal():
    """deny_all AccessFilter translates to the SQL literal 'FALSE'."""
    af = AccessFilter(deny_all=True)
    clause, params = access_filter_to_pg_clause(af, envelope_col="ae.access_envelope")
    assert clause == "FALSE"
    assert params == {}


# ---------------------------------------------------------------------------
# TC-3: allow_all with no deny clauses → None (no restriction)
# ---------------------------------------------------------------------------

def test_tc3_allow_all_no_clauses_returns_none():
    """allow_all with no deny and no allow clauses → None (caller adds no WHERE)."""
    af = AccessFilter(allow_all=True)
    clause, params = access_filter_to_pg_clause(af, envelope_col="ae.access_envelope")
    assert clause is None
    assert params == {}


# ---------------------------------------------------------------------------
# TC-4: empty allow set (not allow_all) → 'FALSE'
# ---------------------------------------------------------------------------

def test_tc4_empty_allow_set_returns_false():
    """An AccessFilter with no allow clauses and allow_all=False → 'FALSE'."""
    af = AccessFilter(allow=(), allow_all=False)
    clause, params = access_filter_to_pg_clause(af, envelope_col="ae.access_envelope")
    assert clause == "FALSE"
    assert params == {}


# ---------------------------------------------------------------------------
# TC-5: range predicate via sidecar alias
# ---------------------------------------------------------------------------

def test_tc5_range_predicate_translates_via_sidecar_alias():
    """RangePredicate on _attrs.score maps to CAST expression via ae.access_envelope."""
    rp = RangePredicate("_attrs.score", "lte", ("100",))
    af = AccessFilter(allow=(AccessClause((rp,)),))
    clause, params = access_filter_to_pg_clause(af, envelope_col="ae.access_envelope")

    assert clause is not None
    assert "ae.access_envelope" in clause
    assert "score" in clause
    assert "<=" in clause or "lte" in clause.lower()
    # Bind param must carry the bound value.
    assert any("score" in k for k in params)


# ---------------------------------------------------------------------------
# TC-6: prepare_upsert_payload returns correct row dict
# ---------------------------------------------------------------------------

def test_tc6_prepare_upsert_payload_returns_row_dict():
    """prepare_upsert_payload builds the sub-table row from _access_envelope context."""
    sidecar = _make_sidecar()
    context = {
        "geoid": "item-001",
        "_access_envelope": {
            "_visibility": "private",
            "_owner": "alice",
            "_attrs": {"dept": "finance"},
        },
    }
    payload = sidecar.prepare_upsert_payload(feature={}, context=context)

    assert payload is not None
    assert payload["geoid"] == "item-001"
    assert "access_envelope" in payload
    envelope = json.loads(payload["access_envelope"])
    assert envelope["visibility"] == "private"
    assert envelope["owner"] == "alice"
    assert envelope["attrs"]["dept"] == "finance"


def test_tc6_prepare_upsert_payload_returns_none_when_absent():
    """prepare_upsert_payload returns None when _access_envelope is not in context."""
    sidecar = _make_sidecar()
    payload = sidecar.prepare_upsert_payload(feature={}, context={"geoid": "x"})
    assert payload is None


def test_tc6_prepare_upsert_payload_returns_none_when_geoid_missing():
    """prepare_upsert_payload returns None when geoid is not in context."""
    sidecar = _make_sidecar()
    payload = sidecar.prepare_upsert_payload(
        feature={},
        context={"_access_envelope": {"_visibility": "public", "_owner": "bob", "_attrs": {}}},
    )
    assert payload is None


# ---------------------------------------------------------------------------
# TC-7: apply_query_context appends WHERE clause when access_filter is set
# ---------------------------------------------------------------------------

def test_tc7_apply_query_context_appends_where_clause():
    """When access_filter is set, apply_query_context appends a non-empty WHERE clause."""
    sidecar = _make_sidecar()
    af = AccessFilter(
        allow=(AccessClause((FieldPredicate("_attrs.dept", ("finance",)),)),)
    )
    request = _make_request(access_filter=af)
    ctx = _empty_context()

    sidecar.apply_query_context(request, ctx)

    assert len(ctx["where_conditions"]) == 1
    clause = ctx["where_conditions"][0]
    assert "ae.access_envelope" in clause
    assert "dept" in clause


def test_tc7_apply_query_context_adds_params():
    """apply_query_context also populates ctx['params'] with bind values."""
    sidecar = _make_sidecar()
    af = AccessFilter(
        allow=(AccessClause((FieldPredicate("_attrs.role", ("admin",)),)),)
    )
    request = _make_request(access_filter=af)
    ctx = _empty_context()

    sidecar.apply_query_context(request, ctx)

    assert any("role" in k for k in ctx["params"])


def test_tc7_allow_all_filter_adds_no_where():
    """An allow_all filter produces no WHERE clause (access_filter_to_pg_clause returns None)."""
    sidecar = _make_sidecar()
    af = AccessFilter(allow_all=True)
    request = _make_request(access_filter=af)
    ctx = _empty_context()

    sidecar.apply_query_context(request, ctx)

    # allow_all → clause is None → no where_conditions added
    assert ctx["where_conditions"] == []


# ---------------------------------------------------------------------------
# TC-8: apply_query_context fails closed when access_filter is None
# ---------------------------------------------------------------------------

def test_tc8_apply_query_context_fails_closed_when_no_filter():
    """When access_filter is None, apply_query_context appends 'FALSE' (fail-closed)."""
    sidecar = _make_sidecar()
    request = _make_request(access_filter=None)
    ctx = _empty_context()

    sidecar.apply_query_context(request, ctx)

    assert "FALSE" in ctx["where_conditions"]


# ---------------------------------------------------------------------------
# TC-9: sidecar is NOT auto-injected
# ---------------------------------------------------------------------------

def test_tc9_sidecar_is_not_auto_injected():
    """AccessEnvelopeSidecar.get_default_config returns None (opt-in only)."""
    result = AccessEnvelopeSidecar.get_default_config(context={})
    assert result is None

    result_stac = AccessEnvelopeSidecar.get_default_config(context={"stac_context": True})
    assert result_stac is None


# ---------------------------------------------------------------------------
# TC-10: config round-trip via discriminator
# ---------------------------------------------------------------------------

def test_tc10_config_round_trip_via_discriminator():
    """AccessEnvelopeSidecarConfig round-trips through model_dump / model_validate."""
    cfg = AccessEnvelopeSidecarConfig(gin_index=True, known_attrs_keys=["dept", "role"])

    dumped = cfg.model_dump()
    assert dumped["sidecar_type"] == "access_envelope"

    # Round-trip via the registry (discriminator-based deserialization).
    reloaded_cls = SidecarConfigRegistry.resolve_config_class("access_envelope")
    assert reloaded_cls is AccessEnvelopeSidecarConfig

    reloaded = reloaded_cls.model_validate(dumped)
    assert reloaded.sidecar_type == "access_envelope"
    assert reloaded.gin_index is True
    assert "dept" in reloaded.known_attrs_keys


def test_tc10_discriminator_retained_on_default_construction():
    """sidecar_type is retained in model_dump(exclude_unset=True) after default construction."""
    cfg = AccessEnvelopeSidecarConfig()
    dumped = cfg.model_dump(exclude_unset=True)
    # The _retain_sidecar_type_on_dump validator must have added sidecar_type.
    assert "sidecar_type" in dumped
    assert dumped["sidecar_type"] == "access_envelope"


# ---------------------------------------------------------------------------
# Additional structural tests
# ---------------------------------------------------------------------------

def test_sidecar_id_and_type():
    """sidecar_id and sidecar_type_id both return 'access_envelope'."""
    sidecar = _make_sidecar()
    assert sidecar.sidecar_id == "access_envelope"
    assert sidecar.sidecar_type_id == "access_envelope"


def test_is_not_mandatory():
    """is_mandatory() returns False (opt-in)."""
    assert _make_sidecar().is_mandatory() is False


def test_get_select_fields_returns_empty():
    """get_select_fields returns [] — envelope is WHERE-only."""
    sidecar = _make_sidecar()
    assert sidecar.get_select_fields() == []


def test_map_row_to_feature_is_noop():
    """map_row_to_feature does not touch feature.properties (security)."""
    from geojson_pydantic import Feature as GeoFeature
    from dynastore.modules.storage.drivers.pg_sidecars.base import FeaturePipelineContext

    sidecar = _make_sidecar()
    feature = GeoFeature(type="Feature", geometry=None, properties={})
    ctx = FeaturePipelineContext()
    sidecar.map_row_to_feature(
        row={"access_envelope": '{"visibility":"private"}'},
        feature=feature,
        context=ctx,
    )
    # envelope must NOT leak into properties
    assert "access_envelope" not in (feature.properties or {})
    assert "visibility" not in (feature.properties or {})


def test_get_queryable_fields_expose_false():
    """The access_envelope field is expose=False — never in public API responses."""
    sidecar = _make_sidecar()
    fields = sidecar.get_queryable_fields()
    assert "access_envelope" in fields
    assert fields["access_envelope"].expose is False


def test_ddl_contains_subtable_name():
    """get_ddl produces SQL referencing the correct sub-table name."""
    sidecar = _make_sidecar()
    ddl = sidecar.get_ddl("t_abc123")
    assert "t_abc123_access_envelope" in ddl
    assert "CREATE TABLE IF NOT EXISTS" in ddl
    assert "PRIMARY KEY (geoid)" in ddl
    # GIN index should be present (gin_index=True by default).
    assert "USING GIN" in ddl


def test_ddl_no_gin_index_when_disabled():
    """GIN index is omitted when gin_index=False."""
    sidecar = _make_sidecar(gin_index=False)
    ddl = sidecar.get_ddl("t_xyz")
    assert "USING GIN" not in ddl


def test_get_join_clause():
    """get_join_clause returns a LEFT JOIN on the correct sub-table with alias 'ae'."""
    sidecar = _make_sidecar()
    clause = sidecar.get_join_clause(schema="myschema", hub_table="t_abc123")
    assert "t_abc123_access_envelope" in clause
    assert "ae" in clause
    assert "LEFT JOIN" in clause
    assert "h.geoid = ae.geoid" in clause
