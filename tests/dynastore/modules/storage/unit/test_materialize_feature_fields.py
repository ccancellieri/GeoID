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

"""Unit tests for the cross-driver projection contract (#1291 / #1285 D5).

``materialize_feature_fields(schema, policy)`` is the single source of truth for
the full materialized field set a driver's ``ensure_storage`` must hold: the
column-materialised schema fields PLUS the policy-derived special fields.

These tests assert:

* the schema-field subset matches the PG bridge column-synthesis precedence
  (so PG and the projection cannot diverge);
* every policy-derived special family is projected with the right name + type;
* the function is robust to ``None`` schema / policy and to an empty config.
"""
from __future__ import annotations

from dynastore.models.protocols.field_definition import (
    FieldAccess,
    FieldCapability,
    FieldDefinition,
)
from dynastore.modules.storage.computed_fields import (
    DeriveSpec,
    GeometryStat,
    SpatialCell,
)
from dynastore.modules.storage.computed_fields import ComputedKind
from dynastore.modules.storage.driver_config import ItemsSchema, ItemsWritePolicy
from dynastore.modules.storage.field_projection import materialize_feature_fields
from dynastore.modules.storage.validity import ValiditySpec


def _fd(name, *, data_type="string", access=FieldAccess.AUTO, caps=None,
        required=False, unique=False):
    return FieldDefinition(
        name=name, data_type=data_type, access=access,
        capabilities=caps or [], required=required, unique=unique,
    )


# --------------------------------------------------------------------------- #
# Schema-field subset mirrors the bridge column-synthesis precedence.
# --------------------------------------------------------------------------- #

class TestSchemaFieldSubset:
    def test_fast_field_included_compact_excluded(self):
        schema = ItemsSchema(fields={
            "code": _fd("code", access=FieldAccess.FAST),
            "note": _fd("note", access=FieldAccess.COMPACT),
        })
        out = materialize_feature_fields(schema, None)
        assert "code" in out
        assert "note" not in out

    def test_geometry_never_projected(self):
        schema = ItemsSchema(fields={
            "geom": _fd("geom", data_type="geometry", access=FieldAccess.FAST),
        })
        out = materialize_feature_fields(schema, None)
        assert "geom" not in out

    def test_auto_with_capability_included(self):
        schema = ItemsSchema(fields={
            "x": _fd("x", access=FieldAccess.AUTO, caps=[FieldCapability.FILTERABLE]),
            "y": _fd("y", access=FieldAccess.AUTO),  # no cap → JSONB
        })
        out = materialize_feature_fields(schema, None)
        assert "x" in out
        assert "y" not in out

    def test_constraint_field_included(self):
        schema = ItemsSchema(fields={
            "u": _fd("u", access=FieldAccess.COMPACT, unique=True),
        })
        out = materialize_feature_fields(schema, None)
        assert "u" in out  # hard constraint beats COMPACT

    def test_default_access_fast_lifts_plain_fields(self):
        schema = ItemsSchema(
            default_access=FieldAccess.FAST,
            fields={"plain": _fd("plain", access=FieldAccess.AUTO)},
        )
        out = materialize_feature_fields(schema, None)
        assert "plain" in out

    def test_field_name_pinned_to_dict_key(self):
        schema = ItemsSchema(fields={
            "renamed": _fd("original_name", access=FieldAccess.FAST),
        })
        out = materialize_feature_fields(schema, None)
        assert out["renamed"].name == "renamed"


# --------------------------------------------------------------------------- #
# Policy-derived special fields.
# --------------------------------------------------------------------------- #

class TestPolicyDerivedFields:
    def test_external_id_asset_id_hashes_cells(self):
        policy = ItemsWritePolicy(
            derive=DeriveSpec(
                external_id="properties.code",
                content_hashes=["geometry", "attributes"],
                spatial_cells=[SpatialCell(grid="geohash", resolution=7)],
            ),
            track_asset_id=True,
        )
        out = materialize_feature_fields(None, policy)
        assert "external_id" in out
        assert "asset_id" in out
        assert "geometry_hash" in out
        assert "attributes_hash" in out
        assert "geohash_7" in out
        # Identity-style keys are exact-match queryable but not publicly exposed.
        assert FieldCapability.FILTERABLE in out["external_id"].capabilities
        assert out["external_id"].expose is False

    def test_geometry_stat_is_numeric(self):
        from dynastore.modules.storage.computed_fields import StatisticStorageMode
        policy = ItemsWritePolicy(
            derive=DeriveSpec(
                geometry_stats=[
                    GeometryStat(stat=ComputedKind.AREA, store=StatisticStorageMode.COLUMNAR),
                ],
            ),
        )
        out = materialize_feature_fields(None, policy)
        assert "area" in out
        assert out["area"].data_type == "double"
        assert FieldCapability.SORTABLE in out["area"].capabilities

    def test_validity_projects_bounds(self):
        policy = ItemsWritePolicy(validity=ValiditySpec())
        out = materialize_feature_fields(None, policy)
        assert out["valid_from"].data_type == "timestamp"
        assert out["valid_to"].data_type == "timestamp"

    def test_no_validity_no_bounds(self):
        policy = ItemsWritePolicy(validity=None)
        out = materialize_feature_fields(None, policy)
        assert "valid_from" not in out
        assert "valid_to" not in out

    def test_no_asset_id_when_untracked(self):
        policy = ItemsWritePolicy(track_asset_id=False)
        out = materialize_feature_fields(None, policy)
        assert "asset_id" not in out


# --------------------------------------------------------------------------- #
# Combined + edge cases.
# --------------------------------------------------------------------------- #

class TestCombinedAndEdges:
    def test_schema_plus_policy_union(self):
        schema = ItemsSchema(fields={"code": _fd("code", access=FieldAccess.FAST)})
        policy = ItemsWritePolicy(
            derive=DeriveSpec(external_id="properties.code"),
            track_asset_id=True,
        )
        out = materialize_feature_fields(schema, policy)
        assert {"code", "external_id", "asset_id"} <= set(out)

    def test_authored_field_wins_on_name_collision(self):
        # An authored schema field named like a special field is kept as authored.
        schema = ItemsSchema(fields={
            "external_id": _fd("external_id", access=FieldAccess.FAST, caps=[FieldCapability.SORTABLE]),
        })
        policy = ItemsWritePolicy(derive=DeriveSpec(external_id="properties.code"))
        out = materialize_feature_fields(schema, policy)
        # The authored definition (with SORTABLE) is preserved, not overwritten.
        assert FieldCapability.SORTABLE in out["external_id"].capabilities

    def test_both_none_is_empty(self):
        assert materialize_feature_fields(None, None) == {}

    def test_empty_schema_default_policy_yields_default_special_fields(self):
        # The default ItemsWritePolicy tracks asset_id, carries a default
        # external_id identity axis, and (since #3155) derives every
        # geometry/place statistic — so the projection surfaces the special
        # fields plus the stored stats even with an empty schema. No
        # user-data fields appear.
        from dynastore.modules.storage.computed_fields import default_derive_spec

        out = materialize_feature_fields(ItemsSchema(), ItemsWritePolicy())
        stat_names = {
            cf.resolved_name for cf in default_derive_spec().to_computed_fields()
        }
        assert set(out) == {"external_id", "asset_id"} | stat_names
        # The stored stats are queryable out of the box.
        assert FieldCapability.FILTERABLE in out["area"].capabilities
        assert FieldCapability.SORTABLE in out["vertex_count"].capabilities

    def test_no_asset_id_no_external_id_minimal_policy(self):
        # A policy with no identity axis and no asset tracking projects nothing.
        policy = ItemsWritePolicy(track_asset_id=False, identity=[])
        out = materialize_feature_fields(ItemsSchema(), policy)
        assert "asset_id" not in out
