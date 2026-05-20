#    Copyright 2025 FAO
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

"""Attribute-derived statistics (``ATTRIBUTE_STAT``) — Phase 4 (#1074).

The compute-preset / ``ComputedField`` pipeline already materialises geometry
statistics into the geometries sidecar. This is the distinct attribute axis: a
value read from the feature's own ``properties`` (e.g. ``properties.population``)
promoted into the attributes sidecar. These tests pin the four halves:

1. the ``ComputedField`` surface (``source`` validation, ``resolved_name``);
2. ``target_sidecar`` classification (the write-time split);
3. ``compute_attribute_derived_fields`` (path extraction);
4. the attributes sidecar's storage-shape mirror of the geometries sidecar —
   DDL, upsert payload, and the read-side expose hooks consumed by the shared
   ``SidecarProtocol.apply_exposed_computed_values`` loop.
"""

import json

import pytest
from geojson_pydantic import Feature

from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    FeatureType,
    SidecarTarget,
    StatisticStorageMode,
    target_sidecar,
)
from dynastore.tools.geospatial import compute_attribute_derived_fields
from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FeaturePipelineContext,
    SidecarProtocol,
)
from dynastore.modules.storage.read_policy import ItemsReadPolicy


# --- helpers ---------------------------------------------------------------


def _attr_field(
    source="properties.population",
    name=None,
    mode=StatisticStorageMode.COLUMNAR,
    indexed=False,
) -> ComputedField:
    return ComputedField(
        kind=ComputedKind.ATTRIBUTE_STAT,
        source=source,
        name=name,
        storage_mode=mode,
        indexed=indexed,
    )


def _sidecar(*fields, storage_mode=AttributeStorageMode.JSONB) -> FeatureAttributeSidecar:
    cfg = FeatureAttributeSidecarConfig(
        storage_mode=storage_mode,
        compute_fields_overlay=list(fields),
    )
    return FeatureAttributeSidecar(cfg)


def _read_ctx(*expose, failure_mode="best_effort") -> FeaturePipelineContext:
    ctx = FeaturePipelineContext()
    ctx["_items_read_policy"] = ItemsReadPolicy(
        feature_type=FeatureType(expose=list(expose), failure_mode=failure_mode)
    )
    return ctx


# --- ComputedField surface -------------------------------------------------


class TestAttributeStatComputedField:
    def test_requires_source(self) -> None:
        with pytest.raises(ValueError):
            ComputedField(
                kind=ComputedKind.ATTRIBUTE_STAT,
                storage_mode=StatisticStorageMode.COLUMNAR,
            )

    def test_source_forbidden_on_other_kinds(self) -> None:
        with pytest.raises(ValueError):
            ComputedField(
                kind=ComputedKind.AREA,
                source="properties.x",
                storage_mode=StatisticStorageMode.COLUMNAR,
            )

    def test_resolved_name_from_source_tail(self) -> None:
        assert _attr_field(source="properties.population").resolved_name == "population"

    def test_resolved_name_bare_path(self) -> None:
        assert _attr_field(source="population").resolved_name == "population"

    def test_resolved_name_explicit_override(self) -> None:
        assert _attr_field(source="properties.pop", name="pop_count").resolved_name == "pop_count"

    def test_storage_mode_allowed(self) -> None:
        # ATTRIBUTE_STAT is a statistic kind → may carry storage_mode.
        assert _attr_field(mode=StatisticStorageMode.JSONB).storage_mode == StatisticStorageMode.JSONB

    def test_jsonb_plus_indexed_rejected(self) -> None:
        with pytest.raises(ValueError):
            _attr_field(mode=StatisticStorageMode.JSONB, indexed=True)


class TestTargetSidecar:
    def test_attribute_stat_to_attributes(self) -> None:
        assert target_sidecar(ComputedKind.ATTRIBUTE_STAT) == SidecarTarget.ATTRIBUTES

    @pytest.mark.parametrize(
        "kind",
        [
            ComputedKind.AREA,
            ComputedKind.PERIMETER,
            ComputedKind.CENTROID,
            ComputedKind.H3,
            ComputedKind.SURFACE_AREA,
            ComputedKind.Z_RANGE,
        ],
    )
    def test_geometry_and_place_kinds_to_geometry(self, kind) -> None:
        assert target_sidecar(kind) == SidecarTarget.GEOMETRY

    def test_split_partitions_mixed_compute(self) -> None:
        fields = [
            ComputedField(kind=ComputedKind.AREA, storage_mode=StatisticStorageMode.COLUMNAR),
            _attr_field(source="properties.pop", name="pop"),
        ]
        geom = [f for f in fields if target_sidecar(f.kind) == SidecarTarget.GEOMETRY]
        attr = [f for f in fields if target_sidecar(f.kind) == SidecarTarget.ATTRIBUTES]
        assert [f.kind for f in geom] == [ComputedKind.AREA]
        assert [f.kind for f in attr] == [ComputedKind.ATTRIBUTE_STAT]


# --- compute helper --------------------------------------------------------


class TestComputeAttributeDerivedFields:
    def test_extracts_value(self) -> None:
        assert compute_attribute_derived_fields(
            {"population": 99}, [_attr_field(source="properties.population")]
        ) == {"population": 99}

    def test_missing_path_skipped(self) -> None:
        assert compute_attribute_derived_fields(
            {"other": 1}, [_attr_field(source="properties.population")]
        ) == {}

    def test_nested_path(self) -> None:
        assert compute_attribute_derived_fields(
            {"stats": {"count": 7}}, [_attr_field(source="properties.stats.count")]
        ) == {"count": 7}

    def test_ignores_non_attribute_kinds(self) -> None:
        geo = ComputedField(kind=ComputedKind.AREA, storage_mode=StatisticStorageMode.COLUMNAR)
        out = compute_attribute_derived_fields(
            {"pop": 5}, [_attr_field(source="properties.pop", name="pop"), geo]
        )
        assert out == {"pop": 5}


# --- DDL -------------------------------------------------------------------


class TestAttributeStatDDL:
    def test_columnar_emits_typed_column(self) -> None:
        ddl = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.COLUMNAR)).get_ddl("phys")
        assert '"pop" DOUBLE PRECISION' in ddl
        assert "attribute_stats" not in ddl

    def test_columnar_indexed_emits_btree(self) -> None:
        ddl = _sidecar(
            _attr_field(name="pop", mode=StatisticStorageMode.COLUMNAR, indexed=True)
        ).get_ddl("phys")
        assert 'idx_phys_attributes_pop' in ddl

    def test_jsonb_emits_shared_column(self) -> None:
        ddl = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.JSONB)).get_ddl("phys")
        assert "attribute_stats JSONB" in ddl
        assert '"pop" DOUBLE PRECISION' not in ddl

    def test_no_stats_no_columns(self) -> None:
        assert "attribute_stats" not in _sidecar().get_ddl("phys")


# --- upsert ----------------------------------------------------------------


class TestAttributeStatUpsert:
    def test_columnar_value_in_payload(self) -> None:
        sc = _sidecar(_attr_field(source="properties.population", name="pop"))
        payload = sc.prepare_upsert_payload(
            {"id": "f1", "properties": {"population": 500}}, {"geoid": "g1"}
        )
        assert payload["pop"] == 500

    def test_jsonb_value_in_blob(self) -> None:
        sc = _sidecar(
            _attr_field(source="properties.population", name="pop", mode=StatisticStorageMode.JSONB)
        )
        payload = sc.prepare_upsert_payload(
            {"id": "f1", "properties": {"population": 500}}, {"geoid": "g1"}
        )
        assert json.loads(payload["attribute_stats"]) == {"pop": 500}

    def test_missing_source_absent_from_payload(self) -> None:
        sc = _sidecar(_attr_field(source="properties.population", name="pop"))
        payload = sc.prepare_upsert_payload(
            {"id": "f1", "properties": {"other": 1}}, {"geoid": "g1"}
        )
        assert "pop" not in payload


# --- read projection -------------------------------------------------------


class TestAttributeStatProjection:
    def test_select_full_includes_columnar(self) -> None:
        fields = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.COLUMNAR)).get_select_fields(
            include_all=True
        )
        assert any('"pop"' in f for f in fields)

    def test_select_full_includes_jsonb_blob(self) -> None:
        fields = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.JSONB)).get_select_fields(
            include_all=True
        )
        assert any("attribute_stats" in f for f in fields)

    def test_field_definitions_columnar(self) -> None:
        defs = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.COLUMNAR)).get_field_definitions()
        assert "pop" in defs


# --- expose hooks + shared loop --------------------------------------------


class TestAttributeStatExposeHooks:
    def test_producible_names(self) -> None:
        sc = _sidecar(
            _attr_field(source="properties.pop", name="pop"),
            _attr_field(source="properties.gdp", name="gdp"),
        )
        assert sc.producible_computed_names() == {"pop", "gdp"}

    def test_resolve_columnar_found(self) -> None:
        sc = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.COLUMNAR))
        assert sc.resolve_computed_value({"pop": 7}, "pop") == (True, 7)

    def test_resolve_columnar_missing(self) -> None:
        sc = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.COLUMNAR))
        assert sc.resolve_computed_value({}, "pop") == (False, None)

    def test_resolve_jsonb_dict_blob(self) -> None:
        sc = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.JSONB))
        assert sc.resolve_computed_value({"attribute_stats": {"pop": 9}}, "pop") == (True, 9)

    def test_resolve_jsonb_string_blob(self) -> None:
        sc = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.JSONB))
        assert sc.resolve_computed_value({"attribute_stats": '{"pop": 9}'}, "pop") == (True, 9)

    def test_resolve_unknown_name(self) -> None:
        sc = _sidecar(_attr_field(name="pop"))
        assert sc.resolve_computed_value({"pop": 1}, "not_mine") == (False, None)


class TestAttributeStatExposeLoop:
    def test_exposed_value_merged(self) -> None:
        sc = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.COLUMNAR))
        feature = Feature(type="Feature", geometry=None, properties={})
        SidecarProtocol.apply_exposed_computed_values(
            [sc], {"geoid": "g1", "pop": 42}, feature, _read_ctx("pop")
        )
        assert feature.properties["pop"] == 42

    def test_jsonb_value_merged(self) -> None:
        sc = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.JSONB))
        feature = Feature(type="Feature", geometry=None, properties={})
        SidecarProtocol.apply_exposed_computed_values(
            [sc], {"geoid": "g1", "attribute_stats": {"pop": 42}}, feature, _read_ctx("pop")
        )
        assert feature.properties["pop"] == 42

    def test_non_exposed_not_leaked(self) -> None:
        sc = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.COLUMNAR))
        feature = Feature(type="Feature", geometry=None, properties={})
        SidecarProtocol.apply_exposed_computed_values(
            [sc], {"geoid": "g1", "pop": 42}, feature, _read_ctx()
        )
        assert "pop" not in (feature.properties or {})

    def test_strict_raises_on_missing(self) -> None:
        sc = _sidecar(_attr_field(name="pop", mode=StatisticStorageMode.COLUMNAR))
        feature = Feature(type="Feature", geometry=None, properties={})
        with pytest.raises(ValueError):
            SidecarProtocol.apply_exposed_computed_values(
                [sc], {"geoid": "g1"}, feature, _read_ctx("pop", failure_mode="strict")
            )
