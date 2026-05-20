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

"""Runtime exposure of ``ItemsReadPolicy.feature_type.expose`` computed values.

Storage-bearing computed fields (area, perimeter, ...) are projected into the
read row by ``get_select_fields``. Surfacing the ``expose`` subset onto
``feature.properties`` happens ONCE at the pipeline level via
``SidecarProtocol.apply_exposed_computed_values``; each sidecar contributes only
its storage-layout knowledge through ``producible_computed_names`` /
``resolve_computed_value``. These tests pin both halves: the geometry sidecar's
hooks, and the shared exposure loop (including ``failure_mode`` and the
cross-sidecar producer resolution).
"""

import pytest

from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    FeatureType,
    StatisticStorageMode,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FeaturePipelineContext,
    SidecarProtocol,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
    GeometriesSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.read_policy import ItemsReadPolicy

from geojson_pydantic import Feature


def _columnar_sidecar() -> GeometriesSidecar:
    """A sidecar declaring two COLUMNAR computed fields: area + perimeter."""
    cfg = GeometriesSidecarConfig(
        compute_fields_overlay=[
            ComputedField(
                kind=ComputedKind.AREA,
                storage_mode=StatisticStorageMode.COLUMNAR,
            ),
            ComputedField(
                kind=ComputedKind.PERIMETER,
                storage_mode=StatisticStorageMode.COLUMNAR,
            ),
        ]
    )
    return GeometriesSidecar(cfg)


def _jsonb_sidecar() -> GeometriesSidecar:
    cfg = GeometriesSidecarConfig(
        compute_fields_overlay=[
            ComputedField(
                kind=ComputedKind.AREA,
                storage_mode=StatisticStorageMode.JSONB,
            ),
        ]
    )
    return GeometriesSidecar(cfg)


class _StubSidecar:
    """Duck-typed sidecar exposing only the two read-shape hooks.

    The exposure loop resolves producers by ``producible_computed_names`` and
    reads values via ``resolve_computed_value`` — so a foreign sidecar can be
    represented without implementing the full ``SidecarProtocol`` ABC.
    """

    def __init__(self, values: dict) -> None:
        self._values = dict(values)

    def producible_computed_names(self) -> set:
        return set(self._values)

    def resolve_computed_value(self, row, resolved_name):
        if resolved_name in self._values:
            return (True, self._values[resolved_name])
        return (False, None)


def _ctx_with_policy(read_policy: ItemsReadPolicy) -> FeaturePipelineContext:
    ctx = FeaturePipelineContext()
    ctx["_items_read_policy"] = read_policy
    return ctx


def _blank_feature(properties=None) -> Feature:
    return Feature(type="Feature", geometry=None, properties=properties or {})


def _apply(sidecars, row, feature, policy=None, ctx=None):
    ctx = ctx if ctx is not None else (_ctx_with_policy(policy) if policy else FeaturePipelineContext())
    SidecarProtocol.apply_exposed_computed_values(sidecars, row, feature, ctx)


class TestGeometrySidecarHooks:
    """The geometry sidecar owns only storage-layout knowledge now."""

    def test_producible_names(self) -> None:
        assert _columnar_sidecar().producible_computed_names() == {"area", "perimeter"}

    def test_resolve_columnar_found(self) -> None:
        found, value = _columnar_sidecar().resolve_computed_value(
            {"area": 12.5}, "area"
        )
        assert (found, value) == (True, 12.5)

    def test_resolve_columnar_missing_column(self) -> None:
        assert _columnar_sidecar().resolve_computed_value({}, "area") == (False, None)

    def test_resolve_jsonb_found(self) -> None:
        found, value = _jsonb_sidecar().resolve_computed_value(
            {"geom_stats": {"area": 42.0}}, "area"
        )
        assert (found, value) == (True, 42.0)

    def test_resolve_unknown_name(self) -> None:
        assert _columnar_sidecar().resolve_computed_value(
            {"area": 1.0}, "not_mine"
        ) == (False, None)

    def test_map_row_does_not_self_merge(self) -> None:
        # Exposure is applied by the pipeline, never by the sidecar's own
        # map_row_to_feature — calling it in isolation must not surface stats.
        sidecar = _columnar_sidecar()
        feature = _blank_feature()
        sidecar.map_row_to_feature(
            {"geoid": "g1", "area": 12.5},
            feature,
            _ctx_with_policy(ItemsReadPolicy(feature_type=FeatureType(expose=["area"]))),
        )
        assert "area" not in (feature.properties or {})


class TestApplyExposedComputedValues:
    def test_exposed_value_merged(self) -> None:
        feature = _blank_feature()
        _apply(
            [_columnar_sidecar()],
            {"geoid": "g1", "area": 12.5, "perimeter": 9.0},
            feature,
            ItemsReadPolicy(feature_type=FeatureType(expose=["area"])),
        )
        assert feature.properties["area"] == 12.5

    def test_non_exposed_value_does_not_leak(self) -> None:
        feature = _blank_feature()
        _apply(
            [_columnar_sidecar()],
            {"geoid": "g1", "area": 12.5, "perimeter": 9.0},
            feature,
            ItemsReadPolicy(feature_type=FeatureType(expose=["area"])),
        )
        assert "perimeter" not in feature.properties

    def test_no_policy_merges_nothing(self) -> None:
        feature = _blank_feature()
        _apply([_columnar_sidecar()], {"area": 12.5}, feature, ctx=FeaturePipelineContext())
        assert "area" not in feature.properties

    def test_empty_expose_merges_nothing(self) -> None:
        feature = _blank_feature()
        _apply(
            [_columnar_sidecar()],
            {"area": 12.5},
            feature,
            ItemsReadPolicy(feature_type=FeatureType(expose=[])),
        )
        assert "area" not in feature.properties

    def test_jsonb_value_merged(self) -> None:
        feature = _blank_feature()
        _apply(
            [_jsonb_sidecar()],
            {"geoid": "g1", "geom_stats": {"area": 42.0}},
            feature,
            ItemsReadPolicy(feature_type=FeatureType(expose=["area"])),
        )
        assert feature.properties["area"] == 42.0

    def test_declared_field_already_present_untouched(self) -> None:
        # A declared schema field surfaced by its sidecar is left alone — even
        # under strict, because it is already on properties.
        feature = _blank_feature(properties={"name": "Rome"})
        _apply(
            [_columnar_sidecar()],
            {"geoid": "g1", "area": 12.5},
            feature,
            ItemsReadPolicy(
                feature_type=FeatureType(expose=["name"], failure_mode="strict")
            ),
        )
        assert feature.properties["name"] == "Rome"

    def test_cross_sidecar_resolution(self) -> None:
        # area from the geometry sidecar, external_id from a foreign producer.
        feature = _blank_feature()
        _apply(
            [_columnar_sidecar(), _StubSidecar({"external_id": "ABC"})],
            {"geoid": "g1", "area": 12.5},
            feature,
            ItemsReadPolicy(
                feature_type=FeatureType(expose=["area", "external_id"])
            ),
        )
        assert feature.properties["area"] == 12.5
        assert feature.properties["external_id"] == "ABC"


class TestApplyExposedFailureMode:
    def test_best_effort_omits_missing_column(self) -> None:
        feature = _blank_feature()
        _apply(
            [_columnar_sidecar()],
            {"geoid": "g1", "area": 7.0},
            feature,
            ItemsReadPolicy(
                feature_type=FeatureType(
                    expose=["area", "perimeter"], failure_mode="best_effort"
                )
            ),
        )
        assert feature.properties["area"] == 7.0
        assert "perimeter" not in feature.properties

    def test_best_effort_omits_none_value(self) -> None:
        feature = _blank_feature()
        _apply(
            [_columnar_sidecar()],
            {"geoid": "g1", "area": None},
            feature,
            ItemsReadPolicy(
                feature_type=FeatureType(expose=["area"], failure_mode="best_effort")
            ),
        )
        assert "area" not in feature.properties

    def test_strict_raises_on_missing_column(self) -> None:
        feature = _blank_feature()
        with pytest.raises(ValueError):
            _apply(
                [_columnar_sidecar()],
                {"geoid": "g1", "area": 7.0},
                feature,
                ItemsReadPolicy(
                    feature_type=FeatureType(
                        expose=["area", "perimeter"], failure_mode="strict"
                    )
                ),
            )

    def test_strict_raises_on_none_value(self) -> None:
        feature = _blank_feature()
        with pytest.raises(ValueError):
            _apply(
                [_columnar_sidecar()],
                {"geoid": "g1", "area": None},
                feature,
                ItemsReadPolicy(
                    feature_type=FeatureType(expose=["area"], failure_mode="strict")
                ),
            )

    def test_strict_raises_when_no_sidecar_produces_name(self) -> None:
        # The exposure loop sees the full producible set, so an exposed name no
        # sidecar can produce surfaces loudly under strict (the single-pass loop
        # fixes the prior per-sidecar blind spot).
        feature = _blank_feature()
        with pytest.raises(ValueError):
            _apply(
                [_columnar_sidecar()],
                {"geoid": "g1", "area": 7.0},
                feature,
                ItemsReadPolicy(
                    feature_type=FeatureType(
                        expose=["area", "orphan"], failure_mode="strict"
                    )
                ),
            )
