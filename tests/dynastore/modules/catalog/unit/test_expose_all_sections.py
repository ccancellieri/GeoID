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

"""ItemService._apply_expose_all_sections — the read-side ``expose_all`` regroup.

#1402: when ``ItemsReadPolicy.feature_type.expose_all`` is True, the assembled
GeoJSON Feature gains two top-level sibling members mirroring the ingestion
report envelope: ``system`` (identity + lifecycle from ``SYSTEM_FIELD_KEYS``) and
``stats`` (every other producible computed value the resolved sidecars surface).
``properties`` stays user-only. These tests pin the regroup in isolation (no DB).
"""

from __future__ import annotations

import pytest

from dynastore.models.ogc import Feature
from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.computed_fields import (
    SYSTEM_FIELD_KEYS,
    FeatureType,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FeaturePipelineContext,
)
from dynastore.modules.storage.read_policy import ItemsReadPolicy


class _StubSidecar:
    """Duck-typed sidecar exposing only the two read-shape hooks."""

    def __init__(self, values: dict) -> None:
        self._values = dict(values)

    def producible_computed_names(self) -> set:
        return set(self._values)

    def resolve_computed_value(self, row, resolved_name):
        if resolved_name in self._values:
            return (True, self._values[resolved_name])
        return (False, None)


def _ctx(expose_all: bool, expose=None) -> FeaturePipelineContext:
    ctx = FeaturePipelineContext()
    ctx["_items_read_policy"] = ItemsReadPolicy(
        feature_type=FeatureType(expose=expose, expose_all=expose_all)
    )
    return ctx


def _feature(properties=None) -> Feature:
    return Feature(type="Feature", geometry=None, properties=properties or {})


def _apply(feature, row, sidecars, ctx):
    ItemService._apply_expose_all_sections(feature, row, sidecars, ctx)


class TestExposeAllOff:
    def test_no_op_when_expose_all_false(self) -> None:
        feature = _feature({"name": "Field A"})
        _apply(feature, {"geoid": "g1", "area": 12.5}, [_StubSidecar({"area": 12.5})],
                _ctx(expose_all=False))
        extra = feature.__pydantic_extra__ or {}
        assert "stats" not in extra and "system" not in extra

    def test_no_op_when_no_policy(self) -> None:
        feature = _feature()
        ctx = FeaturePipelineContext()  # no _items_read_policy
        _apply(feature, {"geoid": "g1"}, [_StubSidecar({"area": 1.0})], ctx)
        extra = feature.__pydantic_extra__ or {}
        assert "stats" not in extra and "system" not in extra


class TestExposeAllOn:
    def test_builds_system_and_stats_sections(self) -> None:
        feature = _feature({"name": "Field A"})
        row = {
            "geoid": "geoid-123",
            "external_id": "ABC",
            "geometry_hash": "9f",
            "deleted_at": None,  # None → omitted from system
            "area": 1234.0,
            "perimeter": 56.0,
        }
        sidecars = [_StubSidecar({"area": 1234.0, "perimeter": 56.0})]
        _apply(feature, row, sidecars, _ctx(expose_all=True))

        dumped = feature.model_dump()
        # properties stays user-only
        assert dumped["properties"] == {"name": "Field A"}
        # system: only keys present in the row with a non-None value
        assert dumped["system"] == {
            "geoid": "geoid-123",
            "external_id": "ABC",
            "geometry_hash": "9f",
        }
        assert "deleted_at" not in dumped["system"]
        # stats: producible computed values, excluding system keys
        assert dumped["stats"] == {"area": 1234.0, "perimeter": 56.0}

    def test_system_keys_excluded_from_stats(self) -> None:
        # A sidecar that produces geometry_hash (a system key) must route it to
        # system, never stats.
        feature = _feature()
        row = {"geometry_hash": "deadbeef", "area": 2.0}
        sidecars = [_StubSidecar({"geometry_hash": "ignored-by-stats", "area": 2.0})]
        _apply(feature, row, sidecars, _ctx(expose_all=True))
        dumped = feature.model_dump()
        assert dumped["stats"] == {"area": 2.0}
        assert dumped["system"] == {"geometry_hash": "deadbeef"}

    def test_flat_foreign_member_folded_into_section(self) -> None:
        # A stat already attached flat (by the sidecar foreign-member bridge)
        # must be folded into stats, not emitted twice.
        feature = _feature()
        if feature.__pydantic_extra__ is not None:
            feature.__pydantic_extra__["external_id"] = "XYZ"  # flat system key
        row = {"area": 3.0}
        _apply(feature, row, [_StubSidecar({"area": 3.0})], _ctx(expose_all=True))
        dumped = feature.model_dump()
        # external_id folded into system, not left flat
        assert dumped["system"] == {"external_id": "XYZ"}
        assert "external_id" not in {
            k for k in dumped if k not in ("type", "geometry", "bbox", "id",
                                           "properties", "stats", "system")
        }

    def test_empty_sections_attached_for_consistent_shape(self) -> None:
        # expose_all always attaches both sections (possibly empty) so consumers
        # see a stable shape — mirrors the reporter always setting stats/system.
        feature = _feature()
        _apply(feature, {}, [], _ctx(expose_all=True))
        dumped = feature.model_dump()
        assert dumped["stats"] == {}
        assert dumped["system"] == {}


class TestExposeAllConfig:
    def test_default_off(self) -> None:
        assert FeatureType().expose_all is False

    def test_accepts_true(self) -> None:
        assert FeatureType(expose_all=True).expose_all is True

    def test_unknown_field_still_rejected(self) -> None:
        with pytest.raises(Exception):
            FeatureType(not_a_field=True)  # type: ignore[call-arg]

    def test_system_field_keys_match_reporter(self) -> None:
        # The read-side SSOT is the same tuple the ingestion reporter uses.
        from dynastore.tasks.ingestion.main_ingestion import _SYSTEM_KEYS
        assert _SYSTEM_KEYS is SYSTEM_FIELD_KEYS
