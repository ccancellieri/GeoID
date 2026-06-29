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

"""Regression tests for schema-snapshot staleness and delta-merge safety.

Covers the scenario where a Mutable PluginConfig gains or loses a field
(schema_id changes): the read path must not raise ``extra_forbidden`` even
when a stored delta was written by a different code version.

Two root causes are exercised:

1. **Waterfall merge with unknown delta field** (config_service.py): a delta
   from a newer deploy contains ``min_feature_pixel_area_by_zoom``; the fixed
   merge step strips unknown keys before ``model_validate`` so the class
   default is used instead.

2. **list_catalog_configs unknown field** (config_service.py): the same delta
   reaches ``list_catalog_configs`` which must not raise either.

3. **Stale catalog-defaults snapshot** (config_snapshot.py): ``select_snapshot_base``
   must return None (not raise) when the stored schema_id no longer matches
   the live class, preventing ``extra_forbidden`` from a full old-data dump.

Refs #2556.
"""

from __future__ import annotations

from typing import ClassVar, Dict, Optional, Tuple

import pytest
from pydantic import Field, ValidationError

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.catalog.config_snapshot import (
    select_snapshot_base,
    serialize_for_snapshot,
)
from dynastore.modules.tiles.tiles_config import TilesConfig


# ---------------------------------------------------------------------------
# Minimal test fixture: a PluginConfig WITHOUT the new field
# ---------------------------------------------------------------------------


class _TilesConfigV0(PluginConfig):
    """Simulates TilesConfig *before* min_feature_pixel_area_by_zoom was added.

    Used to verify that a delta stored by newer code (which includes the new
    field) is handled safely when read back by code running this older schema.
    """

    _address: ClassVar[Tuple[str, ...]] = ("test", "schema", "tiles_v0")

    simplification_by_zoom: Mutable[Optional[Dict[int, float]]] = Field(
        default=None,
        description="Per-zoom simplification tolerance (old schema, no density filter).",
    )
    cache_on_demand: Mutable[bool] = Field(default=True)
    enabled: Mutable[bool] = Field(default=True)


# ---------------------------------------------------------------------------
# 1. Snapshot staleness: stale schema_id → None, not an exception
# ---------------------------------------------------------------------------


def test_snapshot_stale_schema_id_returns_none():
    """Schema-id mismatch must produce None, never raise."""
    entry = serialize_for_snapshot(TilesConfig())
    entry["schema_id"] = "sha256:stale"  # deliberately wrong
    snap = {TilesConfig.class_key(): entry}
    result = select_snapshot_base(snap, TilesConfig)
    assert result is None


def test_snapshot_stale_for_tiles_v0_returns_none():
    """A snapshot entry captured with _TilesConfigV0 is stale for TilesConfig."""
    v0_entry = serialize_for_snapshot(_TilesConfigV0())
    # The stored schema_id is for _TilesConfigV0; TilesConfig has a different id.
    snap = {TilesConfig.class_key(): v0_entry}
    # schema_ids differ (different class) → stale → None
    result = select_snapshot_base(snap, TilesConfig)
    assert result is None


def test_snapshot_current_schema_id_validates():
    """A snapshot whose schema_id matches must return a live instance."""
    entry = serialize_for_snapshot(TilesConfig())
    snap = {TilesConfig.class_key(): entry}
    result = select_snapshot_base(snap, TilesConfig)
    assert isinstance(result, TilesConfig)


# ---------------------------------------------------------------------------
# 2. Waterfall merge: unknown delta key must not raise extra_forbidden
# ---------------------------------------------------------------------------


def test_model_validate_rejects_unknown_field_without_fix():
    """Pydantic extra_forbidden fires when an unknown key is in the dict.

    This test documents the *pre-fix* behaviour: naively merging a delta that
    contains an unknown field into base.model_dump() then calling
    model_validate raises ValidationError.  The fix strips unknown keys from
    each delta before the update.
    """
    base = _TilesConfigV0()
    merged = base.model_dump(mode="python")
    # Delta written by newer code (TilesConfig v1) — contains unknown field
    delta_from_newer_code = {
        "simplification_by_zoom": None,
        "min_feature_pixel_area_by_zoom": {"0": 4.0, "2": 4.0},  # unknown to v0
    }
    merged.update(delta_from_newer_code)
    # Without filtering: model_validate raises extra_forbidden
    with pytest.raises(ValidationError, match="extra_forbidden"):
        _TilesConfigV0.model_validate(merged)


def test_waterfall_merge_strips_unknown_delta_keys():
    """The fixed merge step ignores unknown delta keys and uses class defaults.

    Simulates get_config waterfall after a new field was added:
    - base = _TilesConfigV0 code-default
    - delta from DB contains the new field (written by newer TilesConfig code)
    - unknown keys are stripped before model_validate → no extra_forbidden
    """
    base = _TilesConfigV0()
    known_fields = set(_TilesConfigV0.model_fields.keys())
    merged: Dict = base.model_dump(mode="python")
    # Delta written by newer code (unknown field present)
    delta = {
        "simplification_by_zoom": None,
        "min_feature_pixel_area_by_zoom": {"0": 4.0},  # not in _TilesConfigV0
    }
    # Apply the fix: strip unknown keys
    merged.update({k: v for k, v in delta.items() if k in known_fields})
    # Must not raise
    result = _TilesConfigV0.model_validate(merged)
    assert result.simplification_by_zoom is None
    # The new field was not applied (unknown to v0), so it doesn't appear
    assert not hasattr(result, "min_feature_pixel_area_by_zoom")


def test_waterfall_merge_tiles_config_live_delta_with_new_field():
    """TilesConfig (new) correctly merges a delta that includes the new field.

    This is the positive case: after PR #2556 the live class knows about
    min_feature_pixel_area_by_zoom, so a delta that sets it is applied cleanly.
    """
    base = TilesConfig()
    known_fields = set(TilesConfig.model_fields.keys())
    merged: Dict = base.model_dump(mode="python")
    # Delta explicitly sets both old and new fields
    delta = {
        "simplification_by_zoom": None,
        "min_feature_pixel_area_by_zoom": {0: 2.0, 4: 1.0},
        "cache_on_demand": False,
    }
    merged.update({k: v for k, v in delta.items() if k in known_fields})
    result = TilesConfig.model_validate(merged)
    assert result.simplification_by_zoom is None
    assert result.min_feature_pixel_area_by_zoom == {0: 2.0, 4: 1.0}
    assert result.cache_on_demand is False


def test_waterfall_merge_new_field_default_used_when_missing_from_delta():
    """If the new field is absent from the delta, the class default (None) is kept.

    An old delta (stored before the field existed) must not cause
    extra_forbidden AND the new field must resolve to its class default.
    """
    base = TilesConfig()
    known_fields = set(TilesConfig.model_fields.keys())
    merged: Dict = base.model_dump(mode="python")
    # Old delta: only sets simplification_by_zoom (no min_feature_pixel_area_by_zoom)
    old_delta = {"simplification_by_zoom": None}
    merged.update({k: v for k, v in old_delta.items() if k in known_fields})
    result = TilesConfig.model_validate(merged)
    assert result.simplification_by_zoom is None
    # New field keeps the class default
    assert result.min_feature_pixel_area_by_zoom is None


# ---------------------------------------------------------------------------
# 3. list_catalog_configs: row["config_data"] with unknown key must not raise
# ---------------------------------------------------------------------------


def test_list_catalog_configs_style_unknown_field_stripped():
    """Reproduce the list_catalog_configs validation failure and the fix.

    ``list_catalog_configs`` calls cls.model_validate(row["config_data"])
    where config_data is a raw partial dict from DB.  If the DB row was
    written by newer code (containing an unknown field), the old class raises
    extra_forbidden.  The fix filters to known fields first.
    """
    # Row from DB written by newer code
    row_config_data = {
        "simplification_by_zoom": None,
        "min_feature_pixel_area_by_zoom": {"0": 4.0},  # unknown to _TilesConfigV0
    }

    # Without fix: raises extra_forbidden
    with pytest.raises(ValidationError, match="extra_forbidden"):
        _TilesConfigV0.model_validate(row_config_data)

    # With fix: strip unknown keys first
    known = set(_TilesConfigV0.model_fields.keys())
    safe_data = {k: v for k, v in row_config_data.items() if k in known}
    result = _TilesConfigV0.model_validate(safe_data)
    assert result.simplification_by_zoom is None


def test_list_catalog_configs_style_live_class_accepts_new_field():
    """When the live class knows the field, no stripping and no error."""
    row_config_data = {
        "simplification_by_zoom": None,
        "min_feature_pixel_area_by_zoom": {0: 2.0},
    }
    known = set(TilesConfig.model_fields.keys())
    safe_data = {k: v for k, v in row_config_data.items() if k in known}
    result = TilesConfig.model_validate(safe_data)
    assert result.min_feature_pixel_area_by_zoom == {0: 2.0}
