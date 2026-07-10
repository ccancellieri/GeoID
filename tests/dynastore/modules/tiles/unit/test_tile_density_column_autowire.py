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

"""Auto-discovery of TilesConfig.feature_density_column from a driver's
stored statistics.

Covers ``tiles_module._density_column_from_stats``, the pure helper that
picks the density-ceiling column out of a ``StoredStatsProvider.stored_stats``
result. The wiring in ``get_tile_resolution_params`` only calls this helper
when the operator hasn't set ``feature_density_column`` explicitly — that
None-guard is the sole precedence mechanism, so explicit config always wins
without any further test seam needed.
"""

from __future__ import annotations

from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
    StatisticStorageMode,
)
from dynastore.modules.tiles.tiles_module import _density_column_from_stats


def test_columnar_vertex_count_resolves_to_default_name():
    stats = [
        ComputedField(
            kind=ComputedKind.VERTEX_COUNT,
            storage_mode=StatisticStorageMode.COLUMNAR,
            indexed=True,
        ),
    ]
    assert _density_column_from_stats(stats) == "vertex_count"


def test_custom_named_columnar_vertex_count_resolves_to_custom_name():
    stats = [
        ComputedField(
            kind=ComputedKind.VERTEX_COUNT,
            storage_mode=StatisticStorageMode.COLUMNAR,
            name="vtx_density",
        ),
    ]
    assert _density_column_from_stats(stats) == "vtx_density"


def test_jsonb_mode_vertex_count_returns_none():
    """A JSONB-mode vertex_count cannot back a WHERE-clause ceiling."""
    stats = [
        ComputedField(
            kind=ComputedKind.VERTEX_COUNT,
            storage_mode=StatisticStorageMode.JSONB,
        ),
    ]
    assert _density_column_from_stats(stats) is None


def test_empty_stats_returns_none():
    assert _density_column_from_stats([]) is None


def test_only_other_kinds_returns_none():
    stats = [
        ComputedField(kind=ComputedKind.AREA, storage_mode=StatisticStorageMode.COLUMNAR),
        ComputedField(kind=ComputedKind.Z_RANGE, storage_mode=StatisticStorageMode.COLUMNAR),
    ]
    assert _density_column_from_stats(stats) is None


def test_first_matching_entry_wins():
    """More than one columnar vertex_count entry: the first one wins."""
    first = ComputedField(
        kind=ComputedKind.VERTEX_COUNT,
        storage_mode=StatisticStorageMode.COLUMNAR,
        name="vc_a",
    )
    second = ComputedField(
        kind=ComputedKind.VERTEX_COUNT,
        storage_mode=StatisticStorageMode.COLUMNAR,
        name="vc_b",
    )
    assert _density_column_from_stats([first, second]) == "vc_a"
