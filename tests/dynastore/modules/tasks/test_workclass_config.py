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

"""Unit tests for ``WorkClassConfig`` — the #1807 WorkClass cutover switch.

PR-2 is config-only: no writer reads these flags yet. The contract these
tests pin is exactly that — the defaults are ``legacy`` for both planes so
installing the config changes no behaviour, and the derived predicates map
the three-state enum to the legacy/new write decisions the PR-3/PR-4 seams
will consume.
"""
import pytest

from dynastore.models.plugin_config import list_registered_configs
from dynastore.modules.tasks.workclass_config import EmitTarget, WorkClassConfig


def test_defaults_are_legacy_so_install_is_no_behaviour_change() -> None:
    cfg = WorkClassConfig()
    assert cfg.emit_target_events is EmitTarget.LEGACY
    assert cfg.emit_target_index is EmitTarget.LEGACY
    # Legacy-only writes, nothing to the new tables — identical to today.
    assert cfg.emit_events_to_legacy is True
    assert cfg.emit_events_to_new is False
    assert cfg.emit_index_to_legacy is True
    assert cfg.emit_index_to_new is False


@pytest.mark.parametrize(
    "target, to_legacy, to_new",
    [
        (EmitTarget.LEGACY, True, False),
        (EmitTarget.BOTH, True, True),
        (EmitTarget.NEW, False, True),
    ],
)
def test_event_plane_predicates_track_emit_target(
    target: EmitTarget, to_legacy: bool, to_new: bool
) -> None:
    cfg = WorkClassConfig(emit_target_events=target)
    assert cfg.emit_events_to_legacy is to_legacy
    assert cfg.emit_events_to_new is to_new


@pytest.mark.parametrize(
    "target, to_legacy, to_new",
    [
        (EmitTarget.LEGACY, True, False),
        (EmitTarget.BOTH, True, True),
        (EmitTarget.NEW, False, True),
    ],
)
def test_index_plane_predicates_track_emit_target(
    target: EmitTarget, to_legacy: bool, to_new: bool
) -> None:
    cfg = WorkClassConfig(emit_target_index=target)
    assert cfg.emit_index_to_legacy is to_legacy
    assert cfg.emit_index_to_new is to_new


def test_planes_are_independent() -> None:
    # Cutover advances one plane at a time (event plane first, index last):
    # flipping events must not disturb the index plane's decision.
    cfg = WorkClassConfig(
        emit_target_events=EmitTarget.NEW,
        emit_target_index=EmitTarget.LEGACY,
    )
    assert cfg.emit_events_to_new is True
    assert cfg.emit_events_to_legacy is False
    assert cfg.emit_index_to_legacy is True
    assert cfg.emit_index_to_new is False


def test_address_lives_under_the_tasks_bucket() -> None:
    # Must not introduce a new top-level platform node (plugin_config rule).
    assert WorkClassConfig._address == ("platform", "tasks", "workclass")


def test_config_is_platform_scoped_only() -> None:
    # A uniform emit target across tenants is what keeps drain-to-empty sound;
    # it must not be authorable per-catalog/per-collection.
    assert WorkClassConfig.effective_tiers() == ("platform",)


def test_enum_values_are_the_stable_persisted_strings() -> None:
    assert EmitTarget.LEGACY.value == "legacy"
    assert EmitTarget.BOTH.value == "both"
    assert EmitTarget.NEW.value == "new"


def test_round_trips_through_string_persistence() -> None:
    # Config persistence stores/loads the enum by its string value.
    cfg = WorkClassConfig.model_validate(
        {"emit_target_events": "both", "emit_target_index": "new"}
    )
    assert cfg.emit_target_events is EmitTarget.BOTH
    assert cfg.emit_target_index is EmitTarget.NEW
    dumped = cfg.model_dump(mode="json")
    assert dumped["emit_target_events"] == "both"
    assert dumped["emit_target_index"] == "new"


def test_rejects_unknown_emit_target() -> None:
    with pytest.raises(Exception):
        WorkClassConfig(emit_target_events="sideways")  # type: ignore[arg-type]


def test_emit_target_is_runtime_mutable() -> None:
    # Flipping a plane is a live config change, not a restart/DDL event.
    cfg = WorkClassConfig()
    cfg.emit_target_events = EmitTarget.BOTH
    assert cfg.emit_events_to_new is True


def test_class_is_registered_for_the_config_hub() -> None:
    # The side-effect import in tasks/__init__.py must register the class so
    # the composed config view can render and set it.
    assert WorkClassConfig in list_registered_configs().values()
