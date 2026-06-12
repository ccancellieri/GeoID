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

"""TaskRoutingConfig materializes a non-empty matrix and the routing structure is correct."""
from __future__ import annotations

from dynastore.modules.tasks.routing.model import (
    RunnerTarget,
    TaskRoutingConfig,
)


def test_address_and_freeze_match_platform_tier():
    assert TaskRoutingConfig._address == ("platform", "tasks")
    assert TaskRoutingConfig._freeze_at == "platform"


def test_tasks_bucket_returned_before_processes():
    target = RunnerTarget(runner="background", consumers=["catalog"])
    cfg = TaskRoutingConfig(
        tasks={"index_drain": [target]},
        processes={},
    )
    result = cfg.resolved_targets("index_drain")
    assert result == [target]


def test_processes_bucket_is_fallback():
    target = RunnerTarget(runner="gcp_cloud_run", consumers=["catalog"])
    cfg = TaskRoutingConfig(
        tasks={},
        processes={"gdal": [target]},
    )
    result = cfg.resolved_targets("gdal")
    assert result == [target]


def test_unknown_key_returns_empty():
    cfg = TaskRoutingConfig(
        tasks={"a": [RunnerTarget(runner="background")]},
        processes={},
    )
    assert cfg.resolved_targets("no_such_key") == []


def test_class_key_is_task_routing_config():
    assert TaskRoutingConfig.class_key() == "task_routing_config"


def test_no_legacy_alias_maps_exported():
    """_LEGACY_TASK_KEY_MAP and _LEGACY_TASK_KEY_REVERSE_MAP must be gone."""
    import dynastore.modules.tasks.routing.model as routing_model

    assert not hasattr(routing_model, "_LEGACY_TASK_KEY_MAP"), (
        "_LEGACY_TASK_KEY_MAP must be removed; stored configs are rewritten by "
        "the config_seeder fixup at bootstrap."
    )
    assert not hasattr(routing_model, "_LEGACY_TASK_KEY_REVERSE_MAP"), (
        "_LEGACY_TASK_KEY_REVERSE_MAP must be removed."
    )


def test_outbox_drain_key_not_resolved_by_shim():
    """resolved_targets must not translate 'outbox_drain' → 'index_drain'.

    The seeder fixup ensures stored configs no longer carry 'outbox_drain';
    a direct miss on the key returns an empty list.
    """
    target = RunnerTarget(runner="background", consumers=["worker"])
    cfg = TaskRoutingConfig(
        tasks={"index_drain": [target]},
        processes={},
    )
    # Direct hit still works.
    assert cfg.resolved_targets("index_drain") == [target]
    # Old key is not translated — seeder fixup handles that.
    assert cfg.resolved_targets("outbox_drain") == []
