"""TaskRoutingConfig materializes a non-empty matrix and the routing structure is correct."""
from __future__ import annotations

from dynastore.modules.tasks.routing.model import (
    RunnerTarget,
    TaskRoutingConfig,
    _LEGACY_TASK_KEY_MAP,
    _LEGACY_TASK_KEY_REVERSE_MAP,
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


def test_legacy_outbox_drain_resolves_to_index_drain():
    """A config stored with the old 'outbox_drain' key must resolve via
    the legacy shim when the caller asks for 'index_drain' (and vice-versa
    for old callers that still pass 'outbox_drain').
    """
    target = RunnerTarget(runner="background", consumers=["worker"])

    # Old stored config (outbox_drain key) → caller asks with new key
    cfg_old = TaskRoutingConfig(
        tasks={"outbox_drain": [target]},
        processes={},
    )
    assert cfg_old.resolved_targets("index_drain") == [target], (
        "resolved_targets('index_drain') must find a stored 'outbox_drain' "
        "entry via the legacy shim."
    )

    # Old caller still passing 'outbox_drain' → new stored config (index_drain)
    cfg_new = TaskRoutingConfig(
        tasks={"index_drain": [target]},
        processes={},
    )
    assert cfg_new.resolved_targets("outbox_drain") == [target], (
        "resolved_targets('outbox_drain') must find a stored 'index_drain' "
        "entry via the legacy shim."
    )


def test_legacy_task_key_map_contains_outbox_drain():
    """_LEGACY_TASK_KEY_MAP must map 'outbox_drain' → 'index_drain'."""
    assert "outbox_drain" in _LEGACY_TASK_KEY_MAP, (
        "_LEGACY_TASK_KEY_MAP must contain 'outbox_drain' for the one-release shim."
    )
    assert _LEGACY_TASK_KEY_MAP["outbox_drain"] == "index_drain", (
        "'outbox_drain' must map to 'index_drain' (the new ES index drain key)."
    )


def test_legacy_task_key_reverse_map_contains_index_drain():
    """_LEGACY_TASK_KEY_REVERSE_MAP must map 'index_drain' → 'outbox_drain'.

    The reverse map lets new callers that pass 'index_drain' still find
    routing config entries stored under the old 'outbox_drain' key.
    """
    assert "index_drain" in _LEGACY_TASK_KEY_REVERSE_MAP, (
        "_LEGACY_TASK_KEY_REVERSE_MAP must contain 'index_drain'."
    )
    assert _LEGACY_TASK_KEY_REVERSE_MAP["index_drain"] == "outbox_drain", (
        "'index_drain' must reverse-map to 'outbox_drain'."
    )
