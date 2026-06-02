"""No-DB unit tests for the deployment-profile preset upload enrichment.

Verifies that ``build()`` for each profile yields:

- a ``PresetBundle`` with exactly two entries ("task_routing" and
  "asset_upload");
- the ``asset_upload`` entry contains an ``AssetRoutingConfig`` whose UPLOAD
  operation has exactly one entry pinned to the profile's upload driver with
  ``source="operator"`` (blocking auto-augmentation);
- ``onprem`` pins ``local_upload_module``; ``cloud`` and ``review`` pin
  ``gcp_module``;
- the ``task_routing`` entry is still present and uses ``TaskRoutingConfig``.
"""
from __future__ import annotations

from typing import cast
from unittest.mock import patch

import pytest

from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    Operation,
)
from dynastore.modules.tasks.routing.model import TaskRoutingConfig
from dynastore.modules.storage.presets.protocol import PresetBundle


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_preset(profile: str) -> PresetBundle:
    """Build the preset bundle for ``profile`` with a minimal empty task
    inventory so ``build_routing_matrix`` has nothing to place — we only
    care about the asset_upload entry, not the task routing content."""
    from dynastore.modules.tasks.routing.presets import (
        CloudTaskRoutingPreset,
        OnpremTaskRoutingPreset,
        ReviewTaskRoutingPreset,
    )
    mapping = {
        "cloud": CloudTaskRoutingPreset,
        "onprem": OnpremTaskRoutingPreset,
        "review": ReviewTaskRoutingPreset,
    }
    preset = mapping[profile]
    # Patch _DYNASTORE_TASKS to empty dict so build() does not need a live
    # task registry.
    with patch("dynastore.modules.tasks.routing.presets._DYNASTORE_TASKS", {}):
        return preset.build()


def _asset_config(bundle: PresetBundle) -> AssetRoutingConfig:
    """Extract the AssetRoutingConfig from the asset_upload bundle entry."""
    asset_entry = next(e for e in bundle.entries if e.slot == "asset_upload")
    return cast(AssetRoutingConfig, asset_entry.instance)


# ---------------------------------------------------------------------------
# Bundle structure
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("profile", ["cloud", "onprem", "review"])
def test_bundle_has_two_entries(profile: str) -> None:
    bundle = _build_preset(profile)
    assert len(bundle.entries) == 2


@pytest.mark.parametrize("profile", ["cloud", "onprem", "review"])
def test_bundle_slots(profile: str) -> None:
    bundle = _build_preset(profile)
    slots = [e.slot for e in bundle.entries]
    assert "task_routing" in slots
    assert "asset_upload" in slots


@pytest.mark.parametrize("profile", ["cloud", "onprem", "review"])
def test_task_routing_entry_uses_task_routing_config(profile: str) -> None:
    bundle = _build_preset(profile)
    task_entry = next(e for e in bundle.entries if e.slot == "task_routing")
    assert task_entry.config_cls is TaskRoutingConfig
    assert isinstance(task_entry.instance, TaskRoutingConfig)


@pytest.mark.parametrize("profile", ["cloud", "onprem", "review"])
def test_asset_upload_entry_uses_asset_routing_config(profile: str) -> None:
    bundle = _build_preset(profile)
    asset_entry = next(e for e in bundle.entries if e.slot == "asset_upload")
    assert asset_entry.config_cls is AssetRoutingConfig
    assert isinstance(asset_entry.instance, AssetRoutingConfig)


# ---------------------------------------------------------------------------
# UPLOAD operation content
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("profile", ["cloud", "onprem", "review"])
def test_asset_upload_has_upload_operation(profile: str) -> None:
    bundle = _build_preset(profile)
    cfg = _asset_config(bundle)
    assert Operation.UPLOAD in cfg.operations


@pytest.mark.parametrize("profile", ["cloud", "onprem", "review"])
def test_upload_operation_has_exactly_one_entry(profile: str) -> None:
    bundle = _build_preset(profile)
    cfg = _asset_config(bundle)
    assert len(cfg.operations[Operation.UPLOAD]) == 1


# ---------------------------------------------------------------------------
# Driver ref per profile
# ---------------------------------------------------------------------------

def test_onprem_upload_driver_is_local_upload_module() -> None:
    bundle = _build_preset("onprem")
    entry = _asset_config(bundle).operations[Operation.UPLOAD][0]
    assert entry.driver_ref == "local_upload_module"


def test_cloud_upload_driver_is_gcp_module() -> None:
    bundle = _build_preset("cloud")
    entry = _asset_config(bundle).operations[Operation.UPLOAD][0]
    assert entry.driver_ref == "gcp_module"


def test_review_upload_driver_is_gcp_module() -> None:
    bundle = _build_preset("review")
    entry = _asset_config(bundle).operations[Operation.UPLOAD][0]
    assert entry.driver_ref == "gcp_module"


# ---------------------------------------------------------------------------
# source="operator" blocks auto-augmentation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("profile", ["cloud", "onprem", "review"])
def test_upload_entry_source_is_operator(profile: str) -> None:
    """source="operator" makes the list operator-managed so _self_register_upload_into
    treats it as invariant and does not append additional upload backends."""
    bundle = _build_preset(profile)
    entry = _asset_config(bundle).operations[Operation.UPLOAD][0]
    assert entry.source == "operator"


# ---------------------------------------------------------------------------
# rollback_priority ordering
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("profile", ["cloud", "onprem", "review"])
def test_asset_upload_rollback_priority_is_trailing(profile: str) -> None:
    """asset_upload rolls back after task_routing (higher rollback_priority value
    means later in the rollback queue; iter_rollback sorts lower-first)."""
    bundle = _build_preset(profile)
    task_entry = next(e for e in bundle.entries if e.slot == "task_routing")
    asset_entry = next(e for e in bundle.entries if e.slot == "asset_upload")
    assert asset_entry.rollback_priority >= task_entry.rollback_priority
