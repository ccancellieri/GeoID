"""Deployment-profile presets. cloud/onprem share the matrix; they differ only
in how the generic off_load mode resolves to a concrete runner.

Registered in the existing preset registry so the set of valid placement_preset
values is discoverable and switching is a first-class operation. The rest of the
system reads PLACEMENT_PRESET_RUNNER directly, not the registry, so registration
failing cannot break placement.
"""
from __future__ import annotations

from typing import Dict, List

# off_load -> runner_type per profile (the only cloud/onprem difference).
PLACEMENT_PRESET_RUNNER: Dict[str, str] = {
    "cloud": "gcp_cloud_run",
    "onprem": "worker_queue",
}


def list_placement_presets() -> List[str]:
    return sorted(PLACEMENT_PRESET_RUNNER)


class _PlacementRoutingPreset:
    """Minimal RoutingPreset: materializes a TaskPlacementConfig at platform tier."""

    catalog_scopable = False

    def __init__(self, *, name: str, off_load_runner: str, tier):
        self.name = name
        self.description = f"Task placement profile: off_load -> {off_load_runner}"
        self.tier = tier
        self.off_load_runner = off_load_runner

    def build(self, **_scope):
        from dynastore.modules.storage.presets.protocol import (
            PresetBundle,
            PresetBundleEntry,
        )
        from dynastore.modules.tasks.placement.model import TaskPlacementConfig

        instance = TaskPlacementConfig(placement_preset=self.name)  # validator fills matrix
        return PresetBundle(
            entries=(
                PresetBundleEntry(
                    slot="task_placement",
                    config_cls=TaskPlacementConfig,
                    instance=instance,
                ),
            )
        )


def _register() -> None:
    try:
        from dynastore.modules.storage.presets import register_preset
        from dynastore.modules.storage.presets.protocol import PresetTier

        for name, runner in PLACEMENT_PRESET_RUNNER.items():
            register_preset(
                _PlacementRoutingPreset(name=name, off_load_runner=runner, tier=PresetTier.PLATFORM)
            )
    except Exception:  # registry shape mismatch must not break import
        import logging
        logging.getLogger(__name__).warning(
            "placement presets not registered in storage registry; "
            "using PLACEMENT_PRESET_RUNNER fallback", exc_info=True,
        )


_register()
