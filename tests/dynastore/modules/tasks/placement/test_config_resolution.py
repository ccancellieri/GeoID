"""TaskPlacementConfig materializes a non-empty matrix and layers overrides."""
from __future__ import annotations

from dynastore.modules.tasks.placement.model import (
    OFF_LOAD,
    PlacementEntry,
    TaskPlacementConfig,
)


def test_address_and_freeze_mirror_engines():
    assert TaskPlacementConfig._address == ("platform", "tasks")
    assert TaskPlacementConfig._freeze_at == "platform"


def test_overrides_layer_over_placements():
    cfg = TaskPlacementConfig(
        placement_preset="cloud",
        placements={"gdal": PlacementEntry(consumers=["worker"], mode=OFF_LOAD)},
        overrides={"gdal": PlacementEntry(consumers=["maps"], mode=OFF_LOAD)},
    )
    resolved = cfg.resolved_entry("gdal")
    assert resolved.consumers == ["maps"]   # override wins


def test_default_preset_is_onprem():
    # geoid's default distribution is on-prem (docker compose); the GCP deploy
    # opts into "cloud" via a placement seed.
    cfg = TaskPlacementConfig(placements={"gdal": PlacementEntry(consumers=["worker"], mode=OFF_LOAD)})
    assert cfg.placement_preset == "onprem"
