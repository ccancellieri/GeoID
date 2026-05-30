"""Placement entry + mode constants.

The platform-tier TaskPlacementConfig is added in a later change; this module
currently provides the entry value object and the three placement modes.
"""
from __future__ import annotations

from typing import ClassVar, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, model_validator

from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig

OFF_LOAD = "off_load"   # external executor: Cloud Run job (cloud) / worker machine (onprem)
ASYNC = "async"         # in-process background
SYNC = "sync"           # in-process blocking
MODES = (OFF_LOAD, ASYNC, SYNC)


class PlacementEntry(BaseModel):
    """Where/how a single task runs."""
    consumers: List[str] = Field(default_factory=list)  # logical service names
    mode: str = OFF_LOAD


# geoid's primary distribution is on-prem via docker compose, so off_load
# resolves to a worker queue by default; the GCP deploy opts into "cloud"
# (off_load -> Cloud Run job) by shipping a placement seed.
DEFAULT_PLACEMENT_PRESET = "onprem"


class TaskPlacementConfig(PluginConfig):
    """Platform-tier, sysadmin-mutable placement matrix (mirrors EngineConfig).

    Replaces the legacy task-routing config + event-consumer service list. The
    matrix is materialized non-empty from the registered inventory by the active
    preset; operators read it via /configs and edit ``overrides`` (sparse) for
    env-specific cases (e.g. gdal -> maps in a review environment).
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "tasks")
    _freeze_at: ClassVar[Optional[str]] = "platform"

    placement_preset: Mutable[str] = Field(
        default=DEFAULT_PLACEMENT_PRESET,
        description="Deployment profile; resolves the generic off_load mode to a "
        "concrete runner (cloud -> Cloud Run job, onprem -> worker machine).",
    )
    placements: Mutable[Dict[str, PlacementEntry]] = Field(
        default_factory=dict,
        description="Resolved matrix task_key -> {consumers, mode}. Materialized "
        "from the registered inventory when empty; fully operator-editable.",
    )
    overrides: Mutable[Dict[str, PlacementEntry]] = Field(
        default_factory=dict,
        description="Sparse per-task overrides layered on `placements`.",
    )

    def resolved_entry(self, task_key: str) -> Optional[PlacementEntry]:
        """Override wins over the base matrix; None if the task is unknown."""
        if task_key in self.overrides:
            return self.overrides[task_key]
        return self.placements.get(task_key)

    @model_validator(mode="after")
    def _materialize_if_empty(self) -> "TaskPlacementConfig":
        # Fill the matrix from the live registered inventory if the operator (or
        # default construction) left it empty -- there is never a null matrix.
        if not self.placements:
            from dynastore.modules.tasks.placement.matrix import (
                InventoryItem,
                build_placement_matrix,
            )
            from dynastore.tasks import _DYNASTORE_TASKS, task_kind

            inventory = [
                InventoryItem(
                    task_key=key,
                    kind=task_kind(cfg),
                    affinity_tier=getattr(cfg.cls, "affinity_tier", None),
                )
                for key, cfg in _DYNASTORE_TASKS.items()
            ]
            if inventory:
                object.__setattr__(self, "placements", build_placement_matrix(inventory))
        return self
