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

"""Task routing model — value objects and the platform-tier PluginConfig.

``RunnerTarget`` is the per-entry value object (mirrors
``OperationDriverEntry`` from storage routing). ``TaskRoutingConfig`` is
the platform-tier, sysadmin-mutable config that maps each task/process key
to an ordered list of ``RunnerTarget`` entries.

Key design decisions
--------------------
- ``tasks``     — system/listener/loop tasks not directly API-callable.
- ``processes`` — OGC-Process-API jobs, callable via ``/processes/{id}/execution``.
- Selection semantics mirror the storage router: consumers + can_handle +
  hint superset match; longest-hints wins on tie; first viable wins.
- ``_materialize_if_empty`` fills both maps from the live registry via
  ``build_routing_matrix`` when the operator left them empty.
"""
from __future__ import annotations

from enum import StrEnum
from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field, model_validator

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.tasks.routing.exec_hints import ExecHint


class ActionVerb(StrEnum):
    """What the routing engine does after the target completes (or fails)."""

    ROUTE = "route"             # hand off to a named task/process
    REPORT = "report"           # write completion status back to the caller
    DEAD_LETTER = "dead_letter" # move to the dead-letter queue
    FAIL = "fail"               # propagate failure to the caller immediately


class Action(BaseModel):
    """Post-execution action attached to a RunnerTarget on_success / on_failure."""

    action: ActionVerb = ActionVerb.REPORT
    process: Optional[str] = None      # target task/process key when action==ROUTE
    hints: Set[ExecHint] = Field(default_factory=set)
    payload: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_process_field(self) -> "Action":
        if self.action == ActionVerb.ROUTE and not self.process:
            raise ValueError(
                "Action.process must be a non-empty string when action=='route'"
            )
        if self.action != ActionVerb.ROUTE and self.process is not None:
            raise ValueError(
                "Action.process must be None when action is not 'route'; "
                f"got action={self.action!r}, process={self.process!r}"
            )
        return self


class RunnerTarget(BaseModel):
    """A single execution target in a task routing entry.

    ``consumers`` names the logical service tiers allowed to claim and run
    the task (e.g. ``["catalog"]``, ``["maps"]``); an empty list means any
    capable service may claim it.

    ``runner`` is the runner-type identifier the claiming service resolves
    against its own runner registry (e.g. ``"background"``,
    ``"gcp_cloud_run"``).

    ``hints`` is the set of ``ExecHint`` values this target serves.  An
    empty hints set is treated as "serves any hint" (matches all callers),
    mirroring the storage-router effective-hint semantics.
    """

    consumers: List[str] = Field(
        default_factory=list,
        description="Service tiers that may claim this target; [] = any capable service.",
    )
    runner: str = Field(
        ...,
        min_length=1,
        description='Runner-type identifier, e.g. "background", "gcp_cloud_run".',
    )
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Runner-specific options (e.g. timeout, memory). Discovery supplies job name.",
    )
    hints: Set[ExecHint] = Field(
        default_factory=set,
        description=(
            "Execution hints this target serves.  Empty = matches any caller hint "
            "(no preference filtering)."
        ),
    )
    on_success: Action = Field(
        default_factory=lambda: Action(action=ActionVerb.REPORT),
        description="Action to take when the task completes successfully.",
    )
    on_failure: Action = Field(
        default_factory=lambda: Action(action=ActionVerb.DEAD_LETTER),
        description="Action to take when the task fails after all retries.",
    )
    on_timeout: Action = Field(
        default_factory=lambda: Action(action=ActionVerb.DEAD_LETTER),
        description=(
            "Action to take when the task exceeds its timeout — a DISTINCT "
            "terminal outcome from on_failure.  A sync runner times out when "
            "execution exceeds options['timeout_seconds'] (falling back to "
            "TasksPluginConfig.task_timeout_seconds); an offloaded Cloud Run "
            "Job times out when its execution lease lapses.  Defaulting to "
            "DEAD_LETTER keeps a timed-out task off the queue; set action="
            "'route' to re-route only on timeout (e.g. to a heavier target) "
            "without conflating it with a logic-error failure."
        ),
    )


# One-release backward-compat aliases: routing config rows written before the
# Phase 0 rename used the old key "outbox_drain".  resolved_targets() tries
# all aliases so that:
#   - old callers passing "outbox_drain" find stored "index_drain" entries, and
#   - new callers passing "index_drain" find stored "outbox_drain" entries.
# Remove once all stored configs have been re-applied with the new keys.
_LEGACY_TASK_KEY_MAP: Dict[str, str] = {
    "outbox_drain": "index_drain",
}
# Reverse: new key → old stored key (for reading old configs with new callers).
_LEGACY_TASK_KEY_REVERSE_MAP: Dict[str, str] = {
    v: k for k, v in _LEGACY_TASK_KEY_MAP.items()
}


class TaskRoutingConfig(PluginConfig):
    """Platform-tier, sysadmin-mutable task/process routing.

    Split by kind: ``tasks`` for system/listener/loop tasks (not API-callable but
    fully routable), ``processes`` for OGC-Process-API jobs. Each maps a key to an
    ORDERED list of ``RunnerTarget``; selection mirrors the storage router
    (consumers + can_handle + hint superset match).

    The config is materialized non-empty from the registered task inventory by
    ``build_routing_matrix`` when the operator leaves both maps empty.

    Identity: ``class_key()`` derives to ``"task_routing_config"``.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "tasks")
    _freeze_at: ClassVar[Optional[str]] = "platform"

    tasks: Mutable[Dict[str, List[RunnerTarget]]] = Field(
        default_factory=dict,
        description=(
            "Routing matrix for system tasks (kind='task').  Key = task_key from "
            "the registry; value = ordered list of RunnerTarget entries."
        ),
    )
    processes: Mutable[Dict[str, List[RunnerTarget]]] = Field(
        default_factory=dict,
        description=(
            "Routing matrix for OGC processes (kind='process').  Key = task_key; "
            "value = ordered list of RunnerTarget entries."
        ),
    )

    def resolved_targets(self, task_key: str) -> List[RunnerTarget]:
        """Return the target list for ``task_key``, bucket-first.

        Checks the ``tasks`` bucket first (consistent with ``task_kind()``
        semantics), falls back to ``processes``, returns ``[]`` when the
        key is unknown in both.

        Legacy shim: tries all known aliases for ``task_key`` so that:
        - Old callers passing the pre-rename key find entries stored under
          the new key, and
        - New callers passing the post-rename key find entries stored under
          the old key (old config not yet migrated).
        Remove the alias maps once all stored configs use the new keys.
        """
        candidates = [
            task_key,
            _LEGACY_TASK_KEY_MAP.get(task_key),
            _LEGACY_TASK_KEY_REVERSE_MAP.get(task_key),
        ]
        for key in candidates:
            if key is None:
                continue
            if key in self.tasks:
                return list(self.tasks[key])
            if key in self.processes:
                return list(self.processes[key])
        return []

    @model_validator(mode="after")
    def _materialize_if_empty(self) -> "TaskRoutingConfig":
        """Fill both maps from the live registry when the operator left them empty.

        A TaskRoutingConfig is never null — it always has at least the
        registry-derived defaults. The registry import is lazy so test
        fixtures that construct TaskRoutingConfig() without a live registry
        get an empty config rather than an import error.
        """
        if self.tasks or self.processes:
            return self
        try:
            from dynastore.modules.tasks.routing.matrix import (
                InventoryItem,
                build_routing_matrix,
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
                # Default to the cloud profile (production / dynastore CI build
                # is unaffected). A deployment may opt into a different profile
                # without storing an explicit TaskRoutingConfig by setting
                # ``DYNASTORE_TASK_ROUTING_PRESET`` (e.g. ``onprem`` for a
                # worker-less local/on-prem box). Unknown values fall back to
                # ``cloud`` so a typo can never silently mis-route.
                import os

                preset = os.environ.get(
                    "DYNASTORE_TASK_ROUTING_PRESET", "cloud"
                ).strip().lower()
                if preset not in ("cloud", "onprem", "review"):
                    preset = "cloud"
                tasks_map, processes_map = build_routing_matrix(inventory, preset=preset)
                object.__setattr__(self, "tasks", tasks_map)
                object.__setattr__(self, "processes", processes_map)
        except Exception:
            pass  # empty registry or import error at bootstrap — safe to leave maps empty
        return self
