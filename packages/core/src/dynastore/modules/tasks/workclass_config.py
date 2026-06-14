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

# dynastore/modules/tasks/workclass_config.py
"""``WorkClassConfig`` — migration control for the WorkClass durable-work unification.

Two net-new global hot-plane tables — ``tasks.work_events`` (replacing the
legacy ``events.events`` domain-event outbox) and ``tasks.storage``
(replacing the per-tenant ``{schema}.storage_outbox`` ES-index outbox) — are
cut over from their legacy tables behind a per-plane three-state switch.

``emit_target_events`` and ``emit_target_storage`` each take one of:

* ``legacy`` — write only to the legacy table (current behaviour, the default).
* ``both``   — dual-write to legacy AND the new table (cutover step 1: both
  tables fill; the legacy table stays authoritative and is the rollback
  target — flipping back to ``legacy`` is a config change, not a data recovery).
* ``new``    — write only to the new table (cutover step 3, flipped once the
  legacy table has drained to empty under steady state).

The default is ``legacy`` for both planes, so installing this config changes
no behaviour. The switch is platform-scoped (``_tiers``): it is a global
migration control, not a per-catalog/per-collection knob — a uniform emit
target across tenants keeps the drain-to-empty cutover predicate sound.
Flipping a plane is a runtime config change (``Mutable``); no restart, no DDL.
"""
from enum import Enum
from typing import ClassVar, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class EmitTarget(str, Enum):
    """Per-plane durable-write target during the WorkClass migration.

    The three states are ordered as a one-way migration ratchet
    (``legacy`` → ``both`` → ``new``), but the enum imposes no ordering
    constraint: any value is reachable from any other so a stuck cutover
    can be reverted to ``legacy`` (the rollback target) at any step.
    """

    LEGACY = "legacy"
    BOTH = "both"
    NEW = "new"


class WorkClassConfig(PluginConfig):
    """Per-plane emit-target switches for the WorkClass cutover.

    Read by the dual-write seams (event emit; the co-transactional storage
    enqueue inside the item-write transaction). The derived ``emit_*_to_*``
    predicates are the intended read API — call them rather than comparing
    the enum at each seam, so the legacy/new split lives in one place.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "tasks", "workclass")
    # Platform-scoped migration control: a uniform emit target across all
    # tenants keeps the per-plane drain-to-empty cutover predicate sound, so
    # this is deliberately NOT authorable at the catalog/collection tier.
    _tiers: ClassVar[Optional[Tuple[str, ...]]] = ("platform",)

    emit_target_events: Mutable[EmitTarget] = Field(
        EmitTarget.LEGACY,
        description=(
            "Durable-write target for the domain-event plane. 'legacy' (default) "
            "writes only events.events; 'both' dual-writes events.events AND "
            "tasks.work_events (cutover step 1); 'new' writes only "
            "tasks.work_events (cutover step 3, after events.events drains to "
            "empty). Runtime-reloadable; no restart, no DDL."
        ),
    )

    emit_target_storage: Mutable[EmitTarget] = Field(
        EmitTarget.LEGACY,
        description=(
            "Durable-write target for the storage outbox plane. 'legacy' "
            "(default) writes only {schema}.storage_outbox; 'both' dual-writes "
            "storage_outbox AND tasks.storage inside the item-write "
            "transaction (cutover step 1); 'new' writes only tasks.storage "
            "(cutover step 3, after storage_outbox drains to empty per tenant). "
            "Runtime-reloadable; no restart, no DDL."
        ),
    )

    # --- Derived predicates (the read API for the dual-write seams) -------

    @property
    def emit_events_to_legacy(self) -> bool:
        """Whether the event plane should write the legacy ``events.events`` row."""
        return self.emit_target_events in (EmitTarget.LEGACY, EmitTarget.BOTH)

    @property
    def emit_events_to_new(self) -> bool:
        """Whether the event plane should write the new ``tasks.work_events`` row."""
        return self.emit_target_events in (EmitTarget.BOTH, EmitTarget.NEW)

    @property
    def emit_storage_to_legacy(self) -> bool:
        """Whether the storage plane should write the legacy ``storage_outbox`` row."""
        return self.emit_target_storage in (EmitTarget.LEGACY, EmitTarget.BOTH)

    @property
    def emit_storage_to_new(self) -> bool:
        """Whether the storage plane should write the new ``tasks.storage`` row."""
        return self.emit_target_storage in (EmitTarget.BOTH, EmitTarget.NEW)
