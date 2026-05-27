#    Copyright 2026 FAO
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

"""Payload model for ``task_type='cascade_cleanup'`` rows."""

from __future__ import annotations

from typing import Any, Dict, List, Literal

from pydantic import BaseModel, Field


class CascadeCleanupInputs(BaseModel):
    """Durable payload for a cascade_cleanup task row.

    ``scope_ref`` captures which entity was deleted (scope + id fields).
    ``refs`` is the pre-snapshotted list of :class:`~dynastore.modules.catalog.resource_owner.CleanupRef`
    dicts produced by :meth:`CascadeOrchestrator.snapshot_and_enqueue` before
    the owning schema was dropped — it is the only source of truth once the
    catalog rows are gone.
    """

    scope_ref: Dict[str, Any] = Field(
        description="Serialised ScopeRef (scope + catalog_id + optional ids).",
    )
    mode: Literal["soft", "hard"] = Field(
        description="Cleanup policy passed to each owner's cleanup_one.",
    )
    refs: List[Dict[str, Any]] = Field(
        default_factory=list,
        description=(
            "Serialised CleanupRef dicts (CleanupRef.to_json output). "
            "Each dict carries kind, locator, owner_id, and metadata."
        ),
    )
