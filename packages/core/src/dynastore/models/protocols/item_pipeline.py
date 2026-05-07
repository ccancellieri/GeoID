#    Copyright 2025 FAO
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

"""
ItemPipelineProtocol — per-item pipeline stage.

Stages can enrich, filter, rewrite, or drop items at read time. Registered
via ``register_plugin(stage)`` and discovered via
``get_protocols(ItemPipelineProtocol)``. The ``_apply_item_pipeline()``
helper in ``item_query.py`` wraps the item stream with an async generator
that applies all active stages to each feature, preserving streaming
semantics (items are never collected into a list).

The ``can_apply()`` guard is evaluated **once per query** (not per item)
to avoid per-item overhead.

Contract widened from the former ``ItemEnricherProtocol``: ``apply()`` may
return ``None`` to drop the item from the stream, a new dict to replace
it wholesale, or the (possibly modified) input. Must not mutate the input
in place — return a new dict.

Typical use cases:
  - Authorization filter: drop items the caller isn't authorized to see.
  - Soft-delete masking: drop tombstoned items for non-admin callers.
  - Join DWH statistics (download count) from BigQuery per feature.
  - Add signed asset URLs resolved from GCS/S3.
  - Rewrite style hrefs with tenant-scoped signed URLs.
  - Inject computed fields (derived temporal aggregates).
"""

from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class ItemPipelineProtocol(Protocol):
    """Per-item pipeline stage: enrich, filter, rewrite, or drop.

    Implementations are discovered via the plugin registry. Each stage
    declares a ``priority`` (lower = earlier) and a ``can_apply()`` guard
    that controls whether the stage applies to a given collection.

    The ``apply()`` method receives a single feature dict and returns
    either a (possibly modified) copy or ``None`` to drop the item.
    Stages must not mutate the input dict in place.

    Attributes:
        stage_id: Unique string identifier (e.g. ``"bq_item_stats"``).
        priority: Execution order — lower runs first. Use 100 for
                  general stages; < 100 for guards/filters.
    """

    stage_id: str
    priority: int

    def can_apply(self, catalog_id: str, collection_id: str) -> bool:
        """Return ``True`` if this stage applies to the given collection.

        Called **once per query** before the item stream is iterated.
        """
        ...

    async def apply(
        self,
        catalog_id: str,
        collection_id: str,
        feature: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Apply this stage to a single feature.

        Stages may:
          - Return the (possibly modified) feature dict.
          - Return ``None`` to drop the item from the stream.
          - Return a different-shaped dict to replace the feature wholesale.

        Must not mutate ``feature`` in place — return a new dict.
        """
        ...
