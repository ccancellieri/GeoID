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

"""
CollectionPipelineProtocol — per-collection pipeline stage.

Same record-filter semantics as ItemPipelineProtocol but applied to the
fully-composed collection document at serve time. Stages may:

  - Merge item_assets defaults from other modules (Styles, Render).
  - Rewrite extents based on cross-tenant aggregation.
  - Strip restricted fields per role.
  - Drop the collection entirely (return None → consumer returns 404).

Consumers (STAC, Features, Coverages, Records collection endpoints) run
all active stages in priority order after base composition. A helper
at modules/catalog/collection_pipeline_runner.py does the priority + drop
loop so callers don't duplicate it.
"""

from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class CollectionPipelineProtocol(Protocol):
    """Per-collection pipeline stage: enrich, filter, rewrite, or drop.

    Registered via ``register_plugin(stage)`` and discovered via
    ``get_protocols(CollectionPipelineProtocol)``.

    Attributes:
        pipeline_id: Unique string identifier (e.g. ``"styles_item_assets"``).
        priority: Execution order — lower runs first. Use 100 for general
                  stages; < 100 for guards/filters.
    """

    pipeline_id: str
    priority: int

    def can_apply(self, catalog_id: str, collection_id: str) -> bool:
        """Return ``True`` if this stage applies to the given collection."""
        ...

    async def apply(
        self,
        catalog_id: str,
        collection_id: str,
        collection: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Apply this stage to the fully-composed collection document.

        Stages may:
          - Return the (possibly modified) collection dict.
          - Return ``None`` to drop the collection (→ consumer returns 404).
          - Return a different-shaped dict to replace the collection.

        Must not mutate ``collection`` in place — return a new dict.
        """
        ...
