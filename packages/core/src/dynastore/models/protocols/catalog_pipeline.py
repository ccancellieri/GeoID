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
CatalogPipelineProtocol — per-catalog pipeline stage.

Same record-filter shape as ItemPipelineProtocol and CollectionPipelineProtocol
but applied to the catalog metadata dict at read time. Stages may:

  - Add real-time collection counts from a search driver.
  - Filter out metadata fields per role.
  - Inject computed summaries (storage usage, etc.).
  - Drop the catalog entirely (return None → consumer returns 404).

Consumers run all active stages in priority order. The ``can_apply()``
guard is evaluated **once per request** (not per stage) to skip stages
that don't apply to the current catalog.

Registered via ``register_plugin(stage)`` and discovered via
``get_protocols(CatalogPipelineProtocol)``.
"""

from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class CatalogPipelineProtocol(Protocol):
    """Per-catalog pipeline stage: enrich, filter, rewrite, or drop.

    Attributes:
        pipeline_id: Unique string identifier (e.g. ``"bq_catalog_stats"``).
        priority: Execution order — lower runs first. Use 100 for general
                  stages; < 100 for guards/filters.
    """

    pipeline_id: str
    priority: int

    def can_apply(self, catalog_id: str) -> bool:
        """Return ``True`` if this stage applies to the given catalog."""
        ...

    async def apply(
        self,
        catalog_id: str,
        catalog: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Apply this stage to the catalog metadata dict.

        Stages may:
          - Return the (possibly modified) catalog dict.
          - Return ``None`` to drop the catalog (→ consumer returns 404).
          - Return a different-shaped dict to replace the catalog.

        Must not mutate ``catalog`` in place — return a new dict.
        """
        ...
