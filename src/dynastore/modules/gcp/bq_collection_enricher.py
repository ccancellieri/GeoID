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
BigQueryMetadataTransformDriver — TRANSFORM-capability metadata driver.

Produces a partial collection-metadata envelope containing BigQuery-sourced
statistics (row counts, last-modified timestamps, etc.) under the
``bq_stats`` key.  The router's TRANSFORM chain merges this partial into the
main envelope when an endpoint opts into transform-aware output or the async
reindex pipeline is preparing a transformed INDEX / BACKUP envelope.

Replaces the deleted ``CollectionMetadataEnricherProtocol`` /
``BigQueryCollectionEnricher`` — one mechanism, one priority model, one SLA
model.  See role-based driver plan §Routing / §Transformer.

Activation: per-collection ``bq_stats`` config (via ``ConfigsProtocol``).
Absent config → returns ``None`` from ``get_metadata()``; the router skips
the partial entirely.
"""

import logging
from typing import Any, ClassVar, Dict, FrozenSet, Optional

from dynastore.models.protocols.driver_roles import DriverSla
from dynastore.models.protocols.entity_store import (
    EntityStoreCapability,
    TransformOnlyCollectionStoreMixin,
)
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

BQ_STATS_CONFIG_ID = "bq_stats"


class BigQueryMetadataTransformDriver(TransformOnlyCollectionStoreMixin):
    """TRANSFORM driver backed by BigQuery, returning ``bq_stats`` partials.

    Structurally satisfies :class:`CollectionStore` via the
    :class:`TransformOnlyCollectionStoreMixin` — which provides
    default-raising stubs for the non-TRANSFORM methods — plus this
    class's own ``get_metadata`` / ``is_available`` implementations.

    Role-based driver attributes (plan §Driver roles):
    - ``capabilities`` — ``{TRANSFORM}`` — router never picks this as a
                          Primary for READ / WRITE / SEARCH.
    - ``sla``          — 2 s timeout, ``degrade`` on timeout (BigQuery is
                          high-variance; never block the hot path for it).
    """

    # --- Role-based driver attributes ---------------------------------------
    capabilities: FrozenSet[str] = frozenset({EntityStoreCapability.TRANSFORM})
    sla: ClassVar[DriverSla] = DriverSla(
        timeout_ms=2000,
        on_timeout="degrade",
        required=False,
    )

    # --- TRANSFORM entry point ----------------------------------------------

    async def get_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return a partial envelope with BQ stats, or ``None`` if not configured.

        When the router's TRANSFORM chain invokes this driver, the returned
        dict is merged into the main envelope.  Empty / None → no contribution.
        """
        from dynastore.models.protocols import BigQueryProtocol, ConfigsProtocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return None

        try:
            stats_config = await configs.get_config(
                BQ_STATS_CONFIG_ID,  # type: ignore[arg-type]
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception:
            return None  # no config → nothing to contribute

        if not stats_config:
            return None

        bq = get_protocol(BigQueryProtocol)
        if not bq:
            logger.debug(
                "BigQueryMetadataTransformDriver: BigQueryProtocol not available for %s/%s",
                catalog_id, collection_id,
            )
            return None

        query = getattr(stats_config, "query", None)
        project_id = getattr(stats_config, "project_id", None)
        if not query or not project_id:
            return None

        try:
            records = await bq.execute_query(query, project_id)
        except Exception as exc:
            logger.warning(
                "BigQueryMetadataTransformDriver query failed for %s/%s: %s",
                catalog_id, collection_id, exc,
            )
            return None

        if not records:
            return None
        return {"bq_stats": records[0]}

    # --- Health check override (TransformOnly mixin doesn't implement this) --

    async def is_available(self) -> bool:
        """Available iff the BigQueryProtocol plugin is registered."""
        from dynastore.models.protocols import BigQueryProtocol

        return get_protocol(BigQueryProtocol) is not None
