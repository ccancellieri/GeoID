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
Shared helper for running ``CollectionPipelineProtocol`` stages.

STAC, Features, Coverages, and Records each serve a
``/collections/{collection_id}`` endpoint. Each needs to run the active
pipeline stages in priority order with a uniform drop-to-404 semantic.

This helper does the priority + drop loop once so the consumer endpoints
don't duplicate it. If any stage returns ``None`` the helper returns
``None`` immediately — the caller is responsible for translating that
into an HTTP 404.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


async def apply_collection_pipeline(
    catalog_id: str,
    collection_id: str,
    collection: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Run all applicable CollectionPipelineProtocol stages.

    Stages are discovered via ``get_protocols(CollectionPipelineProtocol)``
    and invoked in ascending ``priority`` order. Only stages whose
    ``can_apply(catalog_id, collection_id)`` returns ``True`` run.

    Returns the final collection dict, or ``None`` if any stage dropped
    the collection. Stage exceptions are logged and the stage is skipped
    — a misbehaving stage must not take down the response.
    """
    try:
        from dynastore.models.protocols.collection_pipeline import (
            CollectionPipelineProtocol,
        )
        from dynastore.tools.discovery import get_protocols
    except Exception:
        return collection  # discovery failure → pass-through

    stages = sorted(
        get_protocols(CollectionPipelineProtocol),
        key=lambda s: getattr(s, "priority", 100),
    )
    if not stages:
        return collection

    ctx = context or {}
    current: Optional[Dict[str, Any]] = collection
    for stage in stages:
        try:
            if not stage.can_apply(catalog_id, collection_id):
                continue
            result = await stage.apply(catalog_id, collection_id, current, ctx)
        except Exception as err:
            logger.warning(
                "CollectionPipeline stage '%s' failed for %s/%s: %s",
                getattr(stage, "pipeline_id", type(stage).__name__),
                catalog_id,
                collection_id,
                err,
            )
            continue
        if result is None:
            return None  # stage dropped the collection
        current = result
    return current
