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

"""collection_hard_delete task runner.

Executes the full hard-delete purge+cascade for a single collection
asynchronously.  The HTTP DELETE handler enqueues this task and returns HTTP
202 immediately; the task runs the same ``delete_collection(force=True)`` path
the synchronous handler used to call inline.

Outcome contract:
- Collection exists and can be purged → task completes successfully.
- Collection already gone (race with another delete) → treated as success
  (idempotent; delete_collection returns False for MISSING).
- Service-layer exception → task raises RuntimeError so the dispatcher retries
  with backoff up to ``max_retries``.
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, Optional

from pydantic import BaseModel, ConfigDict

from dynastore.tasks.protocols import TaskProtocol

logger = logging.getLogger(__name__)


class CollectionHardDeleteInputs(BaseModel):
    """Validated inputs for a ``collection_hard_delete`` task row."""

    model_config = ConfigDict(extra="ignore")

    catalog_id: str
    collection_id: str


class CollectionHardDeleteTask(TaskProtocol):
    """Durable asynchronous hard-delete of a single collection.

    Enqueued by the DELETE /catalogs/{catalog_id}/collections/{collection_id}
    handler when ``force=true``.  Calls the service-layer
    ``delete_collection(force=True)`` which sets lifecycle_status='deleting',
    emits BEFORE/HARD_DELETION/AFTER events, purges storage, and schedules the
    external-cleanup cascade.
    """

    task_type: ClassVar[str] = "collection_hard_delete"
    priority: int = 70
    mandatory: ClassVar[bool] = True
    affinity_tier: ClassVar[Optional[str]] = "catalog"
    payload_model: ClassVar[Optional[type]] = CollectionHardDeleteInputs

    def is_available(self) -> bool:  # pragma: no cover
        return True

    async def run(self, payload: Any) -> Dict[str, Any]:
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.modules import get_protocol

        inputs_raw = getattr(payload, "inputs", None) or {}
        inputs = CollectionHardDeleteInputs.model_validate(inputs_raw)

        catalog_id = inputs.catalog_id
        collection_id = inputs.collection_id

        catalogs_svc = get_protocol(CatalogsProtocol)
        if catalogs_svc is None:
            raise RuntimeError(
                f"collection_hard_delete: CatalogsProtocol not available — "
                f"cannot delete '{catalog_id}:{collection_id}'."
            )

        logger.info(
            "collection_hard_delete: starting purge for '%s:%s'.",
            catalog_id, collection_id,
        )
        try:
            deleted = await catalogs_svc.delete_collection(
                catalog_id, collection_id, force=True
            )
        except Exception as exc:
            logger.error(
                "collection_hard_delete: purge failed for '%s:%s': %s",
                catalog_id, collection_id, exc, exc_info=True,
            )
            raise RuntimeError(
                f"collection_hard_delete: purge failed for "
                f"'{catalog_id}:{collection_id}': {exc}"
            ) from exc

        if not deleted:
            logger.info(
                "collection_hard_delete: '%s:%s' was already gone (MISSING); "
                "treating as success.",
                catalog_id, collection_id,
            )
        else:
            logger.info(
                "collection_hard_delete: purge complete for '%s:%s'.",
                catalog_id, collection_id,
            )

        return {
            "deleted": bool(deleted),
            "catalog_id": catalog_id,
            "collection_id": collection_id,
        }
