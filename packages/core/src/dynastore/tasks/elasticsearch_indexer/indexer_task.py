"""ElasticsearchIndexerTask — canonical entry point for ES bulk reindex.

Single OGC Process (`elasticsearch_indexer`) that branches at runtime based
on the unified ``ElasticsearchIndexerRequest`` payload:

  * ``collection_id`` set  → delegates to ``BulkCollectionReindexTask``
  * ``collection_id`` None → delegates to ``BulkCatalogReindexTask``

Registers under ``task_type = "elasticsearch_indexer"`` so the dispatcher's
:class:`BackgroundRunner` can claim it in-process AND
:class:`GcpJobRunner` can claim it (the deployed
``dynastore-elasticsearch-indexer`` Cloud Run Job advertises
``TASK_TYPE=elasticsearch_indexer``). Selection between the two runners is
made by ``ExecutionEngine.execute()`` based on the requested mode and which
runner reports ``can_handle`` first.

The legacy fan-out task types ``elasticsearch_bulk_reindex_catalog`` and
``elasticsearch_bulk_reindex_collection`` remain registered (the existing
admin reindex routes in ``extensions/search/search_service.py`` still
dispatch them directly). This wrapper is the new canonical entry point for
OGC Processes callers and any new integration sites.
"""
from typing import Any, Dict, Optional

from dynastore.modules.processes.models import (
    ExecuteRequest,
    Process,
    StatusInfo,
)
from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks.protocols import TaskProtocol

from .definition import ELASTICSEARCH_INDEXER_PROCESS_DEFINITION
from .indexer_models import ElasticsearchIndexerRequest

# Importing the bulk task module triggers TaskProtocol.__init_subclass__
# registration for both BulkCatalogReindexTask and BulkCollectionReindexTask.
# This keeps the legacy `elasticsearch_bulk_reindex_*` task types claimable
# by the dispatcher even when only this entry point is loaded.
from . import tasks as _bulk_tasks  # noqa: F401  -- side-effect: register bulk task classes
from .tasks import BulkCatalogReindexInputs, BulkCollectionReindexInputs


class ElasticsearchIndexerTask(
    TaskProtocol[Process, TaskPayload[ExecuteRequest], Optional[StatusInfo]]
):
    priority: int = 100
    task_type = "elasticsearch_indexer"

    @staticmethod
    def get_definition() -> Process:
        return ELASTICSEARCH_INDEXER_PROCESS_DEFINITION

    def __init__(self, app_state: object):
        self.app_state = app_state

    async def run(self, payload: TaskPayload[ExecuteRequest]) -> Dict[str, Any]:
        # OGC Process payload shape: payload.inputs is an ExecuteRequest whose
        # `inputs` field is the user-supplied dict. Adapt to the bulk task's
        # plain-TaskPayload shape (inputs as a dict).
        request_inputs = (
            payload.inputs.inputs
            if hasattr(payload.inputs, "inputs")
            else payload.inputs
        )
        request = ElasticsearchIndexerRequest.model_validate(request_inputs)

        if request.collection_id:
            sub_inputs = BulkCollectionReindexInputs(
                catalog_id=request.catalog_id,
                collection_id=request.collection_id,
                driver=request.driver,
            )
            sub_payload: TaskPayload = TaskPayload(
                task_id=payload.task_id,
                caller_id=payload.caller_id,
                inputs=sub_inputs.model_dump(),
            )
            collection_impl = _get_task_instance("elasticsearch_bulk_reindex_collection")
            return await collection_impl.run(sub_payload)

        sub_inputs = BulkCatalogReindexInputs(
            catalog_id=request.catalog_id,
            driver=request.driver,
        )
        sub_payload = TaskPayload(
            task_id=payload.task_id,
            caller_id=payload.caller_id,
            inputs=sub_inputs.model_dump(),
        )
        catalog_impl = _get_task_instance("elasticsearch_bulk_reindex_catalog")
        return await catalog_impl.run(sub_payload)


def _get_task_instance(task_type: str):
    """Resolve a registered task class via the same lookup the dispatcher uses.

    Lazy import to avoid a circular dependency: this module is loaded by the
    `dynastore.tasks` discovery, which would otherwise be re-entered.
    """
    from dynastore.tasks import get_task_instance
    instance = get_task_instance(task_type)
    if instance is None:
        raise RuntimeError(
            f"ElasticsearchIndexerTask: required sub-task '{task_type}' is not "
            "registered. Ensure dynastore.tasks.elasticsearch_indexer.tasks was "
            "loaded (entry-point side effect)."
        )
    return instance
