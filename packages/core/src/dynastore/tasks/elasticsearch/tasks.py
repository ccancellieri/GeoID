import logging
from typing import Any, Dict, Optional
from pydantic import BaseModel

# Hard runtime dep — see modules/elasticsearch/module.py for rationale.
# Forces entry-point load to fail on services without ``opensearch-py`` so
# the CapabilityMap doesn't list these tasks as claimable there.
import opensearchpy  # noqa: F401

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import TaskPayload

logger = logging.getLogger(__name__)

class ElasticsearchIndexInputs(BaseModel):
    entity_type: str  # 'catalog', 'collection', 'item'
    entity_id: str
    catalog_id: str
    collection_id: Optional[str] = None
    item_id: Optional[str] = None
    payload: Dict[str, Any]

class ElasticsearchIndexTask(TaskProtocol):
    priority: int = 100
    """
    Durable task for indexing or updating a document in Elasticsearch.
    """
    task_type = "elasticsearch_index"

    async def run(self, payload: TaskPayload[ElasticsearchIndexInputs]) -> Dict[str, Any]:
        from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_index_name, get_mapping

        inputs = payload.inputs
        index_name = get_index_name(get_index_prefix(), inputs.entity_type)

        logger.info(f"ElasticsearchIndexTask: Indexing {inputs.entity_type} '{inputs.entity_id}' into '{index_name}'")

        es = get_client()
        if es is None:
            raise RuntimeError("Elasticsearch client is not initialized.")

        # Ensure index exists
        if not await es.indices.exists(index=index_name):
            mapping = get_mapping(inputs.entity_type)
            logger.info(f"Creating index '{index_name}' with mapping for '{inputs.entity_type}'")
            await es.indices.create(index=index_name, body={"mappings": mapping})

        response = await es.index(
            index=index_name,
            id=inputs.entity_id,
            body=inputs.payload
        )

        logger.debug(f"Elasticsearch indexing response: {response}")

        return {
            "entity_id": inputs.entity_id,
            "index": index_name,
            "status": "indexed"
        }

class ElasticsearchDeleteInputs(BaseModel):
    entity_type: str
    entity_id: str

class ElasticsearchDeleteTask(TaskProtocol):
    priority: int = 100
    """
    Durable task for deleting a document from Elasticsearch.
    """
    task_type = "elasticsearch_delete"

    async def run(self, payload: TaskPayload[ElasticsearchDeleteInputs]) -> Dict[str, Any]:
        from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_index_name

        from opensearchpy.exceptions import NotFoundError

        inputs = payload.inputs
        index_name = get_index_name(get_index_prefix(), inputs.entity_type)

        logger.info(f"ElasticsearchDeleteTask: Deleting {inputs.entity_type} '{inputs.entity_id}' from '{index_name}'")

        es = get_client()
        if es is None:
            raise RuntimeError("Elasticsearch client is not initialized.")

        try:
            await es.delete(index=index_name, id=inputs.entity_id)
        except NotFoundError:
            logger.debug(f"Document {inputs.entity_id} not found in {index_name}, safe to ignore.")

        return {
            "entity_id": inputs.entity_id,
            "status": "deleted"
        }
