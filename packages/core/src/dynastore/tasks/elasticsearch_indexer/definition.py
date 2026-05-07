"""Lightweight OGC Process definition for the elasticsearch_indexer task.

Kept free of the elasticsearch / opensearch runtime deps so catalog services
without those installed can still surface the Process via ``/processes``.
"""
from dynastore.modules.processes.models import (
    JobControlOptions,
    Process,
    ProcessOutput,
    ProcessScope,
    TransmissionMode,
)
from dynastore.modules.processes.schema_gen import pydantic_to_process_inputs

from .indexer_models import ElasticsearchIndexerRequest

ELASTICSEARCH_INDEXER_PROCESS_DEFINITION = Process(
    id="elasticsearch_indexer",
    version="1.0.0",
    title="Elasticsearch Indexer",
    description=(
        "Bulk reindex catalog or collection-scoped data into the per-tenant "
        "Elasticsearch items index. When `collection_id` is omitted every "
        "collection of the catalog is processed."
    ),
    scopes=[ProcessScope.CATALOG],
    inputs=pydantic_to_process_inputs(ElasticsearchIndexerRequest),
    outputs={
        "result": ProcessOutput.model_validate(
            {"title": "Result", "schema": {"type": "object"}}
        )
    },
    jobControlOptions=[
        JobControlOptions.ASYNC_EXECUTE,
        JobControlOptions.SYNC_EXECUTE,
    ],
    outputTransmission=[TransmissionMode.VALUE],
)
