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

# title/description are plain str; Process.title/.description are typed
# CoercibleLocalizedText (accepts str at runtime via a BeforeValidator, but not
# statically as a `Process(title=...)` kwarg). model_validate() runs the same
# validators against an untyped mapping, same as the ProcessOutput below.
ELASTICSEARCH_INDEXER_PROCESS_DEFINITION = Process.model_validate({
    "id": "elasticsearch_indexer",
    "version": "1.0.0",
    "title": "Elasticsearch Indexer",
    "description": (
        "Bulk reindex catalog or collection-scoped data into the per-tenant "
        "Elasticsearch items index. When `collection_id` is omitted every "
        "collection of the catalog is processed."
    ),
    "scopes": [ProcessScope.CATALOG],
    "inputs": pydantic_to_process_inputs(ElasticsearchIndexerRequest),
    "outputs": {
        "result": ProcessOutput.model_validate(
            {"title": "Result", "schema": {"type": "object"}}
        )
    },
    "jobControlOptions": [
        JobControlOptions.ASYNC_EXECUTE,
        JobControlOptions.SYNC_EXECUTE,
    ],
    "outputTransmission": [TransmissionMode.VALUE],
})
