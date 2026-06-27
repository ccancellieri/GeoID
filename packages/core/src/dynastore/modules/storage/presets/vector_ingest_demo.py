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

"""``vector_ingest_demo`` preset — one-click vector ingestion demo (PLATFORM tier).

Applies at platform scope (``POST /configs/presets/vector_ingest_demo``):

  1. Creates the ``demo_vector_catalog`` catalog and
     ``demo_vector_collection`` collection (skip-if-exists). The collection
     is configured with PG-primary + Elasticsearch-secondary routing so
     items ingested by the background task appear in STAC search.

  2. Submits an async ``ingestion`` job that reads the Natural Earth 110m
     admin-0 country polygons GeoJSON (~180 features, ~200 KB) and upserts
     every feature into the collection.

Source: Natural Earth 110m countries GeoJSON via the nvkelso/natural-earth-vector
GitHub mirror (raw.githubusercontent.com — reliable, no auth required).

All source columns are mapped automatically (``attributes_source_type=all``).

Revoke: removes items, collection, and catalog in order (collection and
catalog are left if they still hold other data not introduced by this preset).
"""
from __future__ import annotations

from typing import Iterable

from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
)

from .multi_contributor import MultiContributorPreset
from .preset import DataSeed, TaskSeed

_CATALOG_ID = "demo_vector_catalog"
_COLLECTION_ID = "demo_vector_collection"

_CATALOG_DATA = {
    "id": _CATALOG_ID,
    "title": {"en": "Vector Ingestion Demo"},
    "description": {
        "en": (
            "Demo catalog populated by the vector_ingest_demo preset. "
            "Contains country polygon features ingested from Natural Earth."
        ),
    },
    "keywords": ["demo", "vector", "ingestion", "platform"],
    "license": "CC-BY-4.0",
}

_COLLECTION_DATA = {
    "id": _COLLECTION_ID,
    "title": {"en": "Natural Earth Countries (110m)"},
    "description": {
        "en": (
            "Country polygon features at 1:110m scale sourced from Natural Earth "
            "and ingested via the vector_ingest_demo preset."
        ),
    },
}

# Natural Earth 110m admin-0 countries — ~180 polygon features, ~200 KB.
# Hosted on GitHub raw CDN; reliable and publicly accessible without auth.
_NE_COUNTRIES_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
    "master/geojson/ne_110m_admin_0_countries.geojson"
)


def _vector_items_routing() -> ItemsRoutingConfig:
    """PG-primary + Elasticsearch-secondary routing for the demo collection.

    Items written by the ingestion task land in Postgres first; the async
    outbox fan-out indexes them in Elasticsearch so STAC search returns them.
    Mirrors the demo_data routing pattern so the preset is immediately
    searchable after the ingestion job completes.
    """
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    secondary_index=True,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(driver_ref="items_postgresql_driver"),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


class _VectorIngestDemoContributor:
    """Data + task contributor for the vector_ingest_demo preset."""

    def get_data(self) -> Iterable[DataSeed]:
        # Create the catalog and collection so the async ingestion job has a
        # valid target.  No items are seeded here — the ingestion task writes
        # them directly to the collection via the items write pipeline.
        yield DataSeed(
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
            catalog_data=_CATALOG_DATA,
            collection_data=_COLLECTION_DATA,
            items=(),
            manage_catalog=True,
            manage_collection=True,
            items_routing=_vector_items_routing(),
        )

    def get_tasks(self) -> Iterable[TaskSeed]:
        # The ingestion task reads catalog_id + collection_id from the inputs
        # dict at runtime (see tasks/ingestion/ingestion_task.py:84-85).
        # ``ingestion_request`` must match TaskIngestionRequest's shape.
        yield TaskSeed(
            process_id="ingestion",
            inputs={
                "catalog_id": _CATALOG_ID,
                "collection_id": _COLLECTION_ID,
                "ingestion_request": {
                    "asset": {
                        "uri": _NE_COUNTRIES_URL,
                    },
                    "column_mapping": {
                        "attributes_source_type": "all",
                    },
                },
            },
            async_mode=True,
            dedup_key="preset:vector_ingest_demo:ingestion",
        )


VECTOR_INGEST_DEMO_PRESET = MultiContributorPreset(
    name="vector_ingest_demo",
    description=(
        "Create the demo_vector_catalog/demo_vector_collection and submit an async "
        "ingestion job that reads the Natural Earth 110m country polygons GeoJSON "
        "and upserts ~180 features into the collection. Items are indexed in "
        "Elasticsearch via PG-primary + async-secondary routing so STAC search "
        "returns them after the job completes. Designed for CI / demo use."
    ),
    keywords=("demo", "data", "platform", "vector", "ingestion"),
    contributors_factory=lambda: [_VectorIngestDemoContributor()],
)
