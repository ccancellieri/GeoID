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

"""``public_catalog`` preset — PG-first storage + public Elasticsearch
indexers on all three tiers (catalog / collection / items).

No IAM audience opt-ins; anonymous read/listing is governed by the
platform's default ``public_access`` policy. The emitted routing pins
make the implicit model defaults explicit so the admin tooling +
cascade validator have something to inspect.
"""
from __future__ import annotations

from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
)

from typing import ClassVar

from .protocol import PresetBundle, PresetTier


def _public_catalog_routing() -> CatalogRoutingConfig:
    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    source="auto",
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


def _public_collection_template() -> CollectionRoutingConfig:
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    source="auto",
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


def _public_items_template() -> ItemsRoutingConfig:
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
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(driver_ref="items_postgresql_driver"),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    source="auto",
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


class PublicCatalogPreset:
    """PG-first storage + public ES indexers on every tier."""

    name = "public_catalog"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    description = (
        "PG-first storage + public Elasticsearch indexers across the "
        "catalog, collection, and items tiers. No IAM audience opt-ins; "
        "anonymous read/listing is governed by the platform's default "
        "public_access policy. Suitable for fully-public open-data catalogs."
    )

    def build(self, catalog_id: str) -> PresetBundle:  # noqa: ARG002
        return PresetBundle(
            catalog_routing=_public_catalog_routing(),
            collection_template=_public_collection_template(),
            items_template=_public_items_template(),
            audience_configs={},
        )
