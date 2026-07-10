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
)

from typing import ClassVar, Tuple

from .bundle_preset import BundlePreset
from .examples import PresetExample
from .protocol import PresetBundle, PresetBundleEntry, PresetTier


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
            ],
            Operation.READ: [
                OperationDriverEntry(driver_ref="items_postgresql_driver"),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


class PublicCatalogPreset(BundlePreset):
    """PG-first storage + public ES indexers on every tier."""

    name = "public_catalog"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = False
    keywords: ClassVar[Tuple[str, ...]] = (
        "routing", "catalog", "public", "open-data", "elasticsearch", "postgresql",
    )
    description = (
        "PG-first storage + public Elasticsearch indexers across the "
        "catalog, collection, and items tiers. No IAM audience opt-ins; "
        "anonymous read/listing is governed by the platform's default "
        "public_access policy. Suitable for fully-public open-data catalogs."
    )

    examples: ClassVar[Tuple[PresetExample, ...]] = (
        PresetExample(
            name="public-open-data-catalog",
            summary=(
                "Provision a fully-public open-data catalog: PostgreSQL is the system "
                "of record on every tier and a public Elasticsearch secondary index is "
                "wired for SEARCH. Apply at catalog scope via "
                "POST /admin/catalogs/{catalog_id}/presets/public_catalog. The preset "
                "takes no parameters; anonymous access is governed by the platform "
                "public_access policy."
            ),
            params={},
        ),
    )

    def build(self, catalog_id: str, **_scope: str) -> PresetBundle:  # noqa: ARG002
        return PresetBundle(
            entries=(
                PresetBundleEntry(
                    slot="catalog_routing",
                    config_cls=CatalogRoutingConfig,
                    instance=_public_catalog_routing(),
                    rollback_priority=30,
                ),
                PresetBundleEntry(
                    slot="collection_template",
                    config_cls=CollectionRoutingConfig,
                    instance=_public_collection_template(),
                    rollback_priority=20,
                ),
                PresetBundleEntry(
                    slot="items_template",
                    config_cls=ItemsRoutingConfig,
                    instance=_public_items_template(),
                    rollback_priority=10,
                ),
            )
        )
