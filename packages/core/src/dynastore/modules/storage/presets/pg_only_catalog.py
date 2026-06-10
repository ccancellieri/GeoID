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

"""``pg_only_catalog`` preset — catalog-tier PostgreSQL-only routing on all tiers.

A ``CATALOG``-tier preset applied via
``POST /admin/catalogs/{catalog_id}/presets/pg_only_catalog``.

Pins PG-only routing for all four resource tiers (catalog, collection, items,
assets) under a specific catalog. No Elasticsearch at any level — not even a
private items index.

``Operation.SEARCH`` is explicitly set on catalog and collection tiers so
the self-register helper cannot auto-append a discoverable ES store to a new
SEARCH op (#1102 / #1047). Asset ``UPLOAD`` is intentionally omitted — it is
auto-augmented from discovered ``AssetUploadProtocol`` impls.

Use this preset to explicitly lock a catalog to PG routing, overriding any
platform-level defaults that might include ES drivers.
"""
from __future__ import annotations

from typing import ClassVar, Tuple

from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
)

from .bundle_preset import BundlePreset
from .examples import PresetExample
from .protocol import PresetBundle, PresetBundleEntry, PresetTier


def _pg_catalog_routing() -> CatalogRoutingConfig:
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
            # Explicit SEARCH pin prevents _self_register_searchers_into from
            # auto-appending any discoverable ES CatalogStore (#1102 / #1047).
            Operation.SEARCH: [
                OperationDriverEntry(driver_ref="catalog_postgresql_driver"),
            ],
        },
    )


def _pg_collection_routing() -> CollectionRoutingConfig:
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
            # Explicit SEARCH pin — same rationale as catalog tier.
            Operation.SEARCH: [
                OperationDriverEntry(driver_ref="collection_postgresql_driver"),
            ],
        },
    )


def _pg_items_routing() -> ItemsRoutingConfig:
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
        },
    )


def _pg_asset_routing() -> AssetRoutingConfig:
    return AssetRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="asset_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="asset_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            # UPLOAD intentionally omitted — auto-augmented from discovered
            # AssetUploadProtocol impls at validation time.
        },
    )


class PgOnlyCatalogPreset(BundlePreset):
    """Catalog-tier PostgreSQL-only routing across all four resource tiers.

    Routes catalog, collection, items, and assets entirely through PG drivers
    with no Elasticsearch involvement at any level. Explicitly pins
    ``Operation.SEARCH`` on catalog and collection tiers so ES stores cannot
    be auto-registered. Asset ``UPLOAD`` is left to auto-augmentation.
    """

    name = "pg_only_catalog"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = False
    keywords: ClassVar[Tuple[str, ...]] = (
        "routing", "catalog", "postgresql", "pg-only", "no-elasticsearch",
    )
    description = (
        "Catalog-tier PostgreSQL-only routing for all four resource tiers "
        "(catalog, collection, items, assets). No Elasticsearch at any level. "
        "Apply to explicitly lock a catalog to PG-only routing, overriding any "
        "platform-level defaults that include ES drivers. "
        "SEARCH is pinned PG-only on catalog and collection tiers to prevent "
        "ES auto-registration."
    )

    examples: ClassVar[Tuple[PresetExample, ...]] = (
        PresetExample(
            name="lock-catalog-to-postgres",
            summary=(
                "Lock a catalog to PostgreSQL-only routing on all four tiers "
                "(catalog, collection, items, assets), overriding any platform "
                "default that would inject Elasticsearch drivers. SEARCH is pinned "
                "PG-only on the catalog and collection tiers so a discoverable ES "
                "store cannot be auto-registered. Apply at catalog scope via "
                "POST /admin/catalogs/{catalog_id}/presets/pg_only_catalog. Takes no "
                "parameters."
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
                    instance=_pg_catalog_routing(),
                    rollback_priority=30,
                ),
                PresetBundleEntry(
                    slot="collection_template",
                    config_cls=CollectionRoutingConfig,
                    instance=_pg_collection_routing(),
                    rollback_priority=20,
                ),
                PresetBundleEntry(
                    slot="items_template",
                    config_cls=ItemsRoutingConfig,
                    instance=_pg_items_routing(),
                    rollback_priority=10,
                ),
                PresetBundleEntry(
                    slot="asset_template",
                    config_cls=AssetRoutingConfig,
                    instance=_pg_asset_routing(),
                    rollback_priority=10,
                ),
            )
        )
