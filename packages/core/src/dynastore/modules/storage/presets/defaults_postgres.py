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

"""``defaults_postgres`` preset — platform-tier PG-first routing defaults.

A ``PLATFORM``-tier preset (#972): applied with no scope params via
``POST /admin/presets/defaults_postgres``, it pins PG-first routing
templates at the platform scope so freshly-created catalogs inherit a
sane PostgreSQL-only baseline before any catalog/collection override.

Platform scope is the ``set_config(catalog_id=None, collection_id=None)``
path. The preset emits only routing templates (no audience opt-ins) so it
stays safe to apply on a bare platform: it sets defaults, never opens an
anonymous surface.
"""
from __future__ import annotations

from typing import ClassVar, Tuple

from dynastore.modules.storage.routing_config import (
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


class DefaultsPostgresPreset(BundlePreset):
    """Platform-tier PG-first routing defaults for catalog/collection/items."""

    name = "defaults_postgres"
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    keywords: ClassVar[Tuple[str, ...]] = (
        "routing", "platform", "defaults", "baseline", "postgresql", "pg-only",
    )
    description = (
        "Platform-tier PostgreSQL-first routing defaults across the "
        "catalog, collection, and items tiers. Applied with no scope "
        "(POST /admin/presets/defaults_postgres) so new catalogs inherit "
        "a PG-only baseline. No indexers, no audience opt-ins — a safe "
        "starting posture before catalog/collection overrides."
    )

    examples: ClassVar[Tuple[PresetExample, ...]] = (
        PresetExample(
            name="platform-postgres-baseline",
            summary=(
                "Pin a PostgreSQL-only routing baseline at the platform scope so "
                "freshly-created catalogs inherit a safe PG-first posture (no "
                "indexers, no anonymous surface) before any catalog/collection "
                "override. Apply with no scope via "
                "POST /admin/presets/defaults_postgres. Takes no parameters."
            ),
            params={},
        ),
    )

    def build(self, **_scope: str) -> PresetBundle:
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
            )
        )
