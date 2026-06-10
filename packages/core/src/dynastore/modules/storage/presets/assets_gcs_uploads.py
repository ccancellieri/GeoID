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

"""``assets_gcs_uploads`` preset — GCS upload backend, cloud default.

An ``ASSETS``-tier preset (#972) with ``catalog_scopable=True``, so it
reaches both admin URL families:

* **assets @ catalog** —
  ``POST /admin/catalogs/{cat}/presets/assets_gcs_uploads``
  sets the catalog-tier asset routing that future collections inherit.
* **assets @ collection** —
  ``POST /admin/catalogs/{cat}/collections/{col}/presets/assets_gcs_uploads``
  pins the same asset routing on one collection, overriding the catalog
  template for that collection only.

The routing pins ``gcp_module`` as the UPLOAD driver with
``source="operator"`` (invariant under auto-augment) and leaves
WRITE/READ at PG defaults. Asset eventing (GCS Pub/Sub
OBJECT_FINALIZE → finalize activator) is inherent to ``gcp_module``
— no separate ``GcpEventingConfig`` entry is needed.

The bundle leaves ``scope`` empty: the admin endpoint layers the
URL-derived ``catalog_id`` (and ``collection_id`` at collection scope)
on top of each entry, so the same ``build`` output applies correctly at
either scope.
"""
from __future__ import annotations

from typing import Any, ClassVar, Tuple

from dynastore.modules.storage.routing_config import AssetRoutingConfig

from .bundle_preset import BundlePreset
from .examples import PresetExample
from .protocol import PresetBundle, PresetBundleEntry, PresetTier


def _build_gcs_asset_routing() -> Any:
    """AssetRoutingConfig pinning ``gcp_module`` as the UPLOAD backend.

    UPLOAD is pinned ``source="operator"`` so
    ``_self_register_upload_into`` treats the list as operator-managed
    and does not append additional upload backends. WRITE and READ are
    left at the platform defaults (PG only); an operator who also wants
    an ES asset index should compose this preset with a separate
    asset-indexer preset.
    """
    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        FailurePolicy,
        Operation,
        OperationDriverEntry,
        WriteMode,
    )

    return AssetRoutingConfig(
        operations={
            Operation.UPLOAD: [
                OperationDriverEntry(
                    driver_ref="gcp_module",
                    on_failure=FailurePolicy.FATAL,
                    write_mode=WriteMode.SYNC,
                    source="operator",
                ),
            ],
        },
    )


class AssetsGcsUploadsPreset(BundlePreset):
    """Assets-tier GCS upload preset for cloud deployments.

    Pins ``gcp_module`` as the UPLOAD backend (``source="operator"``).
    GCS eventing (Pub/Sub OBJECT_FINALIZE → finalize activator) fires
    automatically once the GCP module is configured for the catalog.
    Applicable at catalog scope (sets the template future collections
    inherit — inherit-only, no retro-apply) and at collection scope
    (overrides upload routing for one collection).
    """

    name = "assets_gcs_uploads"
    tier: ClassVar[PresetTier] = PresetTier.ASSETS
    catalog_scopable: ClassVar[bool] = True
    keywords: ClassVar[Tuple[str, ...]] = (
        "routing", "assets", "upload", "gcs", "cloud", "eventing",
    )
    description = (
        "Assets-tier GCS upload preset (cloud default). Pins gcp_module "
        "as the UPLOAD driver (operator-managed, no auto-augment). GCS "
        "eventing (OBJECT_FINALIZE → finalize activator) is inherent to "
        "the GCS backend. Applied at catalog scope it sets the upload "
        "template future collections inherit (inherit-only, no retro-apply); "
        "applied at collection scope it overrides upload routing for one "
        "collection."
    )

    examples: ClassVar[Tuple[PresetExample, ...]] = (
        PresetExample(
            name="cloud-gcs-uploads",
            summary=(
                "Route asset uploads to Google Cloud Storage for a cloud deployment: "
                "gcp_module is pinned as the UPLOAD backend (operator-managed) and GCS "
                "eventing (OBJECT_FINALIZE → finalize activator) fires automatically "
                "once the GCP module is configured for the catalog. WRITE/READ stay "
                "PG-only. Apply at catalog scope via "
                "POST /admin/catalogs/{catalog_id}/presets/assets_gcs_uploads (or at "
                "collection scope to override one collection). Takes no parameters."
            ),
            params={},
        ),
    )

    def build(self, **_scope: str) -> PresetBundle:
        return PresetBundle(
            entries=(
                PresetBundleEntry(
                    slot="asset_template",
                    config_cls=AssetRoutingConfig,
                    instance=_build_gcs_asset_routing(),
                    rollback_priority=10,
                ),
            )
        )
