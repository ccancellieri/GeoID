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

"""``assets_local_only`` preset — local-disk upload backend, on-prem only.

An ``ASSETS``-tier preset (#972) with ``catalog_scopable=True``, so it
reaches both admin URL families:

* **assets @ catalog** —
  ``POST /admin/catalogs/{cat}/presets/assets_local_only``
  sets the catalog-tier asset routing that future collections inherit.
* **assets @ collection** —
  ``POST /admin/catalogs/{cat}/collections/{col}/presets/assets_local_only``
  pins the same asset routing on one collection, overriding the catalog
  template for that collection only.

The routing pins ``local_upload_module`` as the UPLOAD driver with
``source="operator"`` (invariant under auto-augment). Upload is
synchronous — the client POSTs the file to the server endpoint
(``POST /local-upload/{ticket_id}``) which writes to disk and registers
the asset in a single transaction.

**On-prem restriction**: this preset is only registered when
``google.cloud.storage`` is NOT importable, which signals a local /
docker-compose deployment. In cloud deployments the GCS module is
available and ``assets_gcs_uploads`` is the canonical upload preset.
Attempting to select this preset in a cloud image will yield a 404
(preset not in registry) rather than a silent mis-configuration.

The bundle leaves ``scope`` empty: the admin endpoint layers the
URL-derived ``catalog_id`` (and ``collection_id`` at collection scope)
on top of each entry, so the same ``build`` output applies correctly at
either scope.
"""
from __future__ import annotations

import importlib.util
from typing import Any, ClassVar, Tuple

from dynastore.modules.storage.routing_config import AssetRoutingConfig

from .bundle_preset import BundlePreset
from .examples import PresetExample
from .protocol import PresetBundle, PresetBundleEntry, PresetTier


def _local_backend_available() -> bool:
    """True when ``google.cloud.storage`` is NOT importable.

    When GCS is not installed this is a local / docker-compose deployment
    and the local-disk upload backend is the canonical upload path.
    Mirrors the ``_osgeo_available()`` gate in
    ``modules/tasks/routing/presets.py``.

    ``find_spec`` on a dotted sub-package name raises ``ModuleNotFoundError``
    when the parent namespace package (``google.cloud``) has never been
    initialised. The try/except makes this function safe to call before any
    google import has occurred.
    """
    try:
        return importlib.util.find_spec("google.cloud.storage") is None
    except ModuleNotFoundError:
        return True


def _build_local_asset_routing() -> Any:
    """AssetRoutingConfig pinning ``local_upload_module`` as the UPLOAD backend.

    UPLOAD is pinned ``source="operator"`` so
    ``_self_register_upload_into`` treats the list as operator-managed
    and does not append additional upload backends. WRITE and READ are
    left at the platform defaults (PG only).
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
                    driver_ref="local_upload_module",
                    on_failure=FailurePolicy.FATAL,
                    write_mode=WriteMode.SYNC,
                    source="operator",
                ),
            ],
        },
    )


class AssetsLocalOnlyPreset(BundlePreset):
    """Assets-tier local-disk upload preset for on-prem deployments.

    Pins ``local_upload_module`` as the UPLOAD backend
    (``source="operator"``). The client POSTs the file to the server;
    registration is synchronous. No cloud storage is involved.

    Only registered when ``google.cloud.storage`` is not importable —
    i.e., in a docker-compose / on-prem image without GCS support.
    """

    name = "assets_local_only"
    tier: ClassVar[PresetTier] = PresetTier.ASSETS
    catalog_scopable: ClassVar[bool] = True
    keywords: ClassVar[Tuple[str, ...]] = (
        "routing", "assets", "upload", "local", "on-prem", "docker-compose",
    )
    description = (
        "Assets-tier local-disk upload preset (on-prem / docker-compose "
        "only). Pins local_upload_module as the UPLOAD driver "
        "(operator-managed, no auto-augment). The client POSTs the file "
        "to the server; registration is synchronous. Only available in "
        "deployments without google.cloud.storage installed. Applied at "
        "catalog scope it sets the upload template future collections "
        "inherit (inherit-only, no retro-apply); applied at collection "
        "scope it overrides upload routing for one collection."
    )

    examples: ClassVar[Tuple[PresetExample, ...]] = (
        PresetExample(
            name="on-prem-local-uploads",
            summary=(
                "Route asset uploads to the local-disk backend for an on-prem / "
                "docker-compose deployment: the client POSTs the file to the server "
                "endpoint, which writes it to disk and registers the asset in one "
                "synchronous transaction. WRITE/READ stay PG-only. Apply at catalog "
                "scope via POST /admin/catalogs/{catalog_id}/presets/assets_local_only "
                "(or at collection scope to override one collection). Takes no "
                "parameters. Only registered when google.cloud.storage is absent."
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
                    instance=_build_local_asset_routing(),
                    rollback_priority=10,
                ),
            )
        )
