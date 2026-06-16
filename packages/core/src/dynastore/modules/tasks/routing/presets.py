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

"""Deployment-profile presets for task routing and asset upload.

``CloudTaskRoutingPreset``, ``OnpremTaskRoutingPreset``, and
``ReviewTaskRoutingPreset`` each materialise a ``TaskRoutingConfig``
(worker vs Cloud Run Jobs placement) **and** an ``AssetRoutingConfig``
that pins the UPLOAD driver for the profile, so applying a single preset
configures both task placement and upload backend in one atomic operation.

Upload driver assignment per profile:

- ``onprem``  — UPLOAD → ``local_upload_module`` (local-disk, on-premise)
- ``cloud``   — UPLOAD → ``gcp_module`` (Google Cloud Storage)
- ``review``  — UPLOAD → ``gcp_module`` (Cloud Run images ship with GCS)

All UPLOAD entries carry ``source="operator"`` so the
``_self_register_upload_into`` auto-augment path (routing_config.py) treats
the list as operator-managed and does not append additional upload drivers.

The preset objects are module-level singletons. ``_register()`` runs at
import time (bottom of this module), publishing the presets into the
storage preset registry at ``PresetTier.PLATFORM`` so they surface in the
admin presets UI and ``/configs/presets`` with dry-run/apply/rollback.
"""
from __future__ import annotations

import importlib.util
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


def _osgeo_available() -> bool:
    """True when GDAL's ``osgeo`` bindings are importable.

    Vestigial gate — kept for callers that reference this helper but no longer
    used to condition ``ReviewTaskRoutingPreset`` registration.

    Historical context: the review preset formerly routed gdal to an in-process
    background runner on the catalog service, which required osgeo on that image.
    The review preset now mirrors cloud exactly — gdal offloads to a Cloud Run
    Job (``gcp_cloud_run`` runner, consumers ``["catalog", "maps"]``), same as
    the cloud preset.  Gdal sync execution lives on the maps service, which ships
    osgeo + ``worker_task_gdal`` and handles ``Prefer: respond-sync`` via
    ``SyncRunner``.  Neither path requires osgeo on the catalog image, so the
    gate is functionally pointless and ``ReviewTaskRoutingPreset`` is now
    registered unconditionally.
    """
    return importlib.util.find_spec("osgeo") is not None


def _make_upload_entry(driver_ref: str) -> "Dict[str, List]":
    """Return a ``Dict[str, List[OperationDriverEntry]]`` for UPLOAD, pinned as
    operator-managed.

    ``source="operator"`` marks the list as operator-managed so the
    auto-augment helper ``_self_register_upload_into`` treats it as
    invariant and does not append additional upload backends.
    """
    from dynastore.modules.storage.routing_config import (
        FailurePolicy,
        Operation,
        OperationDriverEntry,
        WriteMode,
    )
    return {
        str(Operation.UPLOAD): [
            OperationDriverEntry(
                driver_ref=driver_ref,
                on_failure=FailurePolicy.FATAL,
                write_mode=WriteMode.SYNC,
                source="operator",
            )
        ]
    }


class _DeploymentProfilePreset:
    """Deployment-profile preset: materialises a TaskRoutingConfig and an
    AssetRoutingConfig at platform tier.

    A thin factory that emits a validated ``PresetBundle`` with two entries:

    1. ``task_routing`` — ``TaskRoutingConfig`` for the active deployment
       profile (worker vs Cloud Run Jobs placement).
    2. ``asset_upload`` — ``AssetRoutingConfig`` whose UPLOAD operation is
       pinned to the profile's canonical upload backend.
    """

    catalog_scopable = False

    def __init__(self, name: str, upload_driver_ref: str) -> None:
        self.name = name
        self.description = f"Deployment profile: {name}"
        self._upload_driver_ref = upload_driver_ref
        # ``tier`` MUST be concrete at registration time: list_presets /
        # search_presets (and the admin presets UI) filter on ``preset.tier``,
        # so a None here would hide the preset from the platform-tier view.
        # PresetTier is imported in ``_get_tier`` to avoid a top-level import
        # cycle; calling it here is safe (protocol.py imports no tasks modules).
        self.tier = self._get_tier()

    def _get_tier(self):
        from dynastore.modules.storage.presets.protocol import PresetTier
        return PresetTier.PLATFORM

    def build(self, **_scope):
        from dynastore.modules.storage.presets.protocol import PresetBundle, PresetBundleEntry
        from dynastore.modules.storage.routing_config import AssetRoutingConfig
        from dynastore.modules.tasks.routing.matrix import InventoryItem, build_routing_matrix
        from dynastore.modules.tasks.routing.model import TaskRoutingConfig
        from dynastore.tasks import _DYNASTORE_TASKS, task_kind

        inventory = [
            InventoryItem(
                task_key=key,
                kind=task_kind(cfg),
                affinity_tier=getattr(cfg.cls, "affinity_tier", None),
            )
            for key, cfg in _DYNASTORE_TASKS.items()
        ]
        tasks_map, processes_map = build_routing_matrix(inventory, preset=self.name)
        task_instance = TaskRoutingConfig(tasks=tasks_map, processes=processes_map)
        asset_instance = AssetRoutingConfig(operations=_make_upload_entry(self._upload_driver_ref))
        return PresetBundle(
            entries=(
                PresetBundleEntry(
                    slot="task_routing",
                    config_cls=TaskRoutingConfig,
                    instance=task_instance,
                    rollback_priority=50,
                ),
                PresetBundleEntry(
                    slot="asset_upload",
                    config_cls=AssetRoutingConfig,
                    instance=asset_instance,
                    rollback_priority=100,
                ),
            )
        )


CloudTaskRoutingPreset = _DeploymentProfilePreset("cloud", upload_driver_ref="gcp_module")
OnpremTaskRoutingPreset = _DeploymentProfilePreset("onprem", upload_driver_ref="local_upload_module")
ReviewTaskRoutingPreset = _DeploymentProfilePreset("review", upload_driver_ref="gcp_module")


def _register() -> None:
    try:
        from dynastore.modules.storage.presets import register_preset
        register_preset(CloudTaskRoutingPreset)
        register_preset(OnpremTaskRoutingPreset)
        # ReviewTaskRoutingPreset is registered unconditionally: it mirrors the
        # cloud preset (gdal -> gcp_cloud_run) and does not require local osgeo.
        register_preset(ReviewTaskRoutingPreset)
    except Exception:
        logger.warning(
            "task routing presets not registered in storage registry",
            exc_info=True,
        )


_register()
