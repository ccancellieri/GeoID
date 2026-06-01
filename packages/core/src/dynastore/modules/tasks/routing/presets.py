"""Deployment-profile presets for task routing.

``CloudTaskRoutingPreset`` and ``OnpremTaskRoutingPreset`` materialize a
``TaskRoutingConfig`` from the live task registry using
``build_routing_matrix``, matching the two deployment topologies.

The preset objects are module-level singletons. ``_register()`` runs at
import time (bottom of this module), publishing both presets into the
storage preset registry at ``PresetTier.PLATFORM`` so they surface in the
admin presets UI and ``/admin/presets`` with dry-run/apply/rollback.
"""
from __future__ import annotations

import importlib.util
import logging

logger = logging.getLogger(__name__)


def _osgeo_available() -> bool:
    """True when GDAL's ``osgeo`` bindings are importable.

    The review preset routes the gdal process to an in-process background
    runner on the catalog service; that only works on an image that ships
    GDAL.  On a GDAL-less image the preset would mis-route gdal work, so it
    is only registered where osgeo is present.
    """
    return importlib.util.find_spec("osgeo") is not None


class _TaskRoutingPreset:
    """Minimal RoutingPreset: materializes a TaskRoutingConfig at platform tier.

    A thin factory that emits a validated ``PresetBundle`` whose single
    entry carries the ``TaskRoutingConfig`` for the active deployment
    profile.
    """

    catalog_scopable = False

    def __init__(self, name: str) -> None:
        self.name = name
        self.description = f"Task routing profile: {name}"
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
        instance = TaskRoutingConfig(tasks=tasks_map, processes=processes_map)
        return PresetBundle(
            entries=(
                PresetBundleEntry(
                    slot="task_routing",
                    config_cls=TaskRoutingConfig,
                    instance=instance,
                ),
            )
        )


CloudTaskRoutingPreset = _TaskRoutingPreset("cloud")
OnpremTaskRoutingPreset = _TaskRoutingPreset("onprem")
ReviewTaskRoutingPreset = _TaskRoutingPreset("review")


def _register() -> None:
    try:
        from dynastore.modules.storage.presets import register_preset
        register_preset(CloudTaskRoutingPreset)
        register_preset(OnpremTaskRoutingPreset)
        if _osgeo_available():
            register_preset(ReviewTaskRoutingPreset)
    except Exception:
        logger.warning(
            "task routing presets not registered in storage registry",
            exc_info=True,
        )


_register()
