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

"""Dimensions cold-boot contributor — first-run default-preset initialisation.

:class:`DimensionsColdBootContributor` applies the ``common_dimensions``
default preset exactly once per database, on application cold-boot. It replaces
the deprecated ``DIMENSIONS_MATERIALIZE_ON_BOOT`` env flag: the standard
dimensions are provisioned through the registered preset (RECORDS skeletons +
the idempotent ``dimensions_materialize`` job the preset triggers) instead of a
bespoke in-lifespan toggle.

IAM-independent by design
-------------------------
The target catalog is **open and runs without the IAM module**, so the
IAM-owned ``iam.applied_presets`` sentinel table does not exist there. This
contributor therefore does NOT use ``bootstrap_preset_if_absent`` /
``apply_preset`` (both record state in ``iam.applied_presets``). Instead it:

* guards "first run only" with a marker in ``catalog.shared_properties`` via
  ``PropertiesProtocol`` — the same IAM-free store the platform bootstrap guard
  uses, provided by the core ``CatalogModule``; and
* applies the preset directly through ``preset.apply()``, which for the
  ``common_dimensions`` contributor only seeds RECORDS collections
  (``CatalogsProtocol``) and dispatches the materialize task — no IAM.

The whole check-apply-mark sequence runs under ``acquire_startup_lock`` (an
advisory lock, IAM-free) so two pods racing to first-boot never both apply.

Scope
-----
A no-op unless a DB engine plus ``CatalogsProtocol`` and ``PropertiesProtocol``
are available, scoping the work to catalog-serving processes (where the
``_dimensions_`` RECORDS catalog lives) and keeping it silent in tools/worker
images that merely generate dimension members in memory.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

PRESET_NAME = "common_dimensions"
SCOPE_KEY = "platform"

# IAM-independent one-shot marker stored in ``catalog.shared_properties``. The
# open catalog has no IAM module, so the IAM-owned ``iam.applied_presets``
# sentinel does not exist here — this property replaces it.
_INIT_PROPERTY_KEY = "dimensions.common_dimensions_initialized"
_PROPERTY_OWNER = "dimensions"
_LOCK_KEY = "dimensions_coldboot:common_dimensions"


class DimensionsColdBootContributor:
    """Apply the ``common_dimensions`` default preset once per DB on cold-boot.

    Runs at ``priority=20`` — after the foundational IAM/auth contributors
    (priority 100/40) where present, so a fully-seeded platform is in place
    before the dimensions catalog/collections are created.
    """

    name: str = "dimensions"
    priority: int = 20

    async def run(self, engine: Any) -> None:
        """Initialise the dimensions default preset once for this database."""
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols.properties import PropertiesProtocol
        from dynastore.tools.discovery import get_protocol

        if engine is None:
            logger.debug("DimensionsColdBoot: no DB engine — skipping.")
            return
        if get_protocol(CatalogsProtocol) is None:
            logger.debug(
                "DimensionsColdBoot: no CatalogsProtocol in this process — "
                "skipping default-preset initialisation.",
            )
            return
        props = get_protocol(PropertiesProtocol)
        if props is None:
            logger.debug(
                "DimensionsColdBoot: no PropertiesProtocol — cannot guard "
                "first-run initialisation; skipping.",
            )
            return

        from dynastore.modules.db_config.locking_tools import acquire_startup_lock

        async with acquire_startup_lock(engine, _LOCK_KEY) as conn:
            if conn is None:
                logger.debug(
                    "DimensionsColdBoot: startup lock busy — another pod is "
                    "initialising; skipping.",
                )
                return
            # Double-checked under the lock: skip if already initialised.
            if await self._already_initialized(props, conn):
                logger.info(
                    "DimensionsColdBoot: %r already initialised on this DB — "
                    "skipping (first-run initialisation only).",
                    PRESET_NAME,
                )
                return

            applied = await self._apply_default_preset(engine)
            if applied:
                await self._mark_initialized(props, conn)
                logger.info(
                    "DimensionsColdBoot: applied %r on first cold-boot — RECORDS "
                    "skeletons registered and dimensions_materialize triggered.",
                    PRESET_NAME,
                )

    async def _already_initialized(self, props: Any, conn: Any) -> bool:
        """Return True if the first-run marker is already set (uncached read)."""
        try:
            value = await props.get_property(_INIT_PROPERTY_KEY, db_resource=conn)
            return value == "true"
        except Exception as exc:
            logger.debug(
                "DimensionsColdBoot: init-marker read failed (%s) — treating as "
                "not initialised.",
                exc,
            )
            return False

    async def _mark_initialized(self, props: Any, conn: Any) -> None:
        """Persist the first-run marker so subsequent boots skip the apply."""
        try:
            await props.set_property(
                _INIT_PROPERTY_KEY, "true", _PROPERTY_OWNER, db_resource=conn,
            )
        except Exception as exc:
            logger.warning(
                "DimensionsColdBoot: failed to persist init-marker (%s) — the "
                "preset may re-apply next boot (idempotent, harmless).",
                exc,
            )

    async def _apply_default_preset(self, engine: Any) -> bool:
        """Apply the ``common_dimensions`` preset directly (IAM-free path).

        Bypasses ``bootstrap_preset_if_absent`` / ``apply_preset`` because both
        record state in the IAM-owned ``iam.applied_presets`` table, absent in
        the open catalog. ``preset.apply`` only seeds RECORDS collections and
        dispatches the materialize task for this preset — no IAM.
        """
        from dynastore.modules.storage.presets.registry import find_preset

        preset = find_preset(PRESET_NAME)
        if preset is None:
            logger.warning(
                "DimensionsColdBoot: preset %r not registered — dimensions will "
                "not be provisioned until it is applied manually.",
                PRESET_NAME,
            )
            return False

        from dynastore.modules.storage.presets.lifecycle import _build_context
        from dynastore.modules.storage.presets.preset import NoParams

        ctx = _build_context(engine, principal=None, scope=SCOPE_KEY)
        await preset.apply(NoParams(), SCOPE_KEY, ctx)
        return True
