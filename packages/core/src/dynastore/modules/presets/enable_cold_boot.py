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

"""Reusable cold-boot self-heal contributor for per-surface ``*_enable`` presets.

Mirrors ``_AuthColdBootContributor`` (``extensions/auth/authentication.py``):
IAM-aware (skips cleanly when ``PermissionProtocol`` is not registered in this
process, so a service with no IAM policy writer never attempts the write and
never hits the ``NoneType.update_policy`` crash), and re-asserts the grant on
every cold boot so a DB whose policy rows were wiped self-heals instead of
leaving anonymous reads 403 forever.

One deliberate difference from ``auth_enable``: ``auth_enable`` grants
anonymous access to the login flow, which every deployment needs, so it is
safe to force-apply unconditionally. A surface preset like ``tiles_enable``
grants anonymous *read* access to real data and is opt-in per deployment
(applied via the ``platform_demo`` composite or a file-backed preset seed) —
force-applying it on every cold boot regardless of deployment intent would
silently make private deployments public. This factory therefore only
self-heals when an ``iam.applied_presets`` row already exists for the preset
(see :func:`dynastore.modules.presets.bootstrap.preset_previously_applied`) —
i.e. this deployment already tried to apply it at least once, successfully or
not (a "not" is exactly the ``NoneType.update_policy`` failure this issue is
about: the attempt landed on a service with no IAM writer, but the intent was
recorded).

Usage — call once from the owning extension's ``lifespan``, matching the
``_AuthColdBootContributor`` registration pattern::

    from dynastore.modules.presets.cold_boot import register_cold_boot_contributor
    from dynastore.modules.presets.enable_cold_boot import make_enable_cold_boot_contributor

    try:
        register_cold_boot_contributor(
            make_enable_cold_boot_contributor(
                name="tiles", priority=35, preset_name="tiles_enable",
            )
        )
    except ValueError:
        logger.debug("already registered; skipping duplicate.")
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def make_enable_cold_boot_contributor(
    *,
    name: str,
    priority: int,
    preset_name: str,
    scope_key: str = "platform",
) -> Any:
    """Build a ``ColdBootContributor`` that self-heals *preset_name* on cold boot.

    Structurally satisfies ``dynastore.modules.presets.cold_boot.ColdBootContributor``
    (``name``, ``priority``, async ``run(engine)``). ``run`` never raises — a
    genuine self-heal error is logged and boot continues, matching every other
    cold-boot contributor in this codebase.
    """

    class _EnableColdBootContributor:
        def __init__(self) -> None:
            self.name = name
            self.priority = priority

        async def run(self, engine: Any) -> None:
            from dynastore.tools.discovery import get_protocol as _gp
            from dynastore.models.protocols.policies import PermissionProtocol

            if _gp(PermissionProtocol) is None:
                logger.info(
                    "%s: IAM module not installed in this process — skipping "
                    "%r self-heal; this anonymous grant is seeded by the "
                    "IAM-running service.",
                    self.name, preset_name,
                )
                return

            try:
                from dynastore.modules.presets.bootstrap import (
                    bootstrap_preset_if_absent,
                    preset_previously_applied,
                )

                if not await preset_previously_applied(
                    engine, preset_name=preset_name, scope_key=scope_key
                ):
                    logger.debug(
                        "%s: no prior %r attempt recorded at scope %r — "
                        "this deployment never opted in; skipping self-heal.",
                        self.name, preset_name, scope_key,
                    )
                    return

                await bootstrap_preset_if_absent(
                    engine,
                    preset_name=preset_name,
                    scope_key=scope_key,
                    force=True,
                )
            except Exception as exc:  # noqa: BLE001 — self-heal must never abort boot
                logger.error(
                    "%s: cold-boot self-heal of %r failed; anonymous reads may "
                    "stay 403 on this surface until applied manually: %s",
                    self.name, preset_name, exc,
                    exc_info=True,
                )

    _EnableColdBootContributor.__name__ = f"_{name.title()}EnableColdBootContributor"
    _EnableColdBootContributor.__qualname__ = _EnableColdBootContributor.__name__
    return _EnableColdBootContributor()
