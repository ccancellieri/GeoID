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

"""File-backed preset seeder cold-boot contributor.

:class:`FilePresetSeederContributor` runs at ``priority=10`` — low priority,
after all foundational auth/IAM presets (priority 100) and extension presets
(priority 40–50).  It enumerates ``*.json`` files under ``PRESETS_DIR`` and
applies each one via :func:`~dynastore.modules.presets.bootstrap.bootstrap_presets`.

Each file is processed in its own ``try/except`` so a bad seed file never
prevents the others from running.  Missing ``PRESETS_DIR`` is silently
ignored (many deployments ship no file-based presets).

Mirror of ``config_seeder``'s resilience posture: warn on bad files, skip
unknown presets, never abort boot.
"""
from __future__ import annotations

import logging
from typing import Any

from dynastore.modules.presets.bootstrap import bootstrap_presets  # noqa: F401 — re-exported for patching
from dynastore.modules.presets.param_loader import PRESETS_DIR, load_preset_payloads  # noqa: F401

logger = logging.getLogger(__name__)


class FilePresetSeederContributor:
    """Apply file-backed preset JSON files from ``PRESETS_DIR`` on cold-boot.

    File enumeration order is lexical (same as :mod:`config_seeder`).  Each
    ``*.json`` file is treated as a preset payload (single / chain / bare
    list — handled by ``load_preset_payloads`` + ``bootstrap_presets``).
    """

    name: str = "file_presets"
    priority: int = 10

    async def run(self, engine: Any) -> None:
        if not PRESETS_DIR.exists():
            logger.debug(
                "FilePresetSeeder: PRESETS_DIR %s does not exist — nothing to seed.",
                PRESETS_DIR,
            )
            return

        json_files = sorted(PRESETS_DIR.glob("*.json"))
        if not json_files:
            logger.debug(
                "FilePresetSeeder: no *.json files found in %s.", PRESETS_DIR
            )
            return

        for path in json_files:
            stem = path.stem
            try:
                payloads = load_preset_payloads(stem)
                if payloads is None:
                    logger.warning(
                        "FilePresetSeeder: could not load payloads from %s — skipping.",
                        path,
                    )
                    continue
                results = await bootstrap_presets(engine, payloads)
                applied = [name for name, ok in results.items() if ok]
                skipped = [name for name, ok in results.items() if not ok]
                if applied:
                    logger.info(
                        "FilePresetSeeder: applied from %s: %s", path.name, applied
                    )
                if skipped:
                    logger.debug(
                        "FilePresetSeeder: skipped (already applied or sentinel set) "
                        "from %s: %s",
                        path.name,
                        skipped,
                    )
            except Exception:
                logger.error(
                    "FilePresetSeeder: unexpected error processing %s — skipping file.",
                    path,
                    exc_info=True,
                )
