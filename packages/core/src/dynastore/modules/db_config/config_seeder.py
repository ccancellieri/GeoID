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

"""Generalised JSON-defaults bootstrap for ``PluginConfig`` subclasses.

Reads every ``*.json`` file under ``${DYNASTORE_CONFIG_ROOT}/defaults/`` and
applies it via ``PlatformConfigsProtocol.set_config``. Each file shape:

    {
      "class_key": "task_routing_config",
      "value":     { "tasks": { ... }, "processes": { ... } },
      "override":  false                       // optional, default false
    }

``class_key`` is the snake_case identity (``cls.class_key()``); same wire
name as ``plugin_id`` on the configs API surface.

Skip policy: a config is applied only if no row exists yet for that class_key,
unless the file carries ``"override": true``. Files are processed in lexical
order; a later overlay file with the same class_key wins (lets fao-aip-catalog
drop overlays on top of dynastore's base). Concurrency is guarded by a
PostgreSQL advisory lock so only one process per cluster runs the seeder at
a time.

This module owns no domain knowledge — it works for any ``PluginConfig``.
``task_routing_config`` is one user; others (e.g. ``tasks_plugin_config``
overrides) just drop a JSON file alongside it.

Invoked once during catalog/db_config startup, after ``PlatformConfigsProtocol``
is registered. Idempotent — safe to re-run on every boot.

Fail-fast posture: when a seed is rejected (missing key, non-object
``value``, malformed JSON), the rejection is summarised at ERROR and — in
non-production tiers — raised as ``ConfigSeederError``. Production startup
keeps going so a bad seed cannot take a service down. Validate seed files in
CI via ``scripts/validate_config_defaults.py``.

Unknown ``class_key`` is *tolerated*, not rejected: it is logged at WARNING
and skipped in every tier. A seed file may legitimately reference a config
class this build doesn't register — e.g. mid cross-repo rename, when one repo
ships the new class while a not-yet-redeployed sibling still carries the old
seed file. Treating that as fatal would couple deploy ordering across repos;
warn-and-skip keeps boot order-independent. Remove the stale seed file once
every service is on the new build.
"""
from __future__ import annotations

import json
from pathlib import Path
import logging
import os
from typing import Any, Dict, List, Optional

from dynastore.modules.db_config.instance import DEFAULTS_DIR
from dynastore.modules.db_config.locking_tools import acquire_startup_lock
from dynastore.models.plugin_config import PluginConfig, resolve_config_class
from dynastore.modules.db_config.query_executor import DbResource, is_lock_not_available_error

logger = logging.getLogger(__name__)


# Listed alongside every other static lock/lease key in
# modules/tasks/durable/lock_registry.py.
_SEED_LOCK_KEY = "config_seeder.defaults"
_PRODUCTION_ENV_NAMES = frozenset({"prod", "production"})


class ConfigSeederError(RuntimeError):
    """Raised in non-production tiers when one or more seeds are rejected."""


def _is_production_env() -> bool:
    label = (
        os.environ.get("DYNASTORE_ENV")
        or os.environ.get("ENVIRONMENT")
        or ""
    ).strip().lower()
    return label in _PRODUCTION_ENV_NAMES


async def seed_default_configs(engine: DbResource) -> None:
    """Apply JSON config defaults found in ``DEFAULTS_DIR``.

    Seeding is a no-op when:
    - The directory doesn't exist (deployment ships no seeds).
    - ``PlatformConfigsProtocol`` isn't registered yet (caller called us
      too early; logged as a warning, seeding is skipped).
    - Another process holds the advisory lock (no contention; we just skip).
    - The platform bootstrap guard is already set (fast path on restarts).

    Rejected seeds (missing class_key, bad value, bad JSON) are collected and
    surfaced together: ERROR-logged in production, raised as
    ``ConfigSeederError`` in non-production tiers (fail-fast). An unknown
    class_key is tolerated — logged at WARNING and skipped in every tier — so
    a stale seed from a not-yet-redeployed sibling repo can't abort boot.
    Per-row ``set_config`` failures are logged at WARNING and never abort.

    Seeds with ``"override": true`` in the JSON file bypass the bootstrap
    guard and are applied on every boot regardless.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol

    config_mgr = None
    json_files: List[Path] = []
    if not DEFAULTS_DIR.exists():
        logger.info(
            "config_seeder: %s missing — no JSON defaults to apply.", DEFAULTS_DIR,
        )
    else:
        json_files = sorted(DEFAULTS_DIR.glob("*.json"))
        if not json_files:
            logger.info("config_seeder: %s has no *.json files.", DEFAULTS_DIR)

    if json_files:
        config_mgr = get_protocol(PlatformConfigsProtocol)
        if config_mgr is None:
            logger.warning(
                "config_seeder: PlatformConfigsProtocol not registered yet — "
                "skipping JSON-defaults bootstrap."
            )

    # One advisory lock per cluster so only one process applies seeds (and
    # the legacy-key fixup) per boot. If we can't acquire within the default
    # 30s, log and proceed without seeding — the holder will do it. A
    # lock-timeout raises (PG 55P03) rather than yielding None, so it's
    # caught explicitly here and folded into the same skip path — another
    # process winning the race is expected, not fatal (#2625).
    try:
        async with acquire_startup_lock(engine, _SEED_LOCK_KEY) as conn:
            if conn is None:
                logger.info(
                    "config_seeder: another process holds %s — skipping.",
                    _SEED_LOCK_KEY,
                )
                return
            if json_files and config_mgr is not None:
                # Cold-boot guard (#2209): ensure ``configs.platform_configs`` exists
                # before writing any seed. ``PlatformConfigService`` (module priority
                # 0) skips its DDL when the DB engine is not yet up (DBService is
                # priority 10), so on a fresh DB the table can be absent when this
                # seeder runs — silently dropping every seed (notably ``idp_config``),
                # which leaves authentication with no IdP registered until a restart.
                # Idempotent ``CREATE ... IF NOT EXISTS`` under the advisory lock we
                # already hold — a bootstrap ensure, not a migration.
                try:
                    from dynastore.modules.db_config.platform_config_service import (
                        PlatformConfigService,
                    )

                    await PlatformConfigService.initialize_storage(conn)
                except Exception:
                    logger.warning(
                        "config_seeder: platform config storage ensure failed; "
                        "seeds may be skipped if the table is absent.",
                        exc_info=True,
                    )
            await _seed_from_files(conn, config_mgr, json_files)
    except Exception as exc:
        if not is_lock_not_available_error(exc):
            raise
        logger.info(
            "config_seeder: %s timed out (another process is likely still "
            "seeding) — skipping: %s",
            _SEED_LOCK_KEY, exc,
        )


async def _seed_from_files(
    conn: DbResource,
    config_mgr: Any,
    json_files: List[Path],
) -> None:
    """Apply the JSON seed files under the already-held advisory lock."""
    if not json_files or config_mgr is None:
        return

    # Re-check bootstrap guard inside the lock (double-checked locking).
    # Seeds with override=true bypass the guard and always apply.
    from dynastore.modules.catalog.bootstrap_guard import is_initialized
    _already_initialized = await is_initialized(db_resource=conn)

    # Lexical-order pass — later files override earlier ones for same class_key.
    # We deduplicate first so an overlay is applied once with the final payload.
    merged: Dict[str, Dict[str, Any]] = {}
    rejections: List[str] = []
    for path in json_files:
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            rejections.append(f"{path.name}: unreadable ({exc})")
            continue
        class_key = payload.get("class_key")
        if not class_key:
            rejections.append(f"{path.name}: missing 'class_key'")
            continue
        merged[class_key] = payload  # last-write-wins per class_key

    if _already_initialized:
        logger.info(
            "config_seeder: bootstrap guard set — skipping non-override seeds.",
        )

    # Fetched once per seeding pass rather than per seed file: ``merged`` is
    # already deduplicated by class_key, so each iteration only ever checks
    # membership of its own (unique) class — no iteration's write can change
    # what a later iteration's check needs to see. A snapshot before the loop
    # is therefore equivalent to a per-iteration re-query.
    try:
        existing_configs: Optional[Dict[type, Any]] = await config_mgr.list_configs()
    except Exception as exc:  # noqa: BLE001 — degrade to apply-anyway
        logger.debug(
            "config_seeder: list_configs failed (%s) — applying seeds without "
            "skip-if-existing check.",
            exc,
        )
        existing_configs = None

    applied = 0
    skipped_initialized = 0
    skipped_unknown = 0
    for class_key, payload in merged.items():
        override = bool(payload.get("override", False))
        if _already_initialized and not override:
            skipped_initialized += 1
            continue
        try:
            outcome = await _apply_one(config_mgr, class_key, payload, existing_configs)
        except Exception as exc:  # noqa: BLE001 — never fail boot
            logger.warning(
                "config_seeder: failed to apply seed for %s: %s",
                class_key, exc,
            )
            continue
        if outcome == "applied":
            applied += 1
        elif outcome == "skipped_unknown_class":
            skipped_unknown += 1
            logger.warning(
                "config_seeder: %s: unknown class_key (not registered) — "
                "skipping. Expected mid cross-repo config rename; remove "
                "the stale seed file once every service is on the new build.",
                class_key,
            )
        elif outcome == "rejected_bad_value":
            rejections.append(f"{class_key}: 'value' must be a JSON object")

    logger.info(
        "config_seeder: applied %d/%d seed(s) from %s "
        "(%d guard-skipped, %d unknown skipped)",
        applied, len(merged), DEFAULTS_DIR, skipped_initialized, skipped_unknown,
    )

    if rejections:
        summary = "; ".join(rejections)
        logger.error(
            "config_seeder: %d seed(s) rejected — %s",
            len(rejections), summary,
        )
        if not _is_production_env():
            raise ConfigSeederError(
                f"{len(rejections)} seed(s) rejected: {summary}. "
                "Run scripts/validate_config_defaults.py before deploy."
            )



async def _apply_one(
    config_mgr: Any,
    class_key: str,
    payload: Dict[str, Any],
    existing_configs: Optional[Dict[type, Any]],
) -> str:
    """Apply a single seed payload.

    ``existing_configs`` is the ``list_configs()`` snapshot taken once by the
    caller before the seeding loop started (see ``_seed_from_files``).

    Returns one of: ``"applied"``, ``"skipped_existing"``,
    ``"skipped_unknown_class"``, ``"rejected_bad_value"``.
    """
    cls: Optional[type[PluginConfig]] = resolve_config_class(class_key)
    if cls is None:
        return "skipped_unknown_class"

    value = payload.get("value")
    if not isinstance(value, dict):
        return "rejected_bad_value"

    override = bool(payload.get("override", False))

    # Skip when a row already exists and override is False. ``get_config``
    # always returns *something* (defaults materialised on demand) so we
    # use ``list_configs`` to distinguish "stored row" from "default".
    if not override and existing_configs is not None and cls in existing_configs:
        logger.info(
            "config_seeder: %s already present — skipping (override=false).",
            class_key,
        )
        return "skipped_existing"

    config = cls.model_validate(value)
    await config_mgr.set_config(cls, config)
    logger.info("config_seeder: applied seed for %s.", class_key)
    return "applied"
