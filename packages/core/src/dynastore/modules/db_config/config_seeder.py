"""Generalised JSON-defaults bootstrap for ``PluginConfig`` subclasses.

Reads every ``*.json`` file under ``${DYNASTORE_CONFIG_ROOT}/defaults/`` and
applies it via ``PlatformConfigsProtocol.set_config``. Each file shape:

    {
      "class_key": "task_routing_config",
      "value":     { "enabled": true, "routing": { ... } },
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
``task_routing_config`` is the first user; future ones (e.g. ``tasks_plugin_config``
overrides) just drop a JSON file alongside it.

Invoked once during catalog/db_config startup, after ``PlatformConfigsProtocol``
is registered. Idempotent — safe to re-run on every boot.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from dynastore.modules.db_config.instance import DEFAULTS_DIR
from dynastore.modules.db_config.locking_tools import acquire_startup_lock
from dynastore.modules.db_config.platform_config_service import (
    PluginConfig,
    resolve_config_class,
)
from dynastore.modules.db_config.query_executor import DbResource

logger = logging.getLogger(__name__)


_SEED_LOCK_KEY = "config_seeder.defaults"


async def seed_default_configs(engine: DbResource) -> None:
    """Apply JSON config defaults found in ``DEFAULTS_DIR``.

    No-op when:
    - The directory doesn't exist (deployment ships no seeds).
    - ``PlatformConfigsProtocol`` isn't registered yet (caller called us
      too early; logged as a warning, returns silently).
    - Another process holds the advisory lock (no contention; we just skip).

    Errors on individual files are logged but do not abort the run — a bad
    seed shouldn't block service startup.
    """
    if not DEFAULTS_DIR.exists():
        logger.info(
            "config_seeder: %s missing — no JSON defaults to apply.", DEFAULTS_DIR,
        )
        return

    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol

    config_mgr = get_protocol(PlatformConfigsProtocol)
    if config_mgr is None:
        logger.warning(
            "config_seeder: PlatformConfigsProtocol not registered yet — "
            "skipping JSON-defaults bootstrap."
        )
        return

    json_files = sorted(DEFAULTS_DIR.glob("*.json"))
    if not json_files:
        logger.info("config_seeder: %s has no *.json files.", DEFAULTS_DIR)
        return

    # One advisory lock per cluster so only one process applies seeds per boot.
    # If we can't acquire within the default 30s, log and proceed without
    # seeding — the holder will do it.
    async with acquire_startup_lock(engine, _SEED_LOCK_KEY) as conn:
        if conn is None:
            logger.info(
                "config_seeder: another process holds %s — skipping.",
                _SEED_LOCK_KEY,
            )
            return

        # Lexical-order pass — later files override earlier ones for same class_key.
        # We deduplicate first so an overlay is applied once with the final payload.
        merged: Dict[str, Dict[str, Any]] = {}
        for path in json_files:
            try:
                payload = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("config_seeder: skipping %s — unreadable: %s", path, exc)
                continue
            class_key = payload.get("class_key")
            if not class_key:
                logger.warning(
                    "config_seeder: %s missing 'class_key' — skipped.", path,
                )
                continue
            merged[class_key] = payload  # last-write-wins per class_key

        applied = 0
        for class_key, payload in merged.items():
            try:
                applied += await _apply_one(config_mgr, class_key, payload)
            except Exception as exc:  # noqa: BLE001 — never fail boot
                logger.warning(
                    "config_seeder: failed to apply seed for %s: %s",
                    class_key, exc,
                )

        logger.info(
            "config_seeder: applied %d/%d seed(s) from %s",
            applied, len(merged), DEFAULTS_DIR,
        )


async def _apply_one(
    config_mgr: Any, class_key: str, payload: Dict[str, Any],
) -> int:
    """Apply a single seed payload. Returns 1 if written, 0 if skipped."""
    cls: Optional[type[PluginConfig]] = resolve_config_class(class_key)
    if cls is None:
        logger.warning(
            "config_seeder: unknown class_key %r — skipped.", class_key,
        )
        return 0

    value = payload.get("value")
    if not isinstance(value, dict):
        logger.warning(
            "config_seeder: %s 'value' must be an object — skipped.", class_key,
        )
        return 0

    override = bool(payload.get("override", False))

    # Skip when a row already exists and override is False. ``get_config``
    # always returns *something* (defaults materialised on demand) so we
    # use ``list_configs`` to distinguish "stored row" from "default".
    if not override:
        try:
            existing = await config_mgr.list_configs()
            if cls in existing:
                logger.info(
                    "config_seeder: %s already present — skipping (override=false).",
                    class_key,
                )
                return 0
        except Exception as exc:  # noqa: BLE001 — degrade to apply-anyway
            logger.debug(
                "config_seeder: list_configs failed (%s) — applying %s anyway.",
                exc, class_key,
            )

    config = cls.model_validate(value)
    await config_mgr.set_config(cls, config)
    logger.info("config_seeder: applied seed for %s.", class_key)
    return 1
