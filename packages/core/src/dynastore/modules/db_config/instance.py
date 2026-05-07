"""Per-instance + per-deployment config artefacts.

One env, one folder, two file shapes inside it:

    ${DYNASTORE_CONFIG_ROOT:-${APP_DIR}/config}/
      instance.json              ← this process's identity
      defaults/
        *.json                   ← seeded PluginConfig defaults

Self-contained: each service image can ship its own ``config/`` next to the
app and run with no mounts. Externalize by pointing ``DYNASTORE_CONFIG_ROOT``
at any path (mounted volume, secret, configmap, …).

Used by:
- ``modules/tasks/dispatcher.py`` — reads ``instance.json`` to learn the
  service name for service-affinity routing.
- ``modules/db_config/config_seeder.py`` — applies every JSON under
  ``defaults/`` via ``PlatformConfigsProtocol`` on first startup.
"""
from __future__ import annotations

import json
import logging
import os
import pathlib
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _resolve_root() -> pathlib.Path:
    explicit = os.environ.get("DYNASTORE_CONFIG_ROOT")
    if explicit:
        return pathlib.Path(explicit)
    app_dir = os.environ.get("APP_DIR", "/dynastore")
    return pathlib.Path(app_dir) / "config"


CONFIG_ROOT: pathlib.Path = _resolve_root()
INSTANCE_FILE: pathlib.Path = CONFIG_ROOT / "instance.json"
DEFAULTS_DIR: pathlib.Path = CONFIG_ROOT / "defaults"


def load_instance() -> Dict[str, Any]:
    """Load this process's ``instance.json`` or return an empty dict.

    Missing file is not an error — the dispatcher falls back to legacy
    "claim anything capable" behaviour. Malformed JSON is logged and
    treated the same way (we never crash a service over a bad config
    file; the loud warning is enough).
    """
    try:
        return json.loads(INSTANCE_FILE.read_text())
    except FileNotFoundError:
        logger.warning(
            "instance config missing at %s — service-affinity routing "
            "inactive (any capable service may claim any task).",
            INSTANCE_FILE,
        )
        return {}
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(
            "instance config %s unreadable (%s) — falling back to "
            "no-affinity behaviour.", INSTANCE_FILE, exc,
        )
        return {}


def get_service_name() -> str | None:
    """Return this process's logical service name, or ``None`` if not set."""
    return load_instance().get("service_name")
