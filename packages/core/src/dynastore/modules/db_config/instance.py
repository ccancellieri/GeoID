"""Per-instance + per-deployment config artefacts.

One env, one folder, three file shapes inside it:

    ${DYNASTORE_CONFIG_ROOT:-${APP_DIR}/config}/
      instance.json              ← this process's identity
      db_config.json             ← DB connection / pool tunables
      defaults/
        *.json                   ← seeded PluginConfig defaults

Self-contained: each service image can ship its own ``config/`` next to the
app and run with no mounts. Externalize by pointing ``DYNASTORE_CONFIG_ROOT``
at any path (mounted volume, secret, configmap, …).

Used by:
- ``modules/tasks/dispatcher.py`` — reads ``instance.json`` to learn the
  service name for service-affinity routing.
- ``modules/db_config/db_config.py`` — reads ``db_config.json`` for DB
  connection / pool tunables (the leak-proof alternative to templating them
  as ``${VAR}`` env vars; see #1581).
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
DB_CONFIG_FILE: pathlib.Path = CONFIG_ROOT / "db_config.json"
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


def load_db_config() -> Dict[str, Any]:
    """Load ``${DYNASTORE_CONFIG_ROOT}/db_config.json`` or return an empty dict.

    The deployment-provided source for DB connection / pool tunables — the
    leak-proof alternative to templating them as ``${VAR}`` env vars. The
    #1581 crash vector was an unsubstituted ``${DB_POOL_RECYCLE}`` reaching
    the container and crashing ``int()`` at import. A JSON *value* is never
    shell-substituted, so a missing key simply isn't present — it can never
    arrive as a literal ``${...}`` placeholder.

    Keys are the env-var names (e.g. ``"DB_POOL_RECYCLE"``, ``"DATABASE_URL"``);
    values may be numbers or strings. ``DBConfig`` resolves each tunable in the
    order **valid env var → this file → code default**, so an explicitly-set
    env var still wins (dev / compose), the file fills the gap a deploy would
    otherwise template, and the code default is the last resort.

    Absence is the normal case (env-based / dev deploys) and is silent. A
    malformed file or non-object content is logged and ignored — we never
    crash a service over a bad config file; ``DBConfig`` falls back to
    env then code defaults.
    """
    try:
        data = json.loads(DB_CONFIG_FILE.read_text())
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(
            "db_config file %s unreadable (%s) — falling back to env / code "
            "defaults for DB tunables.", DB_CONFIG_FILE, exc,
        )
        return {}
    if not isinstance(data, dict):
        logger.warning(
            "db_config file %s is not a JSON object (got %s) — ignoring; "
            "falling back to env / code defaults for DB tunables.",
            DB_CONFIG_FILE, type(data).__name__,
        )
        return {}
    return data
