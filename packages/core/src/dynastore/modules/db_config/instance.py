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
import uuid
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Stable per-process identity, minted once at import time — every connection
# this process opens carries the same token, and a fresh process (redeploy,
# restart, new Cloud Run instance) always mints a different one. Distinct from
# service_name (shared by every replica of the same service): this is what
# lets geoid#2924's zombie-session reaper tell "a dead REPLICA of a healthy
# service" apart from "the service itself is unhealthy". Independent of
# locking_tools._LEASE_OWNER (same "mint once at import" idea, different
# call site — this one is stamped on every connection's application_name,
# not just the lease-table CAS).
_INSTANCE_ID: str = uuid.uuid4().hex


def get_instance_id() -> str:
    """Return this process's stable identity token (minted once at import)."""
    return _INSTANCE_ID


# PostgreSQL truncates the ``application_name`` GUC to NAMEDATALEN-1 = 63
# bytes server-side. "{service}:{32-hex instance id}" is 33 bytes just for the
# ":" + instance id, so a long service name can push the total over the limit
# and silently truncate away part (or all) of the instance id — breaking the
# zombie-session reaper's identity match with no error anywhere. Truncate the
# SERVICE part ourselves instead so the instance id always survives intact.
_MAX_APPLICATION_NAME_BYTES = 63
_warned_application_name_truncated = False


def get_stamped_application_name() -> str:
    """Return ``"{service}:{instance_id}"`` for ``pg_stat_activity.application_name``.

    ``service`` resolves the same way every existing call site already does
    (``instance.json`` → ``SERVICE_NAME`` env → literal ``"dynastore"``);
    appending the per-process ``instance_id`` lets a monitoring/reaper query
    distinguish individual replicas of the same service from each other, not
    just from other services.

    If the combined string would exceed PostgreSQL's 63-byte
    ``application_name`` limit, the service part is truncated (once, with a
    logged warning) so the instance id — the part identity matching actually
    depends on — always survives intact.
    """
    service = get_service_name() or os.getenv("SERVICE_NAME") or "dynastore"
    stamped = f"{service}:{_INSTANCE_ID}"
    if len(stamped.encode("utf-8")) <= _MAX_APPLICATION_NAME_BYTES:
        return stamped

    global _warned_application_name_truncated
    if not _warned_application_name_truncated:
        logger.warning(
            "get_stamped_application_name: %r is %d bytes, exceeding "
            "PostgreSQL's 63-byte application_name limit — truncating the "
            "service name (keeping the instance id intact) so identity "
            "matching still works. Consider a shorter service_name in "
            "instance.json.",
            stamped, len(stamped.encode("utf-8")),
        )
        _warned_application_name_truncated = True

    suffix = f":{_INSTANCE_ID}"
    max_service_bytes = _MAX_APPLICATION_NAME_BYTES - len(suffix.encode("utf-8"))
    truncated_service = service.encode("utf-8")[:max_service_bytes].decode(
        "utf-8", errors="ignore"
    )
    return f"{truncated_service}{suffix}"


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
