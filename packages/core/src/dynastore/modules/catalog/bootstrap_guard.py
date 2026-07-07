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

"""Single-boolean platform bootstrap guard backed by ``catalog.shared_properties``.

One property in the DB records that the one-time platform initialisation has
completed for this deployment.  On subsequent cold-boots (e.g. Cloud Run
scaling events) both initialisers — JSON-config seeding and IAM preset
bootstrapping — skip their guarded work entirely, making reboot cheap.

Property key: ``platform.bootstrap_initialized``
Property value when set: ``"true"``

Usage pattern inside a boot sequence (advisory lock already held externally
by the caller)::

    if await is_initialized():
        return  # fast path — nothing to do
    # ... run idempotent initialisation ...
    await mark_initialized()

The guard is intentionally thin and dependency-free: it only talks to
``PropertiesProtocol`` which is provided by ``CatalogModule`` and contains no
IAM or storage driver logic.  Both consumers can import from here without
creating cross-module cycles.

Concurrency: callers MUST wrap the check-run-mark sequence inside the
existing ``acquire_startup_lock`` advisory lock (same lock used by
``config_seeder`` and ``bootstrap_preset_if_absent``).  The re-check after
lock acquisition (double-checked locking) prevents duplicate work when two
pods race to first boot.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

BOOTSTRAP_GUARD_KEY = "platform.bootstrap_initialized"
_BOOTSTRAP_OWNER = "platform"


async def is_initialized(db_resource: Optional[Any] = None) -> bool:
    """Return ``True`` if the platform bootstrap has already completed.

    Consults ``catalog.shared_properties``.  Prefers ``PropertiesProtocol``
    (cache-backed) once ``CatalogModule`` has registered it; before that — the
    foundational-lifespan window when a low-level caller (e.g.
    ``ensure_init_db`` at ``DatastoreModule`` priority 7) needs the answer but
    ``PropertiesProtocol`` is not up yet — falls back to a direct read of the
    marker row on the supplied ``db_resource``.

    The marker records **data state**, not a protocol dependency: if a prior
    boot fully initialised the platform, the row exists and reads back
    ``"true"`` (everything is in place); if the DB is fresh the row — or the
    ``catalog.shared_properties`` table itself — is absent and the read raises,
    so we degrade to ``False``.  Returns ``False`` — not initialised — when:
    * neither ``PropertiesProtocol`` nor a ``db_resource`` is available;
    * the property row / table does not exist;
    * any DB error (degrade safely; the caller re-runs initialisation, which
      is idempotent).
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.properties import PropertiesProtocol

    props = get_protocol(PropertiesProtocol)
    if props is None:
        # Early-boot fallback: read the marker directly, without the protocol.
        return await _is_initialized_direct(db_resource)

    try:
        value = await props.get_property(BOOTSTRAP_GUARD_KEY, db_resource=db_resource)
        return value == "true"
    except Exception as exc:
        logger.warning(
            "bootstrap_guard: could not read %r (%s) — treating as uninitialised.",
            BOOTSTRAP_GUARD_KEY,
            exc,
        )
        return False


async def _is_initialized_direct(db_resource: Optional[Any]) -> bool:
    """Read the bootstrap marker straight from ``catalog.shared_properties``.

    Used before ``PropertiesProtocol`` is registered. Any error — no resource,
    missing table on a fresh DB, unreachable backend — degrades to ``False``
    (uninitialised), so the worst case is a redundant idempotent re-init, never
    a wrong "already done" skip.
    """
    if db_resource is None:
        logger.debug(
            "bootstrap_guard: no PropertiesProtocol and no db_resource — "
            "treating as uninitialised."
        )
        return False
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

    try:
        value = await DQLQuery(
            "SELECT key_value FROM catalog.shared_properties "
            "WHERE key_name = :key_name",
            result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
        ).execute(db_resource, key_name=BOOTSTRAP_GUARD_KEY)
        return value == "true"
    except Exception as exc:
        logger.debug(
            "bootstrap_guard: direct marker read failed (%s) — treating as "
            "uninitialised.",
            exc,
        )
        return False


async def mark_initialized(db_resource: Optional[Any] = None) -> None:
    """Persist the bootstrap-complete marker to ``catalog.shared_properties``.

    Raises if ``PropertiesProtocol`` is not registered or the write fails —
    the caller should propagate the exception so the boolean remains unset and
    the next boot retries initialisation rather than silently skipping it.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.properties import PropertiesProtocol

    props = get_protocol(PropertiesProtocol)
    if props is None:
        raise RuntimeError(
            "bootstrap_guard.mark_initialized: PropertiesProtocol not registered; "
            "cannot persist bootstrap marker."
        )

    await props.set_property(
        BOOTSTRAP_GUARD_KEY,
        "true",
        _BOOTSTRAP_OWNER,
        db_resource=db_resource,
    )
    logger.info("bootstrap_guard: platform bootstrap marked as complete (%r = 'true').", BOOTSTRAP_GUARD_KEY)
