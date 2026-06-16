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

"""Pluggable, priority-ordered cold-boot orchestrator.

Each module or extension that needs to run logic on application cold-boot
registers a :class:`ColdBootContributor` here.  The orchestrator
(:func:`run_cold_boot`) iterates contributors in DESCENDING priority order
and runs each one in its own ``try/except`` block so a failure in one
contributor never prevents the others from running.

Import discipline: this module MUST stay NEUTRAL.  It may import only from
the stdlib and from ``typing``.  No imports from ``modules/iam``,
``modules/storage``, ``modules/db_config``, or any extensions are permitted.
The engine reference is typed as ``Any`` to avoid pulling in SQLAlchemy or
asyncpg at import time.

Registration:

    from dynastore.modules.presets.cold_boot import register_cold_boot_contributor

    register_cold_boot_contributor(MyContributor())

Ordering: contributors are sorted by DESCENDING ``priority`` (highest
first).  Ties are broken by insertion order (stable sort).  IAM uses
priority 100; web uses 50; auth uses 40; file-preset seeder uses 10.
"""
from __future__ import annotations

import logging
from typing import Any, List, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class ColdBootContributor(Protocol):
    """Structural protocol for a cold-boot contributor.

    Implementors must expose ``name`` (unique string key), ``priority``
    (integer; higher = runs first), and an async ``run`` coroutine that
    receives the database engine.  ``run`` must be fail-soft: it should
    catch its own expected errors internally and either return normally or
    raise only for truly unexpected conditions — :func:`run_cold_boot`
    wraps every contributor in a separate ``try/except`` regardless.
    """

    name: str
    priority: int

    async def run(self, engine: Any) -> None:
        """Execute cold-boot logic.

        ``engine`` is the asyncpg / SQLAlchemy async engine returned by the
        registered ``DatabaseProtocol``.  May be ``None`` when no database
        protocol is available.
        """
        ...


# ---------------------------------------------------------------------------
# Module-level registry (populated at import time by each contributor module)
# ---------------------------------------------------------------------------

_REGISTRY: List[ColdBootContributor] = []


def register_cold_boot_contributor(contributor: ColdBootContributor) -> None:
    """Register *contributor* for cold-boot execution.

    Raises :class:`ValueError` when a contributor with the same ``name`` is
    already registered (mirrors the behaviour of the preset registry).
    """
    existing_names = {c.name for c in _REGISTRY}
    if contributor.name in existing_names:
        raise ValueError(
            f"ColdBootContributor with name {contributor.name!r} is already "
            "registered.  Each contributor name must be unique."
        )
    _REGISTRY.append(contributor)
    logger.debug("ColdBootContributor registered: name=%r priority=%d", contributor.name, contributor.priority)


def get_cold_boot_contributors() -> List[ColdBootContributor]:
    """Return all registered contributors sorted by DESCENDING priority.

    Ties are broken by registration order (stable sort).  The returned list
    is a fresh copy — modifications to it do not affect the registry.
    """
    return sorted(_REGISTRY, key=lambda c: -c.priority)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def run_cold_boot(engine: Any) -> None:
    """Run all registered cold-boot contributors, highest priority first.

    Each contributor runs in its own ``try/except``.  A failure is logged at
    ERROR level with the full traceback but execution continues with the next
    contributor — partial bootstrap is always preferable to a total abort
    because boot resilience matters more than atomicity here.

    This function never raises.
    """
    contributors = get_cold_boot_contributors()
    if not contributors:
        logger.debug("run_cold_boot: no contributors registered; nothing to do.")
        return

    for contributor in contributors:
        try:
            logger.info(
                "run_cold_boot: applying contributor %r (priority=%d)",
                contributor.name,
                contributor.priority,
            )
            await contributor.run(engine)
            logger.info(
                "run_cold_boot: contributor %r completed.",
                contributor.name,
            )
        except Exception:
            logger.error(
                "run_cold_boot: contributor %r raised an unexpected error; "
                "continuing with remaining contributors.",
                contributor.name,
                exc_info=True,
            )
