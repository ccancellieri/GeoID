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

"""Catalog provisioning-checklist registry (#1175).

Catalog readiness (``provisioning_status='ready'``) is the completion of a
**checklist** contributed by the registered provisioners, instead of a single
provider (historically GCP) deciding it. This decouples readiness from any one
backend and makes on-prem (no active provisioner) ready immediately, while a
loaded-but-inactive provider can no longer wedge the catalog.

Model
-----

- A module registers a *provisioner* with a stable ``key``, an ``is_active``
  predicate ``async (catalog_id, conn) -> bool``, a ``priority`` (lower runs
  first; equal-priority provisioners are eligible to run in parallel), and a
  ``scope`` (``"catalog"`` or ``"collection"``).
- Optional ``provision`` and ``deprovision`` callables carry the provisioner's
  actual setup/teardown logic; the registry stores them but does not invoke them
  directly — that responsibility belongs to the executor task (PR2+).
- At catalog creation the checklist is materialised from the *active*
  provisioners (:func:`ProvisioningRegistry.build_checklist`) — every active
  provisioner's key starts ``"pending"``. Building the full checklist up front
  means a step that completes early cannot prematurely flip the catalog ready
  while a slower step is still outstanding (the barrier). Provisioners are
  iterated in ``(priority, key)`` order so the resulting dict's insertion order
  is deterministic.
- An empty checklist means nothing must be awaited — the catalog is ready
  immediately.
- Each provisioner marks its item terminal when its work finishes —
  synchronously, or later from its async task — via
  ``CatalogsProtocol.mark_provisioning_step``.
- :func:`evaluate_checklist` is the terminal rule (the "default last" step):
  when every item is terminal-good (``complete``/``skipped``) the catalog
  becomes ``ready``; any ``failed`` item makes it ``failed``; otherwise it stays
  ``provisioning``.

``skipped`` vs ``failed``: a provisioner that, at execution time, discovers it
is not actually able to act for this deployment (e.g. GCP enabled by config but
the host has no usable credentials) marks its step ``skipped`` so the catalog
still becomes ready. ``failed`` is reserved for a genuine provisioning error.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from itertools import groupby
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

__all__ = [
    "STEP_PENDING",
    "STEP_COMPLETE",
    "STEP_FAILED",
    "STEP_SKIPPED",
    "STEP_DEGRADED",
    "STATUS_PROVISIONING",
    "STATUS_READY",
    "STATUS_FAILED",
    "SCOPE_CATALOG",
    "SCOPE_COLLECTION",
    "LocalizedText",
    "Provisioner",
    "ProvisioningRegistry",
    "provisioning_registry",
    "evaluate_checklist",
]

# Per-step states stored as values in the ``provisioning_checklist`` JSONB.
STEP_PENDING = "pending"
STEP_COMPLETE = "complete"
STEP_FAILED = "failed"
STEP_SKIPPED = "skipped"
# ``degraded``: the step's work completed partially (e.g. eventing setup failed
# due to missing IAM permissions after the bucket was healthy). The catalog
# still reaches ``ready`` — the feature is unavailable but storage/STAC works.
# Operators can repair via POST /catalog/catalogs/{id}/reprovision.
STEP_DEGRADED = "degraded"

# Catalog-level ``provisioning_status`` values this module drives.
STATUS_PROVISIONING = "provisioning"
STATUS_READY = "ready"
STATUS_FAILED = "failed"

# Scope constants: ``SCOPE_CATALOG`` provisioners run at catalog-creation time;
# ``SCOPE_COLLECTION`` provisioners run at collection-creation time.
SCOPE_CATALOG = "catalog"
SCOPE_COLLECTION = "collection"

# Terminal-good states: catalog flips to ``ready`` when all steps are in this set.
# ``degraded`` is intentionally included — a degraded step must not block readiness.
_TERMINAL_GOOD = frozenset({STEP_COMPLETE, STEP_SKIPPED, STEP_DEGRADED})

# ``async (catalog_id, conn) -> bool``
ProvisionerPredicate = Callable[[str, Optional[Any]], Awaitable[bool]]


# Multilanguage text: a plain string or a ``{language-code: text}`` map (BCP-47
# language tags, e.g. ``{"en": "GCP bucket", "fr": "Seau GCP"}``).  A plain
# string is treated as English by convention.
LocalizedText = Union[str, Dict[str, str]]


@dataclass(frozen=True)
class Provisioner:
    """Immutable record describing a single registered provisioner.

    Fields
    ------
    key
        Stable identifier; appears as a key in the provisioning checklist.
    is_active
        Async predicate ``(catalog_id, conn) -> bool``. When it returns
        ``True`` the provisioner contributes a ``"pending"`` entry to the
        checklist for that catalog.
    priority
        Execution order hint (lower = earlier). Equal-priority provisioners
        are eligible to run in parallel.  Defaults to ``100``.
    scope
        Either :data:`SCOPE_CATALOG` (runs at catalog-creation time) or
        :data:`SCOPE_COLLECTION` (runs at collection-creation time).
    name
        Human-readable display name for this provisioning step.  Accepts a
        plain string (English) or a ``{lang: text}`` multilanguage map, e.g.
        ``{"en": "GCP Bucket", "fr": "Seau GCP"}``.  Surfaces in the catalog
        status API so a UI can label each checklist item.
    description
        Longer explanation of what this provisioner does.  Same multilanguage
        format as ``name``.  Surfaces in the catalog status API.
    provision
        Optional callable carrying the provisioner's setup logic.  The
        registry stores it; the executor task invokes it.
    deprovision
        Optional callable carrying the provisioner's teardown logic.
    """

    key: str
    is_active: ProvisionerPredicate
    priority: int = field(default=100)
    scope: str = field(default=SCOPE_CATALOG)
    name: Optional[LocalizedText] = field(default=None)
    description: Optional[LocalizedText] = field(default=None)
    provision: Optional[Callable[..., Any]] = field(default=None)
    deprovision: Optional[Callable[..., Any]] = field(default=None)


def evaluate_checklist(checklist: Optional[Dict[str, str]]) -> Optional[str]:
    """Map a checklist to the catalog ``provisioning_status`` it implies.

    Returns:
        - :data:`STATUS_READY` when there are no items, or every item is
          terminal-good (``complete``/``skipped``/``degraded``);
        - :data:`STATUS_FAILED` when any item is ``failed``;
        - ``None`` when at least one item is still ``pending`` (no change —
          the catalog stays ``provisioning``).

    ``degraded`` steps are terminal-good: the catalog becomes usable for
    storage/STAC even when a best-effort provisioning step (e.g. eventing)
    could not complete.
    """
    if not checklist:
        return STATUS_READY
    values = list(checklist.values())
    if any(v == STEP_FAILED for v in values):
        return STATUS_FAILED
    if all(v in _TERMINAL_GOOD for v in values):
        return STATUS_READY
    return None


class ProvisioningRegistry:
    """Process-wide registry of catalog provisioners (one instance, below).

    Keyed by the provisioner ``key`` so a module re-registering (test reloads,
    repeated lifespan) is naturally idempotent — the latest registration wins.

    Provisioners carry a ``priority`` and a ``scope``.  :meth:`build_checklist`
    and :meth:`active_provisioners` filter by scope and iterate in
    ``(priority, key)`` order so the output order is deterministic.
    """

    def __init__(self) -> None:
        self._provisioners: Dict[str, Provisioner] = {}

    def register(
        self,
        key: str,
        is_active: ProvisionerPredicate,
        *,
        priority: int = 100,
        scope: str = SCOPE_CATALOG,
        name: Optional[LocalizedText] = None,
        description: Optional[LocalizedText] = None,
        provision: Optional[Callable[..., Any]] = None,
        deprovision: Optional[Callable[..., Any]] = None,
    ) -> None:
        """Register (or replace) a provisioner contributing checklist item ``key``.

        Parameters
        ----------
        key
            Non-empty stable identifier; used as the checklist key.
        is_active
            Async predicate deciding, per catalog/collection, whether this
            provisioner has work that must be awaited.
        priority
            Execution-order hint (lower = earlier).  Defaults to ``100``.
        scope
            :data:`SCOPE_CATALOG` or :data:`SCOPE_COLLECTION`.
        name
            Human-readable step name; plain string or ``{lang: text}`` map.
        description
            Longer explanation; plain string or ``{lang: text}`` map.
        provision
            Optional setup callable stored for use by the executor task.
        deprovision
            Optional teardown callable stored for use by the executor task.
        """
        if not key:
            raise ValueError("provisioner key must be a non-empty string")
        self._provisioners[key] = Provisioner(
            key=key,
            is_active=is_active,
            priority=priority,
            scope=scope,
            name=name,
            description=description,
            provision=provision,
            deprovision=deprovision,
        )
        logger.info("Registered catalog provisioner '%s'", key)

    def unregister(self, key: str) -> None:
        self._provisioners.pop(key, None)

    def clear(self) -> None:
        self._provisioners.clear()

    @property
    def keys(self) -> list[str]:
        return list(self._provisioners.keys())

    def _sorted_provisioners(self, scope: str) -> list[Provisioner]:
        """Return provisioners matching ``scope``, sorted by ``(priority, key)``."""
        return sorted(
            (p for p in self._provisioners.values() if p.scope == scope),
            key=lambda p: (p.priority, p.key),
        )

    async def build_checklist(
        self, catalog_id: str, conn: Optional[Any] = None, *, scope: str = SCOPE_CATALOG
    ) -> Dict[str, str]:
        """Materialise the checklist for ``catalog_id`` from active provisioners.

        Only provisioners whose ``scope`` matches ``scope`` are considered.
        They are evaluated in ``(priority, key)`` order so the resulting dict's
        insertion order is deterministic.

        Every active provisioner's key maps to ``"pending"``. A predicate that
        raises is treated as inactive (logged) — a misbehaving provisioner must
        never block catalog readiness.
        """
        checklist: Dict[str, str] = {}
        for provisioner in self._sorted_provisioners(scope):
            try:
                if await provisioner.is_active(catalog_id, conn):
                    checklist[provisioner.key] = STEP_PENDING
            except Exception:  # noqa: BLE001 — a bad predicate can't wedge readiness
                logger.warning(
                    "Provisioner '%s' is_active predicate failed for catalog '%s'; "
                    "treating as inactive.",
                    provisioner.key, catalog_id, exc_info=True,
                )
        return checklist

    async def active_provisioners(
        self, catalog_id: str, conn: Optional[Any] = None, *, scope: str = SCOPE_CATALOG
    ) -> List[List[Provisioner]]:
        """Return the active provisioners for ``scope``, grouped by priority.

        Each inner list contains provisioners that share the same ``priority``
        and are eligible to run in parallel.  The outer list is ordered
        ascending by priority (run group 0 first, then group 1, …).

        Provisioners whose ``is_active`` predicate returns ``False`` or raises
        are excluded (same fail-soft semantics as :meth:`build_checklist`).
        """
        active: list[Provisioner] = []
        for provisioner in self._sorted_provisioners(scope):
            try:
                if await provisioner.is_active(catalog_id, conn):
                    active.append(provisioner)
            except Exception:  # noqa: BLE001
                logger.warning(
                    "Provisioner '%s' is_active predicate failed for catalog '%s'; "
                    "excluding from active list.",
                    provisioner.key, catalog_id, exc_info=True,
                )

        groups: List[List[Provisioner]] = []
        for _, group in groupby(active, key=lambda p: p.priority):
            groups.append(list(group))
        return groups


# Module-level singleton (mirrors ``lifecycle_registry``).
provisioning_registry = ProvisioningRegistry()
