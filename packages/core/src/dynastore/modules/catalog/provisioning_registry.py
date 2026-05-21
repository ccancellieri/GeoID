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

- A module registers a *provisioner* with a stable ``key`` and an ``is_active``
  predicate ``async (catalog_id, conn) -> bool``. The predicate decides, per
  catalog, whether that provisioner has asynchronous setup work that the catalog
  must wait for before it is usable.
- At catalog creation the checklist is materialised from the *active*
  provisioners (:func:`ProvisioningRegistry.build_checklist`) — every active
  provisioner's key starts ``"pending"``. Building the full checklist up front
  means a step that completes early cannot prematurely flip the catalog ready
  while a slower step is still outstanding (the barrier).
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
from typing import Any, Awaitable, Callable, Dict, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "STEP_PENDING",
    "STEP_COMPLETE",
    "STEP_FAILED",
    "STEP_SKIPPED",
    "STATUS_PROVISIONING",
    "STATUS_READY",
    "STATUS_FAILED",
    "ProvisioningRegistry",
    "provisioning_registry",
    "evaluate_checklist",
]

# Per-step states stored as values in the ``provisioning_checklist`` JSONB.
STEP_PENDING = "pending"
STEP_COMPLETE = "complete"
STEP_FAILED = "failed"
STEP_SKIPPED = "skipped"

# Catalog-level ``provisioning_status`` values this module drives.
STATUS_PROVISIONING = "provisioning"
STATUS_READY = "ready"
STATUS_FAILED = "failed"

_TERMINAL_GOOD = frozenset({STEP_COMPLETE, STEP_SKIPPED})

# ``async (catalog_id, conn) -> bool``
ProvisionerPredicate = Callable[[str, Optional[Any]], Awaitable[bool]]


def evaluate_checklist(checklist: Optional[Dict[str, str]]) -> Optional[str]:
    """Map a checklist to the catalog ``provisioning_status`` it implies.

    Returns:
        - :data:`STATUS_READY` when there are no items, or every item is
          terminal-good (``complete``/``skipped``);
        - :data:`STATUS_FAILED` when any item is ``failed``;
        - ``None`` when at least one item is still ``pending`` (no change —
          the catalog stays ``provisioning``).
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
    repeated lifespan) is naturally idempotent — the latest predicate wins.
    """

    def __init__(self) -> None:
        self._provisioners: Dict[str, ProvisionerPredicate] = {}

    def register(self, key: str, is_active: ProvisionerPredicate) -> None:
        """Register (or replace) a provisioner contributing checklist item ``key``."""
        if not key:
            raise ValueError("provisioner key must be a non-empty string")
        self._provisioners[key] = is_active
        logger.info("Registered catalog provisioner '%s'", key)

    def unregister(self, key: str) -> None:
        self._provisioners.pop(key, None)

    def clear(self) -> None:
        self._provisioners.clear()

    @property
    def keys(self) -> list[str]:
        return list(self._provisioners.keys())

    async def build_checklist(
        self, catalog_id: str, conn: Optional[Any] = None
    ) -> Dict[str, str]:
        """Materialise the checklist for ``catalog_id`` from active provisioners.

        Every active provisioner's key maps to ``"pending"``. A predicate that
        raises is treated as inactive (logged) — a misbehaving provisioner must
        never block catalog readiness.
        """
        checklist: Dict[str, str] = {}
        for key, predicate in self._provisioners.items():
            try:
                if await predicate(catalog_id, conn):
                    checklist[key] = STEP_PENDING
            except Exception:  # noqa: BLE001 — a bad predicate can't wedge readiness
                logger.warning(
                    "Provisioner '%s' is_active predicate failed for catalog '%s'; "
                    "treating as inactive.",
                    key, catalog_id, exc_info=True,
                )
        return checklist


# Module-level singleton (mirrors ``lifecycle_registry``).
provisioning_registry = ProvisioningRegistry()
