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

"""
LogCatalogIndexer — M3.2 of the role-based driver refactor.

First concrete INDEX-role driver for the catalog tier.  Does NOT
mirror metadata into a separate search store; instead it emits a
structured log line for every dispatch so operators can verify the
``catalog_metadata_changed`` → :class:`ReindexWorker` → INDEX-driver
pipeline end-to-end without standing up an Elasticsearch cluster.

Intended uses
-------------

* **Pipeline smoke-test**: pin the indexer under ``INDEX`` in a
  staging / dev override of :class:`CatalogRoutingConfig` and make a
  few catalog edits — the log lines prove events flowed all the way
  through.
* **Incident diagnostics**: flip the log level on this module to
  ``DEBUG`` in production to trace which catalogs are being touched
  and by which operation, without adding an ES dependency.
* **Reference implementation**: ~180 lines of the
  :class:`CatalogStore` protocol concretely satisfied.
  Subsequent real indexers (ES, vector-DB, external search service)
  can copy this shape and swap the log calls for backend I/O.

What this driver does NOT do
----------------------------

* ``get_catalog_metadata`` returns ``None`` always — a log indexer
  has no retrievable state.  If a caller reads the router merge
  with this as the only driver, they get ``None`` back (same as
  "no metadata stored").  In a realistic deployment the log
  indexer is ALWAYS paired with a real storing driver (the CORE /
  STAC Postgres Primaries in the shipped defaults), so this is
  never the only READ source.
* ``is_available`` always returns ``True``.  Even a broken Python
  logging backend would not meaningfully fail "log the event".
* Soft-delete is indistinguishable from hard-delete beyond a
  boolean in the log line — no tombstone retention.

Protocol contract
-----------------

:class:`CatalogStore` methods:

* ``capabilities`` — empty frozenset; the indexer reads nothing,
  writes nothing persistently, and serves no queries.  A future
  richer indexer would declare ``WRITE`` + ``SEARCH_FULLTEXT`` +
  whatever else its backend supports.
* ``sla``         — ``None``.  Log calls are synchronous and fast
  enough that the per-dispatch timeout machinery
  (:mod:`reindex_worker`) has no work to do here.  Operators who
  want a timeout can override
  :attr:`CatalogRoutingConfig.operations` INDEX entry to pin an SLA.

Routing-config default
----------------------

NOT a default INDEX-lane entry in
:class:`CatalogRoutingConfig.operations[INDEX]`.  Adding it by default
would turn every write into an INFO log line — too noisy for
production.  Operators opt in via a platform-level config override by
pinning it into the INDEX lane::

    from dynastore.models.protocols.driver_roles import (
        OperationDriverEntry,
    )
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, Operation,
    )

    base = CatalogRoutingConfig()
    override = CatalogRoutingConfig(
        operations={
            **base.operations,
            Operation.INDEX: [
                *base.operations.get(Operation.INDEX, []),
                OperationDriverEntry(driver_ref="LogCatalogIndexer"),
            ],
        },
    )
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, FrozenSet, Optional

from dynastore.models.protocols.driver_roles import DriverSla

logger = logging.getLogger(__name__)


class LogCatalogIndexer:
    """Diagnostic INDEX-role driver that logs every dispatch.

    See module docstring for semantics + usage.

    The class name intentionally elides the ``Driver`` suffix — per
    naming convention (Phase 1 of the harmonisation), role-scoped
    classes use ``<Tier><Role>`` or ``<Tier><Purpose><Backend>``; an
    indexer backed by a log is ``LogCatalogIndexer``, paralleling
    the future ``CatalogCoreEsIndexer``.
    """

    # Empty capabilities set — the indexer neither reads nor writes
    # persistent state.  ``READ`` is intentionally NOT declared so
    # router fallbacks / query-source selection never try to pull
    # from here.
    capabilities: FrozenSet[str] = frozenset()

    # No per-call timeout; log operations are effectively
    # instantaneous relative to the router's SLA granularity.
    sla: ClassVar[Optional[DriverSla]] = None

    # ------------------------------------------------------------------
    # Read side — no-op
    # ------------------------------------------------------------------

    async def get_catalog_metadata(
        self,
        catalog_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Always returns ``None`` — the log indexer has no retrievable state."""
        return None

    # ------------------------------------------------------------------
    # Write / delete — log + return
    # ------------------------------------------------------------------

    async def upsert_catalog_metadata(
        self,
        catalog_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Log the upsert — no persistent side-effect.

        Log shape: ``catalog_id`` + sorted list of keys in the
        envelope (not the values — values can be large, contain PII,
        or change under normalisation; keys are enough to reason
        about which domain wrote what).  Downstream log aggregators
        can filter on this module's logger name
        (``dynastore.modules.storage.drivers.catalog_log_indexer``)
        to build a per-catalog mutation timeline.
        """
        keys = sorted(metadata.keys()) if metadata else []
        logger.info(
            "LogCatalogIndexer: upsert catalog_id=%s domain_keys=%r",
            catalog_id, keys,
        )

    async def delete_catalog_metadata(
        self,
        catalog_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Log the delete — no persistent side-effect.

        ``soft`` is included in the log line so operators can
        correlate with other drivers' delete behaviour, even though
        the log indexer itself has no tombstone concept.
        """
        logger.info(
            "LogCatalogIndexer: delete catalog_id=%s soft=%s",
            catalog_id, soft,
        )

    # ------------------------------------------------------------------
    # Config / health
    # ------------------------------------------------------------------

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """No driver-specific configuration."""
        return None

    async def is_available(self) -> bool:
        """Always True — a logger can always accept a line.

        Returning ``True`` unconditionally is safe because any
        downstream log-backend failure (disk full, syslog
        unreachable, …) would not make "drop the log line" a less
        correct outcome than "fail the dispatch".  A WARNING would
        bubble through the Python logging error-handler and operators
        already alert on those.
        """
        return True

    # ------------------------------------------------------------------
    # Storage lifecycle — no-op (the indexer owns no backing storage)
    # ------------------------------------------------------------------

    async def ensure_storage(
        self, catalog_id: Optional[str] = None, **kwargs: Any
    ) -> None:
        """No-op: the log indexer has no backing storage to provision."""
        return None

    async def drop_storage(
        self, catalog_id: str, *, soft: bool = False
    ) -> None:
        """Log the storage drop — no persistent side-effect, mirrors
        :meth:`delete_catalog_metadata`.  Keeps the indexer structurally
        conformant with the ``CatalogStore`` lifecycle contract.
        """
        logger.info(
            "LogCatalogIndexer: drop_storage catalog_id=%s soft=%s",
            catalog_id, soft,
        )
