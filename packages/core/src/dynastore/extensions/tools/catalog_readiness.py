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

"""Catalog provisioning-readiness guard — single source of truth.

Lifted out of :class:`OGCServiceMixin` so that *every* extension that
touches catalog-scoped state (assets, configs, processes, …) can call
the same fail-fast check.  Previously only OGC routes (STAC create
collection, STAC ingest item, …) checked the guard; asset writes and
config PUTs flowed through to drivers that then either 500'd deep
inside or — worse — silently half-succeeded against a partially-
provisioned catalog.

Usage::

    from dynastore.extensions.tools.catalog_readiness import require_catalog_ready

    catalog = await require_catalog_ready(catalog_id)        # discovers CatalogsProtocol
    catalog = await require_catalog_ready(catalog_id, catalogs_svc=svc)  # explicit

Also home to :func:`wait_for_catalog_ready`, a bounded *poll* (as opposed to
``require_catalog_ready``'s immediate fail-fast) for callers — preset
``apply()`` bodies, background tasks — that create a catalog with
``Hint.DEFER`` and then need its tenant schema to exist before their own
next write, rather than surfacing a 409 to an end user.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from fastapi import HTTPException

from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.db import InvalidIdentifierError, validate_sql_identifier
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


READY = "ready"
PROVISIONING = "provisioning"
FAILED = "failed"
CONFLICT = "conflict"

# Bounded wait for a just-created catalog's core provisioning (tenant PG
# schema) to finish.  Even a ``Hint.DEFER`` create still enqueues an async
# ``catalog_provision`` task for the non-deferrable ``catalog_core`` step —
# ``create_catalog`` returns before that task runs.  ``catalog_core`` only
# creates a PG schema (no external network calls), so it is normally fast;
# this budget is generous headroom for dispatcher pickup latency, not an
# expectation that it takes this long.
_CATALOG_READY_POLL_INTERVAL_S = 1.0
_CATALOG_READY_TIMEOUT_S = 60.0


async def require_catalog_ready(
    catalog_id: str,
    *,
    catalogs_svc: Optional[CatalogsProtocol] = None,
) -> Any:
    """Return the catalog model iff ``provisioning_status == 'ready'``.

    Mutation endpoints (create collection, add item, update catalog,
    create asset, write config, execute process …) MUST call this
    before operating on ``catalog_id``.  Failing fast surfaces the real
    state to the client instead of letting downstream drivers either
    500 or half-write into missing storage.

    Raises:

    - ``HTTPException(400)``  — ``catalog_id`` is malformed, e.g. it still
      carries an unsubstituted ``{{...}}`` template placeholder (issue #1191).
      Caught here so the request fails fast with an actionable message instead
      of flowing through to the routing resolver as an opaque lookup miss.
    - ``HTTPException(404)``  — catalog doesn't exist
    - ``HTTPException(409)``  — provisioning still in flight
      (``'provisioning'``) or terminally failed (``'failed'``).  The
      detail explains which and points at the recovery path.
    """
    try:
        validate_sql_identifier(catalog_id)
    except InvalidIdentifierError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    svc = catalogs_svc if catalogs_svc is not None else get_protocol(CatalogsProtocol)
    if svc is None:
        # No CatalogsProtocol implementor — the service can't enforce
        # the guard.  Be loud rather than silently allow writes.
        raise HTTPException(
            status_code=503,
            detail=(
                "CatalogsProtocol not available — cannot verify catalog "
                f"'{catalog_id}' provisioning status."
            ),
        )

    catalog = await svc.get_catalog_model(catalog_id)
    if catalog is None:
        raise HTTPException(
            status_code=404,
            detail=f"Catalog '{catalog_id}' not found.",
        )

    status_value = getattr(catalog, "provisioning_status", READY) or READY
    if status_value == READY:
        return catalog

    if status_value == PROVISIONING:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Catalog '{catalog_id}' is still provisioning — typically takes 2-5 minutes. "
                f"Retry shortly; operators can follow progress in the service logs "
                f"(gcp_provision_catalog task)."
            ),
        )
    if status_value == FAILED:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Catalog '{catalog_id}' provisioning failed — the "
                f"backing storage was never created.  Delete the "
                f"catalog (DELETE /stac/catalogs/{catalog_id}) and "
                f"recreate it after resolving the underlying cause."
            ),
        )
    if status_value == CONFLICT:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Catalog '{catalog_id}' provisioning hit a storage-name "
                f"conflict — the deterministic bucket name is already taken "
                f"by another project or catalog, and was left intact (no "
                f"data deleted).  Recreate the catalog under a different id "
                f"or physical schema after resolving the collision."
            ),
        )

    raise HTTPException(
        status_code=409,
        detail=(
            f"Catalog '{catalog_id}' is in an unknown provisioning "
            f"state '{status_value}' — cannot proceed with this "
            f"operation."
        ),
    )


async def require_collection_ready(
    catalog_id: str,
    collection_id: str,
    *,
    catalogs_svc: Optional[CatalogsProtocol] = None,
) -> None:
    """Collection lifecycle gate — propagates :class:`CollectionNotAliveError` untouched.

    Mutating collection-scoped endpoints (asset writes, config PUTs/DELETEs,
    upload initiations) MUST call this after :func:`require_catalog_ready` and
    before any write to a collection-scoped resource.  The guard lets the
    globally-registered :class:`CollectionNotAliveExceptionHandler` (see
    ``extensions/tools/exception_handlers.py``) map the error to the correct
    HTTP status:

    * ``reason='missing'``    → 404  (no registry row)
    * ``reason='tombstoned'`` → 410  (soft-deleted)
    * anything else           → 503  (fail-closed; state unknown)

    This is intentionally distinct from :func:`require_catalog_ready`, which
    gates on catalog *provisioning* status and raises ``HTTPException(409)``.
    The collection gate is purely a liveness / tombstone check (issues #2065
    and #1995).

    Raises:
        ``HTTPException(503)`` — :class:`CatalogsProtocol` is not registered
            in the current process.  Loud refusal rather than silent allow.
        :class:`CollectionNotAliveError` — propagated unchanged so the
            registered exception handler converts it to the right HTTP status.
    """
    svc = catalogs_svc if catalogs_svc is not None else get_protocol(CatalogsProtocol)
    if svc is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "CatalogsProtocol not available — cannot verify collection "
                f"'{catalog_id}/{collection_id}' liveness."
            ),
        )

    await svc.ensure_alive(catalog_id, collection_id)


async def wait_for_catalog_ready(
    catalog_id: str,
    *,
    catalogs_svc: Optional[CatalogsProtocol] = None,
    caller: str = "",
    timeout_s: float = _CATALOG_READY_TIMEOUT_S,
    poll_interval_s: float = _CATALOG_READY_POLL_INTERVAL_S,
) -> None:
    """Bounded poll of ``catalog_id`` until ``provisioning_status == 'ready'``.

    For callers that create a catalog with ``Hint.DEFER`` and then need its
    tenant schema to exist before their own next write (a preset's
    ``apply()``, a background task) — as opposed to :func:`require_catalog_ready`,
    which fails fast with an HTTP 409 for an inbound request that hits a
    catalog someone else is provisioning.

    Raises ``RuntimeError`` — loudly, never silently — when the catalog
    reaches a terminal ``failed`` status or the poll budget is exhausted
    before ``ready`` is observed.  ``caller`` is included in the error
    message to identify which code path timed out.
    """
    svc = catalogs_svc if catalogs_svc is not None else get_protocol(CatalogsProtocol)
    if svc is None:
        raise RuntimeError(
            f"{caller}: CatalogsProtocol not available — cannot wait for "
            f"catalog {catalog_id!r} to become ready."
        )

    label = f"{caller}: " if caller else ""
    deadline = time.monotonic() + timeout_s
    while True:
        model = await svc.get_catalog_model(catalog_id)
        status = getattr(model, "provisioning_status", READY) if model is not None else None
        if status == READY:
            return
        if status == FAILED:
            raise RuntimeError(
                f"{label}target catalog {catalog_id!r} provisioning failed — "
                "refusing to proceed against a catalog whose tenant schema "
                "was never created."
            )
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"{label}catalog {catalog_id!r} did not reach 'ready' within "
                f"{timeout_s:.0f}s of creation — refusing to proceed against "
                "a catalog whose tenant schema may not exist yet.  Retry "
                "once the catalog finishes provisioning."
            )
        await asyncio.sleep(poll_interval_s)
