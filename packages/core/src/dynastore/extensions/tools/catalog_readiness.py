#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO

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
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import HTTPException

from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


READY = "ready"
PROVISIONING = "provisioning"
FAILED = "failed"


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

    - ``HTTPException(404)``  — catalog doesn't exist
    - ``HTTPException(409)``  — provisioning still in flight
      (``'provisioning'``) or terminally failed (``'failed'``).  The
      detail explains which and points at the recovery path.
    """
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
                f"Catalog '{catalog_id}' is still provisioning — retry "
                f"in a moment.  Poll GET /stac/catalogs/{catalog_id} "
                f"until provisioning_status = 'ready'."
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

    raise HTTPException(
        status_code=409,
        detail=(
            f"Catalog '{catalog_id}' is in an unknown provisioning "
            f"state '{status_value}' — cannot proceed with this "
            f"operation."
        ),
    )
