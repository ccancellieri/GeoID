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

"""Catalog-status extension service.

Always-on extension that exposes catalog and collection provisioning/task
status.  Recovery operations (reprovision, dead-letter list/requeue) have
moved to the unified Tasks API under the ``/task`` prefix.

URL prefix: ``/catalog``
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import APIRouter, FastAPI, HTTPException

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.modules import get_protocol

from .catalog_status_models import (
    CatalogStatusView,
    CollectionStatusView,
    ProvisioningTaskView,
)

logger = logging.getLogger(__name__)

# Import visibility resolver at module level so tests can patch them cleanly.
from dynastore.models.protocols.visibility import (  # noqa: E402
    resolve_catalog_listing_ids,
    resolve_collection_listing_ids,
)


# ---------------------------------------------------------------------------
# Visibility enforcement helpers
# ---------------------------------------------------------------------------


async def _assert_catalog_visible(catalog_id: str) -> None:
    """Raise 404 (no-leak shape) when the catalog is hidden to this caller.

    Applies the visibility-gating contract from PR #2094: when
    ``resolve_catalog_listing_ids`` returns a non-None frozenset that does
    NOT contain ``catalog_id``, the catalog is indistinguishable from
    missing for this caller.
    """
    visible_ids = await resolve_catalog_listing_ids()
    if visible_ids is not None and catalog_id not in visible_ids:
        raise HTTPException(
            status_code=404, detail=f"Catalog '{catalog_id}' not found."
        )


async def _assert_collection_visible(catalog_id: str, collection_id: str) -> None:
    """Raise 404 when the collection is hidden to this caller.

    Also implicitly gates on catalog visibility (catalog hidden → caller
    cannot enumerate collections → 404 first from ``_assert_catalog_visible``
    which must be called before this helper).
    """
    visible_ids = await resolve_collection_listing_ids(catalog_id)
    if visible_ids is not None and collection_id not in visible_ids:
        raise HTTPException(
            status_code=404,
            detail=f"Collection '{collection_id}' not found in catalog '{catalog_id}'.",
        )


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class CatalogStatusService(ExtensionProtocol):
    """Always-on extension exposing catalog provisioning/task status and
    catalog-admin recovery operations (reprovision, dead-letter).

    Endpoint-level authorization is delegated to ``IamMiddleware`` via the
    policies declared in ``policies.py`` and registered through the preset in
    ``presets/__init__.py``.  The read surfaces additionally enforce
    visibility-gating (404 for hidden catalogs) so a caller cannot probe
    catalog existence via this extension.
    """

    always_on = True
    priority: int = 200

    router: APIRouter = APIRouter(prefix="/catalog", tags=["Catalog Status"])

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        yield

    # -------------------------------------------------------------------------
    # GET /catalog/catalogs/{catalog_id}
    # -------------------------------------------------------------------------

    @router.get(
        "/catalogs/{catalog_id}",
        response_model=CatalogStatusView,
        summary="Catalog provisioning status and most-recent provision task.",
    )
    async def get_catalog_status(catalog_id: str):  # type: ignore[reportGeneralTypeIssues]
        """Return provisioning state and the most-recent ``gcp_provision_catalog``
        task for a catalog.

        Visibility-gated: callers that cannot see the catalog receive the same
        404 they would get for a genuinely missing catalog (no information leak).
        """
        from dynastore.modules.tasks import tasks_module
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.modules.db_config.query_executor import managed_transaction

        # Visibility gate first — no existence leak.
        await _assert_catalog_visible(catalog_id)

        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise HTTPException(
                status_code=503, detail="Catalogs service not available."
            )
        catalog = await catalogs.get_catalog_model(catalog_id)
        if catalog is None:
            raise HTTPException(
                status_code=404, detail=f"Catalog '{catalog_id}' not found."
            )

        provisioning_status = getattr(catalog, "provisioning_status", "ready") or "ready"

        physical_schema: Optional[str] = None
        try:
            physical_schema = await catalogs.resolve_physical_schema(
                catalog_id, allow_missing=True
            )
        except Exception as exc:
            logger.warning(
                "catalog_status: failed to resolve physical schema for catalog %s: %s",
                catalog_id, exc,
                exc_info=True,
            )

        task_view: Optional[ProvisioningTaskView] = None
        if physical_schema:
            db = get_protocol(DatabaseProtocol)
            if db is not None:
                try:
                    async with managed_transaction(db.engine) as conn:
                        tasks = await tasks_module.list_tasks(
                            conn, schema=physical_schema, limit=20, offset=0,
                        )
                    # Accept both the current task type ("catalog_provision",
                    # enqueued by create_catalog since #2329) and the legacy
                    # type ("gcp_provision_catalog", enqueued by the old GCP
                    # module and by the reprovision endpoint).
                    provision_tasks = [
                        t for t in tasks
                        if t.task_type in ("catalog_provision", "gcp_provision_catalog")
                    ]
                    if provision_tasks:
                        t = sorted(
                            provision_tasks,
                            key=lambda x: x.finished_at or x.timestamp,
                            reverse=True,
                        )[0]
                        task_view = ProvisioningTaskView(
                            task_id=t.jobID,
                            status=(
                                t.status.value
                                if hasattr(t.status, "value")
                                else str(t.status)
                            ),
                            error_message=t.error_message,
                            retry_count=t.retry_count,
                            max_retries=t.max_retries,
                            created_at=t.timestamp,
                            updated_at=t.finished_at,
                        )
                except Exception as exc:
                    logger.warning(
                        "catalog_status: failed to query provision tasks for "
                        "catalog %s schema %s: %s",
                        catalog_id, physical_schema, exc,
                        exc_info=True,
                    )

        provisioning_checklist: dict[str, str] = {}
        if physical_schema:
            try:
                provisioning_checklist = await catalogs.get_provisioning_checklist(physical_schema)
            except Exception as exc:
                logger.warning(
                    "catalog_status: failed to read provisioning_checklist for "
                    "catalog %s: %s",
                    catalog_id, exc,
                    exc_info=True,
                )

        return CatalogStatusView(
            external_id=catalog_id,
            id=physical_schema,
            provisioning_status=provisioning_status,
            task=task_view,
            provisioning_checklist=provisioning_checklist,
        )

    # -------------------------------------------------------------------------
    # GET /catalog/catalogs/{catalog_id}/collections/{collection_id}
    # -------------------------------------------------------------------------

    @router.get(
        "/catalogs/{catalog_id}/collections/{collection_id}",
        response_model=CollectionStatusView,
        summary="Collection status within a catalog.",
    )
    async def get_collection_status(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
    ):
        """Return collection registry existence and parent catalog context.

        Note: the ``Collection`` domain model carries no per-collection
        provisioning fields — lifecycle state lives at the catalog level.
        This view therefore exposes registry existence (the collection was
        found and is readable) and the parent catalog's provisioning status.
        Per-collection provisioning information would require a dedicated
        collection-lifecycle table, which is out of scope for this extension.

        Visibility-gated: catalog hidden → 404 before collection lookup.
        Collection hidden → 404 same shape as a genuine miss.
        """
        # Catalog-level gate first so callers cannot probe collection existence
        # in a hidden catalog.
        await _assert_catalog_visible(catalog_id)
        await _assert_collection_visible(catalog_id, collection_id)

        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise HTTPException(
                status_code=503, detail="Catalogs service not available."
            )

        catalog = await catalogs.get_catalog_model(catalog_id)
        if catalog is None:
            raise HTTPException(
                status_code=404, detail=f"Catalog '{catalog_id}' not found."
            )

        # get_collection already enforces visibility (returns None for hidden).
        collection = await catalogs.get_collection(catalog_id, collection_id)
        if collection is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Collection '{collection_id}' not found in catalog '{catalog_id}'."
                ),
            )

        provisioning_status = getattr(catalog, "provisioning_status", "ready") or "ready"

        physical_schema: Optional[str] = None
        try:
            physical_schema = await catalogs.resolve_physical_schema(
                catalog_id, allow_missing=True
            )
        except Exception as exc:
            logger.warning(
                "catalog_status: failed to resolve physical schema for catalog %s: %s",
                catalog_id, exc,
                exc_info=True,
            )

        return CollectionStatusView(
            catalog_external_id=catalog_id,
            collection_id=collection_id,
            catalog_id=physical_schema,
            catalog_provisioning_status=provisioning_status,
        )

