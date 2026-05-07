#    Copyright 2025 FAO
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

"""Optional mixin providing shared infrastructure for OGC-protocol extensions.

``ExtensionProtocol`` is the universal base for *all* DynaStore extensions
(Admin, Auth, GCP, Logs, …).  ``OGCServiceMixin`` is an **opt-in mixin**
that only OGC-specific extensions (Features, STAC, Records, Coverages, EDR,
…) add to their bases.  Non-OGC extensions are unaffected.

Usage::

    class CoveragesService(ExtensionProtocol, OGCServiceMixin):
        conformance_uris = [...]
        prefix = "/coverages"
        protocol_title = "DynaStore OGC API - Coverages"
        protocol_description = "Coverage data access via OGC API"
        ...
"""

import logging
from typing import Any, List, Optional, cast

from fastapi import HTTPException, Request, Response, status

from dynastore.extensions.ogc_models_shared import (
    BulkCreationResponse,
    IngestionReport,
    SidecarRejection,
)
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from dynastore.extensions.tools.ogc_common_models import Conformance, LandingPage
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.models.shared_models import Link
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


class OGCServiceMixin:
    """Shared helpers for OGC-protocol extensions.

    Subclasses set the following class attributes:

    * ``conformance_uris: List[str]`` — OGC conformance class URIs
    * ``prefix: str`` — router path prefix (e.g. ``"/features"``)
    * ``protocol_title: str`` — human-readable protocol name
    * ``protocol_description: str`` — one-line description
    """

    # --- Class attributes to be set by subclasses ---
    conformance_uris: List[str] = []
    prefix: str = ""
    protocol_title: str = ""
    protocol_description: str = ""

    # --- Cached protocol references (per-instance) ---
    _ogc_catalogs_protocol: Optional[CatalogsProtocol] = None
    _ogc_configs_protocol: Optional[ConfigsProtocol] = None

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------

    def register_policies(self) -> None:
        """Override in subclass to register IAM policies.  Default: no-op."""

    # ------------------------------------------------------------------
    # Protocol getters (cached, with standard error handling)
    # ------------------------------------------------------------------

    async def _get_catalogs_service(self) -> CatalogsProtocol:
        if self._ogc_catalogs_protocol is None:
            svc = get_protocol(CatalogsProtocol)
            if not svc:
                raise HTTPException(
                    status_code=500, detail="Catalogs service not available."
                )
            self._ogc_catalogs_protocol = svc
        return cast(CatalogsProtocol, self._ogc_catalogs_protocol)

    async def _get_configs_service(self) -> ConfigsProtocol:
        if self._ogc_configs_protocol is None:
            svc = get_protocol(ConfigsProtocol)
            if not svc:
                raise HTTPException(
                    status_code=500, detail="Configs service not available."
                )
            self._ogc_configs_protocol = svc
        return cast(ConfigsProtocol, self._ogc_configs_protocol)

    # ------------------------------------------------------------------
    # Fail-fast catalog-readiness guard
    # ------------------------------------------------------------------

    async def _require_catalog_ready(
        self,
        catalog_id: str,
        *,
        catalogs_svc: Optional[CatalogsProtocol] = None,
    ) -> Any:
        """Thin wrapper around :func:`extensions.tools.catalog_readiness.require_catalog_ready`.

        Kept on the mixin for back-compat with existing OGC-extension
        call sites; non-OGC extensions (assets, configs, processes, …)
        should import the free function directly so they don't have to
        inherit from :class:`OGCServiceMixin`.
        """
        from dynastore.extensions.tools.catalog_readiness import require_catalog_ready

        if catalogs_svc is None:
            catalogs_svc = await self._get_catalogs_service()
        return await require_catalog_ready(catalog_id, catalogs_svc=catalogs_svc)

    # ------------------------------------------------------------------
    # Standard OGC endpoint handlers
    # ------------------------------------------------------------------

    async def ogc_conformance_handler(self, request: Request) -> Conformance:
        """Standard conformance endpoint returning this protocol's URIs."""
        return Conformance(conformsTo=self.conformance_uris)

    async def ogc_landing_page_handler(self, request: Request) -> LandingPage:
        """Standard landing page with self, conformance, and service-doc links.

        Override in subclass if the protocol needs a custom landing page
        (e.g. STAC returns a root catalog, not a plain landing page).
        """
        root_url = get_root_url(request)
        return LandingPage(
            title=self.protocol_title,
            description=self.protocol_description,
            links=[
                Link(
                    href=f"{root_url}{self.prefix}/",
                    rel="self",
                    type="application/json",
                    title="This document",  # type: ignore[arg-type]
                ),
                Link(
                    href=f"{root_url}{self.prefix}/conformance",
                    rel="conformance",
                    type="application/json",
                    title="Conformance classes",  # type: ignore[arg-type]
                ),
                Link(
                    href=f"{root_url}/api",
                    rel="service-doc",
                    type="application/json",
                    title="API documentation",  # type: ignore[arg-type]
                ),
            ],
        )

    # ------------------------------------------------------------------
    # Shared CRUD helpers
    # ------------------------------------------------------------------

    async def _delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        db_resource,
    ) -> Response:
        """Shared item deletion: delete + 404 check + 204 response.

        The caller is responsible for transaction management (e.g.
        ``managed_transaction``) — this mixin stays decoupled from
        ``modules.db_config``.
        """
        catalogs_svc = await self._get_catalogs_service()
        from dynastore.models.driver_context import DriverContext
        rows_affected = await catalogs_svc.delete_item(
            catalog_id, collection_id, item_id, ctx=DriverContext(db_resource=db_resource)
        )
        if rows_affected == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Item '{item_id}' not found.",
            )
        return Response(status_code=status.HTTP_204_NO_CONTENT)


class OGCTransactionMixin:
    """Shared multi-item ingestion helpers for OGC Features, Records, and STAC.

    Provides two methods that the three OGC write-capable services share:

    * :meth:`_ingest_items` — normalises payload → list, calls
      ``CatalogsProtocol.upsert``, catches ``SidecarRejectedError``,
      returns ``(accepted_rows, rejections, was_single, batch_size)``.

    * :meth:`_build_rejection_response` — constructs the HTTP 207
      ``IngestionReport`` JSONResponse used when any item is rejected.

    * :meth:`_build_bulk_creation_response` — constructs the HTTP 201
      ``BulkCreationResponse`` JSONResponse for a fully-accepted
      multi-item batch.

    The single-item 201 response is intentionally left to the calling
    handler because its shape is protocol-specific (plain GeoJSON Feature
    for OGC Features, a full STAC Item for STAC, a Record for Records).

    Subclasses must also inherit :class:`OGCServiceMixin` (which provides
    ``_get_catalogs_service``).  MRO: put mixins before Protocols::

        class STACService(ExtensionProtocol, ..., OGCServiceMixin, OGCTransactionMixin):
    """

    # ------------------------------------------------------------------
    # Core ingestion helper
    # ------------------------------------------------------------------

    async def _ingest_items(
        self,
        catalog_id: str,
        collection_id: str,
        payload: Any,
        ctx: DriverContext,
        policy_source: str,
    ) -> "tuple[list[Any], list[SidecarRejection], bool, int]":
        """Normalise *payload*, upsert, and collect rejections.

        *payload* may be any of:

        * A Pydantic model with ``type == 'Feature'`` → single item
        * A Pydantic model with ``type == 'FeatureCollection'`` and a
          ``.features`` list → collection
        * A plain ``list`` → multi-item (each element is a dict or model)
        * A plain ``dict`` → single item

        Returns a 4-tuple:
        ``(accepted_rows, rejections, was_single, batch_size)``
        where *was_single* is ``True`` when the caller sent a lone item
        (not wrapped in a collection/array).
        """
        from dynastore.modules.storage.errors import ConflictError, SidecarRejectedError

        # Determine was_single and normalise to list
        payload_type = getattr(payload, "type", None)
        if payload_type == "FeatureCollection":
            was_single = False
            items_list = list(getattr(payload, "features", []) or [])
        elif isinstance(payload, list):
            was_single = False
            items_list = payload
        elif isinstance(payload, dict) and payload.get("type") == "FeatureCollection":
            was_single = False
            items_list = list(payload.get("features", []) or [])
        elif isinstance(payload, dict):
            was_single = True
            items_list = [payload]
        else:
            # Single Pydantic model (Feature, STACItem, …)
            was_single = True
            items_list = [payload]

        batch_size = len(items_list)

        # CatalogsProtocol.upsert accepts the original payload directly so
        # the driver can use any type-specific fast-paths it provides.
        catalogs_svc = await self._get_catalogs_service()  # type: ignore[attr-defined]

        rejections: list[SidecarRejection] = []
        # Seed the typed out-list so the PG write path can record per-row
        # SidecarRejectedError events without collapsing the whole batch.
        # The core service reads/writes ``ctx.extensions["_rejections"]``.
        ctx.extensions["_rejections"] = []
        try:
            created = await catalogs_svc.upsert(
                catalog_id, collection_id, items=payload, ctx=ctx
            )
        except SidecarRejectedError as rej:
            # Non-PG primary drivers still surface rejections as a single
            # batch-level exception; PG now catches per-row and delivers via
            # the out-list below, so we only reach here when the primary
            # driver aborted the whole payload.
            rejections.append(
                SidecarRejection(
                    geoid=rej.geoid,
                    external_id=rej.external_id,
                    sidecar_id=rej.sidecar_id,
                    matcher=rej.matcher,
                    reason=rej.reason,
                    message=str(rej),
                    policy_source=policy_source,
                )
            )
            created = []
        except ConflictError as exc:
            # on_asset_conflict=refuse_asset: duplicate detected → abort batch → 409.
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=str(exc)
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
            )

        # Drain per-row rejections delivered via the DriverContext out-list.
        for entry in ctx.extensions.pop("_rejections", []) or []:
            rejections.append(
                SidecarRejection(
                    geoid=entry.get("geoid"),
                    external_id=entry.get("external_id"),
                    sidecar_id=entry.get("sidecar_id"),
                    matcher=entry.get("matcher"),
                    reason=entry.get("reason") or "sidecar_rejected",
                    message=entry.get("message") or "",
                    policy_source=policy_source,
                )
            )

        accepted_rows: list[Any] = (
            created if isinstance(created, list) else ([created] if created else [])
        )

        if not accepted_rows and not rejections:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create items.",
            )

        return accepted_rows, rejections, was_single, batch_size

    # ------------------------------------------------------------------
    # Response builders (shared between Features, Records, STAC)
    # ------------------------------------------------------------------

    def _resolve_accepted_ids(self, accepted_rows: "list[Any]") -> list[str]:
        """Extract the logical string ID from each upserted row."""
        ids: list[str] = []
        for row in accepted_rows:
            props = getattr(row, "properties", None) or {}
            fid = (
                getattr(row, "id", None)
                or props.get("external_id")
                or props.get("geoid")
            )
            if not fid and isinstance(props.get("attributes"), dict):
                fid = props["attributes"].get("id") or props["attributes"].get(
                    "external_id"
                )
            if not fid and props.get("geoid"):
                fid = props["geoid"]
            if fid is not None and not isinstance(fid, str):
                fid = str(fid)
            if not fid:
                raise RuntimeError(
                    f"Could not determine feature ID from upsert result: "
                    f"properties={getattr(row, 'properties', None)} id={getattr(row, 'id', None)}"
                )
            ids.append(fid)
        return ids

    def _build_rejection_response(
        self,
        accepted_rows: "list[Any]",
        rejections: "list[SidecarRejection]",
        batch_size: int,
    ) -> Response:
        """Return HTTP 207 Multi-Status with an :class:`IngestionReport` body."""
        accepted_ids = self._resolve_accepted_ids(accepted_rows)
        report = IngestionReport(
            accepted_ids=accepted_ids,
            rejections=rejections,
            total=batch_size,
        )
        return JSONResponse(
            content=report.model_dump(by_alias=True, exclude_none=True),
            status_code=status.HTTP_207_MULTI_STATUS,
        )

    def _build_bulk_creation_response(
        self,
        accepted_rows: "list[Any]",
    ) -> Response:
        """Return HTTP 201 Created with a :class:`BulkCreationResponse` body."""
        accepted_ids = self._resolve_accepted_ids(accepted_rows)
        return JSONResponse(
            content=BulkCreationResponse(ids=accepted_ids).model_dump(),
            status_code=status.HTTP_201_CREATED,
        )
