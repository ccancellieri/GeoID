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

# dynastore/extensions/stac/stac_virtual.py

import logging
import asyncio
from typing import TYPE_CHECKING, Any, Optional, cast

import pystac
from fastapi import Depends, HTTPException, Query, Request, status
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from dynastore.models.driver_context import DriverContext
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.models.protocols import (
    AssetsProtocol,
    CatalogsProtocol,
    ConfigsProtocol,
)
from dynastore.modules.stac.stac_config import StacPluginConfig
from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.discovery import get_protocol
from dynastore.extensions.tools.db import get_async_engine
from dynastore.extensions.tools.url import get_url, get_root_url
from dynastore.extensions.tools.language_utils import get_language
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    ResultHandler,
)
from dynastore.extensions.stac.search import ItemSearchRequest
from . import stac_generator, asset_factory, metadata_mapper

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    class _Host:
        async def _get_catalogs_service(self) -> Any: ...
        async def _get_configs_service(self) -> Any: ...
        async def _get_stac_config(
            self, catalog_id: str, collection_id: Optional[str] = None, *, db_resource: Any = None
        ) -> Any: ...
else:
    class _Host: ...


class StacVirtualMixin(_Host):
    """Mixin providing virtual STAC endpoints (asset views and hierarchy views)."""

    # --- Virtual STAC Endpoints ---
    # NOTE: Virtual endpoints might also need localization if they return titles/descriptions.
    # Currently they generate titles dynamically based on IDs/codes which are language-agnostic.
    # We'll pass language where applicable.

    async def get_virtual_asset_list(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        engine=Depends(get_async_engine),
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        """
        Virtual View: Lists all 'Assets' for a collection as if they were STAC Collections.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()

        async with managed_transaction(engine) as conn:
            stac_config = await self._get_stac_config(
                catalog_id, collection_id, db_resource=conn
            )

            if not stac_config.asset_tracking.enabled:
                raise HTTPException(
                    status_code=404,
                    detail="Asset tracking not enabled for this collection.",
                )

            # AssetsProtocol is available via CatalogsProtocol property
            assets = await catalogs_svc.assets.list_assets(
                catalog_id=catalog_id,
                collection_id=collection_id,
                limit=limit,
                offset=offset,
                db_resource=conn,
            )

        base_url = get_url(request, remove_qp=True)
        # Root for assets is /stac/virtual/assets
        assets_root = f"{get_root_url(request)}/stac/virtual/assets"

        virtual_cat = pystac.Catalog(
            id=f"{collection_id}_assets",
            description=f"Virtual collection of assets for {collection_id}",
            title=f"Assets of {collection_id}",
        )
        virtual_cat.set_self_href(base_url)

        for asset in assets:
            # New structure: /virtual/assets/{asset_id}/catalogs/{cat}/collections/{coll}
            child_href = f"{assets_root}/{asset.asset_id}/catalogs/{catalog_id}/collections/{collection_id}"
            virtual_cat.add_link(
                pystac.Link(
                    rel="child",
                    target=child_href,
                    title=asset.asset_id,
                    media_type="application/json",
                )
            )

        # Add pagination links
        if len(assets) == limit:
            virtual_cat.add_link(
                pystac.Link(
                    rel="next",
                    target=f"{base_url}?offset={offset + limit}&limit={limit}",
                )
            )
        if offset > 0:
            virtual_cat.add_link(
                pystac.Link(
                    rel="prev",
                    target=f"{base_url}?offset={max(0, offset - limit)}&limit={limit}",
                )
            )

        return JSONResponse(content=virtual_cat.to_dict())

    async def get_virtual_asset_collection(
        self,
        catalog_id: str,
        collection_id: str,
        asset_code: str,
        request: Request,
        engine=Depends(get_async_engine),
    ):
        """
        Virtual View: Represents a single Asset as a STAC Collection.
        Metadata is generated from the Asset record.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()

        async with managed_transaction(engine) as conn:
            stac_config = await self._get_stac_config(
                catalog_id, collection_id, db_resource=conn
            )

            # AssetsProtocol is also implemented by CatalogModule
            assets_svc = get_protocol(AssetsProtocol)
            if not assets_svc:
                raise HTTPException(
                    status_code=500, detail="Assets protocol not available."
                )

            asset = await assets_svc.get_asset(
                asset_id=asset_code,
                catalog_id=catalog_id,
                collection_id=collection_id,
                db_resource=conn,  # type: ignore[call-arg]
            )
            if not asset:
                logger.warning(
                    f"Virtual Asset not found: catalog={catalog_id}, id={asset_code}, collection={collection_id}"
                )
                raise HTTPException(
                    status_code=404, detail=f"Asset '{asset_code}' not found."
                )

        # Create a Collection representing this Asset
        base_url = get_url(request)
        collection = pystac.Collection(
            id=asset.asset_id,
            description=f"Virtual collection for ingested asset: {asset.asset_id}",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
                temporal=pystac.TemporalExtent([[asset.created_at, None]]),
            ),
            title=asset.asset_id,
            license="proprietary",
        )

        # Enrich from metadata
        metadata_mapper.enrich_collection_from_metadata(
            collection, asset.metadata or {}
        )

        collection.set_self_href(base_url)
        collection.add_link(
            pystac.Link(
                rel="items",
                target=f"{base_url}/items",
                media_type="application/geo+json",
            )
        )

        # Parent is the list of assets for this collection
        parent_href = f"{get_root_url(request)}/stac/virtual/assets/catalogs/{catalog_id}/collections/{collection_id}"
        collection.add_link(
            pystac.Link(rel="parent", target=parent_href, title="Assets List")
        )

        # Add dynamic assets via factory
        asset_context = asset_factory.AssetContext(
            base_url=get_root_url(request),
            catalog_id=catalog_id,
            collection_id=collection_id,
            request=request,
            stac_config=stac_config,
            asset_id=asset.asset_id,
        )
        await asset_factory.add_dynamic_assets_and_links(collection, asset_context)

        return JSONResponse(content=collection.to_dict())

    async def get_virtual_asset_collections(
        self,
        catalog_id: str,
        asset_id: str,
        request: Request,
        engine=Depends(get_async_engine),
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        """Lists every collection in *catalog_id* where *asset_id* has rows,
        rendered as virtual STAC child collections of the asset.

        Each child link points at the existing
        ``/virtual/assets/{asset_id}/catalogs/{cat}/collections/{coll}``
        endpoint, where the items view is filtered by ``asset_id``.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        asset_id = validate_sql_identifier(asset_id)
        catalogs_svc = await self._get_catalogs_service()

        async with managed_transaction(engine) as conn:
            phys_schema = await catalogs_svc.resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            if not phys_schema:
                raise HTTPException(
                    status_code=404, detail=f"Catalog '{catalog_id}' not found."
                )

            # Distinct collection_ids in the catalog where the asset has
            # at least one non-deleted row.  One-off query — no equivalent
            # method on AssetsProtocol (search_assets is partition-scoped
            # and requires a target collection_id).
            sql = (
                f'SELECT DISTINCT collection_id '
                f'FROM "{phys_schema}".assets '
                f'WHERE catalog_id = :catalog_id '
                f'  AND asset_id = :asset_id '
                f'  AND deleted_at IS NULL '
                f'  AND collection_id IS NOT NULL '
                f'ORDER BY collection_id '
                f'LIMIT :limit OFFSET :offset'
            )
            rows = await cast(AsyncConnection, conn).execute(
                text(sql),
                {
                    "catalog_id": catalog_id,
                    "asset_id": asset_id,
                    "limit": limit,
                    "offset": offset,
                },
            )
            collection_ids = [r[0] for r in rows.fetchall()]

        base_url = get_url(request, remove_qp=True)
        assets_root = f"{get_root_url(request)}/stac/virtual/assets"

        virtual_cat = pystac.Catalog(
            id=f"{asset_id}_collections",
            description=(
                f"Virtual collections in catalog '{catalog_id}' that "
                f"contain asset '{asset_id}'."
            ),
            title=f"Collections of asset {asset_id} in {catalog_id}",
        )
        virtual_cat.set_self_href(base_url)

        for coll_id in collection_ids:
            child_href = (
                f"{assets_root}/{asset_id}/catalogs/{catalog_id}"
                f"/collections/{coll_id}"
            )
            virtual_cat.add_link(
                pystac.Link(
                    rel="child",
                    target=child_href,
                    title=coll_id,
                    media_type="application/json",
                )
            )

        if len(collection_ids) == limit:
            virtual_cat.add_link(
                pystac.Link(
                    rel="next",
                    target=f"{base_url}?offset={offset + limit}&limit={limit}",
                )
            )
        if offset > 0:
            virtual_cat.add_link(
                pystac.Link(
                    rel="prev",
                    target=f"{base_url}?offset={max(0, offset - limit)}&limit={limit}",
                )
            )

        return JSONResponse(content=virtual_cat.to_dict())

    async def get_virtual_asset_items(
        self,
        catalog_id: str,
        collection_id: str,
        asset_id: str,
        request: Request,
        engine=Depends(get_async_engine),
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        language: str = Depends(get_language),
    ):
        """
        Virtual View: Lists items belonging to a specific asset.
        Filters the main collection by 'asset_id'.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()

        async with managed_transaction(engine) as conn:
            stac_config = await self._get_stac_config(
                catalog_id, collection_id, db_resource=conn
            )

            # Use ItemsProtocol.stream_items to leverage standard filtering and sidecar logic
            from dynastore.models.protocols import ItemsProtocol

            items_svc = get_protocol(ItemsProtocol)
            if not items_svc:
                raise HTTPException(
                    status_code=500, detail="Items protocol not available."
                )

            # Resolve col_config for sidecar resolution
            col_config = await catalogs_svc.get_collection_config(
                catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
            )

            # Optimized query with QueryRequest
            from dynastore.models.query_builder import QueryRequest, FilterCondition

            # Build request for asset items
            # We assume 'asset_id' is a queryable field in the sidecars (attributes or stac_metadata)
            asset_filter = FilterCondition(field="asset_id", operator="=", value=asset_id)
            query_req = QueryRequest(
                filters=[asset_filter],
                limit=limit,
                offset=offset,
                include_total_count=True,
            )

            # ── Routing-aware items SEARCH-driver dispatch (#1047, #1311) ─────
            # Mirror the OGC Features / Records ``/items`` paths: resolve the
            # items SEARCH driver via routing and dispatch the structural query
            # (the ``asset_id`` equality threads through ``filters``) to that
            # driver's streaming ``read_entities`` + ``count_entities`` contract.
            # The helper threads the HTTP ``request`` so an access-aware envelope
            # driver gets the caller's compiled read scope (fail-closed). It
            # returns ``None`` — and we fall through to the PG ``stream_items``
            # path below unchanged — for a read-primary (PG) driver, a non-ES
            # items driver, or any dispatch error.
            from dynastore.extensions.tools.query import (
                maybe_dispatch_items_to_search_driver,
            )
            from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType

            query_res = await maybe_dispatch_items_to_search_driver(
                catalog_id=catalog_id,
                collection_id=collection_id,
                filters=[asset_filter],
                limit=limit,
                offset=offset,
                has_complex_filter=False,
                request=request,
            )

            # Fetch features using ItemService.stream_items to get QueryResponse
            if query_res is None:
                # PG row-level ABAC: compile and inject access_filter when the
                # collection carries an access_envelope sidecar (user-facing read).
                from dynastore.modules.storage.access_scope import (
                    collection_uses_pg_access_envelope,
                    compile_read_access_filter,
                    principals_from_request_state,
                )

                if await collection_uses_pg_access_envelope(catalog_id, collection_id):
                    principals, principal = principals_from_request_state(request)
                    query_req.access_filter = await compile_read_access_filter(
                        catalog_id=catalog_id,
                        collections=[collection_id],
                        principals=principals,
                        principal=principal,
                    )

                query_res = await items_svc.stream_items(
                    catalog_id, collection_id, request=query_req,
                    ctx=DriverContext(db_resource=conn) if conn is not None else None,
                    consumer=ConsumerType.STAC,
                )

            total_count = query_res.total_count or 0

            # Map features to STAC Items
            stac_items = []
            async for feature in query_res.items:
                # create_item_from_row expects a dict from DB, but QueryResponse.items returns Features (GeoJSON)
                # We need to ensure stac_generator.create_item_from_row can handle high-level Feature models
                # or we use a different generator method.
                # Actually, stac_generator.create_item_from_row uses row.get("geoid") etc.
                # If feature is a Feature model from geojson-pydantic:
                virtual_collection_url = (
                    str(request.url).split("?")[0].replace("/items", "")
                )
                item = await stac_generator.create_item_from_feature(
                    request,
                    catalog_id,
                    collection_id,
                    feature=feature,
                    stac_config=stac_config,
                    view_mode="virtual-asset",
                    lang=language,
                    collection_url_override=virtual_collection_url,
                )
                if item:
                    item_dict = item.to_dict()
                    # PySTAC to_dict() drops duplicate targets with different rels (e.g. parent and collection).
                    # We must explicitly inject it for Virtual Assets
                    has_parent = any(link.get("rel") == "parent" for link in item_dict.get("links", []))
                    if not has_parent:
                        item_dict.setdefault("links", []).append({
                            "rel": "parent",
                            "href": virtual_collection_url,
                            "type": "application/json"
                        })
                    stac_items.append(item_dict)

        item_collection = pystac.ItemCollection(items=stac_items)
        coll_dict = item_collection.to_dict()

        coll_dict["numberMatched"] = total_count
        coll_dict["numberReturned"] = len(stac_items)

        # Paging links
        base_url = get_url(request, remove_qp=True)
        if (offset + limit) < total_count:
            coll_dict.setdefault("links", []).append(
                {
                    "rel": "next",
                    "href": f"{base_url}?offset={offset + limit}&limit={limit}",
                }
            )
        if offset > 0:
            coll_dict.setdefault("links", []).append(
                {
                    "rel": "prev",
                    "href": f"{base_url}?offset={max(0, offset - limit)}&limit={limit}",
                }
            )

        return JSONResponse(content=coll_dict)

    async def resolve_asset_source(
        self,
        catalog_id: str,
        collection_id: str,
        asset_code: str,
        request: Request,
        engine=Depends(get_async_engine),
    ):
        """
        Dynamically resolves the source asset URL.
        Decides between Proxy or Direct access based on STAC configuration.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)

        async with managed_transaction(engine) as conn:
            # 1. Get the asset to find its URI
            asset_mgr = get_protocol(AssetsProtocol)
            if not asset_mgr:
                raise HTTPException(
                    status_code=500, detail="Assets protocol not available."
                )

            asset = await asset_mgr.get_asset(
                asset_id=asset_code, catalog_id=catalog_id, collection_id=collection_id
            )
            if not asset:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Asset '{asset_code}' not found.",
                )

            # 2. Get STAC config to decide on access mode
            # If no collection_id provided, we try to use a default or first collection config
            # But usually we want the collection-specific config.
            cm = get_protocol(ConfigsProtocol)
            if not cm:
                raise HTTPException(
                    status_code=500, detail="Configs protocol not available."
                )
            stac_config = await cm.get_config(
                StacPluginConfig, catalog_id, collection_id, ctx=DriverContext(db_resource=conn
            ))

            from dynastore.modules.stac.stac_config import AssetAccessMode
            from fastapi.responses import RedirectResponse

            if asset.uri is None:
                raise HTTPException(status_code=404, detail="Asset has no URI.")

            if stac_config.asset_tracking.access_mode == AssetAccessMode.PROXY:
                try:
                    from dynastore.modules.proxy.proxy_module import create_short_url

                    proxy_key = await create_short_url(
                        conn,
                        catalog_id=catalog_id,
                        long_url=asset.uri,
                        collection_id=collection_id,
                        comment=f"Dynamic source for {asset_code}",
                    )
                    root_path = request.scope.get("root_path", "").rstrip("/")
                    proxy_url = f"{root_path}/proxy/r/{proxy_key.short_key}"
                    return RedirectResponse(url=proxy_url)
                except Exception as e:
                    logger.error(
                        f"Failed to create proxy for asset {asset_code}: {str(e)}"
                    )
                    return RedirectResponse(url=asset.uri)
            else:
                return RedirectResponse(url=asset.uri)

    def _resolve_hierarchy_rule(
        self,
        stac_config: Any,
        hierarchy_id: str,
    ) -> Optional[Any]:
        """Return a `HierarchyRule` for `hierarchy_id` from either:
          1. `hierarchy.providers[hierarchy_id]` when kind=="data-derived"
             (unwraps the embedded rule), OR
          2. `hierarchy.rules[hierarchy_id]` legacy path.

        Returns ``None`` when no SQL-backed rule is configured.
        dimension-backed / static / external-skos providers return ``None`` —
        items via SQL are not meaningful for those kinds.
        """
        if not stac_config.hierarchy or not stac_config.hierarchy.enabled:
            return None

        provider_cfg_raw = (stac_config.hierarchy.providers or {}).get(hierarchy_id)
        if provider_cfg_raw is not None:
            try:
                from dynastore.extensions.stac.hierarchy import HierarchyProviderConfig
                provider_cfg = (
                    provider_cfg_raw
                    if isinstance(provider_cfg_raw, HierarchyProviderConfig)
                    else HierarchyProviderConfig.model_validate(provider_cfg_raw)
                )
                if provider_cfg.kind == "data-derived" and provider_cfg.rule is not None:
                    return provider_cfg.rule
            except Exception:
                pass

        return stac_config.hierarchy.rules.get(hierarchy_id)

    async def _render_virtual_collection_via_provider(
        self,
        *,
        provider_cfg_raw: Any,
        conn: Any,
        catalog_id: str,
        collection_id: str,
        hierarchy_id: str,
        parent_value: Optional[str],
        limit: int,
        request: Request,
    ) -> JSONResponse:
        """Render a virtual Collection through a pluggable HierarchyProvider.

        Used when `HierarchyConfig.providers[hierarchy_id]` is configured.
        Supports any registered provider kind (data-derived, dimension-backed,
        static, external-skos). Link shape is uniform across kinds.
        """
        from types import SimpleNamespace
        from dynastore.extensions.stac.hierarchy import (
            HierarchyProviderConfig,
            get_hierarchy_provider,
        )

        try:
            provider_cfg = (
                provider_cfg_raw
                if isinstance(provider_cfg_raw, HierarchyProviderConfig)
                else HierarchyProviderConfig.model_validate(provider_cfg_raw)
            )
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Invalid HierarchyProviderConfig for {hierarchy_id!r}: {exc}",
            )

        ctx = SimpleNamespace(
            conn=conn,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )

        try:
            provider = get_hierarchy_provider(provider_cfg, ctx)
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

        if parent_value:
            page = await provider.children(ctx, parent_value, limit=limit, offset=0)
        else:
            page = await provider.roots(ctx, limit=limit, offset=0)
        ext = await provider.extent(ctx, parent_value)

        base_url = get_url(request, remove_qp=True)
        root_url = get_root_url(request)

        title = (
            provider_cfg.collection_title_template
            or f"{provider_cfg.level_name or hierarchy_id} Hierarchy"
        )
        if parent_value:
            title += f" (Parent: {parent_value})"
        description = (
            provider_cfg.collection_description_template
            or f"Virtual collection for {provider_cfg.level_name or hierarchy_id}"
        )

        bbox = ext.spatial_bbox or [-180, -90, 180, 90]
        temporal = [ext.temporal_interval or [None, None]]

        virtual_collection = pystac.Collection(
            id=f"{collection_id}_{hierarchy_id}",
            description=description,
            title=title,
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([bbox]),
                temporal=pystac.TemporalExtent(temporal),  # type: ignore[arg-type]
            ),
            license="proprietary",
        )
        virtual_collection.set_self_href(get_url(request))

        # rel=parent
        if parent_value:
            parent_coll_url = (
                f"{root_url}/stac/virtual/hierarchy/{hierarchy_id}"
                f"/catalogs/{catalog_id}/collections/{collection_id}"
            )
            virtual_collection.add_link(pystac.Link(
                rel="parent", target=parent_coll_url,
                title=f"Parent hierarchy level ({hierarchy_id})",
                media_type="application/json",
            ))
        else:
            main_coll_url = (
                f"{root_url}/stac/catalogs/{catalog_id}/collections/{collection_id}"
            )
            virtual_collection.add_link(pystac.Link(
                rel="parent", target=main_coll_url,
                title="Main Collection", media_type="application/json",
            ))

        # rel=child per member
        for node in page.members:
            child_href = f"{base_url}?parent_value={node.code}"
            virtual_collection.add_link(pystac.Link(
                rel="child", target=child_href,
                title=node.label,
                media_type="application/json",
            ))

        # rel=items
        items_url = f"{base_url}/items"
        if parent_value:
            items_url += f"?parent_value={parent_value}"
        virtual_collection.add_link(pystac.Link(
            rel="items", target=items_url, media_type="application/geo+json",
        ))

        return JSONResponse(content=virtual_collection.to_dict())

    async def get_virtual_hierarchy_collection(
        self,
        hierarchy_id: str,
        catalog_id: str,
        collection_id: str,
        request: Request,
        engine=Depends(get_async_engine),
        parent_value: Optional[str] = Query(None),
        limit: int = Query(100, ge=1, le=1000),
    ):
        """
        Virtual View: Returns a STAC collection representation of a hierarchy level.
        If parent_value is provided, the collection metadata is scoped to that parent.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)

        async with managed_transaction(engine) as conn:
            # Get STAC config to find the hierarchy rule
            config_manager = get_protocol(ConfigsProtocol)
            if not config_manager:
                raise HTTPException(
                    status_code=500, detail="Configs protocol not available."
                )

            stac_config = cast(
                StacPluginConfig,
                await config_manager.get_config(
                    StacPluginConfig, catalog_id, collection_id, ctx=DriverContext(db_resource=conn
                )),
            )

            if not stac_config.hierarchy or not stac_config.hierarchy.enabled:
                raise HTTPException(
                    status_code=404, detail="Hierarchy not enabled for this collection."
                )

            # Pluggable provider path — short-circuits when configured.
            provider_cfg_raw = (stac_config.hierarchy.providers or {}).get(hierarchy_id)
            if provider_cfg_raw is not None:
                return await self._render_virtual_collection_via_provider(
                    provider_cfg_raw=provider_cfg_raw,
                    conn=conn,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    hierarchy_id=hierarchy_id,
                    parent_value=parent_value,
                    limit=limit,
                    request=request,
                )

            # Legacy data-derived path — lookup HierarchyRule.
            matching_rule = stac_config.hierarchy.rules.get(hierarchy_id)
            if not matching_rule:
                raise HTTPException(
                    status_code=404,
                    detail=f"Hierarchy rule '{hierarchy_id}' not found.",
                )

            # Import hierarchy queries
            from dynastore.extensions.stac import stac_hierarchy_queries

            # Get distinct values for this hierarchy level (optionally filtered by parent_value)
            distinct_values = (
                await stac_hierarchy_queries.get_distinct_hierarchy_values(
                    cast(AsyncConnection, conn),
                    catalog_id,
                    collection_id,
                    matching_rule,
                    parent_value=parent_value,
                    limit=limit,
                )
            )

            # Get computed extent (bbox and temporal)
            extent_data = await stac_hierarchy_queries.get_hierarchy_extent(
                cast(AsyncConnection, conn),
                catalog_id,
                collection_id,
                matching_rule,
                parent_value=parent_value,
            )

        # Generate virtual collection
        base_url = get_url(request, remove_qp=True)
        root_url = get_root_url(request)

        # Use template if provided, otherwise use default
        title = (
            matching_rule.collection_title_template
            or f"{matching_rule.level_name or hierarchy_id} Hierarchy"
        )
        if parent_value:
            title += f" (Parent: {parent_value})"

        description = (
            matching_rule.collection_description_template
            or f"Virtual collection for {matching_rule.level_name or hierarchy_id} level"
        )

        virtual_collection = pystac.Collection(
            id=f"{collection_id}_{hierarchy_id}",
            description=description,
            title=title,
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([extent_data["bbox"]]),
                temporal=pystac.TemporalExtent(extent_data["temporal"]),
            ),
            license="proprietary",
        )

        virtual_collection.set_self_href(get_url(request))

        # Add parent link if this rule has a parent level
        if matching_rule.parent_hierarchy_id:
            parent_rule_id = matching_rule.parent_hierarchy_id
            # Link to the parent hierarchy collection
            parent_coll_url = f"{root_url}/stac/virtual/hierarchy/{parent_rule_id}/catalogs/{catalog_id}/collections/{collection_id}"
            virtual_collection.add_link(
                pystac.Link(
                    rel="parent",
                    target=parent_coll_url,
                    title=f"Parent Level: {parent_rule_id}",
                    media_type="application/json",
                )
            )
        else:
            # Root of hierarchy links back to the main collection
            main_coll_url = (
                f"{root_url}/stac/catalogs/{catalog_id}/collections/{collection_id}"
            )
            virtual_collection.add_link(
                pystac.Link(
                    rel="parent",
                    target=main_coll_url,
                    title="Main Collection",
                    media_type="application/json",
                )
            )

        # Add child links for each distinct value
        for value_row in distinct_values:
            code = value_row["code"]
            item_count = value_row.get("item_count", 0)

            # Children are either items (if at leaf) or next level collections
            # For now we link to items scoped by this value
            child_href = f"{base_url}/items?parent_value={code}"

            # If there is a rule that has THIS rule as its parent, we could also link to that
            # but usually the navigation is Value -> Children
            virtual_collection.add_link(
                pystac.Link(
                    rel="child",
                    target=child_href,
                    title=f"Items for {code} ({item_count} items)",
                    media_type="application/geo+json",
                )
            )

        # Add items link (all items in this level)
        items_url = f"{base_url}/items"
        if parent_value:
            items_url += f"?parent_value={parent_value}"

        virtual_collection.add_link(
            pystac.Link(
                rel="items", target=items_url, media_type="application/geo+json"
            )
        )

        return JSONResponse(content=virtual_collection.to_dict())

    async def get_virtual_hierarchy_items(
        self,
        hierarchy_id: str,
        catalog_id: str,
        collection_id: str,
        request: Request,
        engine=Depends(get_async_engine),
        parent_value: Optional[str] = Query(None),
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        language: str = Depends(get_language),
    ):
        """
        Virtual View: Returns items filtered by hierarchy rule.
        If parent_value is provided, filters to children of that parent.

        Row-level ABAC note (#1311): this view runs a raw SQL hierarchy query
        directly against PostgreSQL (``stac_hierarchy_queries``), with no
        storage-driver dispatch seam, so the compiled access filter cannot be
        threaded here without a deeper refactor (e.g. routing the hierarchy
        listing through the SEARCH driver, or compiling the access scope to a
        SQL predicate). Left as a documented gap tracked under #1311; it never
        leaks via the envelope driver since it does not use it.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)

        async with managed_transaction(engine) as conn:
            # Get STAC config to find the hierarchy rule
            config_service = await self._get_configs_service()
            stac_config = cast(
                StacPluginConfig,
                await config_service.get_config(
                    StacPluginConfig, catalog_id, collection_id, ctx=DriverContext(db_resource=conn
                )),
            )

            if not stac_config.hierarchy or not stac_config.hierarchy.enabled:
                raise HTTPException(
                    status_code=404, detail="Hierarchy not enabled for this collection."
                )

            # Resolve rule from providers[hierarchy_id] (data-derived) OR rules[hierarchy_id]
            matching_rule = self._resolve_hierarchy_rule(stac_config, hierarchy_id)

            if not matching_rule:
                raise HTTPException(
                    status_code=404,
                    detail=f"Hierarchy rule '{hierarchy_id}' not found.",
                )

            # Import hierarchy queries
            from dynastore.extensions.stac import stac_hierarchy_queries
            from dynastore.modules.db_config.query_executor import GeoDQLQuery

            # Build and execute query
            sql, params = await stac_hierarchy_queries.build_hierarchy_items_query(
                catalog_id, collection_id, matching_rule, parent_value, limit, offset
            )

            query = GeoDQLQuery(text(sql), result_handler=ResultHandler.ALL)
            items_rows = await query.execute(conn, **params)

            # Get total count
            total_count = await stac_hierarchy_queries.get_hierarchy_item_count(
                cast(AsyncConnection, conn), catalog_id, collection_id, matching_rule, parent_value
            )

            # Map to Features
            from dynastore.models.protocols import ItemsProtocol
            from dynastore.extensions.tools.query import resolve_items_read_policy
            items_svc = get_protocol(ItemsProtocol)
            assert items_svc is not None
            catalogs_svc = await self._get_catalogs_service()
            # Resolve the read policy once per query so the row mapper honours
            # ``feature_type.expose`` and ``external_id_as_feature_id`` on the
            # virtual-hierarchy path too (STAC items already key on external_id
            # by convention; the expose merge surfaces computed values that the
            # STAC item generator then reads from ``feature.properties``).
            read_policy = await resolve_items_read_policy(catalog_id, collection_id)
            features = []
            for row in items_rows:
                # Hierarchy queries return base table rows + sidecar joins if configured
                col_config = await catalogs_svc.get_collection_config(catalog_id, collection_id, ctx=DriverContext(db_resource=conn))
                feat = items_svc.map_row_to_feature(dict(row._mapping) if hasattr(row, "_mapping") else dict(row), col_config, read_policy=read_policy)
                if feat:
                    features.append(feat)

            # Generate STAC items
            stac_items = await asyncio.gather(
                *[
                    stac_generator.create_item_from_feature(
                        request,
                        catalog_id,
                        collection_id,
                        feature=f,
                        stac_config=stac_config,
                        view_mode="virtual-hierarchy",
                        lang=language,
                    )
                    for f in features
                ]
            )

        # Create item collection
        item_collection = pystac.ItemCollection(
            items=[item for item in stac_items if item]
        )
        coll_dict = item_collection.to_dict()

        coll_dict["numberMatched"] = total_count
        coll_dict["numberReturned"] = len(stac_items)

        # Paging links
        base_url = get_url(request, remove_qp=True)
        if (offset + limit) < total_count:
            next_url = f"{base_url}?offset={offset + limit}&limit={limit}"
            if parent_value:
                next_url += f"&parent_value={parent_value}"
            coll_dict.setdefault("links", []).append({"rel": "next", "href": next_url})

        return JSONResponse(content=coll_dict)

    async def search_virtual_hierarchy_items(
        self,
        hierarchy_id: str,
        catalog_id: str,
        collection_id: str,
        search_request: ItemSearchRequest,
        request: Request,
        parent_value: Optional[str] = Query(None),
        engine=Depends(get_async_engine),
        language: str = Depends(get_language),
    ):
        """
        Virtual View: Executes a STAC search scoped to a hierarchy level.
        Combines hierarchy filters with standard search parameters.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)

        # Override catalog_id in search_request to ensure consistency
        search_request.catalog_id = catalog_id
        # Scoped to the specific collection
        search_request.collections = [collection_id]

        async with managed_transaction(engine) as conn:
            # Get STAC config to find the hierarchy rule
            config_service = await self._get_configs_service()
            stac_config = cast(
                StacPluginConfig,
                await config_service.get_config(
                    StacPluginConfig, catalog_id, collection_id, ctx=DriverContext(db_resource=conn
                )),
            )

            if not stac_config.hierarchy or not stac_config.hierarchy.enabled:
                raise HTTPException(
                    status_code=404, detail="Hierarchy not enabled for this collection."
                )

            # Resolve rule from providers[hierarchy_id] (data-derived) OR rules[hierarchy_id]
            matching_rule = self._resolve_hierarchy_rule(stac_config, hierarchy_id)

            if not matching_rule:
                raise HTTPException(
                    status_code=404,
                    detail=f"Hierarchy rule '{hierarchy_id}' not found.",
                )

            # Import hierarchy queries and search
            from dynastore.extensions.stac import (
                stac_hierarchy_queries,
                search as stac_search,
            )

            # Build hierarchy SQL fragment
            hierarchy_params = {}
            hierarchy_sql = stac_hierarchy_queries.build_hierarchy_where_clause(
                matching_rule, parent_value, hierarchy_params
            )

            # Row-level ABAC: thread the caller's principals from request state
            # into ``search_items`` exactly as the main STAC ``/search`` route
            # does (#1311). Without this the anonymous-role token is dropped and
            # an access-aware envelope driver would fail closed even for a
            # legitimately-scoped caller. Behaviour-neutral for non-envelope
            # drivers, which ignore the compiled scope.
            from dynastore.modules.storage.access_scope import (
                principals_from_request_state,
            )

            _principals, _principal = principals_from_request_state(request)

            # Execute search with hierarchy scope
            (
                items_rows,
                total_count,
                aggregation_results,
            ) = await stac_search.search_items(
                conn,
                search_request,
                stac_config,
                hierarchy_sql=hierarchy_sql,
                hierarchy_params=hierarchy_params,
                principals=_principals,
                principal=_principal,
            )

            # Map to Features
            from dynastore.models.protocols import ItemsProtocol
            from dynastore.extensions.tools.query import resolve_items_read_policy
            items_svc = get_protocol(ItemsProtocol)
            catalogs_svc = get_protocol(CatalogsProtocol)
            assert items_svc is not None and catalogs_svc is not None
            # Resolve the read policy once per query so the row mapper honours
            # ``feature_type.expose`` and ``external_id_as_feature_id`` on the
            # virtual-hierarchy search path too (see get_virtual_hierarchy_items).
            read_policy = await resolve_items_read_policy(catalog_id, collection_id)
            features = []
            for row in items_rows:
                col_config = await catalogs_svc.get_collection_config(catalog_id, collection_id, ctx=DriverContext(db_resource=conn))
                feat = items_svc.map_row_to_feature(dict(row._mapping) if hasattr(row, "_mapping") else dict(row), col_config, read_policy=read_policy)
                if feat:
                    features.append(feat)

            # Generate STAC items
            stac_items = await asyncio.gather(
                *[
                    stac_generator.create_item_from_feature(
                        request,
                        catalog_id,
                        collection_id,
                        feature=f,
                        stac_config=stac_config,
                        view_mode="virtual-hierarchy",
                        lang=language,
                    )
                    for f in features
                ]
            )

        # Create item collection
        item_collection = pystac.ItemCollection(
            items=[item for item in stac_items if item]
        )
        coll_dict = item_collection.to_dict()

        coll_dict["numberMatched"] = total_count
        coll_dict["numberReturned"] = len(stac_items)

        # Add aggregation results if present
        if aggregation_results:
            coll_dict["aggregations"] = aggregation_results

        # Paging links
        offset = search_request.offset
        limit = search_request.limit
        base_url = get_url(request, remove_qp=True)
        if (offset + limit) < total_count:
            next_url = f"{base_url}?offset={offset + limit}&limit={limit}"
            if parent_value:
                next_url += f"&parent_value={parent_value}"
            coll_dict.setdefault("links", []).append({"rel": "next", "href": next_url})

        if offset > 0:
            prev_url = f"{base_url}?offset={max(0, offset - limit)}&limit={limit}"
            if parent_value:
                prev_url += f"&parent_value={parent_value}"
            coll_dict.setdefault("links", []).append({"rel": "prev", "href": prev_url})

        return JSONResponse(content=coll_dict)
