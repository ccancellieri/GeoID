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

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import APIRouter, Query, status, Request, FastAPI
from fastapi.responses import Response
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.catalog_readiness import require_catalog_ready
from dynastore.extensions.tools.conflict_handler import conflict_to_409
from dynastore.extensions.tools.exception_handlers import handle_exception
import dynastore.modules.catalog.catalog_module as catalog_manager
from dynastore.modules import get_protocol
from dynastore.models.protocols import WebModuleProtocol, ConfigsProtocol
from dynastore.modules.db_config.platform_config_service import (
    enforce_config_immutability,
    require_config_class,
    resolve_config_class,
    list_registered_configs,
)
from dynastore.modules.db_config.exceptions import (
    ImmutableConfigError,
    PluginNotRegisteredError,
    ConfigValidationError,
    is_conflict_error,
)

from .dto import (
    ConfigEntry,
    ConfigListResponse,
    PluginSchemaInfo,
)
from .config_api_dto import PatchConfigBody
from .config_api_service import ConfigApiService
from .policies import register_configs_policies
from . import problem_details

logger = logging.getLogger(__name__)


class ConfigsService(ExtensionProtocol):
    priority: int = 100
    """
    Unified Configuration Extension.
    Manages the configuration endpoints for all modules/extensions across hierarchies.
    """

    def __init__(self, app: FastAPI):
        super().__init__()
        self.app = app
        # ONE tag for the whole extension — every route under /configs
        # shares ``Configuration API``.  No sub-tags, no per-route overrides.
        self.router = APIRouter(prefix="/configs", tags=["Configuration API"])
        self._setup_routes()

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def configure_app(self, app: FastAPI):
        """Web pages are discovered by WebModule via the WebPageContributor
        capability protocol (see get_web_pages above)."""
        return None

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        register_configs_policies()
        problem_details.register(app)
        logger.info("ConfigsService: Policies + RFC 9457 handler registered.")
        yield

    def _setup_routes(self):
        # ---- Discovery: registry (plugin classes) + driver instances ----
        # ``/registry`` is the catalog of plugin classes (each entry carries
        # JSON Schema + description + scope).  ``/storage/drivers`` is a
        # different concept — runtime driver INSTANCES grouped by Protocol
        # qualname (capabilities, availability) for the operator's driver
        # picker.  The legacy ``/schemas``, ``/plugins`` (discovery list),
        # ``/graph`` and ``/search`` endpoints have been retired.
        self.router.add_api_route(
            "/registry",
            self.get_config_schemas,
            methods=["GET"],
            summary="Plugin registry — list all registered config plugin classes",
        )
        self.router.add_api_route(
            "/registry/{plugin_id}",
            self.get_config_schema,
            methods=["GET"],
            summary="Plugin registry entry — JSON Schema + description for one plugin class",
        )
        self.router.add_api_route(
            "/storage/drivers",
            self.list_storage_drivers,
            methods=["GET"],
            summary="List all registered storage drivers grouped by Protocol qualname",
        )
        # ---- Composed (waterfall-resolved) tree views ----
        self.router.add_api_route(
            "/",
            self.get_platform_config_composed,
            methods=["GET"],
            summary="Platform config — all effective platform configs composed",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.get_catalog_config_composed,
            methods=["GET"],
            summary="Catalog config — all effective catalog configs composed",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.get_collection_config_composed,
            methods=["GET"],
            summary="Collection config — all effective collection configs composed",
        )
        # ---- Multi-plugin partial write (RFC 7396 merge-patch) ----
        # Body: ``{plugin_id: payload | null}``.  ``null`` deletes the override.
        # Atomic at the scope level.  Replaces the legacy ``/bulk`` endpoint.
        self.router.add_api_route(
            "/",
            self._patch_platform_config,
            methods=["PATCH"],
            summary="Partially update platform-level configs (RFC 7396 merge-patch); null value deletes the override",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self._patch_catalog_config,
            methods=["PATCH"],
            summary="Partially update catalog-level configs (RFC 7396 merge-patch); null value deletes the override",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self._patch_collection_config,
            methods=["PATCH"],
            summary="Partially update collection-level configs (RFC 7396 merge-patch); null value deletes the override",
        )
        # ---- Per-plugin CRUD (platform tier) ----
        self.router.add_api_route(
            "/plugins/{plugin_id}",
            self.get_platform_config,
            methods=["GET"],
            summary="Get platform-level plugin configuration",
        )
        self.router.add_api_route(
            "/plugins/{plugin_id}",
            self.update_platform_config,
            methods=["PUT"],
            summary="Set platform-level plugin configuration",
        )
        self.router.add_api_route(
            "/plugins/{plugin_id}",
            self.delete_platform_config,
            methods=["DELETE"],
            summary="Delete platform-level plugin configuration",
            status_code=status.HTTP_204_NO_CONTENT,
        )
        # ---- Per-plugin CRUD (catalog tier) ----
        self.router.add_api_route(
            "/catalogs/{catalog_id}/plugins/{plugin_id}",
            self.get_catalog_config,
            methods=["GET"],
            summary="Get effective plugin configuration for a catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/plugins/{plugin_id}",
            self.update_catalog_config,
            methods=["PUT"],
            summary="Set or update a catalog-level plugin configuration",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/plugins/{plugin_id}",
            self.delete_catalog_config,
            methods=["DELETE"],
            summary="Delete a catalog-level plugin configuration",
            status_code=status.HTTP_204_NO_CONTENT,
        )
        # ---- Per-plugin CRUD (collection tier) ----
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/plugins/{plugin_id}",
            self.get_collection_config,
            methods=["GET"],
            summary="Get effective plugin configuration for a collection",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/plugins/{plugin_id}",
            self.update_collection_config,
            methods=["PUT"],
            summary="Set or update a collection-level plugin configuration",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/plugins/{plugin_id}",
            self.delete_collection_config,
            methods=["DELETE"],
            summary="Delete a collection-level plugin configuration",
            status_code=status.HTTP_204_NO_CONTENT,
        )

    @property
    def configs(self) -> ConfigsProtocol:
        return self.get_protocol(ConfigsProtocol)  # type: ignore[return-value]

    @property
    def _config_api(self) -> ConfigApiService:
        from dynastore.models.protocols import AssetsProtocol, CatalogsProtocol

        return ConfigApiService(
            config_service=self.configs,
            catalogs_service=self.get_protocol(CatalogsProtocol),
            assets_service=self.get_protocol(AssetsProtocol),
        )

    # =========================================================================
    # Invalidate hook — clears the exposure matrix + OpenAPI schema cache
    # =========================================================================

    async def _invalidate_exposure(self) -> None:
        """Invalidate the exposure matrix and the cached OpenAPI schema.

        Must be called after any config write that may affect route visibility.
        Reloads the matrix snapshot so the sync OpenAPI filter sees fresh state.
        Safe to call even when no matrix is attached to app.state.
        """
        matrix = getattr(self.app.state, "exposure_matrix", None)
        if matrix is not None:
            matrix.invalidate()
            await matrix.get()
        self.app.openapi_schema = None

    # =========================================================================
    # PATCH handlers — partial write at platform / catalog scope
    # =========================================================================

    async def _patch_platform_config(self, body: PatchConfigBody) -> Dict[str, Any]:
        """Apply a partial config update at platform scope."""
        from pydantic import ValidationError
        try:
            result = await self._config_api.patch_config(catalog_id=None, body=body.root)
        except ValidationError as e:
            raise problem_details.validation_failed(e)
        except ValueError as e:
            raise problem_details.value_error(e)
        await self._invalidate_exposure()
        return result

    async def _patch_catalog_config(
        self, catalog_id: str, body: PatchConfigBody
    ) -> Dict[str, Any]:
        """Apply a partial config update at catalog scope."""
        from pydantic import ValidationError
        try:
            result = await self._config_api.patch_config(
                catalog_id=catalog_id, body=body.root
            )
        except ValidationError as e:
            raise problem_details.validation_failed(e)
        except ValueError as e:
            raise problem_details.value_error(e)
        await self._invalidate_exposure()
        return result

    async def _patch_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        body: PatchConfigBody,
    ) -> Dict[str, Any]:
        """Apply a partial config update at collection scope.

        Replaces the legacy ``PUT /configs/.../collections/{c}/bulk`` endpoint
        with the standard RFC 7396 merge-patch semantic on the scope root.
        """
        from pydantic import ValidationError
        try:
            result = await self._config_api.patch_config(
                catalog_id=catalog_id,
                collection_id=collection_id,
                body=body.root,
            )
        except ValidationError as e:
            raise problem_details.validation_failed(e)
        except ValueError as e:
            raise problem_details.value_error(e)
        await self._invalidate_exposure()
        return result

    async def get_config_schemas(self) -> Dict[str, Any]:
        """List all registered config schemas grouped by scope and protocol (M9).

        Response shape::

            {
                class_key: {
                    "json_schema": {...},
                    "description": "...",
                    "scope": "platform_waterfall | collection_intrinsic | deployment_env",
                }
            }

        ``scope`` is read from ``ConfigScopeMixin.config_scope`` when present,
        or defaults to ``"platform_waterfall"``.
        """
        from dynastore.modules.storage.schema_types import ConfigScopeMixin

        schemas: Dict[str, Any] = {}
        for class_key, config_class in list_registered_configs().items():
            scope = getattr(config_class, "config_scope", "platform_waterfall")
            schemas[class_key] = {
                "json_schema": config_class.model_json_schema(),
                "description": (config_class.__doc__ or "").strip().split("\n")[0],
                "scope": scope,
            }
        return schemas

    async def get_config_schema(self, plugin_id: str) -> Dict[str, Any]:
        """Return full JSON Schema, description, and example for a single config class (M9).

        ``plugin_id`` is the snake_case ``cls.class_key()`` of a registered
        :class:`PluginConfig` subclass (e.g. ``"collection_routing_config"``).

        Response shape::

            {
                "plugin_id": "collection_routing_config",
                "json_schema": {...},
                "description": "...",
                "scope": "platform_waterfall",
            }
        """
        from dynastore.modules.storage.schema_types import ConfigScopeMixin

        config_class = resolve_config_class(plugin_id)
        if config_class is None:
            raise problem_details.plugin_not_registered(plugin_id)

        scope = getattr(config_class, "config_scope", "platform_waterfall")
        return {
            "plugin_id": plugin_id,
            "json_schema": config_class.model_json_schema(),
            "description": (config_class.__doc__ or "").strip(),
            "scope": scope,
        }

    # =========================================================================
    # Config API — Composed views at Platform / Catalog / Collection
    # =========================================================================

    async def get_platform_config_composed(
        self,
        request: Request,
        depth: int = Query(0, ge=0, le=3, description="Child levels to expand (0 = configs only)."),
        catalogs_page: int = Query(1, ge=1),
        page_size: int = Query(15, ge=1, le=100),
        resolved: bool = Query(
            True,
            description=(
                "When true (default): all registered configs with waterfall-resolved values. "
                "When false: only configs explicitly stored at platform scope."
            ),
        ),
        meta: bool = Query(
            False,
            description=(
                "When true, include per-class tier-of-origin diagnostics under "
                "``meta.{ClassName}.source``.  Off by default to keep the payload slim."
            ),
        ),
        docs: str = Query(
            "field",
            pattern="^(none|field|schema)$",
            description=(
                "Documentation mode. ``field`` (default): each class in the "
                "response gets a lightweight ``{field_name: description}`` map "
                "at ``meta.{ClassName}.field_docs``. ``schema``: each class "
                "gets its full JSON Schema 2020-12 document at "
                "``meta.{ClassName}.json_schema`` (title, description, type, "
                "default, examples, constraints — everything a form-builder "
                "needs; heavier). ``none``: no field documentation. "
                "Independent of ``?meta=true``: combine to get both source "
                "diagnostics and docs."
            ),
        ),
        include: str = Query(
            "scope",
            pattern="^(scope|upstream)$",
            description=(
                "Body-rendering mode. ``scope`` (default): body shows configs "
                "owned by this scope (``_visibility`` matches OR an explicit "
                "row exists here). Upstream-tier configs are summarised in "
                "``inherited`` (class_key → source) — operators see what exists "
                "without the verbose payloads. ``upstream``: every visible "
                "class is rendered with its waterfall-resolved value (today's "
                "verbose mode). At platform scope this flag is a no-op since "
                "platform IS the top tier."
            ),
        ),
    ) -> Any:
        base_url = str(request.url).split("?")[0]
        response = await self._config_api.compose_platform_config(
            base_url=base_url,
            depth=depth,
            catalogs_page=catalogs_page,
            page_size=page_size,
            resolved=resolved,
            meta=meta,
            docs=docs,
            include=include,
        )
        return JSONResponse(content=response.model_dump())

    async def get_catalog_config_composed(
        self,
        catalog_id: str,
        request: Request,
        depth: int = Query(0, ge=0, le=3),
        collections_page: int = Query(1, ge=1),
        assets_page: int = Query(1, ge=1),
        page_size: int = Query(15, ge=1, le=100),
        resolved: bool = Query(
            True,
            description=(
                "When true (default): all registered configs with waterfall-resolved values. "
                "When false: only configs explicitly stored at this catalog scope."
            ),
        ),
        meta: bool = Query(
            False,
            description=(
                "When true, include per-class tier-of-origin diagnostics under "
                "``meta.{ClassName}.source``.  Off by default to keep the payload slim."
            ),
        ),
        docs: str = Query(
            "field",
            pattern="^(none|field|schema)$",
            description=(
                "Documentation mode. ``field`` (default) embeds a lightweight "
                "``{field_name: description}`` map at ``meta.{ClassName}.field_docs``. "
                "``schema`` embeds each class's full JSON Schema at "
                "``meta.{ClassName}.json_schema``. ``none`` suppresses both. "
                "See platform endpoint for full notes."
            ),
        ),
        include: str = Query(
            "scope",
            pattern="^(scope|upstream)$",
            description=(
                "Body-rendering mode. See platform endpoint for full notes. "
                "At catalog scope, ``scope`` filters out platform-tier "
                "configs that have no catalog override; they appear in "
                "``inherited`` instead."
            ),
        ),
    ) -> Any:
        base_url = str(request.url).split("?")[0]
        response = await self._config_api.compose_catalog_config(
            base_url=base_url,
            catalog_id=catalog_id,
            depth=depth,
            collections_page=collections_page,
            assets_page=assets_page,
            page_size=page_size,
            resolved=resolved,
            meta=meta,
            docs=docs,
            include=include,
        )
        return JSONResponse(content=response.model_dump())

    async def get_collection_config_composed(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        depth: int = Query(0, ge=0, le=3),
        assets_page: int = Query(1, ge=1),
        page_size: int = Query(15, ge=1, le=100),
        resolved: bool = Query(
            True,
            description=(
                "When true (default): all registered configs with waterfall-resolved values. "
                "When false: only configs explicitly stored at this collection scope."
            ),
        ),
        meta: bool = Query(
            False,
            description=(
                "When true, include per-class tier-of-origin diagnostics under "
                "``meta.{ClassName}.source``.  Off by default to keep the payload slim."
            ),
        ),
        docs: str = Query(
            "field",
            pattern="^(none|field|schema)$",
            description=(
                "Documentation mode. ``field`` (default) embeds a lightweight "
                "``{field_name: description}`` map at ``meta.{ClassName}.field_docs``. "
                "``schema`` embeds each class's full JSON Schema at "
                "``meta.{ClassName}.json_schema``. ``none`` suppresses both. "
                "See platform endpoint for full notes."
            ),
        ),
        include: str = Query(
            "scope",
            pattern="^(scope|upstream)$",
            description=(
                "Body-rendering mode. ``scope`` (default): body shows only "
                "configs owned by this collection — collection-visibility "
                "configs (routing, items-driver with sidecars) and any "
                "explicit collection-level overrides. Upstream-tier configs "
                "(platform/default) appear in ``inherited`` as class_key → "
                "source pairs. ``upstream``: every visible class is rendered "
                "with its waterfall-resolved value (today's verbose mode, "
                "useful for callers expecting the full payload)."
            ),
        ),
    ) -> Any:
        base_url = str(request.url).split("?")[0]
        response = await self._config_api.compose_collection_config(
            base_url=base_url,
            catalog_id=catalog_id,
            collection_id=collection_id,
            depth=depth,
            assets_page=assets_page,
            page_size=page_size,
            resolved=resolved,
            meta=meta,
            docs=docs,
            include=include,
        )
        return JSONResponse(content=response.model_dump())

    # --- Discovery Endpoint ---

    async def list_storage_drivers(self) -> Any:
        """List all registered drivers grouped by the Protocol they implement (M9 §7).

        Response shape: ``{ProtocolQualname: {class_name: DriverInfo}}``.
        Keys are the ``__qualname__`` of ``CollectionItemsStore``, ``AssetStore``,
        and ``CollectionStore``.  Use the inner class names as driver_id
        values in the routing config's operations list.
        """
        from dynastore.extensions.configs.dto import DriverInfo, DriverListResponse
        from dynastore.models.protocols.asset_driver import AssetStore
        from dynastore.models.protocols.entity_store import CollectionStore
        from dynastore.models.protocols.storage_driver import CollectionItemsStore
        from dynastore.modules.storage.routing_config import derive_supported_operations
        from dynastore.tools.discovery import get_all_protocols

        def _driver_description(driver) -> dict:
            desc = getattr(driver, "description", None)
            if desc is None:
                return {}
            if isinstance(desc, dict):
                return desc
            if hasattr(desc, "model_dump"):
                return {k: v for k, v in desc.model_dump().items() if v}
            return {}

        def _config_caps(class_key: str) -> list:
            try:
                dcls = resolve_config_class(class_key)
                if dcls is not None:
                    return sorted(getattr(dcls(), "capabilities", frozenset()))
            except Exception:
                pass
            return []

        def _routable_info(driver, config_class_key: str, available: bool) -> DriverInfo:
            caps = driver.capabilities
            return DriverInfo(
                description=_driver_description(driver),
                capabilities=sorted(caps),
                driver_capabilities=_config_caps(config_class_key),
                supported_operations=sorted(derive_supported_operations(caps)),
                supported_hints=sorted(driver.supported_hints),
                preferred_for=sorted(driver.preferred_for),
                available=available,
            )

        def _metadata_info(driver, available: bool) -> DriverInfo:
            return DriverInfo(
                description=_driver_description(driver),
                capabilities=sorted(driver.capabilities),
                available=available,
            )

        # Group by Protocol qualname (M9 — §7 protocol-based driver grouping).
        # Inner key is the snake_case driver_id (matches OperationDriverEntry.driver_id).
        from dynastore.tools.typed_store.base import _to_snake

        def _driver_id(d) -> str:
            return _to_snake(type(d).__name__)

        def _config_key(d) -> str:
            return _to_snake(type(d).__name__ + "Config")

        drivers: Dict[str, Dict[str, DriverInfo]] = {
            CollectionItemsStore.__qualname__: {
                _driver_id(d): _routable_info(d, _config_key(d), d.is_available())
                for d in get_all_protocols(CollectionItemsStore)
            },
            AssetStore.__qualname__: {
                _driver_id(d): _routable_info(d, _config_key(d), d.is_available())
                for d in get_all_protocols(AssetStore)
            },
            CollectionStore.__qualname__: {
                _driver_id(d): _metadata_info(d, await d.is_available())
                for d in get_all_protocols(CollectionStore)
            },
        }

        return DriverListResponse(drivers=drivers)

    # --- Specific Plugin Configuration (GET/PUT) ---

    # Collection Level


    async def get_collection_config(
        self, catalog_id: str, collection_id: str, plugin_id: str
    ):
        """
        Retrieves the effective configuration for a specific plugin (extension) on a collection.
        Resolves hierarchy: Collection > Catalog > Platform > Default.
        """
        try:
            cls = resolve_config_class(plugin_id)
            if cls is None:
                raise problem_details.plugin_not_registered(plugin_id)

            config = await self.configs.get_config(cls, catalog_id, collection_id)
            return config
        except problem_details.ProblemException:
            raise
        except Exception as e:
            logger.error(f"Error fetching collection config: {e}", exc_info=True)
            raise problem_details.unexpected_failure(e)


    async def update_collection_config(
        self, catalog_id: str, collection_id: str, plugin_id: str, body: Dict[str, Any]
    ):
        """
        Overrides the configuration for a specific plugin at the Collection level.
        This writes to the 'collection_configs' table.
        """
        await require_catalog_ready(catalog_id)
        try:
            cls = require_config_class(plugin_id)
            config_model = cls.model_validate(body)

            validated_config = await self.configs.set_config(
                cls, config_model, catalog_id, collection_id
            )
            await self._invalidate_exposure()
            return validated_config

        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Collection Config",
                resource_id=f"{catalog_id}:{collection_id}:{plugin_id}",
                operation="Collection configuration update",
            )


    async def delete_collection_config(
        self, catalog_id: str, collection_id: str, plugin_id: str
    ):
        """
        Deletes the configuration override for a plugin at the Collection level.
        The effective configuration will revert to Catalog, Platform, or Code defaults.
        """
        cls = require_config_class(plugin_id)
        await self.configs.delete_config(
            cls, catalog_id=catalog_id, collection_id=collection_id
        )
        await self._invalidate_exposure()
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # Catalog Level


    async def get_catalog_config(self, catalog_id: str, plugin_id: str):
        """
        Retrieves the configuration for a plugin at the Catalog level.
        Resolves hierarchy: Catalog > Platform > Default.
        """
        try:
            cls = resolve_config_class(plugin_id)
            if cls is None:
                raise problem_details.plugin_not_registered(plugin_id)

            config = await self.configs.get_config(
                cls, catalog_id, collection_id=None
            )
            return config
        except problem_details.ProblemException:
            raise
        except Exception as e:
            logger.error(f"Error fetching catalog config: {e}", exc_info=True)
            raise problem_details.unexpected_failure(e)


    async def update_catalog_config(
        self, catalog_id: str, plugin_id: str, body: Dict[str, Any]
    ):
        """
        Overrides the configuration for a plugin at the Catalog level.
        """
        await require_catalog_ready(catalog_id)
        try:
            cls = require_config_class(plugin_id)
            config_model = cls.model_validate(body)

            validated_config = await self.configs.set_config(
                cls, config_model, catalog_id, collection_id=None
            )
            await self._invalidate_exposure()
            return validated_config
        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Catalog Config",
                resource_id=f"{catalog_id}:{plugin_id}",
                operation="Catalog configuration update",
            )


    async def delete_catalog_config(self, catalog_id: str, plugin_id: str):
        """
        Deletes the configuration override for a plugin at the Catalog level.
        The effective configuration will revert to Platform or Code defaults.
        """
        cls = require_config_class(plugin_id)
        await self.configs.delete_config(cls, catalog_id=catalog_id)
        await self._invalidate_exposure()
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # Platform Level


    async def get_platform_config(self, plugin_id: str):
        """
        Retrieves the global platform default configuration for a plugin.
        Resolves hierarchy: Platform > Default.
        """
        try:
            cls = resolve_config_class(plugin_id)
            if cls is None:
                raise problem_details.plugin_not_registered(plugin_id)

            config = await self.configs.get_config(
                cls, catalog_id=None, collection_id=None
            )
            return config
        except problem_details.ProblemException:
            raise
        except Exception as e:
            logger.error(f"Error fetching platform config: {e}", exc_info=True)
            raise problem_details.unexpected_failure(e)


    async def update_platform_config(self, plugin_id: str, body: Dict[str, Any]):
        """
        Sets the global platform default configuration for a plugin.
        """
        cls = resolve_config_class(plugin_id)
        if cls is None:
            raise problem_details.plugin_not_registered(plugin_id)

        try:
            config_model = cls.model_validate(body)

            validated_config = await self.configs.set_config(
                cls, config_model, catalog_id=None, collection_id=None
            )
            await self._invalidate_exposure()
            return validated_config
        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Platform Config",
                resource_id=plugin_id,
                operation="Platform configuration update",
            )


    async def delete_platform_config(self, plugin_id: str):
        """
        Deletes the configuration override for a plugin at the Platform level.
        The effective configuration will revert to the Code defaults.
        """
        cls = require_config_class(plugin_id)
        await self.configs.delete_config(cls)
        await self._invalidate_exposure()
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # --- Bulk Apply ---

