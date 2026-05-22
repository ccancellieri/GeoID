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
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, status, Request, FastAPI
from fastapi.responses import Response
from pydantic import BaseModel
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from dynastore.extensions.configs._composed_query_params import (
    IncludeQuery,
    LinksQuery,
    MetaQuery,
    ResolvedQuery,
    StrictQuery,
    ViewQuery,
)

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.catalog_readiness import require_catalog_ready
from dynastore.extensions.tools.conflict_handler import conflict_to_409
from dynastore.extensions.tools.exception_handlers import handle_exception
from dynastore.tools.db import InvalidIdentifierError
import dynastore.modules.catalog.catalog_module as catalog_manager
from dynastore.modules import get_protocol
from dynastore.models.protocols import WebModuleProtocol, ConfigsProtocol
from dynastore.models.protocols.collections import CollectionsProtocol
from dynastore.modules.db_config.engine_config import EngineConfig
from dynastore.modules.db_config.plugin_config import list_registered_configs, require_config_class, resolve_config_class
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


class ItemsSchemaDeriveRequest(BaseModel):
    """Body for the items_schema derive endpoint.

    ``asset_id`` names a vector asset (in the target collection) whose stored
    ``gdalinfo`` metadata seeds the proposal; ``layer`` selects a layer for
    multi-layer sources (GeoPackage/FileGDB) — the first layer wins when omitted.
    """

    asset_id: str
    layer: Optional[str] = None


class ConfigsService(ExtensionProtocol):
    always_on = True
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
        self.router.add_api_route(
            "/engines",
            self.list_engines,
            methods=["GET"],
            summary=(
                "List registered platform engines + their driver-class "
                "compatibility (Cycle F.4c.0)"
            ),
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
        # ---- items_schema derivation (propose; apply is the existing PUT) ----
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items-schema/derive",
            self.derive_items_schema_proposal,
            methods=["POST"],
            summary="Derive a proposed items_schema for a collection from a vector asset's gdalinfo",
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
    # Cycle F.4b — Tenant-engines write 403 gate
    # =========================================================================

    @staticmethod
    def _reject_engine_write_at_tenant_scope(
        config_cls: type, plugin_id: str, scope: str,
    ) -> None:
        """Raise 403 if ``config_cls`` is an :class:`EngineConfig` subclass.

        Engines are sysadmin-only platform-tier resources (tenant configs
        cannot influence platform resource policy — decisions #15 / #18).
        The existing ``configs_access`` policy already gates the whole
        ``/configs/.*`` surface to SYSADMIN; this routing-layer check is
        defence-in-depth — even if the policy is misconfigured, engine
        writes at catalog / collection scope return a clean 403 with a
        message pointing operators to the platform-tier endpoint.
        """
        if isinstance(config_cls, type) and issubclass(config_cls, EngineConfig):
            raise problem_details.engine_write_forbidden_at_tenant_scope(
                plugin_id, scope=scope,
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
        self._gate_engine_writes_in_patch_body(body.root, scope="catalog")
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
        self._gate_engine_writes_in_patch_body(body.root, scope="collection")
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

    @staticmethod
    def _strip_response_envelopes(body: Dict[str, Any]) -> Dict[str, Any]:
        """Drop ``_meta`` / ``_links`` from a per-plugin PUT body (#946).

        Composed GETs may emit ``_meta`` and routing refs may emit ``_links``
        depending on the query knobs.  ``PersistentModel`` has
        ``extra="forbid"`` (#918), so a payload pulled from GET and PUT
        verbatim would 422 on the envelope keys.  Defensive strip preserves
        the round-trip semantic operators reasonably expect.
        """
        if not isinstance(body, dict):
            return body
        return {k: v for k, v in body.items() if k not in ("_meta", "_links")}

    @staticmethod
    def _gate_engine_writes_in_patch_body(
        body: Dict[str, Any], *, scope: str,
    ) -> None:
        """Cycle F.4b — reject tenant-scope PATCH bodies that include any
        engine config.  Iterates the merge-patch keys, resolves each to a
        config class, and raises 403 on the first ``EngineConfig`` hit.
        Same gate as the per-plugin PUT/DELETE handlers; pre-validates
        before any write fires so the bulk operation stays atomic.
        """
        for plugin_id in body.keys():
            cls = resolve_config_class(plugin_id)
            if cls is None:
                continue  # patch_config will raise ValueError for unknown plugins
            if isinstance(cls, type) and issubclass(cls, EngineConfig):
                raise problem_details.engine_write_forbidden_at_tenant_scope(
                    plugin_id, scope=scope,
                )

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

    async def get_config_schema(
        self,
        plugin_id: str,
        meta: MetaQuery = "none",
    ) -> Dict[str, Any]:
        """Return JSON Schema, description, and scope for a single config class.

        ``plugin_id`` is the snake_case ``cls.class_key()`` of a registered
        :class:`PluginConfig` subclass (e.g. ``"collection_routing_config"``).

        ``meta`` selects the response projection so form-builders can fetch
        the raw shape without unwrapping:

        * ``meta="none"`` (default) — wrapper response
          ``{plugin_id, json_schema, description, scope}``.
        * ``meta="schema"`` — raw JSON Schema 2020-12 dict only (the same
          payload the wrapper carries under ``json_schema``).  This is the
          surface the per-leaf ``rel="schema"`` link points at.
        * ``meta="field"`` — terse field-name → description map only.
        """
        config_class = resolve_config_class(plugin_id)
        if config_class is None:
            raise problem_details.plugin_not_registered(plugin_id)

        if meta == "schema":
            return config_class.model_json_schema()
        if meta == "field":
            return ConfigApiService._extract_docs(config_class)

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
        resolved: ResolvedQuery = True,
        meta: MetaQuery = "field",
        include: IncludeQuery = "scope",
        strict: StrictQuery = True,
        links: LinksQuery = "minimal",
        view: ViewQuery = "effective",
    ) -> Any:
        base_url = str(request.url).split("?")[0]
        response = await self._config_api.compose_platform_config(
            base_url=base_url,
            resolved=resolved,
            meta=meta,
            include=include,
            strict=strict,
            links=links,
            view=view,
        )
        return JSONResponse(content=response.model_dump())

    async def get_catalog_config_composed(
        self,
        catalog_id: str,
        request: Request,
        resolved: ResolvedQuery = True,
        meta: MetaQuery = "field",
        include: IncludeQuery = "scope",
        strict: StrictQuery = True,
        links: LinksQuery = "minimal",
        view: ViewQuery = "effective",
    ) -> Any:
        base_url = str(request.url).split("?")[0]
        response = await self._config_api.compose_catalog_config(
            base_url=base_url,
            catalog_id=catalog_id,
            resolved=resolved,
            meta=meta,
            include=include,
            strict=strict,
            links=links,
            view=view,
        )
        return JSONResponse(content=response.model_dump())

    async def get_collection_config_composed(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        resolved: ResolvedQuery = True,
        meta: MetaQuery = "field",
        include: IncludeQuery = "scope",
        strict: StrictQuery = True,
        links: LinksQuery = "minimal",
        view: ViewQuery = "effective",
    ) -> Any:
        base_url = str(request.url).split("?")[0]
        response = await self._config_api.compose_collection_config(
            base_url=base_url,
            catalog_id=catalog_id,
            collection_id=collection_id,
            resolved=resolved,
            meta=meta,
            include=include,
            strict=strict,
            links=links,
            view=view,
        )
        return JSONResponse(content=response.model_dump())

    # --- Discovery Endpoint ---

    async def list_storage_drivers(self) -> Any:
        """List all registered drivers grouped by the Protocol they implement (M9 §7).

        Response shape: ``{ProtocolQualname: {class_name: DriverInfo}}``.
        Keys are the ``__qualname__`` of ``CollectionItemsStore``, ``AssetStore``,
        and ``CollectionStore``.  Use the inner class names as driver_ref
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
        # Inner key is the snake_case driver_ref (matches OperationDriverEntry.driver_ref).
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

    async def list_engines(self) -> Any:
        """List registered platform engines + their driver-class compatibility.

        Cycle F.4c.0 — additive REST surface that exposes the F.4a
        engine registry to operators.  Returns one entry per concrete
        :class:`EngineConfig` subclass with:

        * ``engine_class`` — the discriminator (``postgresql_engine``,
          ``elasticsearch_engine``, etc.).
        * ``class_key`` — the snake_case wire key (``postgresql_engine_config``).
        * ``compatible_driver_classes`` — every concrete driver config
          whose ``required_engine_class`` matches this engine's
          ``engine_class``.

        F.4c.1+ will widen this to include operator-chosen ref names
        when ref-keyed driver-config storage lands.  Today, the F.1
        single-instance-per-kind contract is in force, so each engine
        kind has exactly one default ref equal to its ``class_key``.
        """
        from dynastore.modules.db_config.engine_registry import (
            list_registered_engines,
        )
        from dynastore.models.protocols.typed_driver import (
            _PluginDriverConfig,
            _registered_pairs,
        )

        engines = list_registered_engines()

        # Build reverse index: engine_class → [driver class_key, ...]
        bound_pairs = _registered_pairs()
        compat: Dict[str, List[str]] = {}
        for cfg_cls in bound_pairs:
            if not issubclass(cfg_cls, _PluginDriverConfig):
                continue
            required = cfg_cls.required_engine_class
            if not required:
                continue
            compat.setdefault(required, []).append(cfg_cls.class_key())
        for k in compat:
            compat[k].sort()

        out: Dict[str, Dict[str, Any]] = {}
        for class_key, eng_cls in sorted(engines.items()):
            out[class_key] = {
                "class_key": class_key,
                "engine_class": eng_cls.engine_class,
                "compatible_driver_classes": compat.get(eng_cls.engine_class, []),
            }
        return {"engines": out}

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
        except InvalidIdentifierError as e:
            # Malformed id (e.g. unsubstituted ``{{...}}`` placeholder) is a
            # client error -> 400, not a backend failure -> 500 (#1201).
            raise problem_details.invalid_identifier(e)
        except Exception as e:
            logger.error(f"Error fetching collection config: {e}", exc_info=True)
            raise problem_details.unexpected_failure(e)


    async def update_collection_config(
        self,
        catalog_id: str,
        collection_id: str,
        plugin_id: str,
        body: Dict[str, Any],
        create_if_missing: bool = False,
    ):
        """
        Overrides the configuration for a specific plugin at the Collection level.
        This writes to the 'collection_configs' table.

        Default (``create_if_missing=False``): the collection MUST
        already exist; an absent collection yields a 404 instead of
        silently materialising a brand-new collection on PUT (issue
        #918 — a typo such as ``sentinal`` for ``sentinel`` used to
        slip through unchecked).  Pass ``?create_if_missing=true`` to
        keep the legacy upfront-configure flow where a thin collection
        registry row is JIT-created as part of the same request.
        """
        await require_catalog_ready(catalog_id)
        if not create_if_missing:
            collections = get_protocol(CollectionsProtocol)
            assert collections is not None, "CollectionsProtocol not registered"
            existing = await collections.get_collection(catalog_id, collection_id)
            if existing is None:
                raise problem_details.collection_not_found(
                    catalog_id, collection_id,
                    instance=(
                        f"/configs/catalogs/{catalog_id}/collections/"
                        f"{collection_id}/plugins/{plugin_id}"
                    ),
                )
        try:
            cls = require_config_class(plugin_id)
            self._reject_engine_write_at_tenant_scope(cls, plugin_id, scope="collection")
            config_model = cls.model_validate(self._strip_response_envelopes(body))

            validated_config = await self.configs.set_config(
                cls, config_model, catalog_id, collection_id
            )
            await self._invalidate_exposure()
            return validated_config

        except problem_details.ProblemException:
            raise
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
        self._reject_engine_write_at_tenant_scope(cls, plugin_id, scope="collection")
        await self.configs.delete_config(
            cls, catalog_id=catalog_id, collection_id=collection_id
        )
        await self._invalidate_exposure()
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    async def derive_items_schema_proposal(
        self,
        catalog_id: str,
        collection_id: str,
        body: ItemsSchemaDeriveRequest,
    ) -> Dict[str, Any]:
        """Derive a *proposed* items_schema for a collection from a vector
        asset's stored ``gdalinfo`` metadata.

        Pure read — nothing is persisted. The returned ``fields`` is a
        merge-patch body ready to apply via
        ``PUT /configs/catalogs/{cat}/collections/{col}/plugins/items_schema``.
        The proposal merges the derivation onto the collection's current schema:
        existing per-field admin tuning is preserved (only ``data_type`` /
        ``subtype`` come from the derivation), fields new in the asset are added,
        and fields the asset lacks are kept. ``summary`` reports what changed.

        Gated to sysadmin by the existing ``configs_access`` policy (the whole
        ``/configs/.*`` surface is deny-by-default for everyone else).
        """
        instance = (
            f"/configs/catalogs/{catalog_id}/collections/"
            f"{collection_id}/items-schema/derive"
        )
        await require_catalog_ready(catalog_id)

        collections = get_protocol(CollectionsProtocol)
        assert collections is not None, "CollectionsProtocol not registered"
        if await collections.get_collection(catalog_id, collection_id) is None:
            raise problem_details.collection_not_found(
                catalog_id, collection_id, instance=instance,
            )

        try:
            from dynastore.models.protocols import AssetsProtocol
            from dynastore.modules.storage.driver_config import ItemsSchema
            from dynastore.tasks.ingestion.schema_from_gdalinfo import (
                derive_schema_from_gdalinfo,
                merge_derived_fields,
            )

            assets = get_protocol(AssetsProtocol)
            assert assets is not None, "AssetsProtocol not registered"
            asset = await assets.get_asset(
                asset_id=body.asset_id,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            if asset is None:
                raise problem_details.asset_not_found(
                    catalog_id, collection_id, body.asset_id, instance=instance,
                )

            gdalinfo = (asset.metadata or {}).get("gdalinfo")
            if not gdalinfo:
                raise problem_details.cannot_derive_schema(
                    f"Asset '{body.asset_id}' carries no 'gdalinfo' metadata; "
                    f"run the gdal task on it before deriving a schema.",
                    instance=instance,
                )
            try:
                derived = derive_schema_from_gdalinfo(gdalinfo, layer_name=body.layer)
            except RuntimeError as e:
                raise problem_details.cannot_derive_schema(str(e), instance=instance)

            current = await self.configs.get_config(
                ItemsSchema, catalog_id, collection_id,
            )
            merged, summary = merge_derived_fields(current.fields or {}, derived)

            return {
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "asset_id": body.asset_id,
                "layer": body.layer,
                "fields": {
                    name: fd.model_dump(mode="json", exclude_none=True)
                    for name, fd in merged.items()
                },
                "summary": summary,
            }
        except problem_details.ProblemException:
            raise
        except InvalidIdentifierError as e:
            raise problem_details.invalid_identifier(e)
        except Exception as e:
            logger.error(
                f"Error deriving items_schema proposal: {e}", exc_info=True,
            )
            raise problem_details.unexpected_failure(e)

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
        except InvalidIdentifierError as e:
            # Malformed id (e.g. unsubstituted ``{{...}}`` placeholder) is a
            # client error -> 400, not a backend failure -> 500 (#1201).
            raise problem_details.invalid_identifier(e)
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
            self._reject_engine_write_at_tenant_scope(cls, plugin_id, scope="catalog")
            config_model = cls.model_validate(self._strip_response_envelopes(body))

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
        self._reject_engine_write_at_tenant_scope(cls, plugin_id, scope="catalog")
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
        except InvalidIdentifierError as e:
            # Malformed id (e.g. unsubstituted ``{{...}}`` placeholder) is a
            # client error -> 400, not a backend failure -> 500 (#1201).
            raise problem_details.invalid_identifier(e)
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
            config_model = cls.model_validate(self._strip_response_envelopes(body))

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

