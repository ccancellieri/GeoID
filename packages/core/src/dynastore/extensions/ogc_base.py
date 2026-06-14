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
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, cast

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
from dynastore.models.protocols import (
    CatalogsProtocol,
    ConfigsProtocol,
    StorageProtocol,
)
from dynastore.models.shared_models import Link
from dynastore.tools.discovery import get_protocol

if TYPE_CHECKING:
    from dynastore.modules.catalog.catalog_config import CollectionKind
    from dynastore.models.plugin_config import PluginConfig

logger = logging.getLogger(__name__)

# Bound to ``PluginConfig`` so ``_get_plugin_config`` narrows its return type
# to the requested config class and the ``config_cls()`` fallback is known to
# be constructible.  Imported under TYPE_CHECKING only — the mixin stays free
# of a runtime dependency on ``modules.db_config``.
_T = TypeVar("_T", bound="PluginConfig")


def ogc_asset_href(
    item: dict, *, error_detail: str = "No asset href on item."
) -> str:
    """Return the first usable asset href from a STAC-style *item* dict.

    Prefers the conventional ``data``/``coverage`` asset keys, then falls
    back to the first asset that carries an ``href``.  Raises ``404`` with
    *error_detail* when no asset href can be resolved.  Shared by the
    Coverages and EDR services, which pass protocol-specific error messages.
    """
    assets = item.get("assets") or {}
    for key in ("data", "coverage"):
        if key in assets and assets[key].get("href"):
            return assets[key]["href"]
    for a in assets.values():
        if a.get("href"):
            return a["href"]
    raise HTTPException(status_code=404, detail=error_detail)


class OGCServiceMixin:
    """Shared helpers for OGC-protocol extensions.

    Subclasses set the following class attributes:

    * ``conformance_uris: List[str]`` — OGC conformance class URIs
    * ``prefix: str`` — router path prefix (e.g. ``"/features"``)
    * ``protocol_title: str`` — human-readable protocol name
    * ``protocol_description: str`` — one-line description
    """

    # --- Class attributes to be set by subclasses ---
    conformance_uris: ClassVar[List[str]] = []
    prefix: str = ""
    protocol_title: str = ""
    protocol_description: str = ""

    # --- Cached protocol references (per-instance) ---
    _ogc_catalogs_protocol: Optional[CatalogsProtocol] = None
    _ogc_configs_protocol: Optional[ConfigsProtocol] = None
    _ogc_storage_protocol: Optional[StorageProtocol] = None

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------

    def register_policies(self) -> None:
        """Override in subclass to register IAM policies.  Default: no-op."""

    @staticmethod
    def register_ogc_preset(
        *,
        name: str,
        description: str,
        keywords: Tuple[str, ...],
        policies_factory: Callable[[], List[Any]],
        role_bindings_factory: Callable[[], List[Any]],
    ) -> None:
        """Register a ``PolicyContributorPreset`` from two pure-data factories.

        Shared registration plumbing for all OGC-extension presets.  Each
        extension calls this from its ``presets/__init__.py`` with its own
        ``name``, ``description``, ``keywords``, and the two callables that
        return its ``Policy`` / ``Role`` declarations.

        The factories are invoked at ``apply`` / ``revoke`` / ``dry_run``
        time (not at registration time), matching the existing
        ``contributor_factory`` semantics of ``PolicyContributorPreset``.

        Both callable shapes are supported:

        * **Module-function style** — pass the imported function directly,
          e.g. ``policies_factory=stac_policies``.
        * **Inline style** — pass a lambda or nested function that returns
          the list, e.g.
          ``policies_factory=lambda: [Policy(id="foo_public_access", ...)]``.

        Behavioral equivalence guarantee: the registered preset is
        structurally identical to one constructed by hand — same ``name``,
        ``description``, ``keywords``, and a ``contributor_factory`` that
        returns an object whose ``get_policies()`` / ``get_role_bindings()``
        results come directly from the supplied factories unchanged.
        """
        from dynastore.modules.storage.presets.policy_contributor_adapter import (
            PolicyContributorPreset,
        )
        from dynastore.modules.storage.presets.registry import register_preset

        _p_factory = policies_factory
        _rb_factory = role_bindings_factory

        class _Contributor:
            def get_policies(self) -> List[Any]:
                return _p_factory()

            def get_role_bindings(self) -> List[Any]:
                return _rb_factory()

        _Contributor.__name__ = f"{name}PresetContributor"
        _Contributor.__qualname__ = f"{name}PresetContributor"

        register_preset(PolicyContributorPreset(
            name=name,
            description=description,
            keywords=keywords,
            contributor_factory=_Contributor,
        ))

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

    async def _get_storage_service(self) -> Optional[StorageProtocol]:
        """Return the storage service protocol, or ``None`` if unavailable.

        Storage is optional (e.g. metadata-only deployments), so callers must
        handle ``None``. The reference is cached once resolved.
        """
        if self._ogc_storage_protocol is None:
            self._ogc_storage_protocol = get_protocol(StorageProtocol)
        return self._ogc_storage_protocol

    # ------------------------------------------------------------------
    # Shared config / item access helpers
    # ------------------------------------------------------------------

    async def _get_plugin_config(
        self,
        config_cls: Type[_T],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
    ) -> _T:
        """Fetch a plugin config via the platform configs service with waterfall.

        Falls back to a default-constructed ``config_cls()`` when the configs
        service is unavailable, keeping handlers resilient in test / stub
        contexts.  Shared by the Coverages, EDR, and DGGS services.
        """
        try:
            configs_svc = await self._get_configs_service()
            return await configs_svc.get_config(config_cls, catalog_id, collection_id)
        except Exception:  # pragma: no cover - defensive fallback
            return config_cls()

    async def _get_first_item(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> Optional[dict]:
        """Return the first item in a collection as a plain dict, or None."""
        from dynastore.models.query_builder import QueryRequest

        catalogs = await self._get_catalogs_service()
        try:
            features = await catalogs.search_items(
                catalog_id, collection_id, QueryRequest(limit=1)
            )
        except Exception:
            return None
        if not features:
            return None
        first = features[0]
        # Feature is a pydantic model — coerce to the same dict shape the
        # domainset/rangetype helpers expect.
        if hasattr(first, "model_dump"):
            return first.model_dump(by_alias=True, exclude_none=True)
        return dict(first)

    # ------------------------------------------------------------------
    # Collection-kind classification (Phase-1.6 ``CollectionInfo`` SSOT)
    # ------------------------------------------------------------------

    async def _collection_kind(
        self, catalog_id: str, collection_id: str
    ) -> "CollectionKind":
        """Resolve a collection's semantic kind from its ``CollectionInfo``.

        ``CollectionInfo.kind`` is the single source of truth for collection
        kind since Phase 1.6 hoisted ``collection_type`` off the per-driver
        config — it was removed from ``ItemsPostgresqlDriverConfig`` entirely.
        Reading the kind off a driver config now silently yields the default
        ``VECTOR`` (the latent misclassification this consolidation removes);
        the kind is a property of the DATA, not of one storage backend.

        A missing config, unavailable configs service, or read error all fall
        back to the model default ``VECTOR`` (via :meth:`_get_plugin_config`),
        matching the fail-open contract WFS relied on (no config → vector).
        """
        from dynastore.modules.catalog.catalog_config import (
            CollectionInfo,
            CollectionKind,
        )

        info = await self._get_plugin_config(
            CollectionInfo, catalog_id, collection_id
        )
        return info.kind if info is not None else CollectionKind.VECTOR

    async def _filter_collections_by_kind(
        self,
        catalog_id: str,
        collections: Iterable[Any],
        kind: "CollectionKind",
    ) -> List[Any]:
        """Return the subset of ``collections`` whose kind equals ``kind``.

        Each collection's kind is resolved via :meth:`_collection_kind`.
        Awaits are sequential (never ``gather``): the underlying config reads
        may share the request's asyncpg connection, and concurrent statements
        on a single connection deadlock asyncpg's one-stream protocol.
        Collections without a resolvable ``id`` are skipped.
        """
        matched: List[Any] = []
        for coll in collections:
            coll_id = (
                coll.id if hasattr(coll, "id")
                else coll.get("id") if isinstance(coll, dict)
                else None
            )
            if not coll_id:
                continue
            if await self._collection_kind(catalog_id, coll_id) == kind:
                matched.append(coll)
        return matched

    # ------------------------------------------------------------------
    # Collection-visibility guard
    # ------------------------------------------------------------------

    async def _require_collection_visible(
        self, catalog_id: str, collection_id: str
    ) -> None:
        """Raise 404 when the caller has no visibility grant for this collection.

        Data routes (coverages/EDR/DGGS/tiles) resolve items or tiles directly
        and bypass CatalogService.get_collection, so they must enforce the same
        direct-get visibility contract (#2050/#2069): a collection the caller
        cannot see is indistinguishable from a missing one.
        resolve_collection_listing_ids returns None when IAM is inactive —
        unfiltered, preserving prior behaviour.
        """
        from dynastore.models.protocols.visibility import resolve_collection_listing_ids

        visible_ids = await resolve_collection_listing_ids(catalog_id)
        if visible_ids is not None and collection_id not in visible_ids:
            raise HTTPException(status_code=404, detail="Collection not found.")

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

    async def _collect_queryable_fields(
        self,
        catalog_id: str,
        collection_id: str,
        conn: Any,
    ) -> "Tuple[list, Any]":
        """Collect driver-introspected field metadata for a queryables response.

        Returns a 2-tuple ``(columns, driver_fields)`` where:

        * ``columns`` is a list of column name strings from
          ``driver.introspect_schema()``.
        * ``driver_fields`` is the result of ``driver.get_entity_fields()``
          when the driver exposes that method, otherwise ``None``.

        Both values degrade gracefully to ``([], None)`` when the driver is
        unavailable, lacks ``Capability.INTROSPECTION``, or raises during
        introspection.  Failures are logged at DEBUG level so they are
        visible in traces without polluting production logs.

        Imports are kept local to avoid import-time coupling on the storage
        router, which is an optional runtime dependency.
        """
        from dynastore.models.protocols.storage_driver import Capability
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation

        columns: list = []
        driver_fields = None
        try:
            driver = await get_driver(Operation.READ, catalog_id, collection_id)
            if (
                driver is not None
                and hasattr(driver, "capabilities")
                and Capability.INTROSPECTION in driver.capabilities
            ):
                schema_info = await driver.introspect_schema(
                    catalog_id, collection_id, db_resource=conn
                )
                columns = [entry.name for entry in schema_info] if schema_info else []
                if hasattr(driver, "get_entity_fields"):
                    try:
                        driver_fields = await driver.get_entity_fields(
                            catalog_id, collection_id, entity_level="item"
                        )
                    except Exception as e:
                        logger.debug(
                            "queryables field introspection failed for %s/%s: %s",
                            catalog_id,
                            collection_id,
                            e,
                            exc_info=True,
                        )
                        driver_fields = None
        except Exception as e:
            logger.debug(
                "queryables field introspection failed for %s/%s: %s",
                catalog_id,
                collection_id,
                e,
                exc_info=True,
            )
            columns = []
            driver_fields = None
        return columns, driver_fields

    @staticmethod
    def _principal_caller_id(request: Request) -> Optional[str]:
        """Derive a ``caller_id`` string for write-attribution from the
        authenticated request principal.

        Format mirrors the existing attribution wiring: ``"{provider}:{subject_id}"``
        when a ``Principal`` is on ``request.state``, otherwise ``None`` (the
        downstream enqueue falls back to ``"system:tile_cache_invalidation"``).
        """
        from dynastore.models.auth import Principal

        principal: Optional[Principal] = getattr(request.state, "principal", None)
        if principal is None or not principal.subject_id:
            return None
        if principal.provider:
            return f"{principal.provider}:{principal.subject_id}"
        return principal.subject_id

    async def _delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        db_resource,
        caller_id: Optional[str] = None,
    ) -> Response:
        """Shared item deletion: delete + 404 check + 204 response.

        The caller is responsible for transaction management (e.g.
        ``managed_transaction``) — this mixin stays decoupled from
        ``modules.db_config``.

        ``caller_id`` is forwarded so the post-commit tile-cache
        invalidation task is attributed to the originating principal —
        matches the create/update attribution shipped in #1404/#1405.
        """
        catalogs_svc = await self._get_catalogs_service()
        from dynastore.models.driver_context import DriverContext
        rows_affected = await catalogs_svc.delete_item(
            catalog_id, collection_id, item_id,
            ctx=DriverContext(db_resource=db_resource),
            caller_id=caller_id,
        )
        if rows_affected == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Item '{item_id}' not found.",
            )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # ------------------------------------------------------------------
    # Overridable hooks — catalog + collection CRUD seams
    # ------------------------------------------------------------------
    # These methods express the points where STAC diverges from the
    # OGC Features behaviour.  Each has a Features-style default that
    # is a no-op (or the standard path), and STACService overrides
    # only the ones that differ.
    #
    # Hook inventory:
    #   _validate_catalog_create()       — pre-create driver check (STAC only)
    #   _require_catalog_write_ready()   — readiness guard on write ops (STAC only)
    #   _make_catalog_create_kwargs()    — extra kwargs for create_catalog (STAC: none)
    #   _make_collection_create_kwargs() — extra kwargs, e.g. stac_context (STAC: True)
    #   _localize_resource()             — localize a returned model (STAC: stac_localize)
    #   _pre_update_collection_validate()— merged validation on PATCH (STAC only)

    def _validate_catalog_create(self) -> None:
        """Assert that the deployment can persist the catalog payload.

        Called before ``create_catalog`` writes to the database.  Default:
        no-op.  STACService overrides this to call
        ``_assert_stac_capable_collection_stack()`` which fails with HTTP 422
        when no registered driver exposes the STAC metadata domain.
        """

    async def _require_catalog_write_ready(
        self,
        catalog_id: str,
        catalogs_svc: Optional[CatalogsProtocol] = None,
    ) -> None:
        """Guard write operations against catalogs that are not yet provisioned.

        Called before replace/update/delete catalog, and before all
        collection write operations.  Default: no-op (Features never checks
        readiness on these paths).  STACService overrides this to call
        ``_require_catalog_ready``.
        """

    def _make_catalog_create_kwargs(self) -> Dict[str, Any]:
        """Return extra keyword arguments injected into ``create_catalog``.

        Default: empty dict.  Override when the service needs to pass
        additional flags (e.g. ``stac_context=True`` on the collection tier).
        """
        return {}

    def _make_collection_create_kwargs(self) -> Dict[str, Any]:
        """Return extra keyword arguments injected into ``create_collection``.

        Default: empty dict.  STACService overrides to return
        ``{"stac_context": True}``.
        """
        return {}

    def _localize_resource(self, model: Any, language: str) -> Tuple[Dict[str, Any], Any]:
        """Localize a model returned by the catalog service.

        Returns a ``(data_dict, available_langs)`` 2-tuple matching the
        contract of both ``model.localize(lang)`` and ``stac_localize(model,
        lang)``.  Default: delegates to ``model.localize(language)``.
        STACService overrides to use ``stac_localize``.
        """
        return model.localize(language)  # type: ignore[no-any-return]

    async def _pre_update_collection_validate(
        self,
        catalog_id: str,
        collection_id: str,
        input_data: Dict[str, Any],
        request: Optional[Request] = None,
    ) -> None:
        """Validate a collection PATCH against the merged (existing + patch) state.

        Called at the start of the shared ``_ogc_update_collection`` body,
        before the catalog service write.  Default: no-op.  STACService
        overrides to fetch the current collection via ``stac_generator`` and
        validate the merged dict with ``validate_stac_collection``.

        The ``request`` argument is required for the STAC override (it
        forwards to ``stac_generator.create_collection``); Features passes
        ``None`` because its handler signature omits ``request``.
        """

    # ------------------------------------------------------------------
    # Shared catalog CRUD bodies (M-2)
    # ------------------------------------------------------------------

    async def _ogc_create_catalog(
        self,
        catalog_data: Dict[str, Any],
        input_dump: Dict[str, Any],
        language: str,
        db_resource: Any,
    ) -> Response:
        """Shared create-catalog body used by Features and STAC.

        *catalog_data* is the payload passed to ``CatalogsProtocol.create_catalog``.
        *input_dump* is the full ``model_dump(exclude_unset=True)`` result used
        solely to detect the language via ``detect_use_lang``.  *db_resource* is
        the database connection (may be ``None`` when the service omits the
        transactional context — STAC catalog creates do not pass a connection).
        """
        from dynastore.extensions.tools.localization_utils import detect_use_lang

        self._validate_catalog_create()
        use_lang = detect_use_lang(input_dump, language)
        catalogs_svc = await self._get_catalogs_service()

        ctx: Optional[DriverContext] = (
            DriverContext(db_resource=db_resource) if db_resource is not None else None
        )
        create_kwargs: Dict[str, Any] = {}
        if ctx is not None:
            create_kwargs["ctx"] = ctx
        create_kwargs.update(self._make_catalog_create_kwargs())

        created = await catalogs_svc.create_catalog(
            catalog_data=catalog_data, lang=use_lang, **create_kwargs
        )
        localized_data, _ = self._localize_resource(created, language)
        return JSONResponse(content=localized_data, status_code=status.HTTP_201_CREATED)

    async def _ogc_replace_catalog(
        self,
        catalog_id: str,
        catalog_dict: Dict[str, Any],
        language: str,
        db_resource: Any,
    ) -> Response:
        """Shared replace-catalog (PUT) body used by Features and STAC.

        *catalog_dict* must already be the result of
        ``normalize_i18n_for_replace``; this method performs no additional
        normalization.  *db_resource* follows the same convention as
        ``_ogc_create_catalog``.
        """
        catalogs_svc = await self._get_catalogs_service()
        await self._require_catalog_write_ready(catalog_id, catalogs_svc=catalogs_svc)

        ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None
        update_kwargs: Dict[str, Any] = {}
        if ctx is not None:
            update_kwargs["ctx"] = ctx

        updated = await catalogs_svc.update_catalog(
            catalog_id, catalog_dict, lang="*", **update_kwargs
        )
        if not updated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Catalog not found",
            )
        localized_data, _ = self._localize_resource(updated, language)
        return JSONResponse(content=localized_data)

    async def _ogc_update_catalog(
        self,
        catalog_id: str,
        catalog_dict: Dict[str, Any],
        language: str,
        db_resource: Any,
    ) -> Response:
        """Shared update-catalog (PATCH) body used by Features and STAC.

        *catalog_dict* must already be the result of
        ``model_dump(exclude_unset=True)``; this method performs no additional
        normalization.
        """
        from dynastore.extensions.tools.localization_utils import detect_use_lang

        use_lang = detect_use_lang(catalog_dict, language)
        catalogs_svc = await self._get_catalogs_service()
        await self._require_catalog_write_ready(catalog_id, catalogs_svc=catalogs_svc)

        ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None
        update_kwargs: Dict[str, Any] = {}
        if ctx is not None:
            update_kwargs["ctx"] = ctx

        updated = await catalogs_svc.update_catalog(
            catalog_id, catalog_dict, lang=use_lang, **update_kwargs
        )
        if not updated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Catalog not found",
            )
        localized_data, _ = self._localize_resource(updated, language)
        return JSONResponse(content=localized_data)

    async def _ogc_delete_catalog(
        self,
        catalog_id: str,
        force: bool,
        db_resource: Any,
    ) -> Response:
        """Shared delete-catalog body used by Features and STAC."""
        catalogs_svc = await self._get_catalogs_service()
        ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None
        delete_kwargs: Dict[str, Any] = {}
        if ctx is not None:
            delete_kwargs["ctx"] = ctx

        if not await catalogs_svc.delete_catalog(
            catalog_id, force=force, **delete_kwargs
        ):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Catalog '{catalog_id}' not found.",
            )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # ------------------------------------------------------------------
    # Shared collection CRUD bodies (M-3)
    # ------------------------------------------------------------------

    async def _ogc_create_collection(
        self,
        catalog_id: str,
        collection_dict: Dict[str, Any],
        language: str,
        db_resource: Any,
    ) -> Response:
        """Shared create-collection body used by Features and STAC.

        *collection_dict* must already be the result of
        ``model_dump(exclude_unset=True)``.  The caller is responsible for
        any pre-validation (e.g. STAC schema validation) before calling
        this method.  The localization hook and the collection-create extra
        kwargs are determined by the service-level overrides.
        """
        from dynastore.extensions.tools.localization_utils import detect_use_lang

        use_lang = detect_use_lang(collection_dict, language)
        catalogs_svc = await self._get_catalogs_service()
        await self._require_catalog_write_ready(catalog_id, catalogs_svc=catalogs_svc)

        ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None
        create_kwargs: Dict[str, Any] = {}
        if ctx is not None:
            create_kwargs["ctx"] = ctx
        create_kwargs.update(self._make_collection_create_kwargs())

        created = await catalogs_svc.create_collection(
            catalog_id, collection_dict, lang=use_lang, **create_kwargs
        )
        localized_data, _ = self._localize_resource(created, language)
        return JSONResponse(content=localized_data, status_code=status.HTTP_201_CREATED)

    async def _ogc_replace_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates_dict: Dict[str, Any],
        language: str,
    ) -> Response:
        """Shared replace-collection (PUT) body used by Features and STAC.

        *updates_dict* must already be the result of
        ``normalize_i18n_for_replace``; this method performs no additional
        normalization.  No ``db_resource`` / transactional context is passed
        on this path (neither Features nor STAC injects one for replace).
        """
        catalogs_svc = await self._get_catalogs_service()
        await self._require_catalog_write_ready(catalog_id, catalogs_svc=catalogs_svc)

        updated = await catalogs_svc.update_collection(
            catalog_id, collection_id, updates_dict, lang="*"
        )
        if not updated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        localized_data, _ = self._localize_resource(updated, language)
        return JSONResponse(content=localized_data)

    async def _ogc_update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates_dict: Dict[str, Any],
        language: str,
        request: Optional[Request] = None,
    ) -> Response:
        """Shared update-collection (PATCH) body used by Features and STAC.

        *updates_dict* must already be the result of
        ``model_dump(exclude_unset=True)``.  ``request`` is forwarded to
        ``_pre_update_collection_validate`` for services (STAC) that need
        to fetch the current state before merging and validating.
        """
        from dynastore.extensions.tools.localization_utils import detect_use_lang

        await self._pre_update_collection_validate(
            catalog_id, collection_id, updates_dict, request
        )
        use_lang = detect_use_lang(updates_dict, language)
        catalogs_svc = await self._get_catalogs_service()
        await self._require_catalog_write_ready(catalog_id, catalogs_svc=catalogs_svc)

        updated = await catalogs_svc.update_collection(
            catalog_id, collection_id, updates_dict, lang=use_lang
        )
        if not updated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        localized_data, _ = self._localize_resource(updated, language)
        return JSONResponse(content=localized_data)

    async def _ogc_delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool,
        db_resource: Any,
    ) -> Response:
        """Shared delete-collection body used by Features and STAC."""
        catalogs_svc = await self._get_catalogs_service()
        ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None
        delete_kwargs: Dict[str, Any] = {}
        if ctx is not None:
            delete_kwargs["ctx"] = ctx

        if not await catalogs_svc.delete_collection(
            catalog_id, collection_id, force, **delete_kwargs
        ):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Collection not found.",
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
            # on_batch_conflict=refuse_batch: duplicate detected → abort batch → 409.
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=str(exc)
            ) from exc
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
            ) from exc

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
