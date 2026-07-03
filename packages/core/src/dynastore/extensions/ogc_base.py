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
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, FrozenSet, Iterable, List, Literal, Optional, Tuple, Type, TypeVar, cast

from fastapi import Depends, HTTPException, Request, Response, status

from dynastore.extensions.ogc_models_shared import (
    BulkCreationResponse,
    IngestionReport,
    SidecarRejection,
)
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.ogc_common_models import Conformance, LandingPage
from dynastore.extensions.tools.response_i18n import localize_model
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
    # Catalog/collection lookup-or-404 (thin wrappers over extensions.tools.resolvers)
    # ------------------------------------------------------------------

    async def _resolve_catalog_or_404(
        self,
        catalog_id: str,
        *,
        detail: Optional[str] = None,
        use_model: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Fetch a catalog model via this service's catalogs protocol or raise 404.

        See :func:`dynastore.extensions.tools.resolvers.resolve_catalog_or_404`.
        """
        from dynastore.extensions.tools.resolvers import resolve_catalog_or_404

        catalogs_svc = await self._get_catalogs_service()
        return await resolve_catalog_or_404(
            catalogs_svc, catalog_id, detail=detail, use_model=use_model, **kwargs
        )

    async def _resolve_collection_or_404(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        detail: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        """Fetch a collection model via this service's catalogs protocol or raise 404.

        See :func:`dynastore.extensions.tools.resolvers.resolve_collection_or_404`.
        """
        from dynastore.extensions.tools.resolvers import resolve_collection_or_404

        catalogs_svc = await self._get_catalogs_service()
        return await resolve_collection_or_404(
            catalogs_svc, catalog_id, collection_id, detail=detail, **kwargs
        )

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

    async def ogc_landing_page_handler(
        self,
        request: Request,
        language: str = Depends(get_language),
    ) -> JSONResponse:
        """Standard landing page with self, conformance, and service-doc links.

        Returns a ``JSONResponse`` with link titles collapsed to a single
        language string (or the full multi-language dict when
        ``language='*'``).  Default language is ``'en'``.

        Override in subclass if the protocol needs a custom landing page
        (e.g. STAC returns a root catalog, not a plain landing page).
        """
        root_url = get_root_url(request)
        landing_page = LandingPage(
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
        return JSONResponse(content=localize_model(landing_page, language))

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
        *,
        hints: Optional[FrozenSet[Any]] = None,
    ) -> Response:
        """Shared create-catalog body used by Features and STAC.

        *catalog_data* is the payload passed to ``CatalogsProtocol.create_catalog``.
        *input_dump* is the full ``model_dump(exclude_unset=True)`` result used
        solely to detect the language via ``detect_use_lang``.  *db_resource* is
        the database connection (may be ``None`` when the service omits the
        transactional context — STAC catalog creates do not pass a connection).
        *hints* carries the request's ``?hints=`` set; ``Hint.DEFER`` defers GCP
        storage provisioning so the catalog is created core-only.
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
        if hints:
            create_kwargs["hints"] = hints
        create_kwargs.update(self._make_catalog_create_kwargs())

        created = await catalogs_svc.create_catalog(
            catalog_data=catalog_data, lang=use_lang, **create_kwargs
        )
        localized_data, _ = self._localize_resource(created, language)

        # Catalog creation is always asynchronous: the catalog row is committed
        # but tenant-schema provisioning runs via a background task.  The service
        # signals this by returning provisioning_status='provisioning'; we map
        # that to 202 Accepted + Location so the client knows to poll.
        prov_status = getattr(created, "provisioning_status", None)
        if prov_status == "provisioning":
            external_id = getattr(created, "external_id", None) or localized_data.get("id", "")
            location = f"/catalog/catalogs/{external_id}"
            import json as _json
            return Response(
                status_code=status.HTTP_202_ACCEPTED,
                headers={"Location": location},
                media_type="application/json",
                content=_json.dumps(localized_data),
            )

        return JSONResponse(content=localized_data, status_code=status.HTTP_201_CREATED)

    async def _ogc_replace_catalog(
        self,
        catalog_id: str,
        catalog_dict: Dict[str, Any],
        language: str,
        db_resource: Any,
        *,
        request: Optional[Request] = None,
        body_id: Optional[str] = None,
        on_id_mismatch: Literal["ignore", "reject"] = "ignore",
    ) -> Response:
        """Shared replace-catalog (PUT) body used by Features and STAC.

        *catalog_dict* must already be the result of
        ``normalize_i18n_for_replace``; this method performs no additional
        normalization.  *db_resource* follows the same convention as
        ``_ogc_create_catalog``.

        When *body_id* differs from *catalog_id* (the path parameter) three
        cases apply:

        1. ``Prefer: handling=move`` present: perform a MOVE (rename the
           catalog to *body_id*); respond with 200 + ``Content-Location``,
           ``Link: rel=canonical``, and ``Preference-Applied: handling=move``.
        2. Move NOT requested, *on_id_mismatch* == ``"reject"`` (STAC): raise
           400 — STAC Transaction mandates that a body ``id`` not matching the
           path ``id`` is an error.
        3. Move NOT requested, *on_id_mismatch* == ``"ignore"`` (OGC Features
           Part 4 Req 11): drop the body id and replace using the path id.

        When *body_id* is absent or equals *catalog_id* the normal replace
        path runs regardless of the header or the mismatch policy.
        """
        catalogs_svc = await self._get_catalogs_service()
        await self._require_catalog_write_ready(catalog_id, catalogs_svc=catalogs_svc)

        # Mismatch branch: body id differs from path id.
        if body_id is not None and body_id != catalog_id:
            # MOVE gate: only rename when the client explicitly opts in.
            if request is not None and self._wants_move(request):
                internal_id, new_external_id, content_location = (
                    await self._ogc_perform_catalog_rename(catalog_id, body_id, request=request)
                )
                ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None
                update_kwargs: Dict[str, Any] = {}
                if ctx is not None:
                    update_kwargs["ctx"] = ctx
                catalog_dict = {**catalog_dict, "id": new_external_id}
                updated = await catalogs_svc.update_catalog(
                    internal_id, catalog_dict, lang="*", **update_kwargs
                )
                if not updated:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Catalog not found after rename.",
                    )
                localized_data, _ = self._localize_resource(updated, language)
                response = JSONResponse(content=localized_data)
                if content_location is not None:
                    response.headers["Content-Location"] = content_location
                    response.headers["Link"] = f'<{content_location}>; rel="canonical"'
                response.headers["Preference-Applied"] = "handling=move"
                return response

            # No MOVE requested: apply per-surface id-mismatch policy.
            if on_id_mismatch == "reject":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"Body id '{body_id}' does not match path id '{catalog_id}'."
                        " Send 'Prefer: handling=move' to rename."
                    ),
                )
            # on_id_mismatch == "ignore": drop body id, replace using path id.
            catalog_dict = {k: v for k, v in catalog_dict.items() if k != "id"}

        # Normal replace branch: body id == path id (or body id dropped/absent).
        ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None
        update_kwargs = {}
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
        *,
        body_id: Optional[str] = None,
        request: Optional[Request] = None,
        on_id_mismatch: Literal["ignore", "reject"] = "ignore",
    ) -> Response:
        """Shared update-catalog (PATCH) body used by Features and STAC.

        *catalog_dict* must already be the result of
        ``model_dump(exclude_unset=True)``; this method performs no additional
        normalization.

        When *body_id* differs from *catalog_id* (the path parameter) three
        cases apply:

        1. ``Prefer: handling=move`` present: MOVE (rename) then patch remaining
           fields; respond with 200 + ``Content-Location``,
           ``Link: rel=canonical``, and ``Preference-Applied: handling=move``.
        2. Move NOT requested, *on_id_mismatch* == ``"reject"`` (STAC): raise 400.
        3. Move NOT requested, *on_id_mismatch* == ``"ignore"`` (OGC Features
           Part 4): drop the body ``"id"`` field and patch normally.

        When *body_id* is absent or equals *catalog_id* the normal partial-update
        path runs regardless.
        """
        from dynastore.extensions.tools.localization_utils import detect_use_lang

        catalogs_svc = await self._get_catalogs_service()
        await self._require_catalog_write_ready(catalog_id, catalogs_svc=catalogs_svc)

        # Mismatch branch: PATCH body carries a different "id".
        if body_id is not None and body_id != catalog_id:
            if request is not None and self._wants_move(request):
                internal_id, new_external_id, content_location = (
                    await self._ogc_perform_catalog_rename(catalog_id, body_id, request=request)
                )
                # Strip "id" — rename already updated it.
                patch_fields = {k: v for k, v in catalog_dict.items() if k != "id"}
                use_lang = detect_use_lang(patch_fields, language)
                ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None
                update_kwargs: Dict[str, Any] = {}
                if ctx is not None:
                    update_kwargs["ctx"] = ctx
                updated = await catalogs_svc.update_catalog(
                    internal_id, patch_fields, lang=use_lang, **update_kwargs
                )
                if not updated:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Catalog not found after rename.",
                    )
                localized_data, _ = self._localize_resource(updated, language)
                response = JSONResponse(content=localized_data)
                if content_location is not None:
                    response.headers["Content-Location"] = content_location
                    response.headers["Link"] = f'<{content_location}>; rel="canonical"'
                response.headers["Preference-Applied"] = "handling=move"
                return response

            if on_id_mismatch == "reject":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"Body id '{body_id}' does not match path id '{catalog_id}'."
                        " Send 'Prefer: handling=move' to rename."
                    ),
                )
            # on_id_mismatch == "ignore": drop body id, patch with path id.
            catalog_dict = {k: v for k, v in catalog_dict.items() if k != "id"}

        # Normal update branch: body id == path id (or body id dropped/absent).
        use_lang = detect_use_lang(catalog_dict, language)
        ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None
        update_kwargs = {}
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
        request: Optional[Request] = None,
    ) -> Response:
        """Shared delete-catalog body used by Features and STAC.

        Soft delete (force=False): synchronous tombstone; returns 204.
        Hard delete (force=True): ``CatalogsProtocol.delete_catalog`` tombstones
        the row and enqueues a durable ``catalog_provision`` deprovision task
        (schema drop + external-resource teardown) rather than dropping the
        schema inline — the request never blocks on the teardown itself. This
        returns 202 with a Location header pointing at the polling endpoint
        when a teardown task is in flight, or 204 when there was nothing to
        tear down (no active provisioners for this catalog).
        """
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

        if not force:
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        # Hard delete: report the teardown task delete_catalog() enqueued so
        # the caller can poll instead of assuming the drop already finished.
        task = await catalogs_svc.get_hard_delete_task(catalog_id)
        if task is None:
            # No active provisioners: delete_catalog() already purged inline.
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        import json as _json

        from dynastore.extensions.tools.url import enforce_https

        task_id_str = str(task.task_id if hasattr(task, "task_id") else task.jobID)
        if request is not None:
            try:
                raw_url = request.url_for(
                    "get_task_status_catalog",
                    catalog_id=catalog_id,
                    task_id=task_id_str,
                )
                status_url = enforce_https(str(raw_url))
            except Exception:
                # url_for fails when the tasks extension isn't mounted; fall
                # back to a path-relative construction.
                base = enforce_https(str(request.base_url).rstrip("/"))
                root_path = request.scope.get("root_path", "").rstrip("/")
                status_url = (
                    f"{base}{root_path}/task/catalogs/{catalog_id}/tasks/{task_id_str}"
                )
        else:
            status_url = f"/task/catalogs/{catalog_id}/tasks/{task_id_str}"

        body = _json.dumps({
            "status": task.status if hasattr(task, "status") else "PENDING",
            "task_id": task_id_str,
            "catalog_id": catalog_id,
            "links": [
                {
                    "rel": "monitor",
                    "href": status_url,
                    "type": "application/json",
                    "title": "Deletion status",
                }
            ],
        })
        return Response(
            status_code=status.HTTP_202_ACCEPTED,
            headers={"Location": status_url},
            media_type="application/json",
            content=body,
        )

    # ------------------------------------------------------------------
    # Shared rename-if-changed helpers (used by both PUT and PATCH paths)
    # ------------------------------------------------------------------

    async def _ogc_perform_catalog_rename(
        self,
        catalog_id: str,
        body_id: str,
        *,
        request: Optional[Request] = None,
    ) -> Tuple[str, str, Optional[str]]:
        """Resolve, rename, and return URL for a catalog MOVE.

        Called when a PUT or PATCH body carries an ``"id"`` that differs from
        the URL path parameter.  Handles 404/409 mapping so the calling handler
        does not need to repeat the error logic.

        Returns a 3-tuple ``(internal_id, new_external_id, content_location_url)``
        where ``content_location_url`` is ``None`` when no ``request`` is
        provided (i.e. the caller cannot build an absolute URL).

        Raises:
            HTTPException(404): catalog not found.
            HTTPException(409): another live catalog already holds ``body_id``.
        """
        from dynastore.modules.db_config.exceptions import CatalogRenameConflictError

        catalogs_svc = await self._get_catalogs_service()
        internal_id = await catalogs_svc.resolve_catalog_id(
            catalog_id, allow_missing=True
        )
        if internal_id is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Catalog '{catalog_id}' not found.",
            )
        try:
            _old, new_external_id = await catalogs_svc.rename_catalog(
                internal_id, body_id
            )
        except CatalogRenameConflictError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc
        content_location: Optional[str] = None
        if request is not None:
            content_location = self._build_catalog_url(request, new_external_id)
        return internal_id, new_external_id, content_location

    async def _ogc_perform_collection_rename(
        self,
        catalog_id: str,
        collection_id: str,
        body_id: str,
        *,
        request: Optional[Request] = None,
    ) -> Tuple[str, str, str, Optional[str]]:
        """Resolve, rename, and return URL for a collection MOVE.

        Called when a PUT or PATCH body carries an ``"id"`` that differs from
        the URL path parameter.

        Returns a 4-tuple
        ``(catalog_internal_id, collection_internal_id, new_external_id,
        content_location_url)`` where ``content_location_url`` is ``None``
        when no ``request`` is provided.

        Raises:
            HTTPException(404): catalog or collection not found.
            HTTPException(409): another live collection already holds ``body_id``.
        """
        from dynastore.modules.db_config.exceptions import CollectionRenameConflictError

        catalogs_svc = await self._get_catalogs_service()
        catalog_internal_id = await catalogs_svc.resolve_catalog_id(
            catalog_id, allow_missing=True
        )
        if catalog_internal_id is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Catalog '{catalog_id}' not found.",
            )
        collection_internal_id = await catalogs_svc.collections.resolve_collection_id(
            catalog_internal_id, collection_id, allow_missing=True
        )
        if collection_internal_id is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        try:
            _old, new_external_id = await catalogs_svc.rename_collection(
                catalog_internal_id, collection_internal_id, body_id
            )
        except CollectionRenameConflictError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=str(exc),
            ) from exc
        content_location: Optional[str] = None
        if request is not None:
            cat_model = await catalogs_svc.get_catalog_model(catalog_internal_id)
            cat_external_id = (
                getattr(cat_model, "external_id", None) or catalog_id
            ) if cat_model else catalog_id
            content_location = self._build_collection_url(
                request, cat_external_id, new_external_id
            )
        return catalog_internal_id, collection_internal_id, new_external_id, content_location

    # ------------------------------------------------------------------
    # RFC 7240 Prefer header helpers
    # ------------------------------------------------------------------

    def _wants_move(self, request: Request) -> bool:
        """Return True iff the client sent ``Prefer: handling=move`` (RFC 7240).

        Parses the ``Prefer`` header as a comma-separated list of
        preference-tokens and checks for a ``handling=move`` token.
        The comparison is case-insensitive; surrounding whitespace is
        stripped; other tokens in the header are ignored.
        """
        prefer_header = request.headers.get("prefer", "")
        for token in prefer_header.split(","):
            if token.strip().lower() == "handling=move":
                return True
        return False

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

    def _build_catalog_url(self, request: Request, catalog_external_id: str) -> str:
        """Build the absolute URL for a catalog's canonical PUT/GET endpoint.

        Uses the incoming request's base URL and the service's path prefix.
        """
        root = get_root_url(request)
        prefix = getattr(self, "prefix", "")
        return f"{root}{prefix}/catalogs/{catalog_external_id}"

    def _build_collection_url(
        self, request: Request, catalog_external_id: str, collection_external_id: str
    ) -> str:
        """Build the absolute URL for a collection's canonical PUT/GET endpoint."""
        root = get_root_url(request)
        prefix = getattr(self, "prefix", "")
        return f"{root}{prefix}/catalogs/{catalog_external_id}/collections/{collection_external_id}"

    async def _ogc_replace_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates_dict: Dict[str, Any],
        language: str,
        *,
        request: Optional[Request] = None,
        body_id: Optional[str] = None,
        on_id_mismatch: Literal["ignore", "reject"] = "ignore",
    ) -> Response:
        """Shared replace-collection (PUT) body used by Features and STAC.

        *updates_dict* must already be the result of
        ``normalize_i18n_for_replace``; this method performs no additional
        normalization.  No ``db_resource`` / transactional context is passed
        on this path (neither Features nor STAC injects one for replace).

        When *body_id* differs from *collection_id* (the path parameter) three
        cases apply:

        1. ``Prefer: handling=move`` present: MOVE (rename) then replace;
           respond with 200 + ``Content-Location``, ``Link: rel=canonical``,
           and ``Preference-Applied: handling=move``.
        2. Move NOT requested, *on_id_mismatch* == ``"reject"`` (STAC): raise 400.
        3. Move NOT requested, *on_id_mismatch* == ``"ignore"`` (OGC Features
           Part 4 Req 11): drop the body id and replace the path-addressed resource.
        """
        catalogs_svc = await self._get_catalogs_service()
        await self._require_catalog_write_ready(catalog_id, catalogs_svc=catalogs_svc)

        # Mismatch branch: body id differs from path id.
        if body_id is not None and body_id != collection_id:
            if request is not None and self._wants_move(request):
                _cat_internal, _col_internal, new_external_id, content_location = (
                    await self._ogc_perform_collection_rename(
                        catalog_id, collection_id, body_id, request=request
                    )
                )
                updates_dict = {**updates_dict, "id": new_external_id}
                # Logical-id contract: service queries take the LOGICAL (external)
                # ids and resolve external->internal themselves. After the rename
                # the collection's new logical id is ``new_external_id`` and the
                # catalog path id is unchanged. Passing the internal surrogate here
                # bypasses the external->internal resolver (which is external-only),
                # so the post-rename re-read cannot find the row and raises a
                # spurious 404 even though the rename committed.
                updated = await catalogs_svc.update_collection(
                    catalog_id, new_external_id, updates_dict, lang="*"
                )
                if not updated:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Collection not found after rename.",
                    )
                localized_data, _ = self._localize_resource(updated, language)
                response = JSONResponse(content=localized_data)
                if content_location is not None:
                    response.headers["Content-Location"] = content_location
                    response.headers["Link"] = f'<{content_location}>; rel="canonical"'
                response.headers["Preference-Applied"] = "handling=move"
                return response

            if on_id_mismatch == "reject":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"Body id '{body_id}' does not match path id '{collection_id}'."
                        " Send 'Prefer: handling=move' to rename."
                    ),
                )
            # on_id_mismatch == "ignore": drop body id, replace path-addressed resource.
            updates_dict = {k: v for k, v in updates_dict.items() if k != "id"}

        # Normal replace branch: body id == path id (or body id dropped/absent).
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
        *,
        body_id: Optional[str] = None,
        on_id_mismatch: Literal["ignore", "reject"] = "ignore",
    ) -> Response:
        """Shared update-collection (PATCH) body used by Features and STAC.

        *updates_dict* must already be the result of
        ``model_dump(exclude_unset=True)``.  ``request`` is forwarded to
        ``_pre_update_collection_validate`` for services (STAC) that need
        to fetch the current state before merging and validating.

        When *body_id* differs from *collection_id* (the path parameter) three
        cases apply:

        1. ``Prefer: handling=move`` present: MOVE (rename) then patch remaining
           fields; respond with 200 + ``Content-Location``,
           ``Link: rel=canonical``, and ``Preference-Applied: handling=move``.
        2. Move NOT requested, *on_id_mismatch* == ``"reject"`` (STAC): raise 400.
        3. Move NOT requested, *on_id_mismatch* == ``"ignore"`` (OGC Features
           Part 4): drop the body ``"id"`` field and patch normally.

        When *body_id* is absent or equals *collection_id* the normal partial-update
        path runs regardless.
        """
        from dynastore.extensions.tools.localization_utils import detect_use_lang

        catalogs_svc = await self._get_catalogs_service()
        await self._require_catalog_write_ready(catalog_id, catalogs_svc=catalogs_svc)

        # Mismatch branch: PATCH body carries a different "id".
        if body_id is not None and body_id != collection_id:
            if request is not None and self._wants_move(request):
                _cat_internal, _col_internal, new_external_id, content_location = (
                    await self._ogc_perform_collection_rename(
                        catalog_id, collection_id, body_id, request=request
                    )
                )
                # Strip "id" — rename already updated it.
                patch_fields = {k: v for k, v in updates_dict.items() if k != "id"}
                # Logical-id contract: pass the new LOGICAL ids (catalog path id
                # unchanged; collection now addressed by ``new_external_id``) so the
                # service resolves external->internal itself. Passing the internal
                # surrogate bypasses that resolver and the post-rename re-read 404s.
                await self._pre_update_collection_validate(
                    catalog_id, new_external_id, patch_fields, request
                )
                use_lang = detect_use_lang(patch_fields, language)
                updated = await catalogs_svc.update_collection(
                    catalog_id, new_external_id, patch_fields, lang=use_lang
                )
                if not updated:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Collection not found after rename.",
                    )
                localized_data, _ = self._localize_resource(updated, language)
                response = JSONResponse(content=localized_data)
                if content_location is not None:
                    response.headers["Content-Location"] = content_location
                    response.headers["Link"] = f'<{content_location}>; rel="canonical"'
                response.headers["Preference-Applied"] = "handling=move"
                return response

            if on_id_mismatch == "reject":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"Body id '{body_id}' does not match path id '{collection_id}'."
                        " Send 'Prefer: handling=move' to rename."
                    ),
                )
            # on_id_mismatch == "ignore": drop body id, patch with path id.
            updates_dict = {k: v for k, v in updates_dict.items() if k != "id"}

        # Normal update branch: body id == path id (or body id dropped/absent).
        await self._pre_update_collection_validate(
            catalog_id, collection_id, updates_dict, request
        )
        use_lang = detect_use_lang(updates_dict, language)

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
        request: Optional[Request] = None,
    ) -> Response:
        """Shared delete-collection body used by Features and STAC.

        Soft delete (force=False): synchronous tombstone; returns 204.
        Hard delete (force=True): enqueues a durable ``collection_hard_delete``
        task and returns 202 with a Location header pointing at the polling
        endpoint.  The collection is observable as lifecycle_status='deleting'
        while the task runs, then disappears when the purge completes.

        Idempotency: if a non-terminal delete task for the same collection
        already exists (dedup hit), the endpoint still returns 202 with the
        existing task's id so the caller can poll it.
        """
        catalogs_svc = await self._get_catalogs_service()

        if not force:
            # --- Soft delete: unchanged synchronous path ---
            ctx = DriverContext(db_resource=db_resource) if db_resource is not None else None
            delete_kwargs: Dict[str, Any] = {}
            if ctx is not None:
                delete_kwargs["ctx"] = ctx
            if not await catalogs_svc.delete_collection(
                catalog_id, collection_id, False, **delete_kwargs
            ):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Collection not found.",
                )
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        # --- Hard delete: async task path ---
        import json as _json

        from dynastore.extensions.tools.url import enforce_https
        from dynastore.models.tasks import TaskCreate, TaskScope
        from dynastore.modules.db_config.query_executor import (
            DQLQuery,
            ResultHandler,
            managed_transaction,
        )
        from dynastore.modules.tasks.tasks_module import (
            _resolve_catalog_schema,
            create_task,
            get_task_schema,
        )
        from dynastore.tools.caller import current_caller_id
        from dynastore.tools.protocol_helpers import get_engine

        # Resolve physical schema so the task lands in the catalog's namespace
        # and is discoverable via GET /task/catalogs/{catalog_id}/tasks/{task_id}.
        engine = get_engine()
        if engine is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database engine not available; cannot enqueue delete task.",
            )

        async with managed_transaction(engine) as _conn:
            physical_schema = await _resolve_catalog_schema(catalog_id, _conn)

        dedup_key = f"collection_hard_delete:{catalog_id}:{collection_id}"
        task_data = TaskCreate(
            task_type="collection_hard_delete",
            caller_id=current_caller_id(),
            inputs={"catalog_id": catalog_id, "collection_id": collection_id},
            scope=TaskScope.CATALOG,
            collection_id=collection_id,
            dedup_key=dedup_key,
        )

        task = await create_task(engine, task_data, schema=physical_schema)

        if task is None:
            # Dedup hit — a non-terminal delete is already in flight.
            # Look it up so we can still return a status link.
            task_schema = get_task_schema()
            async with managed_transaction(engine) as _conn2:
                existing_dict = await DQLQuery(
                    f"SELECT * FROM {task_schema}.tasks"
                    " WHERE dedup_key = :dk AND catalog_id = :sn"
                    " AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')"
                    " ORDER BY timestamp DESC LIMIT 1;",
                    result_handler=ResultHandler.ONE_DICT,
                ).execute(_conn2, dk=dedup_key, sn=physical_schema)

            if existing_dict is None:
                # Edge case: task completed between create_task dedup check
                # and this read.  Return a 409 so the caller knows to retry.
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"A delete task for '{catalog_id}:{collection_id}' just"
                        " completed; reissue the request if the collection still exists."
                    ),
                )
            from dynastore.modules.tasks.models import Task as _Task
            task = _Task.model_validate(existing_dict)

        # Build the external-facing status URL.
        task_id_str = str(task.task_id if hasattr(task, "task_id") else task.jobID)
        if request is not None:
            try:
                raw_url = request.url_for(
                    "get_task_status_catalog",
                    catalog_id=catalog_id,
                    task_id=task_id_str,
                )
                status_url = enforce_https(str(raw_url))
            except Exception:
                # url_for fails when the tasks extension isn't mounted; fall
                # back to a path-relative construction.
                base = enforce_https(str(request.base_url).rstrip("/"))
                root_path = request.scope.get("root_path", "").rstrip("/")
                status_url = (
                    f"{base}{root_path}/task/catalogs/{catalog_id}/tasks/{task_id_str}"
                )
        else:
            status_url = f"/task/catalogs/{catalog_id}/tasks/{task_id_str}"

        body = _json.dumps({
            "status": task.status if hasattr(task, "status") else "PENDING",
            "task_id": task_id_str,
            "collection_id": collection_id,
            "links": [
                {
                    "rel": "monitor",
                    "href": status_url,
                    "type": "application/json",
                    "title": "Deletion status",
                }
            ],
        })
        return Response(
            status_code=status.HTTP_202_ACCEPTED,
            headers={"Location": status_url},
            media_type="application/json",
            content=body,
        )


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

        A single item, or a multi-item payload that fits under both the
        collection's ``sync_ingest_batch_rows`` row cap and
        ``sync_ingest_batch_memory_mb`` byte budget (``CollectionPluginConfig``),
        is written with ONE ``upsert()`` call, exactly as before — full
        upserted rows land in ``accepted_rows``, which callers building a
        protocol-specific 201 body rely on. A multi-item payload that
        exceeds either bound is sub-batched instead, mirroring the bounded
        batching the async ingestion task already applies (#2657 bounds the
        remaining unbounded path — the one direct synchronous bulk POST):
        each sub-batch is upserted, reduced to accepted ID strings, and
        discarded before the next sub-batch is prepared, so the whole
        FeatureCollection is never held in memory at once. In that case
        ``accepted_rows`` holds accumulated ID strings rather than rows.
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

        # A multi-item payload is sub-batched once it exceeds the row cap,
        # or — for a payload under the row cap but with individually large
        # geometries — once its accumulated estimated byte size exceeds the
        # memory budget. A single item is never split.
        needs_split = False
        row_cap = 0
        byte_budget = 0
        if not was_single:
            from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
            from dynastore.tasks.ingestion.main_ingestion import _estimate_feature_bytes

            col_config = await self._get_plugin_config(  # type: ignore[attr-defined]
                CollectionPluginConfig, catalog_id, collection_id
            )
            row_cap = col_config.sync_ingest_batch_rows
            byte_budget = max(1, col_config.sync_ingest_batch_memory_mb) * 1024 * 1024

            if batch_size > row_cap:
                needs_split = True
            else:
                total_bytes = 0
                for item in items_list:
                    _dump = getattr(item, "model_dump", None)
                    total_bytes += _estimate_feature_bytes(
                        _dump() if callable(_dump) else item
                    )
                    if total_bytes > byte_budget:
                        needs_split = True
                        break

        rejections: list[SidecarRejection] = []
        accepted_rows: list[Any]

        if needs_split:
            from dynastore.models.protocols.indexer import MAX_ACCUMULATED_FAILURE_SAMPLES
            from dynastore.modules.catalog.item_service import _merge_index_results_into

            accepted_ids: list[str] = []
            index_results: Dict[str, Any] = {}
            current_batch: list[Any] = []
            current_batch_bytes = 0

            async def _flush(sub_batch: "list[Any]") -> None:
                # Seed the typed out-list so the PG write path can record
                # per-row SidecarRejectedError events without collapsing the
                # whole sub-batch. The core service reads/writes
                # ``ctx.extensions["_rejections"]``.
                ctx.extensions["_rejections"] = []
                try:
                    created = await catalogs_svc.upsert(
                        catalog_id, collection_id, items=sub_batch, ctx=ctx
                    )
                except SidecarRejectedError as rej:
                    # Non-PG primary drivers still surface rejections as a
                    # single batch-level exception; PG now catches per-row
                    # and delivers via the out-list below, so we only reach
                    # here when the primary driver aborted the sub-batch.
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
                    # on_batch_conflict=refuse_batch: duplicate detected → abort → 409.
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
                # Bound the accumulated rejection samples across sub-batches
                # the same way the indexer/ingestion-task accumulators do —
                # keep the most recent MAX_ACCUMULATED_FAILURE_SAMPLES.
                if len(rejections) > MAX_ACCUMULATED_FAILURE_SAMPLES:
                    rejections[:] = rejections[-MAX_ACCUMULATED_FAILURE_SAMPLES:]

                # Reduce this sub-batch's accepted rows to ID strings
                # immediately and let the full rows fall out of scope —
                # no Feature body from this sub-batch survives past here.
                batch_rows = (
                    created if isinstance(created, list) else ([created] if created else [])
                )
                accepted_ids.extend(self._resolve_accepted_ids(batch_rows))
                _merge_index_results_into(
                    index_results, ctx.extensions.pop("_index_results", None) or {}
                )

            for item in items_list:
                current_batch.append(item)
                _dump = getattr(item, "model_dump", None)
                current_batch_bytes += _estimate_feature_bytes(
                    _dump() if callable(_dump) else item
                )
                if len(current_batch) >= row_cap or current_batch_bytes >= byte_budget:
                    await _flush(current_batch)
                    current_batch = []
                    current_batch_bytes = 0
            if current_batch:
                await _flush(current_batch)

            if index_results:
                ctx.extensions["_index_results"] = index_results

            accepted_rows = list(accepted_ids)
        else:
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

            accepted_rows = (
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
            if isinstance(row, str):
                # Sub-batched bulk ingest (#2657) already resolved each
                # sub-batch's IDs before discarding its full rows — pass a
                # pre-resolved ID straight through instead of re-deriving it.
                ids.append(row)
                continue
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
