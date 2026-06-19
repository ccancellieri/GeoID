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

"""Composition driver for PG-backed collection storage.

:class:`CollectionPostgresqlDriver` wraps the existing per-domain PG
``CollectionStore`` drivers (``CollectionCorePostgresqlDriver`` plus the optional
``CollectionStacPostgresqlDriver`` from the stac module) and fans CRUD
out across them at write/read/delete time.  The two inner drivers
already implement :class:`CollectionStore` directly today; this
class is purely a composition layer so that operators can list ONE
``driver_id`` in routing config instead of two and tune the active set
via a single ``sidecars`` discriminated union on the wrapper config.

As of PR 1e step 3b this driver IS entry-point-registered (group
``dynastore.modules``, name ``collection_postgresql``); the two raw
inner drivers' entry-points were removed in the same change so that
``get_protocols(CollectionStore)`` returns only the wrapper.
The catalog-tier raw drivers stay entry-point-registered until step
3c lands ``CatalogPostgresqlDriver``.

Sidecar discriminated union shape mirrors the items-tier
``_PgSidecarConfig`` (`storage/driver_config.py:72`) — same operator
mental model: one wrapper config carries a typed list of sidecar
entries discriminated by ``sidecar_type``, defaults are resolved
lazily from a registry that uses try-import for cross-module sidecars
so a deployment without the stac extra simply omits ``collection_stac``.

NOTE — items-tier ``SidecarProtocol`` is NOT lifted here.  That
machinery (Hub-FK, partition-keys, ``{physical_table}_{sidecar_id}``
naming, per-feature DDL evolution) is items-specific and the wrong
abstraction for tenant-schema-keyed metadata tables whose DDL is
created out-of-band by ``modules.catalog.db_init.metadata_domain_split``
at catalog provisioning time.  Each "metadata sidecar" here is an
ALREADY-EXISTING ``CollectionStore`` driver whose own column
filter (``_PgCollectionCoreBase._filter_payload``) handles the
per-domain slicing.
"""

from __future__ import annotations

import logging
from functools import cached_property
from typing import (
    Annotated,
    Any,
    ClassVar,
    Dict,
    FrozenSet,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
)

from pydantic import BaseModel, Discriminator, Field, model_validator

from dynastore.models.protocols.entity_store import (
    CollectionStore,
    EntityStoreCapability,
)
from dynastore.models.protocols.teardown_lane import TeardownLane
from dynastore.models.protocols.typed_driver import (
    TypedDriver,
    _PluginDriverConfig,
)
from dynastore.models.mutability import Immutable
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.routing_config import Operation
from dynastore.tools.cache import cached

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sidecar discriminated union — mirrors items-tier ``_PgSidecarConfig`` shape
# ---------------------------------------------------------------------------


class _PgCollectionSidecarConfigBase(BaseModel):
    """Common base for collection-metadata PG sidecar configs.

    Subclasses pin ``sidecar_type`` as a ``Literal[...]`` so the
    discriminated union round-trips cleanly through
    ``model_dump(exclude_unset=True) → model_validate``.
    """

    sidecar_type: str

    @model_validator(mode="after")
    def _pin_discriminator_as_set(self) -> "_PgCollectionSidecarConfigBase":
        """Mark ``sidecar_type`` as explicitly set so ``exclude_unset=True`` keeps it.

        Mirrors the items-tier ``SidecarConfig._retain_sidecar_type_on_dump``
        fix (commit ``96fbf8c``) — without this, default-constructed
        ``CollectionCoreSidecarConfig()`` dumps to ``{}`` and re-validating
        the dump against the discriminated union fails.
        """
        self.__pydantic_fields_set__.add("sidecar_type")
        return self


class CollectionCoreSidecarConfig(_PgCollectionSidecarConfigBase):
    """Routes the CORE metadata slice (``title``, ``description``,
    ``keywords``, ``license``, ``extra_metadata``) to
    :class:`CollectionCorePostgresqlDriver`.
    """

    sidecar_type: Literal["collection_core"] = "collection_core"


class CollectionStacSidecarConfig(_PgCollectionSidecarConfigBase):
    """Routes the STAC metadata slice (``stac_version``, ``extent``,
    ``providers``, ``summaries``, ``links``, ``assets``, ``item_assets``)
    to :class:`CollectionStacPostgresqlDriver` from the stac module.

    Resolved via try-import in :class:`CollectionPgSidecarRegistry` — a
    deployment without the stac extra installed will see this entry's
    resolution log a single warning and skip the slice (no crash).
    """

    sidecar_type: Literal["collection_stac"] = "collection_stac"


_PgCollectionSidecarConfig = Annotated[
    Union[
        CollectionCoreSidecarConfig,
        CollectionStacSidecarConfig,
    ],
    Discriminator("sidecar_type"),
]


# ---------------------------------------------------------------------------
# Sidecar registry — maps discriminator string → inner driver class
# ---------------------------------------------------------------------------


class CollectionPgSidecarRegistry:
    """Registry mapping ``sidecar_type`` to inner ``CollectionStore``
    classes.  STAC entry uses try-import so the wrapper works in deployments
    without the stac extra installed.
    """

    _registry: ClassVar[Dict[str, Type[CollectionStore]]] = {}
    _defaults_loaded: bool = False

    @classmethod
    def _ensure_defaults(cls) -> None:
        if cls._defaults_loaded:
            return
        cls._defaults_loaded = True

        # CORE — same module tree, no try-import needed.
        from dynastore.modules.storage.drivers.core_postgresql import (
            CollectionCorePostgresqlDriver,
        )
        cls._registry.setdefault("collection_core", CollectionCorePostgresqlDriver)

        # STAC — different module; deployments without the stac extra
        # silently omit the entry.
        try:
            from dynastore.modules.stac.drivers.postgresql import (
                CollectionStacPostgresqlDriver,
            )
            cls._registry.setdefault(
                "collection_stac", CollectionStacPostgresqlDriver,
            )
        except ImportError as exc:
            logger.debug(
                "CollectionPgSidecarRegistry: stac sidecar unavailable (%s)", exc,
            )

    @classmethod
    def get_driver_cls(
        cls, sidecar_type: str,
    ) -> Optional[Type[CollectionStore]]:
        cls._ensure_defaults()
        return cls._registry.get(sidecar_type)

    @classmethod
    def register(
        cls, sidecar_type: str, driver_cls: Type[CollectionStore],
    ) -> None:
        """Register a new metadata sidecar type — used by extensions that
        contribute a new domain slice (mirrors items-tier
        ``SidecarRegistry.register``).
        """
        cls._registry[sidecar_type] = driver_cls

    @classmethod
    def default_sidecars(cls) -> List[_PgCollectionSidecarConfigBase]:
        """Built-in default — CORE only.

        The STAC collection slice (``collection_stac``) is NOT included by
        default.  It is added at runtime by
        ``CollectionPostgresqlDriver._resolve_sidecars_for_catalog`` when the
        scope's ``StacStorageConfig`` has collection-tier enabled AND includes
        PG storage.  Absent a ``StacPreset``, no STAC slice is materialized
        (opt-in default).
        """
        cls._ensure_defaults()
        return [CollectionCoreSidecarConfig()]

    @classmethod
    def clear(cls) -> None:
        """Test-isolation hook."""
        cls._registry.clear()
        cls._defaults_loaded = False


# ---------------------------------------------------------------------------
# Wrapper config + driver
# ---------------------------------------------------------------------------


class CollectionPostgresqlDriverConfig(_PluginDriverConfig):
    """Configuration for the PG-backed composition ``CollectionStore`` driver.

    ``sidecars`` is the typed list of domain slices the wrapper
    will fan CRUD across.  Empty list → wrapper falls back to the
    :meth:`CollectionPgSidecarRegistry.default_sidecars` default
    (``[collection_core, collection_stac if installed]``).

    Marked ``Immutable`` because changing the active sidecar set after
    rows exist would orphan domain slices in their per-tenant tables —
    the same constraint the items-tier sidecar config already enforces.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "drivers")
    _freeze_at: ClassVar[Optional[str]] = "collection"

    required_engine_class: ClassVar[str] = "postgresql_engine"


    sidecars: Immutable[List[_PgCollectionSidecarConfig]] = Field(
        default_factory=list,
        description=(
            "Metadata sidecar configs — discriminated union on "
            "`sidecar_type`.  Empty → registry default "
            "(`collection_core` always, `collection_stac` if the stac extra "
            "is installed).  Immutable once set — changing it would orphan "
            "rows in the per-domain tables under the per-tenant schema.  "
            "Honored at runtime via per-catalog ``ConfigsProtocol`` fetch "
            "with TTL=60s cache (PR 1e step 4) — operator overrides "
            "propagate within ~60s of being persisted.  Apply handler "
            "additionally bumps the cache invalidation generation for "
            "instant effect when the config service is wired."
        ),
    )


async def _on_apply_collection_pg_driver_config(
    config: Any,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Invalidate the wrapper's per-catalog sidecar cache so the
    operator's submitted ``sidecars`` override takes effect immediately
    instead of waiting for the TTL to expire.

    The wrapper resolves sidecars via a TTL-cached
    ``_resolve_sidecars_for_catalog`` call that fetches the wrapper
    config through ``ConfigsProtocol``.  Without this invalidation, a
    just-PATCHed override would wait up to ~60s (TTL + jitter) before
    appearing in driver behaviour.  Logging at INFO level surfaces the
    intent without operator-warning noise.
    """
    if not isinstance(config, CollectionPostgresqlDriverConfig):
        return
    from dynastore.tools.discovery import get_protocol

    wrapper = get_protocol(CollectionPostgresqlDriver)
    if wrapper is None:
        # Wrapper not yet registered — happens during early lifespan
        # apply flushes; the eventual cache will start cold anyway.
        return
    try:
        wrapper._resolve_sidecars_for_catalog.cache_clear()  # type: ignore[attr-defined]
    except Exception as exc:
        logger.debug(
            "CollectionPostgresqlDriver: cache_clear failed (%s) — TTL "
            "expiry will still propagate the override within ~60s", exc,
        )
        return
    if config.sidecars:
        logger.info(
            "CollectionPostgresqlDriver: sidecar cache invalidated after "
            "config apply at %s scope; new sidecar set %s takes effect on "
            "next metadata operation.",
            f"catalog '{catalog_id}'" if catalog_id else "platform",
            [getattr(s, "sidecar_type", "?") for s in config.sidecars],
        )


CollectionPostgresqlDriverConfig.register_apply_handler(
    _on_apply_collection_pg_driver_config,
)


class CollectionPostgresqlDriver(TypedDriver[CollectionPostgresqlDriverConfig]):
    """Composition driver: fans CollectionStore CRUD across the
    configured PG metadata sidecars (``collection_core``, optionally
    ``collection_stac``, plus any extension-contributed sidecar registered
    via :meth:`CollectionPgSidecarRegistry.register`).

    The wrapper itself owns no SQL — every method delegates to the
    inner drivers, each of which already filters the payload to its own
    column set.  CORE and STAC sidecars therefore peacefully share a
    single ``upsert_metadata`` payload that mixes both domains.

    Capabilities are the **union** of the inner drivers' capabilities
    so the routing layer recognises this single entry as covering
    everything the two raw drivers cover today.
    """

    # Collection metadata rows are in PG and are cleaned up inline with the
    # delete transaction; the async cascade must not re-drop them.
    teardown_lane: ClassVar[TeardownLane] = TeardownLane.INLINE_TXN

    # Collection metadata fallback for SEARCH (PG serves the
    # query-fallback path when ES is unavailable / not registered).
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset({Operation.SEARCH})

    # Hints this driver serves on READ/SEARCH operations.
    # GEOMETRY_EXACT: PG is the system of record with full WKB geometry.
    # METADATA: generic participation in metadata reads.
    supported_hints: ClassVar[FrozenSet[Hint]] = frozenset({
        Hint.GEOMETRY_EXACT,
        Hint.METADATA,
    })

    capabilities: ClassVar[FrozenSet[str]] = frozenset({
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SOFT_DELETE,
        EntityStoreCapability.SEARCH,
        EntityStoreCapability.SEARCH_EXACT,
        EntityStoreCapability.SPATIAL_FILTER,
        EntityStoreCapability.PHYSICAL_ADDRESSING,
        EntityStoreCapability.QUERY_FALLBACK_SOURCE,
        EntityStoreCapability.LIFECYCLE,
    })

    def _resolve_inner_drivers(
        self, sidecars: Optional[List[_PgCollectionSidecarConfigBase]] = None,
    ) -> List[CollectionStore]:
        """Resolve the configured ``sidecars`` list to instantiated inner
        ``CollectionStore`` driver instances.

        ``sidecars=None`` (or empty) falls back to
        :meth:`CollectionPgSidecarRegistry.default_sidecars`.

        Sidecar entries whose ``sidecar_type`` is unknown to the registry
        (e.g. ``collection_stac`` in a deployment without the stac extra)
        log a warning and are skipped — a missing slice is a less-bad
        failure mode than crashing the whole write.

        Production paths call this with ``sidecars=None`` and so go
        through :attr:`_default_inner_drivers` — which caches the
        resolution per wrapper instance, eliminating a registry lookup +
        4 fresh instantiations per request.
        """
        if not sidecars:
            sidecars = CollectionPgSidecarRegistry.default_sidecars()
        out: List[CollectionStore] = []
        for cfg in sidecars:
            sc_type = getattr(cfg, "sidecar_type", None)
            if sc_type is None:
                logger.warning(
                    "CollectionPostgresqlDriver: sidecar entry missing "
                    "`sidecar_type` discriminator — skipping",
                )
                continue
            cls = CollectionPgSidecarRegistry.get_driver_cls(sc_type)
            if cls is None:
                logger.warning(
                    "CollectionPostgresqlDriver: sidecar %r not registered "
                    "(module not installed?) — skipping",
                    sc_type,
                )
                continue
            out.append(cls())
        return out

    @cached_property
    def _default_inner_drivers(self) -> List[CollectionStore]:
        """Cached INSTALL-level resolution — used by code paths that have
        NO ``catalog_id`` in scope (``is_available``,
        ``stac_metadata_columns``).

        Install-level capability surface, distinct from per-catalog
        materialization: includes the ``collection_stac`` inner whenever the
        stac extra is installed (the slice is registered), so
        ``stac_service._has_stac`` correctly reports STAC as *available* for
        a deployment that ships the extra — even though STAC sidecars are
        opt-in per collection via ``StacStorageConfig`` / ``StacPreset``.
        Per-catalog read/write fan-out goes through
        :meth:`_resolve_sidecars_for_catalog`, which stays opt-in (core-only
        until a ``StacStorageConfig`` enables the collection tier).
        """
        sidecars: List[_PgCollectionSidecarConfigBase] = list(
            CollectionPgSidecarRegistry.default_sidecars()
        )
        if "collection_stac" in CollectionPgSidecarRegistry._registry:
            sidecars.append(CollectionStacSidecarConfig())
        return self._resolve_inner_drivers(sidecars)

    @cached(
        maxsize=128, ttl=60, jitter=5,
        namespace="collection_pg_wrapper_sidecars",
        ignore=["self", "db_resource"],
        distributed=False,
    )
    async def _resolve_sidecars_for_catalog(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> List[CollectionStore]:
        """Resolve the inner driver list for ``catalog_id`` honoring any
        operator-submitted ``sidecars`` override at the catalog or
        platform scope.

        Fetches the wrapper config via the 4-tier waterfall
        (``ConfigsProtocol.get_config``); if ``config.sidecars`` is
        non-empty uses those, otherwise builds the effective set from
        :meth:`CollectionPgSidecarRegistry.default_sidecars` plus the
        ``collection_stac`` slice when the scope's ``StacStorageConfig``
        has collection-tier enabled AND PG storage.

        Cached at TTL=60s with jitter — operator overrides propagate
        within ~60s of being persisted, OR immediately when the apply
        handler bumps ``cache_clear``.  ``db_resource`` is excluded from
        the cache key (different transactions may resolve identically;
        the underlying inner driver instances are stateless).
        """
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        sidecars: Optional[List[_PgCollectionSidecarConfigBase]] = None
        configs = get_protocol(ConfigsProtocol)
        if configs is not None:
            try:
                cfg = await configs.get_config(
                    CollectionPostgresqlDriverConfig,
                    catalog_id=catalog_id,
                )
            except Exception as exc:
                logger.warning(
                    "CollectionPostgresqlDriver: config fetch failed for "
                    "catalog %r (%s) — falling back to registry default",
                    catalog_id, exc,
                )
                cfg = None
            if cfg is not None and cfg.sidecars:
                sidecars = list(cfg.sidecars)

        if sidecars is None:
            # No explicit override — build from CORE default + optional STAC
            # slice when StacStorageConfig signals collection-tier + PG.
            sidecars = list(CollectionPgSidecarRegistry.default_sidecars())
            if configs is not None:
                try:
                    from dynastore.modules.stac.stac_storage_config import (
                        StacStorageConfig,
                        collection_stac_enabled,
                        pg_stac,
                    )
                    stac_cfg = await configs.get_config(
                        StacStorageConfig,
                        catalog_id=catalog_id,
                    )
                    if (
                        isinstance(stac_cfg, StacStorageConfig)
                        and collection_stac_enabled(stac_cfg.stac_level)
                        and pg_stac(stac_cfg.stac_storage)
                        and "collection_stac" in CollectionPgSidecarRegistry._registry
                    ):
                        sidecars.append(CollectionStacSidecarConfig())
                except Exception as exc:
                    logger.debug(
                        "CollectionPostgresqlDriver: StacStorageConfig fetch "
                        "failed for catalog %r (%s) — no stac slice added",
                        catalog_id, exc,
                    )

        return self._resolve_inner_drivers(sidecars)

    async def is_available(self) -> bool:
        for inner in self._default_inner_drivers:
            if await inner.is_available():
                return True
        return False

    async def get_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fan-in: read from each inner driver, shallow-merge slices.

        Mirrors :func:`collection_router._safe_get` semantics —
        per-driver failures degrade to ``None`` for that slice, the
        resulting dict carries every successfully-read column.
        Returns ``None`` only when every inner returned ``None``.
        """
        merged: Dict[str, Any] = {}
        inners = await self._resolve_sidecars_for_catalog(
            catalog_id, db_resource=db_resource,
        )
        for inner in inners:
            try:
                slice_ = await inner.get_metadata(
                    catalog_id, collection_id,
                    context=context, db_resource=db_resource,
                )
            except Exception as exc:
                logger.warning(
                    "CollectionPostgresqlDriver: inner %s.get_metadata "
                    "failed: %s", type(inner).__name__, exc,
                )
                continue
            if slice_:
                merged.update(slice_)
        return merged or None

    async def upsert_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Fan-out: hand the full payload to each inner driver.

        Each inner filters the payload to its own ``_columns`` and
        no-ops if the filtered slice is empty — so a CORE-only payload
        reaching the STAC inner is a no-op at that layer, not a NULL
        stamp.  Same default-fast invariant as the existing two-driver
        router fan-out.
        """
        inners = await self._resolve_sidecars_for_catalog(
            catalog_id, db_resource=db_resource,
        )
        for inner in inners:
            await inner.upsert_metadata(
                catalog_id, collection_id, metadata,
                db_resource=db_resource,
            )

    async def delete_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        inners = await self._resolve_sidecars_for_catalog(
            catalog_id, db_resource=db_resource,
        )
        for inner in inners:
            await inner.delete_metadata(
                catalog_id, collection_id,
                soft=soft, db_resource=db_resource,
            )

    async def search_metadata(
        self,
        catalog_id: str,
        *,
        q: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[str] = None,
        filter_cql: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Search via the first SEARCH-capable inner, then hydrate each hit
        with the remaining (non-SEARCH) sidecar slices.

        Today only the CORE inner declares ``SEARCH``; the STAC inner
        contributes ``SPATIAL_FILTER`` only.  CORE owns its column slice
        (id / title / description / keywords / license / extra_metadata) —
        so a bare delegation returns CORE-only rows.  That silently drops
        every STAC field (extent / providers / summaries / links / assets /
        item_assets) from a PG-routed collection listing — e.g. the
        ``private_catalog`` preset, whose collection SEARCH is PG-first —
        even though single-collection GET (which goes through the
        :meth:`get_metadata` fan-in merge) still returns them.

        To keep the composition driver authoritative, each search hit is
        hydrated with the other inners' slices via their ``get_metadata``,
        mirroring the merge in :meth:`get_metadata`.  CORE search values win
        on key overlap (preserving ``id`` and result ordering); the STAC
        columns are disjoint and merge cleanly.  Hydration is skipped
        entirely when no non-SEARCH inner is configured (the common
        core-only catalog), so it costs nothing until a ``StacStorageConfig``
        opts the catalog into the collection STAC tier.

        When/if multiple inners declare ``SEARCH``, the first-wins selection
        of the search executor will need revisiting (union? rank-then-merge?).
        """
        inners = await self._resolve_sidecars_for_catalog(
            catalog_id, db_resource=db_resource,
        )
        search_inner: Optional[CollectionStore] = None
        for inner in inners:
            if EntityStoreCapability.SEARCH in getattr(
                inner, "capabilities", frozenset()
            ):
                search_inner = inner
                break
        if search_inner is None:
            return [], 0

        rows, total = await search_inner.search_metadata(
            catalog_id,
            q=q, bbox=bbox, datetime_range=datetime_range,
            filter_cql=filter_cql, limit=limit, offset=offset,
            context=context, db_resource=db_resource,
        )

        hydrators = [d for d in inners if d is not search_inner]
        if hydrators and rows:
            for row in rows:
                cid = row.get("id")
                if cid is None:
                    continue
                for hydrator in hydrators:
                    try:
                        slice_ = await hydrator.get_metadata(
                            catalog_id, cid,
                            context=context, db_resource=db_resource,
                        )
                    except Exception as exc:
                        logger.warning(
                            "CollectionPostgresqlDriver: search hydrate via "
                            "%s failed for %s/%s: %s",
                            type(hydrator).__name__, catalog_id, cid, exc,
                        )
                        continue
                    if slice_:
                        for key, value in slice_.items():
                            row.setdefault(key, value)
        return rows, total

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """No per-collection driver config today — same default-empty as inners."""
        return None

    async def location(self, catalog_id: str, collection_id: str) -> Any:
        """Delegate to the first inner that supports PHYSICAL_ADDRESSING.

        Sufficient for config-API introspection — operator only needs
        to know the underlying tenant schema, which every inner shares.
        """
        from dynastore.modules.storage.storage_location import StorageLocation
        inners = await self._resolve_sidecars_for_catalog(catalog_id)
        for inner in inners:
            inner_caps = getattr(inner, "capabilities", frozenset())
            if EntityStoreCapability.PHYSICAL_ADDRESSING in inner_caps:
                return await inner.location(catalog_id, collection_id)
        return StorageLocation(
            backend="postgresql",
            canonical_uri=f"postgresql://composition/{catalog_id}/{collection_id}",
            identifiers={"catalog_id": catalog_id, "collection_id": collection_id},
            display_label="composition (no inner with PHYSICAL_ADDRESSING)",
        )

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Inner drivers' tables are created out-of-band by the per-tenant
        DDL in ``modules.catalog.db_init.metadata_domain_split`` at catalog
        provisioning time — same no-op as the inner drivers themselves.
        """
        return None

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        """Symmetric counterpart to :meth:`ensure_storage`.

        The PG metadata sidecars share the per-tenant schema, which is
        created and dropped out-of-band by the catalog (de)provisioning
        DDL — the composition wrapper owns no storage of its own to drop.
        The collection-metadata rows themselves are removed by the
        routing-driven hard-delete cascade through the collection-store
        index driver, not here (see ``routing_driven_cascade_owner``).

        Defining this method keeps the wrapper structurally conformant
        with the ``CollectionStore`` protocol — which gained
        ``drop_storage`` for that cascade — so the structural
        ``isinstance`` check in ``get_protocols(CollectionStore)`` still
        discovers the wrapper after it replaces the raw inner drivers in
        the plugin registry.

        Fan-out is fail-soft and idempotent: any inner driver that grows
        its own ``drop_storage`` is delegated to; inners without one are
        skipped.
        """
        for inner in self._default_inner_drivers:
            drop = getattr(inner, "drop_storage", None)
            if drop is not None:
                await drop(catalog_id, collection_id, soft=soft)

    async def get_lifecycle(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        db_resource: Optional[Any] = None,
        **kwargs: Any,
    ) -> Any:
        """Delegate to the first LIFECYCLE-capable inner driver.

        The CORE inner driver owns the ``"{schema}".collections`` registry
        row and declares ``LIFECYCLE``; no other inner needs to.  Returning
        ``Any`` avoids importing ``CollectionLifecycle`` here — callers that
        care about the type import it from ``entity_store`` directly.
        """
        for inner in self._default_inner_drivers:
            if EntityStoreCapability.LIFECYCLE in getattr(
                inner, "capabilities", frozenset()
            ):
                return await inner.get_lifecycle(
                    catalog_id, collection_id,
                    db_resource=db_resource, **kwargs,
                )
        # No LIFECYCLE-capable inner — schema unresolvable; return MISSING.
        from dynastore.models.protocols.entity_store import CollectionLifecycle
        return CollectionLifecycle.MISSING

    def stac_metadata_columns(self) -> Tuple[str, ...]:
        """Forward the STAC capability marker through to the first inner
        driver that exposes it.

        Defining this method makes the wrapper structurally satisfy
        :class:`extensions.stac.protocols.StacCollectionEntityStoreCapability`,
        so ``get_protocols(CollectionStore)`` -> isinstance check
        in ``stac_service._assert_stac_capable_collection_stack`` keeps
        working after the wrapper replaces the raw STAC driver in the
        plugin registry.

        Returns ``()`` when no inner advertises the marker — e.g. a
        deployment without the stac extra installed, or a wrapper config
        that explicitly omitted the ``collection_stac`` sidecar.  Callers
        that need to distinguish "STAC unavailable" from "STAC available
        but empty" should check for non-empty return.
        """
        for inner in self._default_inner_drivers:
            method = getattr(inner, "stac_metadata_columns", None)
            if method is not None:
                return tuple(method())
        return ()
