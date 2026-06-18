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

"""Composition driver for PG-backed catalog storage.

Catalog-tier sibling of
:mod:`dynastore.modules.storage.drivers.collection_postgresql`.
Wraps the existing per-domain catalog-tier PG ``CatalogStore`` drivers
(:class:`CatalogCorePostgresqlDriver` plus the optional
:class:`CatalogStacPostgresqlDriver` from the stac module) and fans
:class:`CatalogStore` CRUD across them at write/read/delete time.

PR 1e step 3c lands this wrapper with its entry-point active from day
one, mirroring the 3b cutover for the collection tier.  The two raw
catalog drivers' entry-points are removed in the same change so
``get_protocols(CatalogStore)`` returns one PG-tier
implementation (the wrapper) instead of two raw drivers.

The wrapper exposes ``stac_metadata_columns()`` from day one so it
structurally satisfies :class:`extensions.stac.protocols.StacCatalogEntityStoreCapability`
iff a STAC inner is loaded — closing the regression that 3b followed
up on (`747477d`).  ``stac_service._has_stac`` already uses the
non-empty-columns semantic, so an empty STAC sidecar correctly
surfaces as "STAC unavailable".
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
    Union,
)

from pydantic import BaseModel, Discriminator, Field, model_validator

from dynastore.models.protocols.entity_store import (
    CatalogStore,
    EntityStoreCapability,
)
from dynastore.models.protocols.teardown_lane import TeardownLane
from dynastore.models.protocols.typed_driver import (
    TypedDriver,
    _PluginDriverConfig,
)
from dynastore.models.mutability import Immutable
from dynastore.tools.cache import cached

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sidecar discriminated union — mirrors the collection-tier wrapper's shape
# ---------------------------------------------------------------------------


class _PgCatalogSidecarConfigBase(BaseModel):
    """Common base for catalog-metadata PG sidecar configs.

    Subclasses pin ``sidecar_type`` as a ``Literal[...]`` so the
    discriminated union round-trips cleanly through
    ``model_dump(exclude_unset=True) → model_validate``.
    """

    sidecar_type: str

    @model_validator(mode="after")
    def _pin_discriminator_as_set(self) -> "_PgCatalogSidecarConfigBase":
        """Mark ``sidecar_type`` as explicitly set so ``exclude_unset=True`` keeps it.

        Mirrors the collection-tier wrapper's validator (which itself
        mirrors the items-tier ``SidecarConfig._retain_sidecar_type_on_dump``
        fix from commit ``96fbf8c``).
        """
        self.__pydantic_fields_set__.add("sidecar_type")
        return self


class CatalogCoreSidecarConfig(_PgCatalogSidecarConfigBase):
    """Routes the CORE catalog metadata slice (``title``, ``description``,
    ``keywords``, ``license``, ``extra_metadata``) to
    :class:`CatalogCorePostgresqlDriver`.
    """

    sidecar_type: Literal["catalog_core"] = "catalog_core"


class CatalogStacSidecarConfig(_PgCatalogSidecarConfigBase):
    """Routes the STAC catalog metadata slice (``stac_version``,
    ``stac_extensions``, ``conforms_to``, ``links``, ``assets``) to
    :class:`CatalogStacPostgresqlDriver` from the stac module.

    Resolved via try-import in the unified :class:`SidecarRegistry`
    (catalog-tier slice family) — a deployment without the stac extra
    installed will see this entry's resolution log a single warning and skip
    the slice (no crash).
    """

    sidecar_type: Literal["catalog_stac"] = "catalog_stac"


_PgCatalogSidecarConfig = Annotated[
    Union[
        CatalogCoreSidecarConfig,
        CatalogStacSidecarConfig,
    ],
    Discriminator("sidecar_type"),
]


# ---------------------------------------------------------------------------
# Catalog-tier slice registration (Phase 3 Decision 3 — ONE registry).
#
# The former separate catalog-tier registry is folded into the single
# :class:`SidecarRegistry`; the catalog-tier slice family lives behind its
# ``register_catalog_store`` / ``get_catalog_store_cls`` /
# ``default_catalog_sidecars`` methods. Module-level access here goes through
# that one registry.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Wrapper config + driver
# ---------------------------------------------------------------------------


class CatalogPostgresqlDriverConfig(_PluginDriverConfig):
    """Configuration for the PG-backed composition ``CatalogStore`` driver.

    ``sidecars`` is the typed list of catalog-tier domain slices the
    wrapper will fan CRUD across.  Empty list → wrapper falls back to
    :meth:`SidecarRegistry.default_catalog_sidecars`
    (``[catalog_core, catalog_stac if installed]``).

    Marked ``Immutable`` because changing the active sidecar set after
    rows exist would orphan domain slices in ``catalog.catalog_metadata_*``.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "drivers")
    _freeze_at: ClassVar[Optional[str]] = "catalog"

    required_engine_class: ClassVar[str] = "postgresql_engine"


    sidecars: Immutable[List[_PgCatalogSidecarConfig]] = Field(
        default_factory=list,
        description=(
            "Catalog metadata sidecar configs — discriminated union on "
            "`sidecar_type`.  Empty → registry default "
            "(`catalog_core` always, `catalog_stac` "
            "if the stac extra is installed).  Immutable once set — "
            "changing it would orphan rows in the per-domain "
            "``catalog.catalog_metadata_*`` tables.  Honored at runtime "
            "via per-catalog ``ConfigsProtocol`` fetch with TTL=60s cache "
            "(PR 1e step 4); apply handler bumps the cache for instant "
            "effect on operator override."
        ),
    )


async def _on_apply_catalog_pg_driver_config(
    config: Any,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Invalidate the wrapper's per-catalog sidecar cache so the
    operator's submitted ``sidecars`` override takes effect immediately.
    Sister handler to the collection tier's
    ``_on_apply_collection_pg_driver_config``.
    """
    if not isinstance(config, CatalogPostgresqlDriverConfig):
        return
    from dynastore.tools.discovery import get_protocol

    wrapper = get_protocol(CatalogPostgresqlDriver)
    if wrapper is None:
        return
    try:
        wrapper._resolve_sidecars_for_catalog.cache_clear()  # type: ignore[attr-defined]
    except Exception as exc:
        logger.debug(
            "CatalogPostgresqlDriver: cache_clear failed (%s) — TTL "
            "expiry will still propagate the override within ~60s", exc,
        )
        return
    if config.sidecars:
        logger.info(
            "CatalogPostgresqlDriver: sidecar cache invalidated after "
            "config apply at %s scope; new sidecar set %s takes effect on "
            "next metadata operation.",
            f"catalog '{catalog_id}'" if catalog_id else "platform",
            [getattr(s, "sidecar_type", "?") for s in config.sidecars],
        )


CatalogPostgresqlDriverConfig.register_apply_handler(
    _on_apply_catalog_pg_driver_config,
)


class CatalogPostgresqlDriver(TypedDriver[CatalogPostgresqlDriverConfig]):
    """Composition driver: fans CatalogStore CRUD across the
    configured PG catalog-metadata sidecars (``catalog_core``,
    optionally ``catalog_stac``, plus any extension-contributed
    sidecar registered via
    :meth:`SidecarRegistry.register_catalog_store`).

    The wrapper itself owns no SQL — every method delegates to the
    inner drivers, each of which already filters the payload to its
    own column set via ``_PgCatalogCoreBase._upsert_catalog_row``.
    """

    # Catalog metadata rows are in PG and are cleaned up inline with the
    # delete transaction; the async cascade must not re-drop them.
    teardown_lane: ClassVar[TeardownLane] = TeardownLane.INLINE_TXN

    capabilities: ClassVar[FrozenSet[str]] = frozenset({
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SOFT_DELETE,
        EntityStoreCapability.QUERY_FALLBACK_SOURCE,
    })

    def _resolve_inner_drivers(
        self,
        sidecars: Optional[List[_PgCatalogSidecarConfigBase]] = None,
    ) -> List[CatalogStore]:
        """Resolve the configured ``sidecars`` list to instantiated inner
        ``CatalogStore`` driver instances.  Production paths
        call this with ``sidecars=None`` and so go through
        :attr:`_default_inner_drivers`.
        """
        from dynastore.modules.storage.drivers.pg_sidecars.registry import (
            SidecarRegistry,
        )
        if not sidecars:
            sidecars = SidecarRegistry.default_catalog_sidecars()
        out: List[CatalogStore] = []
        for cfg in sidecars:
            sc_type = getattr(cfg, "sidecar_type", None)
            if sc_type is None:
                logger.warning(
                    "CatalogPostgresqlDriver: sidecar entry missing "
                    "`sidecar_type` discriminator — skipping",
                )
                continue
            cls = SidecarRegistry.get_catalog_store_cls(sc_type)
            if cls is None:
                logger.warning(
                    "CatalogPostgresqlDriver: sidecar %r not registered "
                    "(module not installed?) — skipping",
                    sc_type,
                )
                continue
            out.append(cls())
        return out

    @cached_property
    def _default_inner_drivers(self) -> List[CatalogStore]:
        """Cached INSTALL-level resolution — used by code paths that have
        no ``catalog_id`` in scope (``is_available``,
        ``stac_metadata_columns``).

        This is the install-level capability surface, distinct from
        per-catalog materialization: it includes the ``catalog_stac`` inner
        whenever the stac extra is installed (the slice is registered), so
        ``stac_service._has_stac`` correctly reports STAC as *available* for
        a deployment that ships the extra — even though STAC sidecars are
        opt-in per catalog via ``StacStorageConfig`` / ``StacPreset``.  The
        catalog-scoped read/write fan-out goes through
        :meth:`_resolve_sidecars_for_catalog`, which stays opt-in (core-only
        until a ``StacStorageConfig`` enables the catalog tier).
        """
        from dynastore.modules.storage.drivers.pg_sidecars.registry import (
            SidecarRegistry,
        )
        sidecars: List[_PgCatalogSidecarConfigBase] = list(
            SidecarRegistry.default_catalog_sidecars()
        )
        if SidecarRegistry.has_catalog_store("catalog_stac"):
            sidecars.append(CatalogStacSidecarConfig())
        return self._resolve_inner_drivers(sidecars)

    @cached(
        maxsize=128, ttl=60, jitter=5,
        namespace="catalog_pg_wrapper_sidecars",
        ignore=["self", "db_resource"],
        distributed=False,
    )
    async def _resolve_sidecars_for_catalog(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> List[CatalogStore]:
        """Resolve the inner driver list for ``catalog_id`` honoring any
        operator-submitted ``sidecars`` override at the catalog or
        platform scope.  Mirrors the collection wrapper's same-named
        method — TTL=60s + jitter, apply handler invalidates for instant
        effect, fetch failure falls back to registry default.

        When no explicit ``sidecars`` override is present, builds from
        CORE default plus the ``catalog_stac`` slice when the scope's
        ``StacStorageConfig`` has catalog-tier enabled AND PG storage.
        """
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry
        from dynastore.tools.discovery import get_protocol

        sidecars: Optional[List[_PgCatalogSidecarConfigBase]] = None
        configs = get_protocol(ConfigsProtocol)
        if configs is not None:
            try:
                cfg = await configs.get_config(
                    CatalogPostgresqlDriverConfig,
                    catalog_id=catalog_id,
                )
            except Exception as exc:
                logger.warning(
                    "CatalogPostgresqlDriver: config fetch failed for "
                    "catalog %r (%s) — falling back to registry default",
                    catalog_id, exc,
                )
                cfg = None
            if cfg is not None and cfg.sidecars:
                sidecars = list(cfg.sidecars)

        if sidecars is None:
            # No explicit override — build from CORE default + optional STAC
            # slice when StacStorageConfig signals catalog-tier + PG.
            sidecars = list(SidecarRegistry.default_catalog_sidecars())
            if configs is not None:
                try:
                    from dynastore.modules.stac.stac_storage_config import (
                        StacStorageConfig,
                        catalog_stac_enabled,
                        pg_stac,
                    )
                    stac_cfg = await configs.get_config(
                        StacStorageConfig,
                        catalog_id=catalog_id,
                    )
                    if (
                        isinstance(stac_cfg, StacStorageConfig)
                        and catalog_stac_enabled(stac_cfg.stac_level)
                        and pg_stac(stac_cfg.stac_storage)
                        and SidecarRegistry.has_catalog_store("catalog_stac")
                    ):
                        sidecars.append(CatalogStacSidecarConfig())
                except Exception as exc:
                    logger.debug(
                        "CatalogPostgresqlDriver: StacStorageConfig fetch "
                        "failed for catalog %r (%s) — no stac slice added",
                        catalog_id, exc,
                    )

        return self._resolve_inner_drivers(sidecars)

    async def is_available(self) -> bool:
        for inner in self._default_inner_drivers:
            if await inner.is_available():
                return True
        return False

    async def get_catalog_metadata(
        self,
        catalog_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fan-in: read from each inner, shallow-merge slices.  Returns
        ``None`` only when every inner returned ``None``.  Per-inner
        failures degrade to ``None`` for that slice, mirroring the
        existing ``catalog_router`` _safe_get semantics.
        """
        merged: Dict[str, Any] = {}
        inners = await self._resolve_sidecars_for_catalog(
            catalog_id, db_resource=db_resource,
        )
        for inner in inners:
            try:
                slice_ = await inner.get_catalog_metadata(
                    catalog_id, context=context, db_resource=db_resource,
                )
            except Exception as exc:
                logger.warning(
                    "CatalogPostgresqlDriver: inner %s.get_catalog_metadata "
                    "failed: %s", type(inner).__name__, exc,
                )
                continue
            if slice_:
                merged.update(slice_)
        return merged or None

    async def upsert_catalog_metadata(
        self,
        catalog_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Fan-out: hand the full payload to each inner driver.  Each
        inner filters to its own ``_columns`` and no-ops if the filtered
        slice is empty (existing ``_PgCatalogCoreBase`` invariant).
        """
        inners = await self._resolve_sidecars_for_catalog(
            catalog_id, db_resource=db_resource,
        )
        for inner in inners:
            await inner.upsert_catalog_metadata(
                catalog_id, metadata, db_resource=db_resource,
            )

    async def delete_catalog_metadata(
        self,
        catalog_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        inners = await self._resolve_sidecars_for_catalog(
            catalog_id, db_resource=db_resource,
        )
        for inner in inners:
            await inner.delete_catalog_metadata(
                catalog_id, soft=soft, db_resource=db_resource,
            )

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """No per-catalog driver config today — same default-empty as inners."""
        return None

    async def ensure_storage(
        self, catalog_id: Optional[str] = None, **kwargs: Any
    ) -> None:
        """No-op: inner PG catalog tables are created out-of-band by the
        catalog-provisioning DDL (same as each inner driver)."""
        return None

    async def drop_storage(
        self, catalog_id: str, *, soft: bool = False
    ) -> None:
        """Fail-soft fan-out of the lifecycle drop across inner drivers.

        Symmetric to :meth:`ensure_storage`.  The per-tenant schema backing
        the inner catalog-metadata tables is dropped out-of-band when the
        catalog is deprovisioned, so every inner is a no-op today; this fans
        to any inner that grows a real ``drop_storage`` and keeps the wrapper
        structurally conformant with the ``CatalogStore`` lifecycle contract.
        """
        for inner in self._default_inner_drivers:
            drop = getattr(inner, "drop_storage", None)
            if drop is not None:
                await drop(catalog_id, soft=soft)

    def stac_metadata_columns(self) -> Tuple[str, ...]:
        """Forward the STAC capability marker through to the first inner
        driver that exposes it.

        Defining this method makes the wrapper structurally satisfy
        :class:`extensions.stac.protocols.StacCatalogEntityStoreCapability`
        iff a STAC inner is loaded.  Returns ``()`` when no inner
        advertises the marker; ``stac_service._has_stac`` treats the
        empty tuple as "STAC unavailable" so the catalog-tier hard-reject
        at ``_assert_stac_capable_collection_stack`` still fires correctly
        in stac-less deployments.
        """
        for inner in self._default_inner_drivers:
            method = getattr(inner, "stac_metadata_columns", None)
            if method is not None:
                return tuple(method())
        return ()
