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

import logging
from typing import Any, ClassVar, Dict, List, Mapping, Optional, Type, Union

from dynastore.modules.storage.drivers.pg_sidecars.base import (
    SidecarConfig,
    SidecarConfigRegistry,
    SidecarProtocol,
)

logger = logging.getLogger(__name__)


class SidecarRegistry:
    """
    The SINGLE sidecar registry (Phase 3 Decision 3 — one registry).

    It serves two distinct sidecar families under one uniform
    registration/resolution contract:

    1. **Items-tier physical sidecars** (``geometries`` / ``attributes`` /
       ``item_metadata`` / ``stac_metadata``) — each maps a ``sidecar_type``
       to a :class:`SidecarProtocol` implementation. These own real per-
       collection PG tables (Hub-FK, partition keys, per-feature DDL). Use
       :meth:`register` / :meth:`get_sidecar`.

    2. **Catalog-tier metadata domain slices** (``catalog_core`` /
       ``catalog_stac``) — each maps a ``sidecar_type`` to a
       :class:`~dynastore.models.protocols.entity_store.CatalogStore` driver
       class. These route a metadata slice to an already-existing entity-store
       driver whose own column filter slices the payload (no per-collection
       DDL). Use :meth:`register_catalog_store` / :meth:`get_catalog_store_cls`
       / :meth:`default_catalog_sidecars`.

    The two families have DIFFERENT value types (``SidecarProtocol`` vs
    ``CatalogStore``) and DIFFERENT lifecycles, so they live in separate
    internal maps — but operators and extensions now reach BOTH through this
    one class instead of the former separate catalog-tier registry (folded in
    here). The items family looks sidecars up by their type identifier (e.g.
    'attributes', 'geometries') rather than config class type.
    """

    _registry: ClassVar[Dict[str, Type[SidecarProtocol]]] = {}

    # Catalog-tier domain slices (folded in from the former separate
    # catalog-tier registry). ``sidecar_type`` -> ``CatalogStore`` class.
    _catalog_registry: ClassVar[Dict[str, Type[Any]]] = {}
    _catalog_defaults_loaded: bool = False

    # Optional items-tier sidecars (e.g. 'stac_metadata') whose backing
    # extension has already been confirmed not-installed in this process.
    # `_ensure_defaults` runs on essentially every engine/config snapshot
    # rebuild (per catalog, per rebuild, per instance boot), so without this
    # cache a slim SCOPE re-attempts the import and re-logs the same
    # ImportError on every single call (#2988). This set makes the
    # "not installed" fact get logged once per sidecar name, per process.
    _unavailable_optional_sidecars: ClassVar[set] = set()

    @classmethod
    def _ensure_defaults(cls):
        """Initialize default sidecars with local imports to avoid circularity."""
        # Check if core sidecars are already registered to avoid wiping external ones
        if "geometries" not in cls._registry:
            from dynastore.modules.storage.drivers.pg_sidecars.geometries import GeometriesSidecar
            cls._registry["geometries"] = GeometriesSidecar

        if "attributes" not in cls._registry:
            from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
                FeatureAttributeSidecar,
            )
            cls._registry["attributes"] = FeatureAttributeSidecar

        if "item_metadata" not in cls._registry:
            from dynastore.modules.storage.drivers.pg_sidecars.item_metadata import (
                ItemMetadataSidecar,
            )
            cls._registry["item_metadata"] = ItemMetadataSidecar

        if (
            "stac_metadata" not in cls._registry
            and "stac_metadata" not in cls._unavailable_optional_sidecars
        ):
            stac_module = "dynastore.extensions.stac.stac_items_sidecar"
            try:
                from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar
                cls._registry["stac_metadata"] = StacItemsSidecar
                logger.debug("SidecarRegistry: Successfully registered 'stac_metadata' sidecar")
            except ModuleNotFoundError as e:
                # `e.name` is the deepest package/module the import machinery
                # failed to find, which is an ANCESTOR of `stac_module` (or
                # `stac_module` itself) when the extension simply isn't
                # installed on this image — e.g. a slim SCOPE reports
                # `e.name == "dynastore.extensions.stac"`, not the leaf
                # sidecar module. Only treat it as "genuinely missing" when
                # it's on that ancestor chain; anything else means some
                # OTHER module the (installed) extension imports internally
                # is broken, which is a real defect and must keep warning
                # loudly instead of being cached away.
                is_extension_missing = e.name is not None and (
                    stac_module == e.name or stac_module.startswith(e.name + ".")
                )
                if is_extension_missing:
                    cls._unavailable_optional_sidecars.add("stac_metadata")
                    logger.info(
                        "SidecarRegistry: 'stac_metadata' sidecar not available in "
                        f"this process, will not retry: {e}"
                    )
                else:
                    logger.warning(
                        f"SidecarRegistry: Failed to register 'stac_metadata' sidecar: {e}"
                    )
            except ImportError as e:
                logger.warning(
                    f"SidecarRegistry: Failed to register 'stac_metadata' sidecar: {e}"
                )

    @classmethod
    def get_sidecar(
        cls,
        config: Union[SidecarConfig, Mapping[str, Any]],
        lenient: bool = False,
    ) -> Optional[SidecarProtocol]:
        """
        Factory method to instantiate the correct SidecarProtocol implementation
        for a given configuration object.

        Accepts either a typed ``SidecarConfig`` instance or a mapping carrying a
        ``sidecar_type`` discriminator — the mapping form is coerced to the
        appropriate subclass via ``SidecarConfigRegistry``. Partially-hydrated
        configs (e.g. a DB round-trip that bypassed the container validator)
        therefore no longer crash the consumer.

        Uses the config's sidecar_type field to look up the implementation.

        If lenient=True, returns None if implementation is not registered.

        Note (#957/#974/#1126/#1168): validity SSOT is
        ``ItemsWritePolicy.validity`` (null-object ValiditySpec); its presence
        is the toggle. The PG driver sets its own fixed ``validity`` column on
        ``FeatureAttributeSidecarConfig.validity_column`` at ``ensure_storage``
        time (the policy never names a physical column), so the factory itself
        stays policy-agnostic.
        Sidecar ctors accept ``**_kwargs`` to absorb forward-compatible
        factory inputs.
        """
        cls._ensure_defaults()

        if isinstance(config, Mapping):
            if "sidecar_type" not in config:
                raise TypeError(
                    f"SidecarRegistry.get_sidecar: mapping input missing "
                    f"'sidecar_type' discriminator. Keys: {sorted(config.keys())}"
                )
            config_cls = SidecarConfigRegistry.resolve_config_class(config["sidecar_type"])
            config = config_cls.model_validate(dict(config))
        elif not isinstance(config, SidecarConfig):
            raise TypeError(
                f"SidecarRegistry.get_sidecar: expected SidecarConfig or Mapping, "
                f"got {type(config).__name__}"
            )

        sidecar_type = config.sidecar_type
        sidecar_cls = cls._registry.get(sidecar_type)

        if not sidecar_cls:
            if lenient:
                return None
            raise ValueError(
                f"No sidecar implementation registered for sidecar_type: {sidecar_type}. "
                f"Available types: {list(cls._registry.keys())}"
            )

        return sidecar_cls(config)

    @classmethod
    def register(cls, sidecar_type_id: str, impl_cls: Type[SidecarProtocol]):
        """
        Register a new sidecar type dynamically.
        
        Args:
            sidecar_type_id: The type identifier (e.g., 'metrics', 'custom')
            impl_cls: The SidecarProtocol implementation class
        """
        cls._registry[sidecar_type_id] = impl_cls
    
    @classmethod
    def get_available_types(cls) -> list[str]:
        """Returns list of registered sidecar type IDs."""
        cls._ensure_defaults()
        return list(cls._registry.keys())

    @classmethod
    def get_injected_sidecar_configs(cls, context: Dict[str, Any]) -> list[SidecarConfig]:
        """
        Aggregates default configurations from all registered sidecars
        that wish to be injected based on the context.
        """
        cls._ensure_defaults()
        injected = []
        for sidecar_cls in cls._registry.values():
            config = sidecar_cls.get_default_config(context)
            if config:
                injected.append(config)
        return injected

    @classmethod
    def clear_registry(cls) -> None:
        """Clears all registered sidecars. Useful for test isolation.

        Note: defaults are re-registered lazily on next `_ensure_defaults()` call.
        Also resets the optional-sidecar "not available" cache so a test that
        monkeypatches its way around the missing extension gets a clean retry.
        """
        logger.debug("SidecarRegistry cleared.")
        cls._registry.clear()
        cls._unavailable_optional_sidecars.clear()

    # ------------------------------------------------------------------
    # Catalog-tier domain slices (folded from the former catalog-tier registry)
    # ------------------------------------------------------------------

    @classmethod
    def _ensure_catalog_defaults(cls) -> None:
        """Register the built-in catalog-tier metadata slices.

        CORE is always available (same module tree); STAC uses try-import so
        a deployment without the stac extra silently omits ``catalog_stac``
        (mirrors the items ``stac_metadata`` opaque-fallback posture).
        """
        if cls._catalog_defaults_loaded:
            return
        cls._catalog_defaults_loaded = True

        from dynastore.modules.storage.drivers.core_postgresql import (
            CatalogCorePostgresqlDriver,
        )
        cls._catalog_registry.setdefault("catalog_core", CatalogCorePostgresqlDriver)

        try:
            from dynastore.modules.stac.drivers.postgresql import (
                CatalogStacPostgresqlDriver,
            )
            cls._catalog_registry.setdefault(
                "catalog_stac", CatalogStacPostgresqlDriver,
            )
        except ImportError as exc:
            logger.debug(
                "SidecarRegistry: catalog_stac sidecar unavailable (%s)", exc,
            )

    @classmethod
    def register_catalog_store(cls, sidecar_type: str, driver_cls: Type[Any]) -> None:
        """Register a catalog-tier metadata sidecar type (``sidecar_type`` ->
        ``CatalogStore`` class) — used by extensions contributing a new
        catalog-metadata domain slice.
        """
        cls._catalog_registry[sidecar_type] = driver_cls

    @classmethod
    def get_catalog_store_cls(cls, sidecar_type: str) -> Optional[Type[Any]]:
        """Resolve a catalog-tier ``sidecar_type`` to its ``CatalogStore`` class."""
        cls._ensure_catalog_defaults()
        return cls._catalog_registry.get(sidecar_type)

    @classmethod
    def has_catalog_store(cls, sidecar_type: str) -> bool:
        cls._ensure_catalog_defaults()
        return sidecar_type in cls._catalog_registry

    @classmethod
    def default_catalog_sidecars(cls) -> List[Any]:
        """Built-in default catalog-tier slice list — CORE only.

        The STAC catalog slice (``catalog_stac``) is NOT included by default.
        It is added at runtime by ``CatalogPostgresqlDriver._resolve_sidecars_for_catalog``
        when the scope's ``StacStorageConfig`` has catalog-tier enabled AND
        includes PG storage.  Absent a ``StacPreset``, no STAC slice is
        materialized (opt-in default).
        """
        cls._ensure_catalog_defaults()
        from dynastore.modules.storage.drivers.catalog_postgresql import (
            CatalogCoreSidecarConfig,
        )
        return [CatalogCoreSidecarConfig()]

    @classmethod
    def clear_catalog_registry(cls) -> None:
        """Test-isolation hook for the catalog-tier slice registry."""
        cls._catalog_registry.clear()
        cls._catalog_defaults_loaded = False
