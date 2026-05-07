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

from typing import Any, Dict, Mapping, Optional, Type, Union

from dynastore.modules.storage.drivers.pg_sidecars.base import (
    SidecarConfig,
    SidecarConfigRegistry,
    SidecarProtocol,
)


class SidecarRegistry:
    """
    Registry for sidecar implementations.
    Maps sidecar_type_id strings to their corresponding SidecarProtocol implementation classes.
    
    This enables protocol-driven discovery where sidecars are looked up by their
    type identifier (e.g., 'attributes', 'geometries') rather than config class type.
    """

    _registry: Dict[str, Type[SidecarProtocol]] = {}

    @classmethod
    def _ensure_defaults(cls):
        """Initialize default sidecars with local imports to avoid circularity."""
        import logging
        logger = logging.getLogger(__name__)

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

        if "stac_metadata" not in cls._registry:
            try:
                from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar
                cls._registry["stac_metadata"] = StacItemsSidecar
                logger.debug("SidecarRegistry: Successfully registered 'stac_metadata' sidecar")
            except ImportError as e:
                logger.debug(f"SidecarRegistry: Failed to register 'stac_metadata' sidecar: {e}")
                pass  # STAC extension not installed

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
        """
        import logging
        logging.getLogger(__name__).debug("SidecarRegistry cleared.")
        cls._registry.clear()
