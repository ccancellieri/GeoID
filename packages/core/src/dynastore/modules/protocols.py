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

# dynastore/modules/protocols.py

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Protocol, Tuple, Type, TypeVar

from dynastore.tools.plugin import ProtocolPlugin

if TYPE_CHECKING:
    from dynastore.models.protocols.configs import ConfigsProtocol

logger = logging.getLogger(__name__)

_P = TypeVar("_P")


class HasConfigService(Protocol):
    """
    Protocol for any component (Module, Extension, Task) that provides access
    to the centralized Configuration Manager.
    """

    def get_config_service(self) -> Optional["ConfigsProtocol"]:
        """Returns the configuration manager instance."""
        ...

    def get_protocol(self, protocol_type: Type[_P]) -> Optional[_P]:
        """
        Discovers and caches a protocol implementation at the instance level.
        Provides a standardized way for modules and extensions to access other protocols.
        """
        cache: dict[type, Any] = getattr(self, "_protocol_cache", {})
        if not cache:
            object.__setattr__(self, "_protocol_cache", cache)
        if protocol_type not in cache:
            from dynastore.tools.discovery import get_protocol
            instance = get_protocol(protocol_type)
            if not instance:
                logger.debug(
                    f"Protocol {getattr(protocol_type, '__name__', protocol_type)} not found at this stage."
                )
                return None
            cache[protocol_type] = instance
        return cache[protocol_type]


class ModuleProtocol(ProtocolPlugin[object], HasConfigService):
    """
    Defines the contract for a DynaStore foundational Module.

    Modules are core, reusable services that provide foundational capabilities
    (like database connections or configuration management) to the rest of the
    application. They are independent of FastAPI.

    Each Module is a ``ProtocolPlugin[object]``, meaning:
    - Its ``lifespan(app_state: object)`` is the single lifecycle hook.
    - Its ``priority`` is compared *only* against other modules (same category).
    - ``is_available()`` controls whether it is returned by protocol discovery.

    Subclasses are **automatically registered** in ``_DYNASTORE_MODULES`` when
    their defining Python module is imported (no decorator required).

    ---
    Example:
    ```python
    class MyModule(ModuleProtocol):

        @asynccontextmanager
        async def lifespan(self, app_state: object):
            app_state.my_service = MyService()
            try:
                yield
            finally:
                del app_state.my_service
    ```
    """

    #: Protocols this module *delegates* to inner services registered during
    #: its async ``lifespan()`` (via ``register_plugin(svc)``). The post-
    #: lifespan audit walks every live module's MRO + this tuple to derive
    #: the set of expected protocols for the current SCOPE — so a module
    #: that delegates ``CatalogsProtocol`` to an inner ``CatalogService``
    #: should declare ``provides_extra = (CatalogsProtocol,)``. Protocols
    #: that the module class implements *directly* (i.e. that appear in
    #: its MRO) need not be repeated here — the audit picks them up from
    #: the MRO walk.
    provides_extra: ClassVar[Tuple[type, ...]] = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-register every concrete subclass when it is first defined.

        Skipped for classes that live deeper than ``dynastore.modules.<X>.<file>``
        (e.g. drivers under ``dynastore.modules.storage.drivers.postgresql``):
        those are *always* registered through the entry-point pass in
        ``discover_modules`` under their explicit entry-point key. Auto-
        registering them via the path-derivation rule in ``_register_module``
        would collide on the bare segment after ``modules`` (every storage
        driver derives to ``"storage"``), emitting a noisy
        "Module 'storage' is already registered with a different class"
        warning per driver on every boot — without changing the final state
        because entry-point discovery re-keys them anyway.
        """
        super().__init_subclass__(**kwargs)
        # Skip abstract classes (those that still have unimplemented abstract methods)
        if getattr(cls, '__abstractmethods__', None):
            return
        # Skip nested driver classes (entry-point-discovered, see docstring above).
        # ``dynastore.modules.catalog.catalog_module`` (4 segments) → register;
        # ``dynastore.modules.storage.drivers.postgresql`` (5+ segments) → skip.
        try:
            parts = cls.__module__.split('.')
            modules_idx = parts.index('modules')
            if len(parts) - modules_idx > 3:
                return
        except ValueError:
            # Class lives outside ``dynastore.modules.*`` — let
            # ``_register_module`` fall back to ``cls.__name__``.
            pass
        # Defer import to avoid circular imports at module load time
        try:
            from dynastore.modules import _register_module
            _register_module(cls)
        except Exception as e:
            logger.debug(f"ModuleProtocol.__init_subclass__: skipping auto-registration of {cls.__name__}: {e}")

    @classmethod
    def get_name(cls) -> str:
        """
        Returns the registered name of the module.
        """
        return getattr(cls, "_registered_name", "unregistered_module")
