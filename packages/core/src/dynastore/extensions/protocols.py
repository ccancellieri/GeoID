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

from typing import Protocol, Optional, Any, TYPE_CHECKING, runtime_checkable, AsyncGenerator, List, Tuple

if TYPE_CHECKING:
    from fastapi import FastAPI, APIRouter
else:
    FastAPI = Any
    APIRouter = Any

from contextlib import asynccontextmanager
from dynastore.modules.protocols import HasConfigService
from dynastore.tools.plugin import ProtocolPlugin


class ExtensionProtocol(ProtocolPlugin["FastAPI"], HasConfigService):
    """
    Defines the contract for a DynaStore FastAPI Web Extension.

    Each Extension is a ``ProtocolPlugin[FastAPI]``, meaning:
    - Its ``lifespan(app_state: FastAPI)`` receives the FastAPI application.
    - Its ``priority`` is compared *only* against other extensions (same category).
    - ``is_available()`` controls optional extension availability.

    ---
    Conventional Members (all optional, discovered at runtime):

    - ``router: Optional[APIRouter]`` — auto-mounted to the FastAPI app.
    - ``configure_app(app: FastAPI)`` — early config hook (middleware, etc.).
    - ``lifespan(app: FastAPI)`` — async context manager for resource lifecycle.
    - ``readme.md`` — if present and a router exists, a docs endpoint is added.
    ---

    Example (Extension adding Middleware):
    ```python
    from fastapi.middleware.gzip import GZipMiddleware

    class GzipExtension(ExtensionProtocol):
        def configure_app(self, app: FastAPI):
            app.add_middleware(GZipMiddleware, minimum_size=1000)
    ```
    """

    def configure_app(self, app: "FastAPI") -> None:
        """
        Optional early configuration hook for the FastAPI app.
        """
        ...

    @asynccontextmanager
    async def lifespan(self, app_state: "FastAPI") -> AsyncGenerator[None, None]:
        """
        Async context manager for managing extension resources.
        ``app_state`` is the FastAPI application instance.
        """
        yield

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-register every concrete subclass in _DYNASTORE_EXTENSIONS on import."""
        super().__init_subclass__(**kwargs)
        # Skip classes that still have unimplemented abstract methods
        if getattr(cls, '__abstractmethods__', None):
            return
        try:
            from dynastore.extensions.registry import _register_extension
            _register_extension(cls)
        except Exception as e:
            import logging
            logging.getLogger(__name__).debug(
                f"ExtensionProtocol.__init_subclass__: skipping auto-registration of {cls.__name__}: {e}"
            )

    @classmethod
    def get_name(cls) -> str:
        """
        Returns the registered name of the extension.
        """
        return getattr(cls, "_registered_name", "unregistered_extension")
