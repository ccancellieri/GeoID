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

"""
ProtocolPlugin — the unified base class for all lifecycle-managed plugins.

Design principles:
  - ``ProtocolPlugin[AppStateT]`` is a Generic base, so each plugin family
    can precisely type the ``app_state`` received in ``lifespan``.
  - Any direct (or indirect) subclass of ``ProtocolPlugin`` forms an open
    **plugin category**. Priority is meaningful only among instances of the
    *same* category (same immediate ProtocolPlugin subclass).
  - ``lifespan`` is the single lifecycle hook. No ``initialize``,
    ``startup`` or ``shutdown`` methods should be added on top of it.
  - ``is_available()`` allows protocol discovery to skip unavailable plugins
    gracefully (e.g. optional GCP driver when not running on GCP).

Usage example::

    from dynastore.tools.plugin import ProtocolPlugin

    class AbstractStatsDriver(ProtocolPlugin["AppState"]):
        priority: int = 0

        @asynccontextmanager
        async def lifespan(self, app_state: "AppState"):
            await self._start()
            try:
                yield
            finally:
                await self._stop()

    class PostgresStatsDriver(AbstractStatsDriver):
        priority: int = 10   # compared only to other AbstractStatsDriver subclasses
        ...
"""

import abc
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Generic, TypeVar, Protocol

logger = logging.getLogger(__name__)

AppStateT = TypeVar("AppStateT")


class ProtocolPlugin(abc.ABC, Generic[AppStateT]):
    """
    Abstract base class for all lifecycle-managed plugins (modules, extensions,
    tasks, drivers, storage backends …).

    Subclass this (directly or indirectly) to define a **plugin category**.
    Instances are discovered and sorted by ``priority`` **within their category**
    (i.e. within the direct ``ProtocolPlugin`` subclass they belong to).
    """

    priority: int = 0

    def is_available(self) -> bool:
        """
        Return ``True`` if this plugin can currently provide its service.

        Override to guard against missing optional dependencies, unconfigured
        credentials, etc.  The discovery layer will silently skip unavailable
        plugins when resolving a protocol.
        """
        return True

    @asynccontextmanager
    async def lifespan(self, app_state: AppStateT) -> AsyncGenerator[None, None]:
        """
        Async context manager that owns the plugin's full lifecycle.

        Subclasses should perform startup work **before** the ``yield`` and
        cleanup work in the ``finally`` block **after** it.  The calling
        framework enters all plugin lifespans (in priority order, per category)
        via an ``AsyncExitStack`` so that teardown happens in reverse order
        automatically.

        The default implementation is a no-op, making it safe to call even on
        plugins that have no lifecycle requirements.
        """
        yield
