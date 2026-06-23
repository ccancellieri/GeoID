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

"""Central registry for :class:`~dynastore.models.protocols.physical_names.PhysicalNameResolver`.

Provides three public functions::

    register_physical_name_resolver(resolver)
    get_physical_name_resolver(backend) -> PhysicalNameResolver
    physical_name(backend, kind, *, catalog_physical_id, ...) -> str

Design rationale
----------------
Physical name resolvers are stateless, singleton-like objects that need no
startup hooks, no plugin lifecycle, and no lazy discovery.  The
``get_protocols`` / ``register_plugin`` mechanism in
:mod:`~dynastore.modules.storage.driver_registry` builds an L0 cache from the
plugin registry and is appropriate for heavyweight driver instances.  A small
explicit ``dict`` registry is the right fit here: it is simpler, has O(1)
lookup, and does not couple name-mapping to the plugin lifecycle.

The five built-in resolvers (``pg``, ``gcs``, ``pubsub``, ``es``,
``es_private``) are registered at the bottom of this module so they are
available as soon as the module is imported.
"""

from __future__ import annotations

import threading
from typing import Dict, Optional

from dynastore.models.protocols.physical_names import PhysicalNameResolver, ResourceKind

# ---------------------------------------------------------------------------
# Module-level registry
# ---------------------------------------------------------------------------

_registry: Dict[str, PhysicalNameResolver] = {}
_lock = threading.Lock()


def register_physical_name_resolver(resolver: PhysicalNameResolver) -> None:
    """Register ``resolver`` under its ``backend`` identifier.

    Raises :exc:`TypeError` if ``resolver`` does not implement
    :class:`~dynastore.models.protocols.physical_names.PhysicalNameResolver`.
    Raises :exc:`ValueError` if a resolver for the same backend is already
    registered (use :func:`unregister_physical_name_resolver` first to replace
    one intentionally).
    """
    if not isinstance(resolver, PhysicalNameResolver):
        raise TypeError(
            f"register_physical_name_resolver: {type(resolver)!r} does not "
            "implement the PhysicalNameResolver protocol."
        )
    backend = type(resolver).backend
    with _lock:
        if backend in _registry:
            raise ValueError(
                f"register_physical_name_resolver: a resolver for backend "
                f"{backend!r} is already registered ({type(_registry[backend]).__name__}). "
                "Call unregister_physical_name_resolver() first to replace it."
            )
        _registry[backend] = resolver


def unregister_physical_name_resolver(backend: str) -> None:
    """Remove the resolver for ``backend`` from the registry.

    A no-op if the backend was not registered.
    """
    with _lock:
        _registry.pop(backend, None)


def get_physical_name_resolver(backend: str) -> PhysicalNameResolver:
    """Return the resolver registered for ``backend``.

    Raises :exc:`KeyError` with a descriptive message when the backend is
    unknown.
    """
    resolver = _registry.get(backend)
    if resolver is None:
        registered = sorted(_registry)
        raise KeyError(
            f"No PhysicalNameResolver registered for backend {backend!r}. "
            f"Registered backends: {registered}"
        )
    return resolver


def physical_name(
    backend: str,
    kind: ResourceKind,
    *,
    catalog_physical_id: str,
    collection_physical_id: Optional[str] = None,
    prefix: Optional[str] = None,
) -> str:
    """Resolve the concrete resource name for ``(backend, kind)``.

    Convenience dispatcher: fetches the resolver for ``backend`` from the
    registry, verifies that ``kind`` is in ``resolver.supported_kinds``, and
    delegates to :meth:`~PhysicalNameResolver.physical_name`.

    Parameters
    ----------
    backend:
        Short backend identifier, e.g. ``"pg"``, ``"es"``, ``"gcs"``.
    kind:
        The class of resource whose name is needed.
    catalog_physical_id:
        Immutable physical identifier of the catalog.  Must never be the
        mutable logical/user-visible catalog id.
    collection_physical_id:
        Immutable physical identifier of the collection, when applicable.
    prefix:
        Optional platform-wide prefix forwarded to the resolver.

    Raises
    ------
    KeyError
        When ``backend`` is not registered.
    ValueError
        When the registered resolver for ``backend`` does not support ``kind``.
    """
    resolver = get_physical_name_resolver(backend)
    if kind not in type(resolver).supported_kinds:
        raise ValueError(
            f"Resolver for backend {backend!r} ({type(resolver).__name__}) does "
            f"not support ResourceKind.{kind.name}. "
            f"Supported: {sorted(k.name for k in type(resolver).supported_kinds)}"
        )
    return resolver.physical_name(
        kind,
        catalog_physical_id=catalog_physical_id,
        collection_physical_id=collection_physical_id,
        prefix=prefix,
    )


# ---------------------------------------------------------------------------
# Built-in resolver registrations
# ---------------------------------------------------------------------------

def _register_builtin_resolvers() -> None:
    """Register the five built-in PhysicalNameResolver implementations.

    Called once at module import.  Each import is isolated — the resolvers are
    only instantiated here, not at startup of the full application.
    """
    from dynastore.modules.storage.drivers.postgresql_physical_names import PgPhysicalNames
    from dynastore.modules.gcp.physical_names import GcsPhysicalNames, PubSubPhysicalNames
    from dynastore.modules.elasticsearch.physical_names import (
        EsPhysicalNames,
        EsPrivatePhysicalNames,
    )

    for resolver in (
        PgPhysicalNames(),
        GcsPhysicalNames(),
        PubSubPhysicalNames(),
        EsPhysicalNames(),
        EsPrivatePhysicalNames(),
    ):
        register_physical_name_resolver(resolver)


_register_builtin_resolvers()
