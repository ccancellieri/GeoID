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

"""PhysicalNameResolver protocol and ResourceKind enum.

Abstracts the mapping from a resolved ``physical_id`` to the concrete
names of the backend resources (PG schema, items table, GCS bucket, …)
a storage backend manages for a catalog or collection.

Callers resolve ``physical_id`` ONCE at the service boundary and pass it
to implementations; implementations MUST NOT accept or format a logical
``id`` (the mutable user-facing name). This separation ensures that
renaming a catalog's logical id never silently changes the underlying
storage resource names.
"""

from __future__ import annotations

from enum import Enum
from typing import ClassVar, FrozenSet, Optional, Protocol, runtime_checkable


class ResourceKind(str, Enum):
    """The class of physical backend resource a name refers to.

    Members inherit from ``str`` so values can be used directly wherever
    a plain string is expected (e.g., as a dict key or log annotation).
    """

    SCHEMA = "schema"
    """PostgreSQL schema that isolates a tenant's tables."""

    ITEMS = "items"
    """Primary items / features table for a collection."""

    PRIVATE_ITEMS = "private_items"
    """Private (access-controlled) items index (e.g., ES private alias)."""

    ASSETS = "assets"
    """Asset metadata table or index for a collection."""

    BUCKET = "bucket"
    """Cloud object-storage bucket for a catalog's assets."""

    TOPIC = "topic"
    """Pub/Sub or message-bus topic for a catalog's events."""

    SUBSCRIPTION = "subscription"
    """Pub/Sub or message-bus subscription paired with a topic."""

    OBJECT_PREFIX = "object_prefix"
    """Path prefix within a shared bucket for a catalog or collection."""


@runtime_checkable
class PhysicalNameResolver(Protocol):
    """Backend-specific mapper from physical ids to concrete resource names.

    Each storage backend (PostgreSQL, Elasticsearch, GCS, …) implements
    this protocol to translate the stable ``physical_id`` of a catalog or
    collection into the exact name used for each resource it owns.

    Class attributes
    ----------------
    backend
        Short identifier for the backend, e.g. ``"pg"``, ``"es"``, ``"gcs"``.
        Must be unique across all registered resolvers.
    supported_kinds
        The :class:`ResourceKind` values this resolver can produce names for.
        Callers check membership before calling :meth:`physical_name`.

    Callers resolve ``physical_id`` ONCE at the service boundary and pass
    it here; implementations MUST NOT accept or format a logical ``id``.
    """

    backend: ClassVar[str]
    supported_kinds: ClassVar[FrozenSet[ResourceKind]]

    def physical_name(
        self,
        kind: ResourceKind,
        *,
        catalog_physical_id: str,
        collection_physical_id: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> str:
        """Return the concrete backend resource name for the given kind.

        Parameters
        ----------
        kind:
            The type of resource name to produce.
        catalog_physical_id:
            Immutable physical identifier of the parent catalog (e.g.
            ``"s_2ka8fbc3"``).  Never a logical/user-visible catalog id.
        collection_physical_id:
            Immutable physical identifier of the collection, when applicable
            (``None`` for catalog-level resources such as ``SCHEMA`` or
            ``BUCKET``).
        prefix:
            Optional platform-wide prefix (e.g. ``"ds"``).  Resolvers may
            ignore this when the platform prefix is baked into ``physical_id``
            at mint time.
        """
        ...
