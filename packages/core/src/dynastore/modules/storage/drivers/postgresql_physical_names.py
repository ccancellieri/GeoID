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

"""PostgreSQL PhysicalNameResolver implementation.

:class:`PgPhysicalNames` maps the immutable ``physical_id`` values stored
in the catalog and collection registries to the concrete PostgreSQL resource
names the PG driver uses.

Supported kinds:

* :attr:`~ResourceKind.SCHEMA` — the per-tenant PostgreSQL schema name.
  For the PG backend this is the catalog ``physical_id`` verbatim
  (historically called ``physical_schema``).
* :attr:`~ResourceKind.ITEMS` — the physical table name for a collection's
  items hub table.  For the PG backend this is the collection ``physical_id``
  verbatim (historically called ``physical_table``).

Both values are byte-identical to what the PG driver currently stores in
``catalog.catalogs.physical_schema`` / ``{schema}.collections.physical_id``.
This resolver exists to expose that mapping through the standard
:class:`~dynastore.models.protocols.physical_names.PhysicalNameResolver`
protocol so future drivers and tooling can resolve PG names without reaching
into driver internals.

This module must NOT change how the PG driver derives or stores physical names.
"""

from __future__ import annotations

from typing import ClassVar, FrozenSet, Optional

from dynastore.models.protocols.physical_names import PhysicalNameResolver, ResourceKind


class PgPhysicalNames:
    """Maps catalog/collection physical ids to PostgreSQL resource names.

    Supported kinds: :attr:`~ResourceKind.SCHEMA`, :attr:`~ResourceKind.ITEMS`.

    SCHEMA
        The per-tenant PostgreSQL schema name.  Equal to the catalog
        ``physical_id`` (the value stored in
        ``catalog.catalogs.physical_schema`` / resolved via
        ``CatalogsProtocol.resolve_physical_id``).  Requires only
        ``catalog_physical_id``.

    ITEMS
        The physical table name for a collection's items hub table.  Equal to
        the collection ``physical_id`` (the value stored in
        ``{schema}.collections.physical_id`` / resolved via
        ``CatalogsProtocol.resolve_physical_id(catalog_id, collection_id)``).
        Requires ``collection_physical_id``.
    """

    backend: ClassVar[str] = "pg"
    supported_kinds: ClassVar[FrozenSet[ResourceKind]] = frozenset(
        {ResourceKind.SCHEMA, ResourceKind.ITEMS}
    )

    def physical_name(
        self,
        kind: ResourceKind,
        *,
        catalog_physical_id: str,
        collection_physical_id: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> str:
        """Return the concrete PostgreSQL resource name for ``kind``.

        Parameters
        ----------
        kind:
            ``SCHEMA`` or ``ITEMS``.
        catalog_physical_id:
            Immutable physical identifier of the catalog (e.g. ``"s_2ka8fbc3"``).
            For ``SCHEMA`` this value is returned verbatim as the PG schema name.
            Must not be empty.
        collection_physical_id:
            Immutable physical identifier of the collection.  Required for
            ``ITEMS``; not used for ``SCHEMA``.
        prefix:
            Not used by the PG backend — PG schema and table names are derived
            directly from the ``physical_id`` with no platform prefix.
        """
        if not catalog_physical_id:
            raise ValueError(
                "PgPhysicalNames.physical_name: catalog_physical_id must not be empty."
            )

        if kind == ResourceKind.SCHEMA:
            return catalog_physical_id

        if kind == ResourceKind.ITEMS:
            if not collection_physical_id:
                raise ValueError(
                    "PgPhysicalNames.physical_name: 'collection_physical_id' "
                    "is required for ResourceKind.ITEMS."
                )
            return collection_physical_id

        raise ValueError(
            f"PgPhysicalNames does not support ResourceKind.{kind.name}. "
            f"Supported: {sorted(k.name for k in self.supported_kinds)}"
        )


# Verify structural conformance at import time (runtime_checkable protocol).
assert isinstance(PgPhysicalNames(), PhysicalNameResolver), (
    "PgPhysicalNames must satisfy the PhysicalNameResolver protocol"
)
