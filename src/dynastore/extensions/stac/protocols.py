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

"""STAC capability sub-Protocols — owned by the STAC extension.

Two ``@runtime_checkable`` Protocols extend the core
``CollectionStore`` / ``CatalogStore`` surfaces to mark a
driver as STAC-slice-capable. STAC code dispatches via
``isinstance(d, StacCollectionEntityStoreCapability)`` instead of inspecting
a string ``domain`` ClassVar — class identity, not data identity.

Drivers structurally satisfy these by implementing the base Protocols and
living in ``modules/stac/`` (the practical "this driver persists STAC
fields" gate). A future tighter contract (e.g. a ``materialise_stac_extent()``
method) can be added here without touching core protocols.
"""

from __future__ import annotations

from typing import Protocol, Tuple, runtime_checkable

from dynastore.models.protocols.entity_store import (
    CatalogStore,
    CollectionStore,
)


@runtime_checkable
class StacCollectionEntityStoreCapability(CollectionStore, Protocol):
    """A ``CollectionStore`` that persists the STAC slice of metadata.

    The PG-tier composition wrapper ``CollectionPostgresqlDriver``
    satisfies this structurally by exposing ``stac_metadata_columns()``
    that delegates to its first STAC-capable inner sidecar (or returns
    ``()`` if no STAC sidecar is loaded — e.g. a deployment without the
    stac extra).  ``stac_service._has_stac`` requires both the
    ``isinstance`` check AND a non-empty column tuple, so the empty
    delegation correctly identifies STAC as unavailable in those
    deployments.  Non-STAC drivers like ``CollectionElasticsearchDriver``
    lack the marker method entirely and fail the ``isinstance`` check
    outright.
    """

    def stac_metadata_columns(self) -> Tuple[str, ...]:
        """Return the tuple of STAC-slice columns this driver persists.

        Marker method that distinguishes STAC drivers from CORE drivers
        at runtime. Implementations typically return ``self._columns``.
        """
        ...


@runtime_checkable
class StacCatalogEntityStoreCapability(CatalogStore, Protocol):
    """A ``CatalogStore`` that persists the STAC slice of metadata.

    Catalog-tier sibling of ``StacCollectionEntityStoreCapability``. Same
    marker-method discriminator pattern.
    """

    def stac_metadata_columns(self) -> Tuple[str, ...]:
        """Return the tuple of STAC-slice columns this driver persists."""
        ...
