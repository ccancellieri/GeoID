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

"""STAC-slice PG metadata drivers — relocated to the STAC module.

The two collection/catalog-tier STAC drivers persist the STAC subset of
the metadata envelope into the per-tenant ``{schema}.collection_stac``
and global ``catalog.catalog_stac`` tables respectively.

The shared CRUD bodies live on ``_PgCollectionCoreBase`` /
``_PgCatalogCoreBase`` in the core PG metadata module
(``modules/storage/drivers/metadata_postgresql.py``) — imported here,
not duplicated.
"""

from __future__ import annotations

from typing import ClassVar, FrozenSet, Optional, Tuple

from dynastore.models.protocols.entity_store import EntityStoreCapability
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.modules.storage.driver_config import DriverPluginConfig
from dynastore.modules.storage.drivers.core_postgresql import (
    _PgCatalogCoreBase,
    _PgCollectionCoreBase,
)


_COLLECTION_STAC_COLUMNS: Tuple[str, ...] = (
    "stac_version", "stac_extensions", "extent", "providers",
    "summaries", "links", "assets", "item_assets",
)
_CATALOG_STAC_COLUMNS: Tuple[str, ...] = (
    "stac_version", "stac_extensions", "conforms_to", "links", "assets",
)


class CollectionStacPostgresqlDriverConfig(DriverPluginConfig):
    """Identity marker for CollectionStacPostgresqlDriver."""
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "drivers")
    _visibility: ClassVar[Optional[str]] = "collection"



class CatalogStacPostgresqlDriverConfig(DriverPluginConfig):
    """Identity marker for CatalogStacPostgresqlDriver."""
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "drivers")
    _visibility: ClassVar[Optional[str]] = "catalog"



class CollectionStacPostgresqlDriver(
    TypedDriver[CollectionStacPostgresqlDriverConfig], _PgCollectionCoreBase,
):
    """Inner driver for the STAC collection metadata slice (``extent``,
    ``providers``, …).

    Backs ``{schema}.collection_stac``. Declares ``SPATIAL_FILTER``
    because the ``extent`` column carries the STAC bbox the spatial-filter
    endpoints match against.  Composed inside the PG-tier wrapper
    ``CollectionPostgresqlDriver`` via the ``collection_stac`` sidecar
    (PR 1e step 3b); not a standalone ``CollectionStore`` plugin
    anymore.  The wrapper forwards the ``stac_metadata_columns()`` marker
    to this class via ``CollectionPgSidecarRegistry`` try-import.
    """

    _table: ClassVar[str] = "collection_stac"
    _columns: ClassVar[Tuple[str, ...]] = _COLLECTION_STAC_COLUMNS

    capabilities: FrozenSet[str] = frozenset({
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SOFT_DELETE,
        EntityStoreCapability.SPATIAL_FILTER,
        EntityStoreCapability.PHYSICAL_ADDRESSING,
    })

    def stac_metadata_columns(self) -> Tuple[str, ...]:
        return self._columns


class CatalogStacPostgresqlDriver(
    TypedDriver[CatalogStacPostgresqlDriverConfig], _PgCatalogCoreBase,
):
    """Primary driver for STAC catalog metadata.

    Backs ``catalog.catalog_stac``. Scope: ``stac_version``,
    ``stac_extensions``, ``conforms_to``, ``links``, ``assets``.

    Structurally satisfies ``StacCatalogEntityStoreCapability`` via the
    ``stac_metadata_columns()`` marker method.
    """

    _table: ClassVar[str] = "catalog_stac"
    _columns: ClassVar[Tuple[str, ...]] = _CATALOG_STAC_COLUMNS

    capabilities: FrozenSet[str] = frozenset({
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SOFT_DELETE,
    })

    def stac_metadata_columns(self) -> Tuple[str, ...]:
        return self._columns
