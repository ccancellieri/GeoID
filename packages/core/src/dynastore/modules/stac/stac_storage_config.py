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

"""STAC storage scope config — the single SSOT for STAC materialization.

Presence of this config at a scope (catalog or collection) signals that
STAC is enabled there.  Absence => zero STAC slices, sidecars, or routes.
The platform-level code default sets ``stac_level=NONE`` so that an
installation without a ``StacPreset`` applied carries no STAC overhead.

This config is written by ``StacPreset`` and read by:

- ``StacItemsSidecar.get_default_config`` — injects ``stac_metadata``
  only when ``items`` tier is enabled and PG storage includes PG.
- ``CollectionPostgresqlDriver._resolve_sidecars_for_catalog`` — includes
  the ``collection_stac`` wrapper slice iff ``collection``+ level AND PG.
- ``CatalogPostgresqlDriver._resolve_sidecars_for_catalog`` — includes
  the ``catalog_stac`` wrapper slice iff ``catalog``+ level AND PG.

``StacLevel`` is cumulative: ``items`` implies collection + catalog are
also enabled.  ``StacStorageBackend`` governs which physical backends
materialize: ``PG`` / ``ES`` / ``ES_PG`` (default).
"""
from __future__ import annotations

from enum import Enum
from typing import ClassVar, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class StacStorageBackend(str, Enum):
    """Physical backends for STAC metadata storage."""

    ES = "ES"
    PG = "PG"
    ES_PG = "ES_PG"


class StacLevel(str, Enum):
    """Cumulative depth of STAC materialization.

    - ``NONE``       — no STAC slices, sidecars, or ES routes.
    - ``CATALOG``    — catalog-tier STAC metadata only.
    - ``COLLECTION`` — catalog + collection STAC metadata (no per-item sidecar).
    - ``ITEMS``      — catalog + collection + per-item ``stac_metadata`` sidecar.
    """

    NONE = "none"
    CATALOG = "catalog"
    COLLECTION = "collection"
    ITEMS = "items"


def catalog_stac_enabled(level: StacLevel) -> bool:
    """True when catalog-tier STAC metadata should be materialized."""
    return level in {StacLevel.CATALOG, StacLevel.COLLECTION, StacLevel.ITEMS}


def collection_stac_enabled(level: StacLevel) -> bool:
    """True when collection-tier STAC metadata should be materialized."""
    return level in {StacLevel.COLLECTION, StacLevel.ITEMS}


def items_stac_enabled(level: StacLevel) -> bool:
    """True when per-item ``stac_metadata`` sidecar should be injected."""
    return level == StacLevel.ITEMS


def pg_stac(backend: StacStorageBackend) -> bool:
    """True when PostgreSQL is part of the STAC storage backend."""
    return backend in {StacStorageBackend.PG, StacStorageBackend.ES_PG}


def es_stac(backend: StacStorageBackend) -> bool:
    """True when Elasticsearch is part of the STAC storage backend."""
    return backend in {StacStorageBackend.ES, StacStorageBackend.ES_PG}


class StacStorageConfig(PluginConfig):
    """Scope config controlling STAC materialization depth and backend.

    Written by ``StacPreset`` at the catalog (or collection) scope.
    Absent at the platform level => no STAC anywhere (``stac_level=NONE``
    default).  Resolution cascade: collection → catalog → platform →
    code default (NONE).

    The ``_freeze_at="collection"`` boundary means an operator can
    override at the collection scope without propagating to other
    collections in the same catalog.
    """

    _address: ClassVar[Tuple[str, ...]] = (
        "platform",
        "catalog",
        "collection",
        "stac_storage",
    )
    _freeze_at: ClassVar[Optional[str]] = "collection"

    stac_level: Mutable[StacLevel] = Field(
        default=StacLevel.NONE,
        description=(
            "Cumulative STAC materialization depth.  NONE (default) means "
            "no STAC slices, sidecars, or ES routes are materialized.  "
            "CATALOG enables catalog-tier STAC metadata only; COLLECTION "
            "adds collection-tier; ITEMS also adds the per-item "
            "``stac_metadata`` sidecar."
        ),
    )
    stac_storage: Mutable[StacStorageBackend] = Field(
        default=StacStorageBackend.ES_PG,
        description=(
            "Physical backend(s) for STAC metadata.  ES routes to "
            "Elasticsearch drivers; PG materializes the PG wrapper "
            "``catalog_stac`` / ``collection_stac`` slices and the "
            "items ``stac_metadata`` sidecar.  ES_PG (default) does both: "
            "PG preferred for WRITE/READ (exact geometry), ES for SEARCH "
            "and async secondary indexing."
        ),
    )
