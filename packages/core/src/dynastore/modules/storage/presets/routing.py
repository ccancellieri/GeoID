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

"""``routing`` — a single parametric storage-routing preset.

Routing is *where data lives*: which drivers handle WRITE / READ / INDEX at
each tier (search is derived from INDEX-then-READ, never configured
directly).  This module owns the per-tier × per-backend routing builders and
exposes one preset, ``routing``, parametrised by a ``drivers`` combination:

- ``pg``      — PostgreSQL only.
- ``es``      — **items** in public Elasticsearch only.  Collection and catalog
  *metadata* always keep PG as the system of record (the ES collection/catalog
  drivers are write-only INDEX-lane backends, never READ stores), with a
  public ES INDEX entry.  There is no ES-only metadata tier.
- ``pg_es``   — PG primary (WRITE/READ) + public ES async INDEX (materialization + search).
- ``pg_pes``  — PG primary + **private** ES async INDEX (materialization + search).
  Private ES is items-only (there is no private collection-tier index), so the
  collection tier stays PG.

The preset is **scope-aware**, mirroring how a catalog contains collections
and a collection contains items:

- applied at **catalog** scope it writes the ``CollectionRoutingConfig`` and
  ``ItemsRoutingConfig`` templates that the catalog's (future) collections and
  items inherit;
- applied at **collection** scope it writes only the ``ItemsRoutingConfig`` for
  that one collection.

Catalog-tier routing (``CatalogRoutingConfig`` — where the catalog record
itself lives) is intentionally *not* pinned here: the catalog row already
exists in the PG registry before any routing preset runs, and is left on the
platform default.

This preset writes **no** ``StacStorageConfig``.  Enabling STAC materialisation
(the PG ``catalog_stac`` / ``collection_stac`` wrapper slices and the per-item
``stac_metadata`` sidecar) is the orthogonal job of the ``stac_storage`` preset,
which writes that SSOT signal; the PG/ES drivers read it at runtime.  Pair the
two when you want a STAC catalog; apply ``routing`` alone for plain storage
routing.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from dynastore.modules.stac.stac_storage_config import StacStorageBackend
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
)

from .bundle_preset import BundlePreset
from .examples import PresetExample
from .protocol import PresetBundle, PresetBundleEntry, PresetTier


class RoutingDrivers(str, Enum):
    """Driver combination for storage routing.

    - ``PG``     — PostgreSQL only.
    - ``ES``     — items in public Elasticsearch only; collection/catalog metadata
      stay PG system-of-record + public ES INDEX entry (no ES-only metadata).
    - ``PG_ES``  — PG primary + public ES async INDEX (materialization + search).
    - ``PG_PES`` — PG primary + private ES async INDEX (items only; collection
      tier stays PG since private ES has no collection-tier index).
    """

    PG = "pg"
    ES = "es"
    PG_ES = "pg_es"
    PG_PES = "pg_pes"


class RoutingPresetParams(BaseModel):
    """Parameters for the ``routing`` preset."""

    drivers: RoutingDrivers = Field(
        default=RoutingDrivers.PG_ES,
        description=(
            "Driver combination: ``pg`` (PostgreSQL only), ``es`` (items in "
            "public Elasticsearch only — collection/catalog metadata stay PG "
            "system-of-record + ES index), ``pg_es`` (PG primary + public ES "
            "INDEX materialization and search), or ``pg_pes`` (PG primary + "
            "private ES INDEX materialization and search; items only)."
        ),
    )


# ---------------------------------------------------------------------------
# Per-tier × per-backend routing builders (the routing SSOT).
#
# ``stac.py`` re-exports these for backward compatibility.
# ---------------------------------------------------------------------------


def _catalog_routing_es() -> CatalogRoutingConfig:
    """Catalog routing for an ES catalog — PG metadata + ES index, NOT ES-only.

    There is no "ES-only" catalog-metadata tier: ``catalog_elasticsearch_driver``
    is a write-only INDEX-lane backend, never a READ-capable ``CatalogStore``
    (the only registered ``CatalogStore`` is the PG driver). Authoring ES as
    a READ/WRITE-primary driver fails routing validation. Catalog metadata
    stays on PG as the system of record; the ES INDEX entry is declared
    explicitly here rather than left to routing self-registration — an
    operator who explicitly requested the ``es``/``pg_es`` driver combination
    must get a deterministic INDEX entry regardless of what happens to be
    discoverable in the running process at apply time.
    """
    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


def _catalog_routing_es_pg() -> CatalogRoutingConfig:
    """ES_PG catalog routing — same as :func:`_catalog_routing_es` on this
    platform: catalog metadata is always PG system-of-record with an
    explicit ES INDEX entry.
    """
    return _catalog_routing_es()


def _catalog_routing_pg() -> CatalogRoutingConfig:
    """PG-only catalog routing — no ES."""
    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


def _collection_routing_es() -> CollectionRoutingConfig:
    """Collection routing for an ES-items catalog — PG metadata + ES index.

    There is no "ES-only" collection tier: ``CollectionElasticsearchDriver`` is
    a write-only INDEX-lane backend (``auto_register_for_routing={INDEX, READ}``,
    ``is_collection_indexer=True``) — never a READ-capable ``CollectionStore``.
    Authoring it as a READ/WRITE-primary driver fails routing validation
    (``operations[READ] driver 'collection_elasticsearch_driver' is not
    registered``).  Collection metadata therefore stays on PG as the system of
    record; the ES INDEX entry is declared explicitly here rather than left
    to routing self-registration — an operator who explicitly requested the
    ``es``/``pg_es`` driver combination must get a deterministic INDEX entry
    regardless of what happens to be discoverable in the running process at
    apply time.
    """
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


def _collection_routing_es_pg() -> CollectionRoutingConfig:
    """ES_PG collection routing — same as :func:`_collection_routing_es` on this
    platform: collection metadata is always PG system-of-record with an
    explicit ES INDEX entry.
    """
    return _collection_routing_es()


def _collection_routing_pg() -> CollectionRoutingConfig:
    """PG-only collection routing."""
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


def _items_routing_es() -> ItemsRoutingConfig:
    """ES-only items routing."""
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


def _items_routing_es_pg() -> ItemsRoutingConfig:
    """ES_PG items routing — PG WRITE/READ primary, ES async INDEX."""
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                ),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


def _items_routing_pg() -> ItemsRoutingConfig:
    """PG-only items routing."""
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                ),
            ],
        },
    )


def _items_routing_pg_pes() -> ItemsRoutingConfig:
    """PG primary + private-ES INDEX items routing.

    Reuses the single private-items routing SSOT (``_build_private_items_routing``)
    so ``routing(drivers=pg_pes)`` and the ``private_catalog`` / ``items_es_private``
    presets stay byte-identical on the items tier.  Imported lazily to avoid a
    storage→catalog import cycle at module load.
    """
    from dynastore.modules.catalog.catalog_config import (
        _build_private_items_routing,
    )

    return _build_private_items_routing()


# ---------------------------------------------------------------------------
# drivers → per-tier config + backend mappings
# ---------------------------------------------------------------------------


def collection_routing_for(drivers: RoutingDrivers) -> CollectionRoutingConfig:
    """Collection-tier routing for a driver combination.

    ``pg_pes`` keeps the collection tier on PG: private ES is items-only.
    """
    if drivers == RoutingDrivers.ES:
        return _collection_routing_es()
    if drivers == RoutingDrivers.PG_ES:
        return _collection_routing_es_pg()
    # PG and PG_PES → collection metadata in PG.
    return _collection_routing_pg()


def items_routing_for(drivers: RoutingDrivers) -> ItemsRoutingConfig:
    """Items-tier routing for a driver combination."""
    if drivers == RoutingDrivers.ES:
        return _items_routing_es()
    if drivers == RoutingDrivers.PG_ES:
        return _items_routing_es_pg()
    if drivers == RoutingDrivers.PG_PES:
        return _items_routing_pg_pes()
    return _items_routing_pg()


def backend_from_drivers(drivers: RoutingDrivers) -> StacStorageBackend:
    """Map a ``drivers`` combination to the ``StacStorageConfig`` backend.

    Drives the ``stac_storage`` SSOT a harvest pins alongside ``routing``:
    PG is "in the backend" (enables the PG STAC sidecar) for every combo except
    pure ``es``; ES is "in the backend" (enables the public ES STAC route) only
    for ``es`` / ``pg_es``.  ``pg_pes`` → ``PG``: the private ES index is not a
    public STAC route, so only the PG sidecar is enabled.
    """
    if drivers == RoutingDrivers.ES:
        return StacStorageBackend.ES
    if drivers == RoutingDrivers.PG_ES:
        return StacStorageBackend.ES_PG
    # PG and PG_PES → PG sidecar, no public ES STAC route.
    return StacStorageBackend.PG


def drivers_from_backend(backend: Optional[StacStorageBackend]) -> RoutingDrivers:
    """Inverse of :func:`backend_from_drivers` for the ``stac`` composite.

    ``StacStorageBackend`` has no private variant, so it never yields
    ``PG_PES`` — private routing is reached only via an explicit ``drivers``.
    """
    if backend == StacStorageBackend.ES:
        return RoutingDrivers.ES
    if backend == StacStorageBackend.PG:
        return RoutingDrivers.PG
    # ES_PG or unknown → PG primary + public ES.
    return RoutingDrivers.PG_ES


def coerce_routing_params(params: Any) -> RoutingPresetParams:
    """Coerce arbitrary params to ``RoutingPresetParams``.

    Accepts a native ``RoutingPresetParams``, or the ``stac`` composite's
    ``StacPresetParams`` (which forwards an optional ``drivers`` and always
    carries ``stac_storage``): an explicit ``drivers`` wins, else it is derived
    from ``stac_storage``.
    """
    if isinstance(params, RoutingPresetParams):
        return params
    drivers = getattr(params, "drivers", None)
    if drivers is None:
        drivers = drivers_from_backend(getattr(params, "stac_storage", None))
    return RoutingPresetParams(drivers=drivers)


def _build_routing_bundle(
    params: RoutingPresetParams,
    *,
    catalog_id: str = "",  # noqa: ARG001 — scope carried by the lifecycle
    collection_id: Optional[str] = None,
) -> PresetBundle:
    """Build the scope-aware routing bundle for the chosen drivers.

    Collection scope (``collection_id`` set) → items template only.
    Catalog scope → collection + items templates.
    """
    drivers = params.drivers
    entries: List[PresetBundleEntry] = []

    if not collection_id:
        # Catalog scope: pin the collection template that future collections
        # inherit.  Skipped at collection scope (a collection has no
        # sub-collections to template).
        entries.append(
            PresetBundleEntry(
                slot="collection_template",
                config_cls=CollectionRoutingConfig,
                instance=collection_routing_for(drivers),
                rollback_priority=20,
            )
        )

    entries.append(
        PresetBundleEntry(
            slot="items_template",
            config_cls=ItemsRoutingConfig,
            instance=items_routing_for(drivers),
            rollback_priority=10,
        )
    )

    return PresetBundle(entries=tuple(entries))


class RoutingPreset(BundlePreset):
    """Parametric storage routing — scope-aware, driver-parametrised.

    Applied at catalog scope it writes the collection + items routing templates
    the catalog's collections and items inherit; applied at collection scope it
    writes only that collection's items routing.  Parametrised by ``drivers``
    (``pg`` / ``es`` / ``pg_es`` / ``pg_pes``).  Writes no ``StacStorageConfig``
    — pair with the ``stac_storage`` preset to enable STAC materialisation.
    """

    name = "routing"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = True
    keywords: ClassVar[Tuple[str, ...]] = (
        "routing", "storage", "drivers", "postgresql", "elasticsearch",
    )
    params_model = RoutingPresetParams
    description = (
        "Parametric storage routing.  Applied at catalog scope it pins the "
        "collection + items routing templates future collections inherit; "
        "applied at collection scope it pins items routing for one collection.  "
        "Parametrised by ``drivers``: pg / es / pg_es / pg_pes (PES = private "
        "Elasticsearch).  Writes no StacStorageConfig — pair with the "
        "``stac_storage`` preset to enable STAC materialisation.  Default: "
        "drivers=pg_es."
    )

    examples: ClassVar[Tuple[PresetExample, ...]] = (
        PresetExample(
            name="catalog-pg-es",
            summary=(
                "Route a catalog's collections and items to PG primary with a "
                "public ES INDEX (materialization + search).  Apply at catalog scope via "
                "POST /configs/catalogs/{catalog_id}/presets/routing with "
                "params {\"drivers\": \"pg_es\"}.  Future collections inherit "
                "the templates (inherit-only — already-materialised collections "
                "are not retro-mutated)."
            ),
            params={"drivers": "pg_es"},
        ),
        PresetExample(
            name="collection-es-only",
            summary=(
                "Route one collection's items to public Elasticsearch only.  "
                "Apply at collection scope via POST /configs/catalogs/"
                "{catalog_id}/collections/{collection_id}/presets/routing with "
                "params {\"drivers\": \"es\"}.  Only the items template is "
                "written at collection scope."
            ),
            params={"drivers": "es"},
        ),
        PresetExample(
            name="catalog-pg-private-es",
            summary=(
                "Route a catalog to PG primary with a private ES INDEX "
                "(materialization + search) for items (collection metadata stays in PG).  Apply at "
                "catalog scope with params {\"drivers\": \"pg_pes\"}."
            ),
            params={"drivers": "pg_pes"},
        ),
    )

    def build(
        self, catalog_id: str = "", collection_id: Optional[str] = None, **_scope: str
    ) -> PresetBundle:
        # Param-less default builder (default drivers=pg_es).  Parametrised
        # callers reach ``_build_bundle`` via the lifecycle, NOT this method;
        # ``build`` is intentionally params-ignorant (the BundlePreset contract).
        return _build_routing_bundle(
            RoutingPresetParams(), catalog_id=catalog_id, collection_id=collection_id
        )

    def _build_bundle(
        self, params: BaseModel, scope_kwargs: Dict[str, str]
    ) -> PresetBundle:
        return _build_routing_bundle(
            coerce_routing_params(params),
            catalog_id=scope_kwargs.get("catalog_id", ""),
            collection_id=scope_kwargs.get("collection_id"),
        )
