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

"""``stac`` preset — kind-agnostic, parameterized STAC opt-in.

Applying this preset at a catalog (or collection) scope writes the
``StacStorageConfig`` SSOT entry that drives STAC materialization at
all three tiers.  Absent this preset, no STAC slices, sidecars, or
ES STAC routes are materialized.

Parameters:

- ``stac_level`` (default ``COLLECTION``) — cumulative depth:
  ``none`` revokes STAC; ``catalog`` enables catalog STAC only;
  ``collection`` also adds collection STAC; ``items`` also adds the
  per-item ``stac_metadata`` sidecar.
- ``stac_storage`` (default ``ES_PG``) — backend(s):
  ``ES`` routes STAC to Elasticsearch only; ``PG`` materializes the PG
  wrapper slices and items sidecar; ``ES_PG`` does both (PG preferred
  for WRITE/READ, ES for SEARCH + async secondary).

The bundle emitted by ``build()`` contains:

1. A ``StacStorageConfig`` entry — the single SSOT.
2. Per-enabled-tier routing entries for the chosen storage:
   - ES routing: per-tier ES SEARCH (and WRITE/READ for ES-only);
   - PG routing: wrapper driver-config entries whose ``sidecars`` list
     includes the relevant STAC slice (authorable ``Immutable`` field).
3. For ``stac_level=none``: an empty ``StacStorageConfig`` that carries
   ``stac_level=NONE`` — ``BundlePreset.revoke`` removes it; the
   absence then restores the default (no STAC).
"""
from __future__ import annotations

from typing import ClassVar, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from dynastore.modules.stac.stac_storage_config import (
    StacLevel,
    StacStorageBackend,
    StacStorageConfig,
    catalog_stac_enabled,
    collection_stac_enabled,
    es_stac,
    items_stac_enabled,
    pg_stac,
)
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
)

from .bundle_preset import BundlePreset
from .examples import PresetExample
from .protocol import PresetBundle, PresetBundleEntry, PresetTier


class StacPresetParams(BaseModel):
    """Parameters for the ``stac`` preset."""

    stac_level: StacLevel = Field(
        default=StacLevel.COLLECTION,
        description=(
            "Cumulative depth of STAC materialization at the applied scope. "
            "Each level includes the ones above it:\n"
            "- ``none`` — revoke STAC from the scope (the bundle carries an empty "
            "``StacStorageConfig`` that revoke removes, restoring the no-STAC default).\n"
            "- ``catalog`` — materialize catalog-tier STAC only.\n"
            "- ``collection`` (default) — also materialize collection-tier STAC.\n"
            "- ``items`` — also add the per-item ``stac_metadata`` sidecar so "
            "individual items carry full STAC item documents."
        ),
        examples=["collection", "items", "catalog", "none"],
    )
    stac_storage: StacStorageBackend = Field(
        default=StacStorageBackend.ES_PG,
        description=(
            "Backend(s) the chosen STAC tiers are routed to:\n"
            "- ``ES`` — Elasticsearch only (all ops routed to the ES driver).\n"
            "- ``PG`` — PostgreSQL only; materializes the PG wrapper slices and, at "
            "``items`` level, the items sidecar. No ES STAC routes.\n"
            "- ``ES_PG`` (default) — both: PG preferred for WRITE/READ, ES for SEARCH "
            "plus an async secondary index."
        ),
        examples=["ES_PG", "ES", "PG"],
    )


# ---------------------------------------------------------------------------
# Routing helpers
# ---------------------------------------------------------------------------


def _catalog_routing_es() -> CatalogRoutingConfig:
    """ES-only catalog routing — all ops to catalog_elasticsearch_driver."""
    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    source="operator",
                ),
            ],
        },
    )


def _catalog_routing_es_pg() -> CatalogRoutingConfig:
    """ES_PG catalog routing — PG preferred WRITE/READ, ES for SEARCH + async."""
    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    secondary_index=True,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


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
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    source="operator",
                ),
            ],
        },
    )


def _collection_routing_es() -> CollectionRoutingConfig:
    """ES-only collection routing."""
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    source="operator",
                ),
            ],
        },
    )


def _collection_routing_es_pg() -> CollectionRoutingConfig:
    """ES_PG collection routing — PG preferred WRITE/READ, ES SEARCH + async."""
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    secondary_index=True,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


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
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    source="operator",
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
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    source="operator",
                ),
            ],
        },
    )


def _items_routing_es_pg() -> ItemsRoutingConfig:
    """ES_PG items routing — PG preferred WRITE/READ, ES SEARCH + async."""
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    secondary_index=True,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                ),
            ],
            Operation.SEARCH: [
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
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    source="operator",
                ),
            ],
        },
    )


# ---------------------------------------------------------------------------
# Preset
# ---------------------------------------------------------------------------


class StacPreset(BundlePreset):
    """Kind-agnostic, parameterized STAC opt-in preset.

    Applying this preset writes ``StacStorageConfig`` at the chosen scope
    and emits per-tier routing entries matching the requested
    ``stac_level`` and ``stac_storage``.

    ``stac_level=none`` clears STAC from the scope (bundle contains a
    ``StacStorageConfig(stac_level=NONE)`` entry that revoke removes).

    See module docstring for full parameter semantics.
    """

    name = "stac"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = True
    params_model = StacPresetParams
    keywords: ClassVar[Tuple[str, ...]] = (
        "routing", "stac", "catalog", "collection", "items",
        "elasticsearch", "postgresql", "metadata",
    )
    description = (
        "Kind-agnostic STAC opt-in preset.  Writes a ``StacStorageConfig`` "
        "entry (the SSOT signal) and wires per-tier routing entries for the "
        "chosen depth (``stac_level``) and backend (``stac_storage``).  "
        "Default: level=collection, storage=ES_PG.  Set stac_level=none to "
        "revoke STAC from the scope.  Absent this preset, no STAC "
        "slices, sidecars, or ES STAC routes are materialized."
    )

    examples: ClassVar[Tuple[PresetExample, ...]] = (
        PresetExample(
            name="default-collection-es-pg",
            summary=(
                "Enable STAC down to the collection tier on both backends (the "
                "defaults): PG preferred for WRITE/READ, ES for SEARCH plus an async "
                "secondary index. Apply at catalog scope via "
                "POST /admin/catalogs/{catalog_id}/presets/stac."
            ),
            params={"stac_level": "collection", "stac_storage": "ES_PG"},
        ),
        PresetExample(
            name="full-items-elasticsearch-only",
            summary=(
                "Enable STAC all the way to per-item ``stac_metadata`` sidecars, "
                "routed to Elasticsearch only — useful for an ES-backed STAC API with "
                "no PostgreSQL wrapper slices."
            ),
            params={"stac_level": "items", "stac_storage": "ES"},
        ),
        PresetExample(
            name="catalog-only-postgres",
            summary=(
                "Materialize catalog-tier STAC in PostgreSQL only (no collection/item "
                "STAC, no Elasticsearch) — a minimal STAC catalog envelope without an "
                "ES dependency."
            ),
            params={"stac_level": "catalog", "stac_storage": "PG"},
        ),
        PresetExample(
            name="revoke-stac",
            summary=(
                "Revoke STAC from the scope: the bundle carries a "
                "``StacStorageConfig(stac_level=none)`` entry that revoke removes, "
                "restoring the default of no STAC slices, sidecars, or ES routes."
            ),
            params={"stac_level": "none"},
        ),
    )

    def build(self, catalog_id: str = "", **_scope: str) -> PresetBundle:  # noqa: ARG002
        """Default-params fallback (COLLECTION / ES_PG).

        The parameterized path runs through :meth:`_build_bundle`, which the
        base ``BundlePreset`` lifecycle (dry_run / apply / revoke) calls with
        the validated params. This method only satisfies the
        ``BundlePreset.build(**scope)`` contract for any default-params call.
        """
        return _build_stac_bundle(
            StacPresetParams(), catalog_id=catalog_id,
        )

    def _build_bundle(
        self, params: BaseModel, scope_kwargs: Dict[str, str]
    ) -> PresetBundle:
        """Resolve the STAC bundle for the supplied params + scope.

        The base ``BundlePreset`` lifecycle (dry_run / apply / revoke) calls
        this with the validated params and persists them in the applied
        descriptor, so revoke rebuilds the SAME bundle that apply created.
        ``params`` is coerced to :class:`StacPresetParams` defensively.
        """
        if not isinstance(params, StacPresetParams):
            params = StacPresetParams.model_validate(
                params.model_dump() if hasattr(params, "model_dump") else {}
            )
        return _build_stac_bundle(
            params,
            catalog_id=scope_kwargs.get("catalog_id", ""),
            collection_id=scope_kwargs.get("collection_id"),
        )


def _build_stac_bundle(
    params: StacPresetParams,
    *,
    catalog_id: str,
    collection_id: Optional[str] = None,
) -> PresetBundle:
    """Build the ``PresetBundle`` for the given stac params + scope."""
    entries: List[PresetBundleEntry] = []

    # 1. StacStorageConfig — the SSOT entry (always present, even for NONE)
    entries.append(
        PresetBundleEntry(
            slot="stac_storage_config",
            config_cls=StacStorageConfig,
            instance=StacStorageConfig(
                stac_level=params.stac_level,
                stac_storage=params.stac_storage,
            ),
            rollback_priority=5,
        )
    )

    level = params.stac_level
    backend = params.stac_storage

    if level == StacLevel.NONE:
        # No routing entries — just the SSOT config that signals zero STAC.
        return PresetBundle(entries=tuple(entries))

    # 2. Catalog-tier routing (enabled when catalog+ level)
    if catalog_stac_enabled(level):
        if es_stac(backend) and pg_stac(backend):
            cat_routing = _catalog_routing_es_pg()
        elif es_stac(backend):
            cat_routing = _catalog_routing_es()
        else:
            cat_routing = _catalog_routing_pg()

        entries.append(
            PresetBundleEntry(
                slot="catalog_routing",
                config_cls=CatalogRoutingConfig,
                instance=cat_routing,
                rollback_priority=30,
            )
        )
        # The catalog_stac PG slice is NOT authored here: when pg_stac is
        # active, CatalogPostgresqlDriver._resolve_sidecars_for_catalog adds
        # it at runtime by reading this scope's StacStorageConfig (the SSOT).

    # 3. Collection-tier routing (enabled when collection+ level)
    if collection_stac_enabled(level):
        if es_stac(backend) and pg_stac(backend):
            coll_routing = _collection_routing_es_pg()
        elif es_stac(backend):
            coll_routing = _collection_routing_es()
        else:
            coll_routing = _collection_routing_pg()

        entries.append(
            PresetBundleEntry(
                slot="collection_template",
                config_cls=CollectionRoutingConfig,
                instance=coll_routing,
                rollback_priority=20,
            )
        )
        # The collection_stac PG slice is NOT authored here: when pg_stac is
        # active, CollectionPostgresqlDriver._resolve_sidecars_for_catalog adds
        # it at runtime by reading this scope's StacStorageConfig (the SSOT).

    # 4. Items-tier routing (enabled when items level)
    if items_stac_enabled(level):
        if es_stac(backend) and pg_stac(backend):
            items_routing = _items_routing_es_pg()
        elif es_stac(backend):
            items_routing = _items_routing_es()
        else:
            items_routing = _items_routing_pg()

        entries.append(
            PresetBundleEntry(
                slot="items_template",
                config_cls=ItemsRoutingConfig,
                instance=items_routing,
                rollback_priority=10,
            )
        )
        # Items stac_metadata sidecar is handled by StacStorageConfig signal
        # (SidecarRegistry injection via StacItemsSidecar.get_default_config),
        # not by an authored sidecars list — no driver-config entry needed here.

    return PresetBundle(entries=tuple(entries))
