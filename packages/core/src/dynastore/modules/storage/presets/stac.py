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

"""STAC opt-in presets — the ``stac_storage`` SSOT flip and the ``stac`` composite.

STAC opt-in is two orthogonal concerns:

- *Decide the routing* — where collection / item data lives.  That is the job
  of the parametric ``routing`` preset (see ``routing.py``), parametrised by a
  ``drivers`` combination and scope-aware (catalog scope pins collection + items
  templates, collection scope pins items only).  ``stac`` composes it.
- *Enable STAC* — ``stac_storage`` writes the single ``StacStorageConfig`` SSOT
  entry, the signal the PG/ES drivers read at runtime to materialise the
  ``catalog_stac`` / ``collection_stac`` wrapper slices and the per-item
  ``stac_metadata`` sidecar.  Writes no routing.
- ``stac`` — the composite ``compose=("routing", "stac_storage")``.  One-shot:
  applies routing first (so the drivers the SSOT references exist before the
  signal flips), then the SSOT; revoke reverses.

The split lets an operator stage the work: apply ``routing`` to decide where
data goes, then ``stac_storage`` to enable STAC, then ingest — or apply ``stac``
to do both at once.  Absent any of these, no STAC slices, sidecars, or ES STAC
routes are materialized.

Shared parameters (``StacPresetParams``):

- ``stac_level`` (default ``COLLECTION``) — cumulative depth:
  ``none`` revokes STAC; ``catalog`` enables catalog STAC only;
  ``collection`` also adds collection STAC; ``items`` also adds the
  per-item ``stac_metadata`` sidecar.  Governs ``stac_storage`` only.
- ``stac_storage`` (default ``ES_PG``) — backend(s) for STAC materialisation:
  ``ES`` routes STAC to Elasticsearch only; ``PG`` materializes the PG wrapper
  slices and items sidecar; ``ES_PG`` does both.
- ``drivers`` (optional) — routing driver combination forwarded to the
  ``routing`` child by the ``stac`` composite.  When unset it is derived from
  ``stac_storage`` (ES→es, PG→pg, ES_PG→pg_es); set it explicitly (e.g.
  ``pg_pes``) to route through private Elasticsearch.
"""
from __future__ import annotations

from typing import ClassVar, Dict, Optional, Tuple, Type

from pydantic import BaseModel

from dynastore.modules.stac.stac_storage_config import (
    StacLevel,
    StacStorageBackend,
    StacStorageConfig,
)

# Re-export the per-tier routing builders (their home is ``routing.py``) so
# existing importers — e.g. ``items_es_public`` — keep working unchanged.
from .routing import (  # noqa: F401
    RoutingDrivers,
    _catalog_routing_es,
    _catalog_routing_es_pg,
    _catalog_routing_pg,
    _collection_routing_es,
    _collection_routing_es_pg,
    _collection_routing_pg,
    _items_routing_es,
    _items_routing_es_pg,
    _items_routing_pg,
)
from .bundle_preset import BundlePreset
from .preset import CompositePreset
from .protocol import PresetBundle, PresetBundleEntry, PresetTier


class StacPresetParams(BaseModel):
    """Parameters for the ``stac`` composite and the ``stac_storage`` child."""

    stac_level: StacLevel = StacLevel.COLLECTION
    stac_storage: StacStorageBackend = StacStorageBackend.ES_PG
    drivers: Optional[RoutingDrivers] = None


# ---------------------------------------------------------------------------
# Bundle builder — the SSOT-only half
# ---------------------------------------------------------------------------


def _coerce_params(params: BaseModel) -> StacPresetParams:
    """Coerce an arbitrary params model to ``StacPresetParams`` defensively.

    The composite lifecycle validates incoming params against the composite's
    ``params_model`` (``StacPresetParams``) and forwards the SAME instance to
    each child's ``apply``; this guard also covers a child applied directly
    with a foreign params model.
    """
    if isinstance(params, StacPresetParams):
        return params
    return StacPresetParams.model_validate(
        params.model_dump() if hasattr(params, "model_dump") else {}
    )


def _build_stac_storage_bundle(
    params: StacPresetParams,
    *,
    catalog_id: str = "",  # noqa: ARG001 — scope carried by lifecycle, not the SSOT body
    collection_id: Optional[str] = None,  # noqa: ARG001
) -> PresetBundle:
    """Build the SSOT-only bundle: a single ``StacStorageConfig`` entry.

    This is the "enable STAC" half — the SSOT signal the PG/ES drivers read at
    runtime to materialize STAC slices, sidecars, and ES routes.  Present even
    for ``stac_level=none`` (carries ``stac_level=NONE``); revoke removes it and
    the absence restores the platform default (no STAC).
    """
    return PresetBundle(
        entries=(
            PresetBundleEntry(
                slot="stac_storage_config",
                config_cls=StacStorageConfig,
                instance=StacStorageConfig(
                    stac_level=params.stac_level,
                    stac_storage=params.stac_storage,
                ),
                rollback_priority=5,
            ),
        )
    )


# ---------------------------------------------------------------------------
# Presets — the SSOT child + the composite that chains routing + SSOT
# ---------------------------------------------------------------------------


class StacStoragePreset(BundlePreset):
    """The ``StacStorageConfig`` SSOT flip — the "enable STAC" half.

    Writes the single ``StacStorageConfig`` entry that signals STAC
    materialization at the scope.  The PG/ES drivers read this SSOT at runtime
    to add the ``catalog_stac`` / ``collection_stac`` wrapper slices and the
    per-item ``stac_metadata`` sidecar.  Writes NO routing — pair with the
    ``routing`` preset (apply routing first) so the drivers it references exist
    before the signal flips.

    ``stac_level=none`` writes ``StacStorageConfig(stac_level=NONE)``; revoke
    removes it and the absence restores the default (no STAC).
    """

    name = "stac_storage"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = True
    keywords: ClassVar[Tuple[str, ...]] = ("stac", "config")
    params_model = StacPresetParams
    description = (
        "Writes the StacStorageConfig SSOT that enables STAC materialization "
        "at the scope (the signal PG/ES drivers read to add STAC slices, "
        "sidecars, and ES routes).  Parameters: ``stac_level`` (none/catalog/"
        "collection/items) and ``stac_storage`` (ES/PG/ES_PG).  Writes no "
        "routing — pair with ``routing`` (applied first).  Set "
        "stac_level=none to revoke STAC from the scope."
    )

    def build(self, catalog_id: str = "", **_scope: str) -> PresetBundle:  # noqa: ARG002
        return _build_stac_storage_bundle(StacPresetParams(), catalog_id=catalog_id)

    def _build_bundle(
        self, params: BaseModel, scope_kwargs: Dict[str, str]
    ) -> PresetBundle:
        return _build_stac_storage_bundle(
            _coerce_params(params),
            catalog_id=scope_kwargs.get("catalog_id", ""),
            collection_id=scope_kwargs.get("collection_id"),
        )


class StacPreset(CompositePreset):
    """One-shot STAC opt-in — composes ``routing`` + the StacStorageConfig flip.

    Umbrella over the two single-responsibility children: ``routing`` (decide
    where data lives) then ``stac_storage`` (enable STAC).  Apply order matters
    — routing is wired first so the drivers the SSOT references exist before the
    signal flips; revoke reverses (SSOT off, then routing).  Both children
    receive the same ``StacPresetParams``: ``routing`` reads ``drivers`` (derived
    from ``stac_storage`` when unset), ``stac_storage`` reads
    ``stac_level``/``stac_storage``.

    Equivalent to applying ``routing`` and ``stac_storage`` by hand; use this
    when you want the whole STAC stack in a single call, or apply the two
    children separately to stage routing and STAC independently.
    """

    name: ClassVar[str] = "stac"
    description: ClassVar[str] = (
        "One-shot STAC opt-in: composes ``routing`` (parametric per-tier "
        "routing for the chosen drivers) then ``stac_storage`` (the "
        "StacStorageConfig SSOT that enables materialization).  Default: "
        "level=collection, storage=ES_PG (drivers=pg_es).  Set stac_level=none "
        "to revoke STAC.  Apply the two children separately to stage routing "
        "and STAC independently."
    )
    keywords: ClassVar[Tuple[str, ...]] = ("stac", "routing", "composite")
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = True
    params_model: ClassVar[Type[BaseModel]] = StacPresetParams
    compose: ClassVar[Tuple[str, ...]] = ("routing", "stac_storage")
