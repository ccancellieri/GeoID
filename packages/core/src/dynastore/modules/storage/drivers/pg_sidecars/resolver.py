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
Sidecar-resolution helper for the PG driver — ``_effective_sidecars``.

This module lives in the PG sidecars subpackage so it's colocated with
the configs it resolves, and so ``storage/drivers/postgresql.py``,
``modules/catalog/item_service.py``, and their test fixtures can all
import it without reaching into another module's internals.

Role in the role-based driver refactor (plan §M1b):

- M1b.1 retyped ``ItemsPostgresqlDriverConfig.sidecars`` as a
  discriminated union and kept an eager ``[geometries, attributes]``
  default for backwards-compat.
- M1b.2 (this module) supplies the **lazy** resolution path the driver
  now uses at DDL / read / write time: core PG defaults + registry
  injections get assembled here, on demand, based on actual collection
  context — so the field's ``default_factory`` can finally flip to
  empty (see ``driver_config.py``).  A default-body
  ``POST /collections/{id}`` now persists **zero** sidecar rows; the
  first DDL or write call resolves defaults here.
- M1b.3 uses this helper from the PG driver's new ``init_collection``
  hook when a caller explicitly supplies PG-specific ``layer_config``.

The caller-supplied ``col_config`` is typed as ``Any`` to avoid a
circular import with ``storage.driver_config`` (which imports the
concrete sidecar configs from this package).  The helper only reads
``.sidecars`` and ``.collection_type`` — duck typing is sufficient.
"""

from typing import Any, Dict, List, Optional

from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry


def _effective_sidecars(
    col_config: Optional[Any],
    *,
    catalog_id: str,
    collection_id: str = "",
    collection_type: str = "VECTOR",
    context: Optional[Dict[str, Any]] = None,
) -> List[Any]:
    """Resolve the effective sidecar list for a PG-backed collection.

    Layering (in order):

    1. **Explicit caller config** — if ``col_config.sidecars`` is non-empty,
       use it as-is.  Caller opted in to a specific PG layout.

       For RECORDS collections, geometry sidecars in the explicit list are
       silently dropped here — RECORDS have no spatial component.  Phase
       1.6 relocated this from a model_validator on
       ``ItemsPostgresqlDriverConfig.strip_geometry_for_records``.

    2. **Registry injections** — ``SidecarRegistry.get_injected_sidecar_configs``
       is the source of truth for collection_type-specific defaults
       (``VECTOR`` → ``[geometries, attributes]``, ``RECORDS`` →
       ``[attributes]``) and extension-contributed sidecars (e.g.
       ``item_metadata`` from STAC).  Entries whose ``sidecar_type`` is
       already present from step 1 are **not** duplicated; missing
       types are appended in registry-declaration order.

       So an empty ``col_config.sidecars`` with ``collection_type=VECTOR``
       still yields ``[geometries, attributes, ...]`` at use time — same
       effective behaviour as the pre-M1b.2 eager ``_default_sidecars()``
       default, but without persisting those defaults into
       ``collection_configs`` (plan §Principle — default-fast).

    Args:
        col_config: A ``ItemsPostgresqlDriverConfig`` (or ``None`` when the
            driver couldn't load one — fallback path).  Only ``.sidecars``
            is read; typed as ``Any`` to avoid a circular import with
            ``storage.driver_config``.
        catalog_id: Passed into the registry injection context so
            extension-scoped injections can respect per-catalog policy.
        collection_id: Same role as ``catalog_id``.  Defaults to ``""``
            for call sites with no specific collection in scope (e.g.
            catalog-wide introspection).
        collection_type: ``"VECTOR"`` (default), ``"RECORDS"``, or
            ``"RASTER"``.  Hoisted out of ``ItemsPostgresqlDriverConfig``
            in Phase 1.6 — async callers fetch the
            :class:`~dynastore.modules.catalog.catalog_config.CollectionInfo`
            PluginConfig and pass ``ct.kind.value`` here.
        context: Caller-supplied injection context dict.  Merged with
            the derived defaults (``catalog_id``, ``collection_id``,
            ``collection_type``) before the registry call.

    Returns:
        Ordered list of concrete ``SidecarConfig`` subclass instances
        (``GeometriesSidecarConfig``, ``FeatureAttributeSidecarConfig``,
        ``ItemMetadataSidecarConfig``, …).  Never ``None``.
    """
    # Step 1 — explicit caller config
    explicit: List[Any] = (
        list(col_config.sidecars)
        if col_config is not None and getattr(col_config, "sidecars", None)
        else []
    )

    # Phase 1.6: drop geometry sidecars for RECORDS — relocated from the
    # ``strip_geometry_for_records`` model_validator on
    # ``ItemsPostgresqlDriverConfig`` (which can no longer see
    # ``collection_type`` since the field was hoisted to ``CollectionInfo``).
    if collection_type == "RECORDS" and explicit:
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
        )
        explicit = [
            s for s in explicit if not isinstance(s, GeometriesSidecarConfig)
        ]

    # Step 2 — registry injections.  When `explicit` is empty, this step
    # provides the effective defaults for the collection type; when
    # `explicit` is non-empty, it only fills in types the caller omitted.
    injection_ctx: Dict[str, Any] = dict(context or {})
    injection_ctx.setdefault("catalog_id", catalog_id)
    injection_ctx.setdefault("collection_id", collection_id)
    injection_ctx.setdefault("collection_type", collection_type)

    try:
        injected = SidecarRegistry.get_injected_sidecar_configs(injection_ctx) or []
    except Exception:
        # Registry failure must never break DDL / write / read — fall back
        # to whatever we resolved in step 1.  Matches the old inline-
        # injection block's failure semantics in collection_service.py.
        injected = []

    existing_types = {getattr(s, "sidecar_type", None) for s in explicit}
    merged = list(explicit)
    for inj in injected:
        sc_type = getattr(inj, "sidecar_type", None)
        if sc_type not in existing_types:
            merged.append(inj)
            existing_types.add(sc_type)

    return merged
