#    Copyright 2025 FAO
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

"""OGC Dimensions extension — exposes dimension providers as REST API
and materializes dimension members as OGC API - Records.

Wraps the ogc-dimensions package (pip dependency) into a Dynastore
ExtensionProtocol so it can be deployed on the tools Cloud Run service.

At startup the extension:

1. Registers all dimension providers in the ogc-dimensions router
   (live API at ``/dimensions/{id}/items``, ``/inverse``, etc.).
2. Materializes every dimension's members into a RECORDS-type collection
   so they are also browsable via the OGC API - Records extension at
   ``/records/catalogs/_dimensions_/collections/{dim_id}/items``.

The extension declares OGC API - Dimensions conformance as a
**profile of OGC API - Records**, packaged as OGC Building Blocks:

- ``dimension-collection`` — a Records catalogue with a generator object
- ``dimension-member`` — a Record with ``time.interval`` + ``labels`` map
- ``dimension-pagination`` — OGC Common Part 2 pagination profile
- ``dimension-inverse`` — value → member mapping (new conformance class)
- ``dimension-hierarchical`` — children / ancestors navigation
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, FastAPI

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.models.driver_context import DriverContext

# ``ogc_dimensions`` is an optional extra (extension_dimensions).
# Imported at module top so a missing dep is caught at entry-point load time
# (clean WARN "Skipping dynastore.extensions plugin 'dimensions': … No module
# named 'ogc_dimensions'") instead of crashing every cold start with
# "Failed to instantiate extension 'dimensions'". Services whose SCOPE excludes
# extension_dimensions silently skip this extension; helpers like
# ``get_registered_dimensions`` below already use class-name matching to avoid
# triggering this import from the rest of the codebase.
from ogc_dimensions.api.routes import (
    DIMENSIONS, DimensionConfig, router as dimensions_router,
)
from ogc_dimensions.providers import (
    DailyPeriodProvider,
    IntegerRangeProvider,
    StaticTreeProvider,
    LeveledTreeProvider,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DIMENSIONS_CATALOG_ID = "_dimensions_"
"""Internal catalog ID for materialized dimension collections."""

UPSERT_BATCH_SIZE = 500
"""Number of records to upsert per batch to avoid memory spikes."""

# ---------------------------------------------------------------------------
# OGC Dimensions conformance URIs (profile of OGC API - Records)
# ---------------------------------------------------------------------------
# Mirrors spec/building-blocks/bblocks.json in ogc-dimensions — keep both in sync.
OGC_DIMENSIONS_URIS = [
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/core",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-collection",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-member",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-pagination",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-inverse",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-hierarchical",
]


# ---------------------------------------------------------------------------
# Member → Feature conversion
# ---------------------------------------------------------------------------


def _member_to_feature(
    member: Any,
    dim_name: str,
    dim_type: str,
) -> Dict[str, Any]:
    """Convert a ``GeneratedMember`` to a GeoJSON Feature dict (Record).

    Parameters
    ----------
    member
        A ``GeneratedMember`` from the ogc-dimensions generator.
    dim_name
        Dimension identifier (e.g. ``"temporal-dekadal"``).
    dim_type
        Dimension type string (``"temporal"``, ``"nominal"``, ``"ordinal"``).
    """
    code = member.code or str(member.value)
    extra = member.extra or {}

    props: Dict[str, Any] = {
        "title": extra.get("label") or code,
        "recordType": "dimension-member",
        "dimension:type": dim_type,
        "dimension:code": code,
        "dimension:index": member.index,
    }

    # Temporal interval — OGC Records time object + dimension:start/end
    if member.start is not None and member.end is not None:
        props["time"] = {"interval": [str(member.start), str(member.end)]}
        props["dimension:start"] = str(member.start)
        props["dimension:end"] = str(member.end)
        props["valid_from"] = str(member.start)
        props["valid_to"] = str(member.end)

    # Multilingual labels
    labels = extra.get("labels")
    if labels and isinstance(labels, dict):
        props["labels"] = labels

    # Hierarchy
    parent_code = extra.get("parent_code")
    if parent_code is not None:
        props["dimension:parent"] = parent_code
    level = extra.get("level")
    if level is not None:
        props["dimension:level"] = level
    if member.has_children:
        props["dimension:has_children"] = True

    # Extra fields (unit, etc.) — pass through
    for key in ("unit", "rank"):
        if key in extra:
            props[key] = extra[key]

    # Lower/upper for integer-range
    if "lower" in extra:
        props["dimension:start"] = extra["lower"]
    if "upper" in extra:
        props["dimension:end"] = extra["upper"]

    return {
        "type": "Feature",
        "id": code,
        "geometry": None,
        "properties": props,
    }


def _infer_dim_type(generator: Any) -> str:
    """Infer the dimension type string from the generator class."""
    cls_name = type(generator).__name__.lower()
    if "period" in cls_name or "temporal" in cls_name:
        return "temporal"
    if "integer" in cls_name or "range" in cls_name:
        return "ordinal"
    if "leveled" in cls_name:
        return "nominal"
    if "tree" in cls_name:
        return "nominal"
    return "other"


# ---------------------------------------------------------------------------
# cube:dimensions metadata builder
# ---------------------------------------------------------------------------


def _build_provider(generator: Any) -> Dict[str, Any]:
    """Build the full provider object (stored at collection level).

    Mirrors the ``provider`` block produced by the ogc-dimensions reference
    implementation's ``_dimension_to_collection()``.
    """
    provider: Dict[str, Any] = {
        "type": getattr(generator, "provider_type", type(generator).__name__),
        "invertible": getattr(generator, "invertible", False),
        "hierarchical": getattr(generator, "hierarchical", False),
    }
    if hasattr(generator, "config_as_dict"):
        provider["config"] = generator.config_as_dict()
    if hasattr(generator, "search_protocols"):
        provider["search"] = [s.value for s in generator.search_protocols]
    return provider


def _build_cube_dimensions(
    dim_name: str,
    dim_type: str,
    generator: Any,
) -> Dict[str, Any]:
    """Build the slim ``cube:dimensions`` entry for STAC clients.

    Only carries ``type`` and a slim ``provider: {type}`` reference.
    Full provider details live at the collection level under ``provider``.
    """
    provider_type = getattr(generator, "provider_type", type(generator).__name__)
    return {
        dim_name: {
            "type": dim_type,
            "provider": {"type": provider_type},
        }
    }


# ---------------------------------------------------------------------------
# Materialization helpers
# ---------------------------------------------------------------------------


async def _enumerate_all_members(
    generator: Any,
    extent_min: str,
    extent_max: str,
    dim_name: str,
    dim_type: str,
) -> List[Dict[str, Any]]:
    """Enumerate all members from a generator, handling pagination.

    For temporal/integer providers with large extents this paginates
    through the full sequence.  For tree providers the total is bounded
    by the node count.
    """
    features: List[Dict[str, Any]] = []
    page_size = 500
    offset = 0

    while True:
        result = generator.generate(
            extent_min, extent_max, limit=page_size, offset=offset,
        )
        for m in result.members:
            features.append(_member_to_feature(m, dim_name, dim_type))

        if result.number_returned < page_size:
            break
        offset += page_size

    return features


def _extract_stored_cube_dimensions(existing: Any) -> Dict[str, Any]:
    """Return the ``cube:dimensions`` subtree stored on an existing collection.

    Handles both localized (``LocalizedExtraMetadata``) and plain-dict shapes
    of ``extra_metadata`` — the read path returns a Pydantic model whose
    ``resolve("en")`` yields the flat dict, while some callers pass the raw
    dict. Missing values resolve to ``{}``.
    """
    if existing is None:
        return {}
    em = getattr(existing, "extra_metadata", None) or {}
    if hasattr(em, "resolve"):
        try:
            em = em.resolve("en") or {}  # type: ignore[reportAttributeAccessIssue]
        except Exception:
            em = {}
    if not isinstance(em, dict):
        return {}
    cube = em.get("cube:dimensions") or {}
    return cube if isinstance(cube, dict) else {}


async def materialize_dimension(
    catalogs: Any,
    dim_name: str,
    dim_config: Any,
    db_resource: Any,
    *,
    force: bool = False,
) -> Dict[str, Any]:
    """Create (if missing) a RECORDS collection and upsert all dimension
    members for ``dim_name``.

    Idempotency is gated by a **deep-equal comparison** between the
    collection's persisted ``extra_metadata["cube:dimensions"]`` and the
    value built from the current ``dim_config``. When they match the
    upsert loop is skipped entirely — the existing collection already
    reflects the desired state. This replaces the previous
    ``PropertiesProtocol``-backed SHA256 fingerprint (opaque, split-state,
    fragile when the property store was missing).

    Pass ``force=True`` to bypass the skip and re-upsert unconditionally
    (used by the ``dimensions_materialize`` OGC Process when the caller
    explicitly asks for a rebuild).

    Returns a small summary dict: ``{"materialized": int, "skipped": bool,
    "reason": str}``. Callers can aggregate these across dimensions.
    """
    generator = dim_config.provider
    dim_type = _infer_dim_type(generator)
    cube_dimensions = _build_cube_dimensions(dim_name, dim_type, generator)
    provider = _build_provider(generator)

    # Extent for temporal dimensions — STAC Collection requires `spatial`;
    # dimension records are not geographically bounded, so default to global.
    extent = None
    if dim_type == "temporal" and dim_config.extent_min and dim_config.extent_max:
        extent = {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {
                "interval": [
                    [f"{dim_config.extent_min}T00:00:00Z", f"{dim_config.extent_max}T00:00:00Z"]
                ]
            },
        }

    # Defensive: ensure the dimensions catalog row is visible inside THIS
    # transaction. The outer caller may have pre-created it, but multi-replica
    # races can leave the row invisible to this tx. ``ensure_catalog_exists``
    # is idempotent thanks to ON CONFLICT.
    await catalogs.ensure_catalog_exists(
        DIMENSIONS_CATALOG_ID, ctx=DriverContext(db_resource=db_resource),
    )

    existing = await catalogs.get_collection(
        DIMENSIONS_CATALOG_ID, dim_name, lang="en",
        ctx=DriverContext(db_resource=db_resource),
    )

    # Fast-path: collection exists and its persisted cube:dimensions matches
    # what the current dim_config would produce → nothing to do.
    if existing and not force:
        stored_cube = _extract_stored_cube_dimensions(existing)
        if stored_cube == cube_dimensions:
            logger.info(
                "Dimension '%s' unchanged (cube:dimensions match) — skipping.",
                dim_name,
            )
            return {"materialized": 0, "skipped": True, "reason": "unchanged"}

    desired_extra: Dict[str, Any] = {
        "provider": provider,
        "cube:dimensions": cube_dimensions,
        "itemType": "record",
    }

    if not existing:
        collection_def: Dict[str, Any] = {
            "id": dim_name,
            "title": dim_name.replace("-", " ").title(),
            "description": dim_config.description,
            "layer_config": {"collection_type": "RECORDS"},
            "extra_metadata": desired_extra,
        }
        if extent:
            collection_def["extent"] = extent

        await catalogs.create_collection(
            DIMENSIONS_CATALOG_ID,
            collection_def,
            ctx=DriverContext(db_resource=db_resource),
        )
        logger.info("Created RECORDS collection: %s/%s", DIMENSIONS_CATALOG_ID, dim_name)

    # Generate all members.
    features = await _enumerate_all_members(
        generator, dim_config.extent_min, dim_config.extent_max, dim_name, dim_type,
    )

    if not features:
        # Still refresh cube:dimensions on the collection so a future run
        # with members can detect "unchanged" correctly.
        if existing:
            await _update_collection_cube_dims(
                catalogs, dim_name, desired_extra, extent, db_resource,
            )
        return {"materialized": 0, "skipped": False, "reason": "no_members"}

    total = 0
    for i in range(0, len(features), UPSERT_BATCH_SIZE):
        batch = features[i : i + UPSERT_BATCH_SIZE]
        await catalogs.upsert(
            DIMENSIONS_CATALOG_ID, dim_name, batch, ctx=DriverContext(db_resource=db_resource),
        )
        total += len(batch)
        logger.debug(
            "Upserted batch %d–%d for %s (%d total)",
            i, i + len(batch), dim_name, total,
        )

    # Refresh cube:dimensions / provider / extent on the collection so the
    # next run's equality check sees the new state (and the cache drops
    # its stale copy via the update_collection invalidation hook).
    if existing:
        await _update_collection_cube_dims(
            catalogs, dim_name, desired_extra, extent, db_resource,
        )

    return {"materialized": total, "skipped": False, "reason": "materialized"}


async def _update_collection_cube_dims(
    catalogs: Any,
    dim_name: str,
    desired_extra: Dict[str, Any],
    extent: Optional[Dict[str, Any]],
    db_resource: Any,
) -> None:
    """Update a dimensions-catalog collection's extra_metadata (and extent)
    to reflect the latest ``cube:dimensions`` / ``provider`` values.

    Called after the upsert loop so the persisted collection row is the
    single source of truth for future equality-based skip decisions, and
    so ``update_collection``'s cache invalidation propagates the new
    values to the L1/L2 cache.
    """
    updates: Dict[str, Any] = {"extra_metadata": desired_extra}
    if extent:
        updates["extent"] = extent
    try:
        await catalogs.update_collection(
            DIMENSIONS_CATALOG_ID,
            dim_name,
            updates,
            ctx=DriverContext(db_resource=db_resource),
        )
    except Exception as exc:  # noqa: BLE001 — non-fatal; next run will retry
        logger.warning(
            "Failed to refresh cube:dimensions on '%s/%s': %s. "
            "Next run will re-materialise and try again.",
            DIMENSIONS_CATALOG_ID, dim_name, exc,
        )


def _register_default_dimensions() -> Dict[str, Any]:
    """Populate the module-level ``DIMENSIONS`` dict with the standard set.

    Idempotent: re-invocations overwrite the same keys with equal-by-value
    DimensionConfig objects. Extracted from ``DimensionsExtension.__init__``
    so that worker contexts (Cloud Run Job containers without a FastAPI app)
    can populate the registry without instantiating the FastAPI extension —
    which is what ``dimensions_materialize`` task needs.
    """
    from .use_cases import ADMIN_NODES, INDICATOR_NODES, SPECIES_NODES

    DIMENSIONS["temporal-dekadal"] = DimensionConfig(
        provider=DailyPeriodProvider(period_days=10, scheme="monthly"),
        description=(
            "Dekadal temporal dimension — 10-day periods, 36 per year, "
            "month-aligned (D1=1-10, D2=11-20, D3=remainder). "
            "Widely used for agricultural monitoring and early warning systems. "
            "100-year extent (1950-2050) demonstrates OGC-style pagination over "
            "large temporal dimensions (3 600+ members)."
        ),
        extent_min="1950-01-01",
        extent_max="2050-12-31",
    )
    DIMENSIONS["temporal-pentadal-monthly"] = DimensionConfig(
        provider=DailyPeriodProvider(period_days=5, scheme="monthly"),
        description=(
            "Pentadal-monthly temporal dimension — 5-day periods, 72 per year, "
            "month-aligned (P1=1-5, P2=6-10, ..., P6=26-EOM). "
            "Used by rainfall estimation products that align dekads and pentads "
            "to the same month boundaries (e.g. CHIRPS, CDT). "
            "100-year extent yields 7 200+ members; illustrates that pentadal "
            "and dekadal periods from the same producer are directly comparable."
        ),
        extent_min="1950-01-01",
        extent_max="2050-12-31",
    )
    DIMENSIONS["temporal-pentadal-annual"] = DimensionConfig(
        provider=DailyPeriodProvider(period_days=5, scheme="annual"),
        description=(
            "Pentadal-annual temporal dimension — 5-day periods, 73 per year, "
            "year-start-aligned (P1=Jan 1-5, ..., P73=Dec 27-31). "
            "Used by global precipitation climatology products that count pentads "
            "from January 1 regardless of month boundaries (e.g. GPCP, CPC/NOAA). "
            "Interoperability note: pentad #12 in the monthly system and pentad "
            "#12 in the annual system refer to different calendar intervals — "
            "a client must know which encoding was used before combining datasets."
        ),
        extent_min="1950-01-01",
        extent_max="2050-12-31",
    )
    DIMENSIONS["indicator-tree"] = DimensionConfig(
        provider=StaticTreeProvider(nodes=INDICATOR_NODES),
        description=(
            "Statistical indicator tree. "
            "Recursive hierarchy: Domain -> Group -> Indicator."
        ),
        extent_min="",
        extent_max="",
    )
    DIMENSIONS["admin-boundaries"] = DimensionConfig(
        provider=LeveledTreeProvider(nodes=ADMIN_NODES),
        description=(
            "Administrative boundaries. Leveled hierarchy: "
            "Continent (L0) -> Country (L1) -> Region (L2). "
            "Supports ?level= filter."
        ),
        extent_min="",
        extent_max="",
    )
    DIMENSIONS["forestry-species"] = DimensionConfig(
        provider=StaticTreeProvider(nodes=SPECIES_NODES),
        description=(
            "Forestry species classification. "
            "Recursive hierarchy with search (exact + like)."
        ),
        extent_min="",
        extent_max="",
    )
    DIMENSIONS["elevation-bands"] = DimensionConfig(
        provider=IntegerRangeProvider(step=50),
        description=(
            "Elevation bands (50 m step, 0-8848 m). "
            "Invertible, searchable, supports /inverse."
        ),
        extent_min="0",
        extent_max="8848",
    )
    return DIMENSIONS


def get_registered_dimensions() -> Dict[str, Any]:
    """Return the ``{dim_name: DimensionConfig}`` registry.

    Populates the module-level ``DIMENSIONS`` dict on first call by
    invoking ``_register_default_dimensions()``. This lets Cloud Run Job
    workers (which never instantiate the FastAPI ``DimensionsExtension``)
    still see the registered dimensions — the dimensions_materialize task
    crashed pre-fix because the registry was only populated as a side
    effect of ``DimensionsExtension.__init__`` running in the FastAPI
    catalog/maps API process, not in the worker container.

    Idempotent: each call re-asserts the same DimensionConfig values into
    the (possibly already populated) registry. Cheap; safe to call from
    the dimensions_materialize task at run time.

    Returns an empty dict only if the ogc_dimensions package isn't
    importable in this SCOPE (which is also what would have happened
    pre-fix when the extension itself failed to import).
    """
    try:
        return _register_default_dimensions()
    except Exception as e:
        logger.warning(
            "get_registered_dimensions: failed to populate registry: %s", e,
            exc_info=True,
        )
        return DIMENSIONS  # whatever partial state we ended up in


async def materialize_all_dimensions(
    dimensions: Dict[str, Any],
    *,
    dim_names: Optional[List[str]] = None,
    force: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """Materialize the configured dimensions as Records collections.

    ``dim_names=None`` (default) materialises every dimension in
    ``dimensions``; otherwise only the named subset is processed.

    ``force=True`` bypasses the per-dimension cube:dimensions equality
    skip. Use it from the OGC Process task when the caller explicitly
    asks for a rebuild.

    Returns a summary dict ``{dim_name: {"materialized": int,
    "skipped": bool, "reason": str, "error": str | None}}`` so callers
    (the task, a notebook, a test) can report per-dimension results.
    """
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    from dynastore.models.protocols import PropertiesProtocol
    from dynastore.tools.discovery import get_protocol
    from dynastore.tools.protocol_helpers import get_engine

    results: Dict[str, Dict[str, Any]] = {}

    catalogs = get_protocol(CatalogsProtocol)
    if not catalogs:
        logger.warning("CatalogsProtocol not available — skipping dimension materialization.")
        return results

    engine = get_engine()
    if not engine:
        logger.warning("DB engine not available — skipping dimension materialization.")
        return results

    try:
        # Pass engine, not a held conn: ensure_catalog_exists opens its own
        # short-lived tx. Holding one outer tx across the full materialisation
        # caused asyncpg `connection was closed in the middle of operation`
        # after thousands of nested savepoints (one SAVEPOINT per item via
        # configs.get_config → resolve_physical_schema).
        await catalogs.ensure_catalog_exists(
            DIMENSIONS_CATALOG_ID, ctx=DriverContext(db_resource=engine),
        )
    except Exception as exc:
        logger.error("Failed to ensure dimensions catalog: %s", exc)
        return results

    targets: Dict[str, Any]
    if dim_names is None:
        targets = dimensions
    else:
        targets = {k: v for k, v in dimensions.items() if k in set(dim_names)}
        missing = set(dim_names) - set(targets.keys())
        for m in missing:
            results[m] = {
                "materialized": 0, "skipped": False,
                "reason": "not_registered", "error": None,
            }

    grand_total = 0
    for dim_name, dim_config in targets.items():
        try:
            summary = await materialize_dimension(
                catalogs, dim_name, dim_config, db_resource=engine, force=force,
            )
            results[dim_name] = {**summary, "error": None}
            grand_total += summary.get("materialized", 0)
            if summary.get("skipped"):
                continue
            logger.info(
                "Materialized %d records for dimension '%s'",
                summary.get("materialized", 0), dim_name,
            )
        except Exception as exc:
            logger.error(
                "Failed to materialize dimension '%s': %s", dim_name, exc,
                exc_info=True,
            )
            results[dim_name] = {
                "materialized": 0, "skipped": False,
                "reason": "error", "error": str(exc),
            }

    logger.info(
        "Dimension materialization complete: %d total records across %d dimensions.",
        grand_total,
        len(targets),
    )
    return results


# ---------------------------------------------------------------------------
# Extension
# ---------------------------------------------------------------------------


class DimensionsExtension(ExtensionProtocol):
    priority: int = 200
    conformance_uris = OGC_DIMENSIONS_URIS

    def __init__(self, app: FastAPI):
        self.app = app

        # Populate the module-level DIMENSIONS registry. Extracted to a free
        # function so worker contexts (Cloud Run Jobs without a FastAPI app)
        # can call it directly via get_registered_dimensions() — the
        # dimensions_materialize task needs this. See the function definition
        # higher in this module for the temporal/hierarchical/integer demos.
        _register_default_dimensions()
        self._dimensions = DIMENSIONS

        self.router = APIRouter(prefix="/dimensions", tags=["OGC Dimensions"])
        self.router.include_router(dimensions_router)

        logger.info(
            "OGC Dimensions extension loaded — %d dimensions registered",
            len(DIMENSIONS),
        )

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        # Heavy dimension materialisation used to run on every pod boot.
        # On Cloud Run that blocked readiness and stormed the DB at scale.
        # The work now lives in the `dimensions_materialize` OGC Process
        # task — trigger it explicitly once per deploy (manually or via a
        # deploy hook). The lifespan only keeps the fallback for local/dev
        # convenience, gated by an env flag (default off).
        on_boot = os.getenv("DIMENSIONS_MATERIALIZE_ON_BOOT", "").lower() in (
            "1", "true", "yes", "on",
        )
        if on_boot:
            logger.info(
                "DIMENSIONS_MATERIALIZE_ON_BOOT=1 — running in-lifespan "
                "materialisation. Prefer the dimensions_materialize task "
                "for production.",
            )
            await materialize_all_dimensions(self._dimensions)
        else:
            logger.info(
                "DimensionsExtension: lifespan materialisation skipped "
                "(set DIMENSIONS_MATERIALIZE_ON_BOOT=1 to enable). Trigger "
                "the 'dimensions_materialize' OGC Process to populate.",
            )
        yield
