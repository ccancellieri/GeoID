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

"""Unified tile-render engine.

Consolidates the TMS-resolution / SRID-resolution / collection-metadata-
resolution blocks that used to be duplicated between
``dynastore.extensions.tiles.tiles_service`` (per-request path) and
``dynastore.tasks.tiles_preseed.task`` (preseed loop), and dispatches
rendering to the registered :class:`~dynastore.modules.tiles.tiles_source.TileSourceProtocol`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional

from dynastore.tools.cache import cached
from dynastore.tools.geospatial import SimplificationAlgorithm
from dynastore.modules.tiles.tiles_source import TileSourceProtocol, TileSourceNotSupported
from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS

logger = logging.getLogger(__name__)

# Returns None to continue, or a short reason string ("budget" /
# "disconnected") to abort. Checked at loop boundaries only (never
# per-feature) so it adds no measurable overhead to the render itself.
ShouldAbort = Callable[[], Awaitable[Optional[str]]]


class RenderAborted(Exception):
    """Raised when a ``should_abort`` callback reports the render must stop.

    ``reason`` is the string returned by the callback (e.g. ``"budget"`` or
    ``"disconnected"``) so callers can respond differently — a request-scoped
    caller typically maps ``"budget"`` to a 503 and ``"disconnected"`` to a
    quiet drop.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"render aborted: {reason}")

try:
    import morecantile
except ImportError:  # pragma: no cover — optional dep, see hard-import gate for tasks/tiles_preseed
    morecantile = None


@dataclass
class TileRenderContext:
    """Everything ``render_tile`` needs to produce bytes for one (catalog,
    collections, tms) combination, resolved once per request/preseed-tile-loop
    iteration."""

    catalog_id: str
    resolved_collections: List[Dict[str, Any]]
    driver: Any
    source: TileSourceProtocol
    tms_def: Any
    target_srid: int


async def _resolve_tms(catalog_id: str, tms_id: str, *, morecantile_compatible: bool) -> Optional[Any]:
    """Resolve custom TMS (DB) -> BUILTIN -> morecantile registry, in that order.

    ``morecantile_compatible=True`` converts a resolved Pydantic
    ``TileMatrixSet`` (custom or builtin) to a ``morecantile.TileMatrixSet``
    so callers needing ``.tiles(*bbox, zooms=[z])`` iteration (the preseed
    task) get a usable object. Request-path callers resolve one z/x/y at a
    time and leave this False.
    """
    from dynastore.modules.tiles import tiles_module

    tms_def = await tiles_module.get_custom_tms(catalog_id=catalog_id, tms_id=tms_id)
    if not tms_def:
        tms_def = BUILTIN_TILE_MATRIX_SETS.get(tms_id)
        if not tms_def:
            if morecantile is None:
                raise ValueError(
                    f"TMS '{tms_id}' not found and morecantile is not installed."
                )
            tms_def = morecantile.tms.get(tms_id)

    if morecantile_compatible and tms_def is not None and not hasattr(tms_def, "tiles"):
        if morecantile is None:
            raise ValueError(
                f"TMS '{tms_id}' needs morecantile tile-iteration but morecantile "
                "is not installed."
            )
        tms_dict = (
            tms_def.model_dump(exclude_none=True) if hasattr(tms_def, "model_dump") else tms_def
        )
        tms_def = morecantile.TileMatrixSet.model_validate(tms_dict)
    return tms_def


async def build_render_context(
    catalog_id: str,
    collection_ids: List[str],
    tms_id: str,
    *,
    engine: Any = None,
    morecantile_compatible: bool = False,
    format: str = "mvt",
    should_abort: Optional[ShouldAbort] = None,
) -> Optional[TileRenderContext]:
    """Resolve metadata, TMS, target SRID, and TileSource for a tile render.

    ``driver``/``source`` are resolved from the FIRST collection id — v1 has
    exactly one registered ``TileSourceProtocol`` per format (PostGIS for
    mvt/pbf) so this is sufficient; a future multi-driver render would need a
    per-collection source lookup instead.

    ``format`` (default ``"mvt"``) narrows source selection to the registered
    ``TileSourceProtocol`` whose ``supports(driver, format)`` returns True —
    e.g. the maps extension's PNG renderer registers for ``format="png"``
    alongside the core PostGIS MVT source.

    ``should_abort``, when given, is checked once per collection before its
    metadata is resolved — the natural loop boundary here (#2898). Raises
    :class:`RenderAborted` immediately when it reports a reason, so a slow
    multi-collection resolution (or a client that already disconnected)
    doesn't keep spending routing/DB work on collections that will never be
    used.

    Returns ``None`` when no collection's metadata resolves or the TMS can't
    be resolved (both cases are already logged by the callee). Raises
    :class:`TileSourceNotSupported` when collections and TMS resolve fine but
    no registered source can render the resolved driver.
    """
    from dynastore.modules import get_protocols
    from dynastore.modules.tiles import tiles_module
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation
    from dynastore.modules.storage.hints import Hint

    resolved_collections: List[Dict[str, Any]] = []
    for coll_id in collection_ids:
        if should_abort is not None:
            reason = await should_abort()
            if reason:
                raise RenderAborted(reason)
        meta = await tiles_module.get_tile_resolution_params(catalog_id, coll_id)
        if meta:
            resolved_collections.append(meta)

    if not resolved_collections:
        return None

    try:
        tms_def = await _resolve_tms(
            catalog_id, tms_id, morecantile_compatible=morecantile_compatible
        )
    except ValueError as exc:
        logger.error("build_render_context: TMS resolution failed for %r: %s", tms_id, exc)
        return None
    if tms_def is None:
        return None

    target_srid = 3857
    crs = getattr(tms_def, "crs", None)
    if crs is not None:
        try:
            target_srid = await tiles_module.resolve_srid(
                conn=engine, crs_str=str(crs), catalog_id=catalog_id,
            )
        except Exception as exc:
            logger.warning(
                "build_render_context: SRID resolution failed for %r: %s; using 3857.",
                crs, exc,
            )

    primary_collection = collection_ids[0]
    try:
        driver = await get_driver(
            Operation.READ, catalog_id, primary_collection,
            hints=frozenset({Hint.TILES}),
        )
    except Exception as exc:
        logger.warning(
            "build_render_context: no tile-capable driver for %s/%s: %s",
            catalog_id, primary_collection, exc,
        )
        return None

    source: Optional[TileSourceProtocol] = None
    for candidate in get_protocols(TileSourceProtocol):
        if candidate.supports(driver, format):
            source = candidate
            break
    if source is None:
        raise TileSourceNotSupported(type(driver).__name__)

    return TileRenderContext(
        catalog_id=catalog_id,
        resolved_collections=resolved_collections,
        driver=driver,
        source=source,
        tms_def=tms_def,
        target_srid=target_srid,
    )


@cached(
    maxsize=512,
    ttl=60,
    jitter=5,
    namespace="mvt_l1",
    ignore=["conn"],
    condition=lambda r: r is not None,
)
async def _cached_render_tile(
    conn: Any,
    source: TileSourceProtocol,
    resolved_collections: List[Dict[str, Any]],
    tms_def: Any,
    target_srid: int,
    z: str,
    x: int,
    y: int,
    format: str,
    datetime_str: Optional[str],
    cql_filter: Optional[str],
    filter_lang: str,
    filter_crs_srid: Optional[int],
    subset_params: Optional[Dict[str, Any]],
    simplification: Optional[float],
    simplification_algorithm: SimplificationAlgorithm,
) -> Optional[bytes]:
    """In-process L1 cache above the storage-provider L2 cache.

    Mirrors the ``mvt_l1`` cache previously on
    ``tiles_service.TilesService._generate_mvt``: same TTL/maxsize/jitter, and
    ``conn`` stays out of the key (a fresh connection is acquired per request
    but represents the same underlying data).
    """
    return await source.render_tile(
        conn,
        resolved_collections=resolved_collections,
        tms_def=tms_def,
        target_srid=target_srid,
        z=z,
        x=x,
        y=y,
        format=format,
        datetime_str=datetime_str,
        cql_filter=cql_filter,
        filter_lang=filter_lang,
        filter_crs_srid=filter_crs_srid,
        subset_params=subset_params,
        simplification=simplification,
        simplification_algorithm=simplification_algorithm,
    )


async def render_tile(
    conn: Any,
    ctx: TileRenderContext,
    z: str,
    x: int,
    y: int,
    *,
    format: str = "mvt",
    use_l1_cache: bool = False,
    datetime_str: Optional[str] = None,
    cql_filter: Optional[str] = None,
    filter_lang: str = "cql2-text",
    filter_crs_srid: Optional[int] = None,
    subset_params: Optional[Dict[str, Any]] = None,
    simplification: Optional[float] = None,
    simplification_algorithm: SimplificationAlgorithm = SimplificationAlgorithm.TOPOLOGY_PRESERVING,
) -> Optional[bytes]:
    """Render one tile's bytes via ``ctx.source``.

    ``use_l1_cache=True`` wraps the call with the process-local ``mvt_l1``
    cache (the live request-serving path); the preseed task passes False —
    every tile is rendered exactly once per preseed run, so an L1 cache would
    only add memory pressure without a repeat-read to amortize.
    """
    if use_l1_cache:
        return await _cached_render_tile(
            conn,
            ctx.source,
            ctx.resolved_collections,
            ctx.tms_def,
            ctx.target_srid,
            z,
            x,
            y,
            format,
            datetime_str,
            cql_filter,
            filter_lang,
            filter_crs_srid,
            subset_params,
            simplification,
            simplification_algorithm,
        )
    return await ctx.source.render_tile(
        conn,
        resolved_collections=ctx.resolved_collections,
        tms_def=ctx.tms_def,
        target_srid=ctx.target_srid,
        z=z,
        x=x,
        y=y,
        format=format,
        datetime_str=datetime_str,
        cql_filter=cql_filter,
        filter_lang=filter_lang,
        filter_crs_srid=filter_crs_srid,
        subset_params=subset_params,
        simplification=simplification,
        simplification_algorithm=simplification_algorithm,
    )
