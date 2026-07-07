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

"""``TileSourceProtocol`` — pluggable tile-byte rendering backends.

Co-located with the tiles module (not ``models/protocols``) because it
depends on tiles-specific types (``TileMatrixSet``,
``SimplificationAlgorithm``) rather than being a cross-module protocol.

v1 ships a single implementation, ``PostgisTileSource``, which thin-delegates
to ``tiles_db.get_features_as_mvt_filtered`` (native ``ST_AsMVT``). Additional
sources (e.g. an application-level encoder for a non-PG driver) register via
``register_plugin`` and are selected by the first whose ``supports(driver)``
returns True.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Protocol, Union, runtime_checkable

from dynastore.tools.geospatial import SimplificationAlgorithm
from dynastore.modules.tiles.tiles_models import TileMatrixSet

logger = logging.getLogger(__name__)


class TileSourceNotSupported(RuntimeError):
    """Raised when no registered ``TileSourceProtocol`` supports a driver.

    Distinct from a render-time miss (which returns ``None``/empty bytes):
    this means the platform has no way at all to produce tile bytes for the
    resolved driver, not that the tile happens to have no features.
    """

    def __init__(self, driver_kind: str) -> None:
        self.driver_kind = driver_kind
        super().__init__(
            f"No TileSourceProtocol registered that supports driver kind "
            f"{driver_kind!r}."
        )


@runtime_checkable
class TileSourceProtocol(Protocol):
    """Protocol for rendering tile bytes from a resolved storage driver."""

    def supports(self, driver: Any, format: str = "mvt") -> bool:
        """True if this source can render ``format`` tiles for the given resolved driver.

        ``format`` defaults to ``"mvt"`` so existing single-source deployments
        (v1 shipped only ``PostgisTileSource``) keep working unmodified; a
        second source registered for a different output format (e.g. the maps
        extension's PNG renderer) narrows on the format it accepts.
        """
        ...

    async def render_tile(
        self,
        conn: Any,
        *,
        resolved_collections: List[Dict[str, Any]],
        tms_def: Union[TileMatrixSet, Any],
        target_srid: int,
        z: str,
        x: int,
        y: int,
        format: str = "mvt",
        datetime_str: Optional[str] = None,
        cql_filter: Optional[str] = None,
        filter_lang: str = "cql2-text",
        filter_crs_srid: Optional[int] = None,
        subset_params: Optional[Dict[str, Any]] = None,
        simplification: Optional[float] = None,
        simplification_algorithm: SimplificationAlgorithm = SimplificationAlgorithm.TOPOLOGY_PRESERVING,
    ) -> Optional[bytes]:
        """Render one tile's bytes, or None when there is nothing to emit."""
        ...


class PostgisTileSource(TileSourceProtocol):
    """Renders MVT tiles from PostGIS via native ``ST_AsMVT``.

    ``supports`` duck-types on ``_get_effective_driver_config`` — the same
    PG-specific attribute probe already used to select the effective driver
    config in ``tiles_module.get_tile_resolution_params`` — rather than an
    isinstance/class-name check, so any driver implementation offering that
    method is accepted.
    """

    def supports(self, driver: Any, format: str = "mvt") -> bool:
        return (
            format in ("mvt", "pbf")
            and getattr(driver, "_get_effective_driver_config", None) is not None
        )

    async def render_tile(
        self,
        conn: Any,
        *,
        resolved_collections: List[Dict[str, Any]],
        tms_def: Union[TileMatrixSet, Any],
        target_srid: int,
        z: str,
        x: int,
        y: int,
        format: str = "mvt",
        datetime_str: Optional[str] = None,
        cql_filter: Optional[str] = None,
        filter_lang: str = "cql2-text",
        filter_crs_srid: Optional[int] = None,
        subset_params: Optional[Dict[str, Any]] = None,
        simplification: Optional[float] = None,
        simplification_algorithm: SimplificationAlgorithm = SimplificationAlgorithm.TOPOLOGY_PRESERVING,
    ) -> Optional[bytes]:
        from dynastore.modules.tiles import tiles_db

        try:
            return await tiles_db.get_features_as_mvt_filtered(
                conn=conn,
                resolved_collections=resolved_collections,
                tms_def=tms_def,
                target_srid=target_srid,
                z=z,
                x=x,
                y=y,
                datetime_str=datetime_str,
                cql_filter=cql_filter,
                filter_lang=filter_lang,
                filter_crs_srid=filter_crs_srid,
                subset_params=subset_params,
                simplification=simplification,
                simplification_algorithm=simplification_algorithm,
            )
        except ValueError as exc:
            if str(exc).startswith("Invalid CQL filter"):
                raise
            # Storage resolution failed mid-pipeline (e.g. driver config has no
            # physical_table) — the render was never attempted, so this is
            # `None` (not cacheable), distinct from `tiles_db`'s `b""` return
            # for a query that ran and confirmed zero features (cacheable).
            logger.warning("PostgisTileSource: render skipped (storage unresolved): %s", exc)
            return None
