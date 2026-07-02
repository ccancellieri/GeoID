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

"""Shared DB-row bootstrap helpers for the Features and Records OGC generators.

Both ``extensions.features.ogc_generator`` and
``extensions.records.records_generator`` build their protocol-specific wire
format (``Feature`` vs. ``Record``) from the same starting point: a raw DB
row that has to be mapped through the sidecar pipeline, and a ``validity``
TSTZRANGE that has to be parsed into ISO temporal bounds. This module holds
that shared, protocol-agnostic slice; the response envelope, media types,
link rels, and queryables stay in the respective generators (#2704).
"""

import logging
import re
from typing import Any, Optional, Tuple, Union

from geojson_pydantic import Feature as GeoJSONFeature

from dynastore.models.protocols import ItemsProtocol
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def resolve_mapped_feature(
    item: Union[dict, Any],
    layer_config: Optional[ItemsPostgresqlDriverConfig],
    read_policy: Optional[Any] = None,
) -> Any:
    """Map a raw DB row into a GeoJSON ``Feature`` via the sidecar pipeline.

    Items that are already a ``Feature`` (the canonical ``get_item``/
    ``stream_items`` path) pass through unchanged. ``read_policy`` is
    threaded into the sidecar pipeline so the raw-row fallback honours
    ``feature_type.expose`` / ``external_id_as_feature_id``.

    Return type is ``Any`` rather than ``geojson_pydantic.Feature``: the
    sidecar pipeline (``ItemsProtocol.map_row_to_feature``) actually returns
    ``dynastore.models.ogc.Feature``, a subclass with extra fields — pinning
    the annotation to the base type would misrepresent what callers get.
    """
    if isinstance(item, GeoJSONFeature):
        return item
    items_mod = get_protocol(ItemsProtocol)
    if items_mod and layer_config:
        return items_mod.map_row_to_feature(item, layer_config, read_policy=read_policy)
    logger.warning(
        "Cannot map DB row: ItemsProtocol unavailable or no layer_config. "
        "Returning empty feature."
    )
    return GeoJSONFeature(type="Feature", geometry=None, properties={})


def parse_temporal_range(value: Any) -> Optional[Tuple[Optional[str], Optional[str]]]:
    """Parse a ``validity`` TSTZRANGE-like value into ``(start_iso, end_iso)``.

    Handles both the driver-native range object (``.lower``/``.upper``,
    optionally exposing ``is_infinite()``) and the Postgres range string
    notation (e.g. ``[2024-01-01,2024-06-01)``) a raw-row fallback may hand
    back. An unbounded side (``-infinity``/``infinity``, or a range object
    reporting ``is_infinite()``) maps to ``None`` rather than a literal
    string.

    Returns ``None`` (not a ``(None, None)`` tuple) when *value* is not a
    recognisable range at all — an unparseable string or a parse error —
    so callers can distinguish "no range found" from "an open-ended range
    was found".
    """
    if value is None:
        return None
    try:
        if hasattr(value, "lower"):
            start, end = value.lower, value.upper
            start_iso: Optional[str] = None
            end_iso: Optional[str] = None
            if start and not getattr(start, "is_infinite", lambda: False)():
                start_iso = start.isoformat() if hasattr(start, "isoformat") else str(start)
            if end and not getattr(end, "is_infinite", lambda: False)():
                end_iso = end.isoformat() if hasattr(end, "isoformat") else str(end)
            return start_iso, end_iso

        match = re.search(r"[\[\(]([^,]*),\s*([^\]\)]*)", str(value))
        if not match:
            return None
        raw_start, raw_end = match.groups()
        start_iso = (
            raw_start.strip().strip('"')
            if raw_start and raw_start.strip() and raw_start.strip() != "-infinity"
            else None
        )
        end_iso = (
            raw_end.strip().strip('"')
            if raw_end and raw_end.strip() and raw_end.strip() != "infinity"
            else None
        )
        return start_iso, end_iso
    except Exception as e:
        logger.warning("Failed to parse validity range %s: %s", value, e)
        return None
