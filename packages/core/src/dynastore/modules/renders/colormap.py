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

"""SLD ColorMap → rio-tiler colormap (discrete dict or interval sequence).

Parses an SLD ``<ColorMap>`` element (SLD 1.0 or 1.1, any namespace) and
produces a value suitable for passing directly to
``rio_tiler.models.ImageData.render(colormap=...)``:

- ``type="values"`` → ``dict[int | float, RGBA]`` (rio-tiler's discrete
  colormap; pixels must match an entry exactly, per GeoServer semantics).
- ``type="intervals"`` → ``[((lo, hi), RGBA), ...]`` (rio-tiler's interval
  colormap; each entry's quantity is the lower bound of its interval).
- ``type="ramp"`` — the SLD **default** when the attribute is absent — →
  interval colormap approximating GeoServer's linear color interpolation:
  each segment between adjacent entries is subdivided into
  ``_RAMP_STEPS_PER_SEGMENT`` linearly-interpolated sub-intervals, with
  values clamped to the first/last entry color outside the entry range.

Design constraints:
- Pure function, no I/O, no DB, no FastAPI — unit-testable without any
  infrastructure.
- Only ``lxml`` is required (already a transitive dependency via the styles
  extension, and explicitly listed in the renders extension's own
  pyproject.toml).
- Quantities are floats (gismgr SLDs emit ``quantity="0.0"``); entries whose
  ``quantity`` cannot be parsed as a number are skipped with a warning,
  rather than failing the whole parse.
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

# rio-tiler colormap type aliases for documentation clarity. Both shapes are
# accepted verbatim by ``ImageData.render(colormap=...)`` (dict → discrete
# lookup, sequence → interval lookup).
RGBA = Tuple[int, int, int, int]
DiscreteColormap = Dict[Union[int, float], RGBA]
IntervalColormap = List[Tuple[Tuple[float, float], RGBA]]
RioColormap = Union[DiscreteColormap, IntervalColormap]

# Sub-intervals generated per adjacent-entry segment when approximating a
# ramp. 16 steps keeps the color error per step imperceptible (< 1/16 of the
# segment's color delta) while bounding the interval count (~16×segments).
_RAMP_STEPS_PER_SEGMENT = 16

# SLD XML namespaces used when parsing the document.
_SLD_NS = {
    "sld": "http://www.opengis.net/sld",
    "se": "http://www.opengis.net/se",
    "ogc": "http://www.opengis.net/ogc",
}

_HEX_RE = re.compile(r"^#([0-9a-fA-F]{6})$")


def _parse_hex_color(hex_color: str) -> Tuple[int, int, int]:
    """Parse a six-digit CSS hex color string to (R, G, B).

    Raises ``ValueError`` for non-conforming input.
    """
    m = _HEX_RE.match(hex_color.strip())
    if not m:
        raise ValueError(f"Expected six-digit hex color, got: {hex_color!r}")
    h = m.group(1)
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _opacity_to_alpha(opacity: Optional[str]) -> int:
    """Convert an SLD opacity string (0.0–1.0) to an alpha byte (0–255).

    Defaults to fully opaque (255) when the attribute is absent or
    unparseable.
    """
    if opacity is None:
        return 255
    try:
        val = float(opacity)
        return max(0, min(255, round(val * 255)))
    except (ValueError, TypeError):
        logger.debug("Unparseable opacity %r; defaulting to 255", opacity)
        return 255


def extract_sld_body(style_obj: object) -> Optional[str]:
    """Extract the SLD body string from a ``Style`` model.

    Looks for the first ``SLDContent`` stylesheet in the style object's
    ``stylesheets`` list (the shape returned by ``StylesProtocol.get_style``).
    Returns ``None`` when no SLD stylesheet is present.

    This is the module-level counterpart of the helper previously inlined in
    the renders extension service, decoupled here so the preseed task and the
    tiles extension can use it without importing the renders extension.
    """
    from dynastore.modules.styles.models import SLDContent, StyleFormatEnum

    stylesheets = getattr(style_obj, "stylesheets", None) or []
    for sheet in stylesheets:
        content = getattr(sheet, "content", None)
        if content is None:
            continue
        if isinstance(content, SLDContent):
            return content.sld_body
        # Also handle the case where content is a dict (from JSON deserialisation)
        if isinstance(content, dict) and content.get("format") == StyleFormatEnum.SLD_1_1:
            return content.get("sld_body")
    return None


def _lerp_rgba(a: RGBA, b: RGBA, t: float) -> RGBA:
    """Linearly interpolate two RGBA tuples at ``t`` in [0, 1]."""
    return (
        round(a[0] + (b[0] - a[0]) * t),
        round(a[1] + (b[1] - a[1]) * t),
        round(a[2] + (b[2] - a[2]) * t),
        round(a[3] + (b[3] - a[3]) * t),
    )


def _ramp_colormap(entries: List[Tuple[float, RGBA]]) -> IntervalColormap:
    """Approximate SLD ramp (linear interpolation) as interval sub-steps.

    ``entries`` must be sorted by quantity. Values below the first entry get
    the first color and values above the last get the last color, matching
    GeoServer's edge clamping.
    """
    first_q, first_rgba = entries[0]
    last_q, last_rgba = entries[-1]
    out: IntervalColormap = [((float("-inf"), first_q), first_rgba)]
    for (qa, ca), (qb, cb) in zip(entries, entries[1:]):
        if qb <= qa:
            continue
        for j in range(_RAMP_STEPS_PER_SEGMENT):
            lo = qa + (qb - qa) * j / _RAMP_STEPS_PER_SEGMENT
            hi = qa + (qb - qa) * (j + 1) / _RAMP_STEPS_PER_SEGMENT
            # Sample at the sub-interval's lower bound so a value exactly on
            # an entry quantity gets that entry's exact color — load-bearing
            # for opacity-0 nodata entries like gismgr's -9999.
            out.append(((lo, hi), _lerp_rgba(ca, cb, j / _RAMP_STEPS_PER_SEGMENT)))
    out.append(((last_q, float("inf")), last_rgba))
    return out


def _intervals_colormap(entries: List[Tuple[float, RGBA]]) -> IntervalColormap:
    """Build an interval colormap with each entry's quantity as lower bound.

    ``entries`` must be sorted by quantity. GeoServer renders each interval
    between two entries with the lower entry's color; values below the first
    entry are left unrendered (transparent) and values at or above the last
    entry are clamped to the last color.
    """
    out: IntervalColormap = []
    for (qa, ca), (qb, _) in zip(entries, entries[1:]):
        if qb > qa:
            out.append(((qa, qb), ca))
    last_q, last_rgba = entries[-1]
    out.append(((last_q, float("inf")), last_rgba))
    return out


def parse_sld_colormap(sld_body: str) -> RioColormap:
    """Parse an SLD XML document and extract a rio-tiler colormap.

    The function locates the first ``<ColorMap>`` element in the document
    (via namespace-agnostic search), reads each ``<ColorMapEntry>``, and
    dispatches on the ColorMap ``type`` attribute (``ramp`` — the SLD
    default — ``intervals``, or ``values``; see module docstring).

    Args:
        sld_body: A well-formed SLD XML string (the ``SLDContent.sld_body``
            value from the styles module).

    Returns:
        For ``type="values"``: a dict mapping pixel values to (R, G, B, A)
        tuples (integral quantities keep ``int`` keys). For ``ramp`` and
        ``intervals``: a list of ``((lo, hi), (R, G, B, A))`` intervals.
        Empty dict when no parseable entries exist.

    Raises:
        ValueError: If the XML cannot be parsed at all.
    """
    try:
        from lxml import etree  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "lxml is required for SLD colormap parsing. "
            "Install the renders extension which lists it as a dependency."
        ) from exc

    try:
        root = etree.fromstring(sld_body.encode("utf-8"))
    except etree.XMLSyntaxError as exc:
        raise ValueError(f"SLD colormap: XML parse failed: {exc}") from exc

    # Find the first <ColorMap> regardless of namespace prefix.
    colormap_el = root.find(".//{http://www.opengis.net/se}ColorMap")
    if colormap_el is None:
        colormap_el = root.find(".//{http://www.opengis.net/sld}ColorMap")
    if colormap_el is None:
        # Try namespace-less (some SLD producers omit namespace declarations)
        colormap_el = root.find(".//ColorMap")
    if colormap_el is None:
        logger.warning("parse_sld_colormap: no <ColorMap> element found in SLD")
        return {}

    entries: List[Tuple[float, RGBA]] = []

    for entry in colormap_el.iter():
        local = etree.QName(entry.tag).localname if "}" in entry.tag else entry.tag
        if local != "ColorMapEntry":
            continue

        attrib = entry.attrib
        quantity_str = attrib.get("quantity")
        color_str = attrib.get("color")
        if quantity_str is None or color_str is None:
            logger.debug(
                "Skipping ColorMapEntry missing quantity or color: %s", attrib
            )
            continue

        try:
            quantity = float(quantity_str)
        except ValueError:
            logger.warning(
                "parse_sld_colormap: non-numeric quantity %r skipped", quantity_str
            )
            continue

        try:
            r, g, b = _parse_hex_color(color_str)
        except ValueError as exc:
            logger.warning(
                "parse_sld_colormap: bad color on entry quantity=%s: %s", quantity, exc
            )
            continue

        alpha = _opacity_to_alpha(attrib.get("opacity"))
        entries.append((quantity, (r, g, b, alpha)))

    if not entries:
        return {}

    map_type = (colormap_el.get("type") or "ramp").strip().lower()

    if map_type == "values":
        return {
            (int(q) if q.is_integer() else q): rgba for q, rgba in entries
        }

    entries.sort(key=lambda e: e[0])
    if map_type == "intervals":
        return _intervals_colormap(entries)
    if map_type != "ramp":
        logger.warning(
            "parse_sld_colormap: unknown ColorMap type %r; treating as 'ramp'",
            map_type,
        )
    return _ramp_colormap(entries)
