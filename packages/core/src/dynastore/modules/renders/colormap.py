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

"""SLD ColorMap → rio-tiler discrete colormap dict.

Parses an SLD 1.1 ``<ColorMap type="values">`` element and produces a
``dict[int, tuple[int, int, int, int]]`` suitable for passing directly to
``rio_tiler.models.ImageData.render(colormap=...)``.

Design constraints:
- Pure function, no I/O, no DB, no FastAPI — unit-testable without any
  infrastructure.
- Only ``lxml`` is required (already a transitive dependency via the styles
  extension, and explicitly listed in the renders extension's own
  pyproject.toml).
- Handles ``type="values"`` (discrete) only; ``type="ramp"`` and
  ``type="intervals"`` are out of scope for Slice 1.
- Each ``<ColorMapEntry>`` maps an integer pixel quantity → RGBA. Entries
  whose ``quantity`` cannot be parsed as an integer are skipped with a
  warning, rather than failing the whole parse.
"""

from __future__ import annotations

import logging
import re
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# rio-tiler colormap type alias for documentation clarity.
RioColormap = Dict[int, Tuple[int, int, int, int]]

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


def parse_sld_colormap(sld_body: str) -> RioColormap:
    """Parse an SLD 1.1 XML document and extract a discrete colormap.

    The function locates the first ``<ColorMap>`` element in the document
    (via namespace-agnostic search) and reads each ``<ColorMapEntry>``.

    Args:
        sld_body: A well-formed SLD XML string (the ``SLDContent.sld_body``
            value from the styles module).

    Returns:
        A dict mapping integer pixel values to (R, G, B, A) tuples.
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

    cmap: RioColormap = {}

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
            quantity = int(quantity_str)
        except ValueError:
            logger.warning(
                "parse_sld_colormap: non-integer quantity %r skipped", quantity_str
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
        cmap[quantity] = (r, g, b, alpha)

    return cmap
