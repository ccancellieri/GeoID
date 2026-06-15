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

"""Wire-shape regression tests for Tile Matrix Set link titles.

The OGC API Tiles extension uses a local ``Link`` model
(``dynastore.modules.tiles.tiles_models.Link``) whose ``title`` field is a
plain ``str``, not a ``LocalizedText``.  As a result, the serialized
``TileMatrixSetList`` and ``TileMatrixSetRef`` responses must never produce a
language-keyed dict ``{"en": "..."}`` on the wire for link titles or TMS
titles — both should always be plain strings.

These tests act as a regression guard: if someone replaces the local
``Link`` with ``dynastore.models.shared_models.Link`` (which carries a
``LocalizedText`` validator that wraps plain strings into dicts), the
assertions here will fail and surface the change before it ships.
"""

from __future__ import annotations

import json
from typing import Any, Dict

from dynastore.modules.tiles.tiles_models import (
    Link,
    TileMatrixSetList,
    TileMatrixSetRef,
)
from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS
from dynastore.extensions.tools.response_i18n import resolve_links


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_HREF = "https://example.com/tiles/tileMatrixSets"


def _make_tms_list() -> TileMatrixSetList:
    """Build a TileMatrixSetList the same way ``get_tile_matrix_sets`` does."""
    tms_refs = []
    for tms_id, tms_def in BUILTIN_TILE_MATRIX_SETS.items():
        tms_refs.append(
            TileMatrixSetRef(
                id=tms_id,
                title=tms_def.title,
                links=[
                    Link(
                        href=f"{BASE_HREF}/{tms_id}",
                        rel="self",
                        type="application/json",
                        title=tms_def.title,
                        hreflang="en",
                    )
                ],
            )
        )
    return TileMatrixSetList(tileMatrixSets=tms_refs)


def _dump(model) -> Dict[str, Any]:
    return model.model_dump(by_alias=True, exclude_none=True)


# ---------------------------------------------------------------------------
# TileMatrixSetList — link title wire shape
# ---------------------------------------------------------------------------


def test_link_title_is_plain_string_not_dict():
    """Link titles in a TileMatrixSetList must serialize as plain strings."""
    tms_list = _make_tms_list()
    dumped = _dump(tms_list)

    for ref in dumped["tileMatrixSets"]:
        for link in ref.get("links", []):
            title = link.get("title")
            assert title is not None, "Expected a title on the self link"
            assert isinstance(title, str), (
                f"link title must be str, got {type(title).__name__!r}: {title!r}"
            )
            assert not isinstance(title, dict), (
                f"link title must not be a language-keyed dict; got {title!r}"
            )


def test_tms_ref_title_is_plain_string_not_dict():
    """TileMatrixSetRef.title must serialize as a plain string."""
    tms_list = _make_tms_list()
    dumped = _dump(tms_list)

    for ref in dumped["tileMatrixSets"]:
        ref_title = ref.get("title")
        if ref_title is not None:
            assert isinstance(ref_title, str), (
                f"TileMatrixSetRef title must be str, got {type(ref_title).__name__!r}: {ref_title!r}"
            )
            assert not isinstance(ref_title, dict), (
                f"TileMatrixSetRef title must not be a language-keyed dict; got {ref_title!r}"
            )


# ---------------------------------------------------------------------------
# resolve_links compatibility
# ---------------------------------------------------------------------------


def test_resolve_links_default_en_keeps_plain_string():
    """``resolve_links(..., 'en')`` on tiles links returns unchanged plain strings."""
    links = [
        Link(
            href=f"{BASE_HREF}/WebMercatorQuad",
            rel="self",
            type="application/json",
            title="Google Maps Compatible for the World",
            hreflang="en",
        )
    ]
    resolved = resolve_links(links, "en")
    assert resolved is not None
    assert len(resolved) == 1
    title = resolved[0].get("title")
    assert isinstance(title, str)
    assert title == "Google Maps Compatible for the World"


def test_resolve_links_unknown_language_keeps_plain_string():
    """Unknown language with a plain-string title returns the string unchanged."""
    links = [
        Link(
            href=f"{BASE_HREF}/WebMercatorQuad",
            rel="self",
            title="Google Maps Compatible for the World",
        )
    ]
    resolved = resolve_links(links, "fr")
    assert resolved is not None
    title = resolved[0].get("title")
    assert isinstance(title, str)
    assert title == "Google Maps Compatible for the World"


def test_resolve_links_wildcard_keeps_plain_string():
    """``lang='*'`` passthrough: plain-string titles remain plain strings."""
    links = [
        Link(
            href=f"{BASE_HREF}/WorldCRS84Quad",
            rel="self",
            title="CRS84 for the World",
        )
    ]
    resolved = resolve_links(links, "*")
    assert resolved is not None
    title = resolved[0].get("title")
    # Plain strings are not multi-language dicts; they come through unchanged
    # regardless of whether lang=='*' or a specific code.
    assert isinstance(title, str)
    assert title == "CRS84 for the World"


# ---------------------------------------------------------------------------
# JSON round-trip: no dict-valued title on the wire
# ---------------------------------------------------------------------------


def test_json_roundtrip_no_dict_titles():
    """End-to-end JSON serialization of the full TileMatrixSetList has no dict titles."""
    tms_list = _make_tms_list()
    wire = json.loads(json.dumps(_dump(tms_list)))

    for ref in wire["tileMatrixSets"]:
        ref_title = ref.get("title")
        if ref_title is not None:
            assert not isinstance(ref_title, dict), (
                f"TileMatrixSetRef.title must not be a dict on the wire; got {ref_title!r}"
            )
        for link in ref.get("links", []):
            link_title = link.get("title")
            if link_title is not None:
                assert not isinstance(link_title, dict), (
                    f"Link.title must not be a dict on the wire; got {link_title!r}"
                )


# ---------------------------------------------------------------------------
# Builtin TMS coverage: both built-in entries are present and correct
# ---------------------------------------------------------------------------


def test_builtin_tms_entries_present():
    """Both built-in TMS definitions contribute entries with non-empty plain titles."""
    tms_list = _make_tms_list()
    dumped = _dump(tms_list)
    ids = {ref["id"] for ref in dumped["tileMatrixSets"]}
    assert "WebMercatorQuad" in ids
    assert "WorldCRS84Quad" in ids


def test_builtin_tms_link_titles_are_non_empty_strings():
    """Each built-in TMS entry has a non-empty plain string title on the self link."""
    tms_list = _make_tms_list()
    dumped = _dump(tms_list)
    for ref in dumped["tileMatrixSets"]:
        for link in ref.get("links", []):
            title = link.get("title")
            assert title, f"Expected non-empty title on link in TMS {ref['id']!r}"
            assert isinstance(title, str)
