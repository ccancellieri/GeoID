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

"""Records link-title localization.

``get_records`` and ``get_record`` serialize a ``RecordCollection`` /
``Record`` to a dict and then resolve ``Link.title`` (a ``LocalizedText``) to
a single language string.  These tests exercise the serialization + resolution
path directly, mirroring tests/dynastore/extensions/maps/test_mapcontent_i18n.py.

Key facts verified here:
- ``RecordProperties.title`` is ``Optional[str]`` — already a plain string,
  no localization needed.
- ``Link.title`` is ``Optional[LocalizedText]`` — serializes as a dict after
  ``model_dump``; ``_resolve_links_titles`` collapses it in-place.
- ``lang='*'`` leaves the full dict intact on the wire.
- Unknown language falls back to 'en'.
"""
from __future__ import annotations

import pytest

from dynastore.models.localization import LocalizedText
from dynastore.models.shared_models import Link
from dynastore.extensions.records import records_models as rm
from dynastore.extensions.records.records_service import _resolve_links_titles


# ---------------------------------------------------------------------------
# RecordProperties.title — plain str, no localization
# ---------------------------------------------------------------------------


def test_record_properties_title_is_plain_str():
    """RecordProperties.title is Optional[str] — not a LocalizedText."""
    props = rm.RecordProperties(title="My Record")
    assert props.title == "My Record"
    dumped = props.model_dump(exclude_none=True)
    assert dumped["title"] == "My Record"
    assert isinstance(dumped["title"], str)


def test_record_properties_title_none_allowed():
    props = rm.RecordProperties()
    assert props.title is None


# ---------------------------------------------------------------------------
# Link.title — LocalizedText, resolved on serialized dict
# ---------------------------------------------------------------------------


def test_link_title_serializes_as_dict():
    """Link.title is LocalizedText; model_dump produces a dict, not str."""
    link = Link(href="https://example.com", rel="self", title=LocalizedText(en="Self", fr="Soi"))
    dumped = link.model_dump(exclude_none=True)
    assert isinstance(dumped["title"], dict)
    assert dumped["title"] == {"en": "Self", "fr": "Soi"}


def test_resolve_links_titles_default_lang_en():
    """Default language 'en' collapses LocalizedText title to plain string."""
    links = [
        {"href": "https://example.com", "rel": "self", "title": {"en": "Self", "fr": "Soi"}},
    ]
    _resolve_links_titles(links, "en")
    assert links[0]["title"] == "Self"
    assert isinstance(links[0]["title"], str)


def test_resolve_links_titles_requested_lang_fr():
    """Requesting 'fr' resolves to the French title."""
    links = [
        {"href": "https://example.com", "rel": "self", "title": {"en": "Records in this collection", "fr": "Enregistrements de cette collection"}},
    ]
    _resolve_links_titles(links, "fr")
    assert links[0]["title"] == "Enregistrements de cette collection"


def test_resolve_links_titles_unknown_lang_falls_back_to_en():
    """Unknown language (e.g. 'de') falls back to 'en'."""
    links = [
        {"href": "https://example.com", "rel": "items", "title": {"en": "Records", "fr": "Enregistrements"}},
    ]
    _resolve_links_titles(links, "de")
    assert links[0]["title"] == "Records"


def test_resolve_links_titles_wildcard_preserves_dict():
    """lang='*' keeps the full multi-language dict on the wire."""
    links = [
        {"href": "https://example.com", "rel": "items", "title": {"en": "Records", "fr": "Enregistrements"}},
    ]
    _resolve_links_titles(links, "*")
    assert links[0]["title"] == {"en": "Records", "fr": "Enregistrements"}


def test_resolve_links_titles_no_title_key_unchanged():
    """Links without a title key are left untouched."""
    links = [{"href": "https://example.com", "rel": "self"}]
    _resolve_links_titles(links, "en")
    assert "title" not in links[0]


def test_resolve_links_titles_plain_str_title_unchanged():
    """Links whose title is already a plain str (not a dict) are left untouched."""
    links = [{"href": "https://example.com", "rel": "self", "title": "Already resolved"}]
    _resolve_links_titles(links, "en")
    assert links[0]["title"] == "Already resolved"


def test_resolve_links_titles_none_input():
    """Passing None for the links list is a no-op."""
    _resolve_links_titles(None, "en")  # must not raise


def test_resolve_links_titles_empty_list():
    """Empty links list is a no-op."""
    links: list = []
    _resolve_links_titles(links, "en")
    assert links == []


# ---------------------------------------------------------------------------
# Record model round-trip
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# list_catalogs — consolidated onto OGCServiceMixin._ogc_list_catalogs (#2692)
#
# #3073 reverted this delegation because the shared helper's lang='*' path
# leaked a raw LocalizedText model (null-padded once serialized). Once the
# helper switched to resolve_localized_field (clean, filtered dict), Records
# folds back onto it instead of keeping its own copy of the same logic.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_catalogs_delegates_to_shared_ogc_helper(monkeypatch):
    from unittest.mock import AsyncMock

    from dynastore.extensions.records.config import RecordsPluginConfig
    from dynastore.extensions.records.records_service import RecordsService

    svc = RecordsService.__new__(RecordsService)
    catalogs = AsyncMock()
    catalogs.list_catalogs = AsyncMock(
        return_value=[
            type("_Catalog", (), {"id": "internal-1", "external_id": "public-cat", "title": LocalizedText(en="Catalog")})()
        ]
    )

    async def _get_catalogs_service():
        return catalogs

    async def _get_plugin_config(config_cls, catalog_id=None, collection_id=None):
        return RecordsPluginConfig()

    svc._get_catalogs_service = _get_catalogs_service
    svc._get_plugin_config = _get_plugin_config

    result = await svc.list_catalogs(language="en", limit=None, offset=0)

    assert result["catalogs"] == [{"id": "public-cat", "title": "Catalog"}]


@pytest.mark.asyncio
async def test_list_catalogs_wildcard_lang_returns_clean_dict_no_null_padding():
    """Pins the fix: lang='*' must not serialize unset languages as nulls."""
    from unittest.mock import AsyncMock

    from dynastore.extensions.records.config import RecordsPluginConfig
    from dynastore.extensions.records.records_service import RecordsService

    svc = RecordsService.__new__(RecordsService)
    catalogs = AsyncMock()
    catalogs.list_catalogs = AsyncMock(
        return_value=[
            type("_Catalog", (), {"id": "c1", "external_id": "c1", "title": LocalizedText(en="Catalog")})()
        ]
    )

    async def _get_catalogs_service():
        return catalogs

    async def _get_plugin_config(config_cls, catalog_id=None, collection_id=None):
        return RecordsPluginConfig()

    svc._get_catalogs_service = _get_catalogs_service
    svc._get_plugin_config = _get_plugin_config

    result = await svc.list_catalogs(language="*", limit=None, offset=0)

    title = result["catalogs"][0]["title"]
    assert title == {"en": "Catalog"}
    assert "fr" not in title


def test_record_model_dump_links_contain_localized_title():
    """Record.model_dump produces link titles as dicts (before _resolve_links_titles)."""
    link = Link(href="https://example.com/rec/1", rel="self", title=LocalizedText(en="Record 1"))
    record = rm.Record(
        type="Feature",
        id="rec-1",
        geometry=None,
        properties=rm.RecordProperties(title="Record 1"),
        links=[link],
    )
    dumped = record.model_dump(exclude_none=True)
    # Link title is a dict before resolution
    assert isinstance(dumped["links"][0]["title"], dict)
    # properties.title is already a plain str
    assert isinstance(dumped["properties"]["title"], str)

    # Apply resolution in-place
    _resolve_links_titles(dumped.get("links"), "en")
    assert dumped["links"][0]["title"] == "Record 1"
    assert isinstance(dumped["links"][0]["title"], str)
