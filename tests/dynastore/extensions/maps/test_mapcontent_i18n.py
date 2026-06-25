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

"""MapContent title localization.

Regression: the maps metadata routes returned 422 because MapContent.title was a
plain ``str`` while the collection title is a multi-language ``LocalizedText``.
The routes now resolve the title to a single language (lang query param /
Accept-Language header, default 'en'), and ``lang='*'`` returns the full
multi-language object. These tests mirror what the aligned catalog/collection
maps metadata routes do: ``coll.title.resolve(language)`` then ``MapContent(title=...)``.
"""
from __future__ import annotations

from dynastore.models.localization import LocalizedText
from dynastore.extensions.maps.maps_models import MapContent


def test_mapcontent_no_longer_422s_on_full_localized_object():
    """The original failure mode: a LocalizedText must be accepted, not rejected."""
    title = LocalizedText(en="Sentinel-2", fr="Sentinelle-2")
    mc = MapContent(title=title, links=[])  # must not raise
    assert mc.title is not None


def test_route_resolves_to_single_language_by_default():
    """lang='en' (or default) collapses the title to a single string."""
    title = LocalizedText(en="Sentinel-2", fr="Sentinelle-2")
    mc = MapContent(title=title.resolve("en"), links=[])
    assert mc.title == "Sentinel-2"
    assert mc.model_dump(exclude_none=True)["title"] == "Sentinel-2"


def test_route_resolves_requested_language():
    title = LocalizedText(en="Sentinel-2", fr="Sentinelle-2")
    mc = MapContent(title=title.resolve("fr"), links=[])
    assert mc.title == "Sentinelle-2"


def test_unknown_language_falls_back_to_default_en():
    title = LocalizedText(en="Sentinel-2", fr="Sentinelle-2")
    mc = MapContent(title=title.resolve("de"), links=[])
    assert mc.title == "Sentinel-2"


def test_wildcard_returns_full_multilanguage_object():
    """lang='*' keeps the full multi-language model on the wire."""
    title = LocalizedText(en="Sentinel-2", fr="Sentinelle-2")
    mc = MapContent(title=title.resolve("*"), links=[])
    dumped = mc.model_dump(exclude_none=True)
    assert dumped["title"] == {"en": "Sentinel-2", "fr": "Sentinelle-2"}


def test_none_title_is_allowed():
    mc = MapContent(title=None, links=[])
    assert mc.title is None
