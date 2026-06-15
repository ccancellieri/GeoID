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

"""EDR collection title/description localization tests.

Covers ``build_edr_collection`` with a ``LocalizedText`` title and description:
default language ``en``, explicit ``fr``, unknown-language fallback to ``en``,
and ``lang='*'`` passthrough of the full multi-language object.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict

from dynastore.models.localization import LocalizedText
from dynastore.modules.edr.collection_metadata import build_edr_collection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_collection(
    collection_id: str = "my-collection",
    title: Any = None,
    description: Any = None,
) -> SimpleNamespace:
    """Minimal stub that mimics a STAC collection model for the fields we care about."""
    return SimpleNamespace(id=collection_id, title=title, description=description, extent=None)


BASE_URL = "https://example.com"


# ---------------------------------------------------------------------------
# Title resolution
# ---------------------------------------------------------------------------


def test_default_language_en_resolves_title():
    coll = _make_collection(title=LocalizedText(en="Precipitation", fr="Précipitations"))
    result = build_edr_collection("cat1", coll, base_url=BASE_URL, language="en")
    assert result["title"] == "Precipitation"


def test_requested_language_fr_resolves_title():
    coll = _make_collection(title=LocalizedText(en="Precipitation", fr="Précipitations"))
    result = build_edr_collection("cat1", coll, base_url=BASE_URL, language="fr")
    assert result["title"] == "Précipitations"


def test_unknown_language_falls_back_to_en():
    coll = _make_collection(title=LocalizedText(en="Precipitation", fr="Précipitations"))
    result = build_edr_collection("cat1", coll, base_url=BASE_URL, language="de")
    assert result["title"] == "Precipitation"


def test_wildcard_preserves_full_multilanguage_title():
    """lang='*' must pass the LocalizedText through unchanged (full round-trip)."""
    coll = _make_collection(title=LocalizedText(en="Precipitation", fr="Précipitations"))
    result = build_edr_collection("cat1", coll, base_url=BASE_URL, language="*")
    # The title is the LocalizedText object itself (or its dict repr) — not a plain str.
    title = result["title"]
    if hasattr(title, "model_dump"):
        dumped: Dict[str, Any] = title.model_dump(exclude_none=True)
    else:
        dumped = dict(title)
    assert dumped.get("en") == "Precipitation"
    assert dumped.get("fr") == "Précipitations"


def test_plain_str_title_passthrough():
    """A pre-resolved plain string title is returned unchanged."""
    coll = _make_collection(title="Already resolved")
    result = build_edr_collection("cat1", coll, base_url=BASE_URL, language="en")
    assert result["title"] == "Already resolved"


def test_none_title_falls_back_to_collection_id():
    coll = _make_collection(collection_id="fallback-id", title=None)
    result = build_edr_collection("cat1", coll, base_url=BASE_URL, language="en")
    assert result["title"] == "fallback-id"


# ---------------------------------------------------------------------------
# Description resolution
# ---------------------------------------------------------------------------


def test_description_resolved_for_requested_language():
    coll = _make_collection(
        title=LocalizedText(en="Precipitation"),
        description=LocalizedText(en="Daily precipitation totals", fr="Totaux journaliers"),
    )
    result = build_edr_collection("cat1", coll, base_url=BASE_URL, language="fr")
    assert result["description"] == "Totaux journaliers"


def test_description_fallback_to_en_on_unknown_language():
    coll = _make_collection(
        title=LocalizedText(en="Precipitation"),
        description=LocalizedText(en="Daily precipitation totals", fr="Totaux journaliers"),
    )
    result = build_edr_collection("cat1", coll, base_url=BASE_URL, language="de")
    assert result["description"] == "Daily precipitation totals"


def test_wildcard_preserves_full_multilanguage_description():
    coll = _make_collection(
        title=LocalizedText(en="Precipitation"),
        description=LocalizedText(en="Daily totals", fr="Totaux journaliers"),
    )
    result = build_edr_collection("cat1", coll, base_url=BASE_URL, language="*")
    desc = result["description"]
    if hasattr(desc, "model_dump"):
        dumped: Dict[str, Any] = desc.model_dump(exclude_none=True)
    else:
        dumped = dict(desc)
    assert dumped.get("en") == "Daily totals"
    assert dumped.get("fr") == "Totaux journaliers"


# ---------------------------------------------------------------------------
# Wire-contract guard: required top-level keys always present
# ---------------------------------------------------------------------------


def test_result_contains_required_keys():
    coll = _make_collection(title=LocalizedText(en="Temperature"))
    result = build_edr_collection("cat1", coll, base_url=BASE_URL, language="en")
    for key in ("id", "title", "description", "data_queries", "crs", "output_formats", "links"):
        assert key in result, f"Missing key {key!r} in build_edr_collection result"
