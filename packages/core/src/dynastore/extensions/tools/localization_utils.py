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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Shared localization helpers for OGC-protocol extensions."""

from typing import Any, Dict, Tuple

from dynastore.models.localization import is_multilanguage_input

# Fields inspected to decide whether a write carries multi-language input.
_I18N_FIELDS: Tuple[str, ...] = (
    "title",
    "description",
    "keywords",
    "license",
    "extra_metadata",
)


def detect_use_lang(
    data: Dict[str, Any], default: str, fields: Tuple[str, ...] = _I18N_FIELDS
) -> str:
    """Return ``"*"`` when any inspected field holds multi-language input.

    On a catalog/collection write we accept either a single-language value
    (keyed by the request ``default`` language) or a per-language mapping. When
    any of ``fields`` is a multi-language mapping the caller must persist all
    languages, signalled by the ``"*"`` sentinel; otherwise the request's
    ``default`` language is used.
    """
    if any(is_multilanguage_input(data.get(f)) for f in fields):
        return "*"
    return default
