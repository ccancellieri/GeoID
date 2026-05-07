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

"""
StylesResolver — precedence cascade for the default style on a
(catalog, collection) pair.

Precedence, highest wins:
  1. ``CoveragesConfig.default_style_id`` if set on the collection
     (coverages is authoritative for its own defaults)
  2. STAC ``item_assets`` default-style reference (platform-wide default)
  3. ``None`` — caller emits only ``rel=styles`` (list link), not ``rel=style``

The resolver is a pure function that applies precedence to already-loaded
inputs. DB access (fetching registered styles, loading the coverages
config) is the caller's responsibility — the caller has request-scoped
access to a connection; the resolver does not. This keeps the resolver
pure, testable without a database, and reusable from producers that
don't run inside a request handler (plugin-discovered contributors).

Owned by ``modules/styles/`` because the full cascade touches three
concerns (Styles registry, CoveragesConfig, STAC item_assets) and
centralising it here avoids those concerns importing each other.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class StyleResolution:
    """Outcome of resolving applicable styles for a resource.

    Attributes:
        registered_style_ids: All style IDs registered at the (catalog,
            collection) scope. May be empty.
        default_style_id: The ID the caller should treat as the default
            (subject to the precedence cascade), or ``None`` if no
            default applies.
        stylesheets_by_style_id: Mapping of style ID → list of stylesheet
            records. Present for every entry in ``registered_style_ids``.
            Each stylesheet record is the consumer-defined shape passed
            in by the caller — typically ``modules/styles.models.StyleSheet``.
    """

    registered_style_ids: List[str]
    default_style_id: Optional[str]
    stylesheets_by_style_id: Dict[str, List[Any]] = field(default_factory=dict)


class StylesResolver:
    """Pure precedence resolver.

    Construction takes no dependencies; all inputs are passed per call so
    the resolver is trivially stubbable in tests.
    """

    def resolve(
        self,
        *,
        available: Dict[str, List[Any]],
        coverages_config_default_id: Optional[str],
        item_assets_default_id: Optional[str],
    ) -> StyleResolution:
        """Apply the precedence cascade.

        Args:
            available: Mapping of style ID → stylesheets. Typically
                loaded by the caller from ``modules/styles/db.py``.
            coverages_config_default_id: ``CoveragesConfig.default_style_id``
                (pass ``None`` if CoveragesConfig isn't set or the
                collection isn't coverage-backed).
            item_assets_default_id: Default style ID from the STAC
                ``item_assets`` extension for the collection, or ``None``.

        Returns:
            StyleResolution with the resolved default ID + registered IDs.
            A default ID is only set when it actually exists in
            ``available`` — stale references never propagate.
        """
        registered = list(available.keys())
        default_id = self._pick_default(
            registered,
            coverages_config_default_id,
            item_assets_default_id,
        )
        return StyleResolution(
            registered_style_ids=registered,
            default_style_id=default_id,
            stylesheets_by_style_id=dict(available),
        )

    @staticmethod
    def _pick_default(
        registered: List[str],
        coverages_default: Optional[str],
        item_assets_default: Optional[str],
    ) -> Optional[str]:
        if coverages_default is not None and coverages_default in registered:
            return coverages_default
        if item_assets_default is not None and item_assets_default in registered:
            return item_assets_default
        return None
