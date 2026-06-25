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

"""Style-binding SSOT — ``StyleBindingConfig`` + ``StyleBinding``.

``StyleBindingConfig`` is the single source of truth that binds styles to
data.  It is a ``PluginConfig`` stored via the existing ``/configs`` API at
the catalog or collection tier.

**Keying**: the config scope is keyed on the *immutable internal* catalog /
collection id (``physical_id`` / internal surrogate), never the renamable
``external_id``.  Callers must resolve external → internal once at the
request boundary before addressing the config.  This makes binding
rename-immune with no additional wiring in the binding layer itself.

**CQL2 selectors**: each ``StyleBinding`` entry carries an optional
``selector`` (a CQL2-JSON filter fragment).  Selectors are evaluated
in-memory against STAC item properties via the existing pygeofilter
pipeline (``modules/tools/cql.py``) — no per-item DB round-trip.  A
``selector=None`` entry matches every item in the collection (whole-
collection binding).

**Resolution cascade** (highest to lowest, implemented by ``StylesResolver``):

1. ``?styles=<id>`` request override (Maps / Tiles)
2. Collection-tier binding whose CQL2 selector matches → highest ``priority``
3. Collection ``default_style_id``
4. Catalog-tier binding whose CQL2 selector matches → highest ``priority``
5. Catalog ``default_style_id``
6. Harvested STAC ``render:renders`` (item, then collection)
7. Existing fallbacks: ``CoveragesConfig.default_style_id`` → ``item_assets`` → ``None``

**No conformance URI**: OGC API — Styles (OGC 20-009 DRAFT) has no item-
level or attribute-level style-association class.  Binding is outside the
standard.  The binding state never appears on the wire — only its projection
(``render:renders`` + style links emitted by ``RendersStacContributor``) does.
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig

logger = logging.getLogger(__name__)


class StyleBinding(BaseModel):
    """A single entry in the binding table.

    The ``selector`` is a CQL2-JSON filter dict evaluated against the STAC
    item properties dict.  ``None`` means "match every item" (whole-collection
    binding).  When multiple bindings match, the one with the highest
    ``priority`` wins.
    """

    style_id: str = Field(
        description="ID of the style registered in the OGC API - Styles registry."
    )
    selector: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "CQL2-JSON filter evaluated against STAC item properties. "
            "``null`` matches every item in the collection."
        ),
    )
    priority: int = Field(
        default=0,
        description=(
            "Tie-breaker when multiple selectors match. Higher wins. "
            "Within the same priority, first-listed wins."
        ),
    )


class StyleBindingConfig(PluginConfig):
    """Operator-editable binding table that maps styles to data.

    Stored as a ``PluginConfig`` so zero new DDL is required and the
    platform→catalog→collection cascade provides inheritance for free.

    The config is intentionally a whole-document replace (no per-binding
    surrogate id).  That is fine at the expected scale of a few styles +
    a few attribute groups per collection; SQL-level FK enforcement and
    individual addressability would only add value at much higher volume.

    ``default_style_id`` is the fallback when no binding selector matches.
    ``bindings`` is the ordered list of selector → style_id rules.  On
    resolution, all entries in the catalog-tier list AND the collection-tier
    list are concatenated, sorted by ``priority`` (descending), and evaluated
    in order — the first match wins.

    The config-scope key is the *internal* (immutable) catalog / collection
    id so renaming via ``external_id`` never invalidates a binding.
    """

    _address: ClassVar[Tuple[str, ...]] = (
        "platform",
        "catalog",
        "styles",
        "bindings",
    )

    default_style_id: Mutable[Optional[str]] = Field(
        default=None,
        description=(
            "Default style ID used when no binding selector matches an item. "
            "Must reference a style registered in the OGC API - Styles registry."
        ),
    )
    bindings: Mutable[List[StyleBinding]] = Field(
        default_factory=list,
        description=(
            "Ordered list of CQL2 selector → style_id rules. "
            "Rules from the catalog tier and collection tier are concatenated; "
            "the highest-priority matching rule wins."
        ),
    )
