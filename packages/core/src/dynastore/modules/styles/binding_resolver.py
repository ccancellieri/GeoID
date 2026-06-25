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

"""Style-binding resolution against in-memory STAC item properties.

Two entry points:

``StyleBindingResolver.resolve(bindings, item_properties)``
    Pure, synchronous.  Evaluates a list of ``StyleBinding`` entries (already
    concatenated and sorted from catalog + collection tier) against a STAC
    item properties dict using the pygeofilter CQL2-JSON backend.  Returns
    the winning ``style_id`` or ``None``.

``resolve_binding_style_id(internal_catalog_id, internal_collection_id,
                            item_properties)``
    Async convenience that loads ``StyleBindingConfig`` for both catalog and
    collection tiers via ``PlatformConfigsProtocol``, concatenates the
    ``bindings`` lists (catalog-wide first, then collection-specific), sorts
    by priority (descending), and delegates to ``StyleBindingResolver``.
    Falls through to ``config.default_style_id`` (collection tier, then
    catalog tier) when no selector matches.

**CQL2 selector evaluation**: selectors are CQL2-JSON filter dicts evaluated
in-memory against the STAC item properties via pygeofilter's
``parse_cql2_json``.  The evaluation is purely structural (no DB, no
geometry transform) — the same AST the filter pipeline uses for SQL
generation is reused here for in-memory predicate evaluation, keeping one
parsing mechanism for both paths.

The ``item_properties`` dict represents the flat STAC item properties
namespace.  Dotted paths like ``properties.theme`` in the selector resolve
to ``item_properties["theme"]`` (callers unwrap the ``properties`` wrapper
before passing in, since the pygeofilter in-memory evaluator receives a
plain dict, not a nested STAC JSON blob).

**No conformance URI**: binding carries no OGC conformance claim.  OGC
20-009 (Styles) has no item-level or attribute-level style-association class.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dynastore.modules.styles.binding_config import StyleBinding


# ---------------------------------------------------------------------------
# CQL2-JSON in-memory evaluator
# ---------------------------------------------------------------------------

def _eval_cql2_json_selector(
    selector: Dict[str, Any],
    item_properties: Dict[str, Any],
) -> bool:
    """Evaluate a CQL2-JSON filter dict against a flat item-properties dict.

    Uses pygeofilter's ``parse_cql2_json`` to build the AST, then evaluates
    it in-memory with ``NativeEvaluator`` (no SQL, no DB).  ``use_getattr``
    is disabled so attribute resolution uses dict key access, which matches
    the flat STAC properties dict shape.

    Returns ``True`` when the predicate matches, ``False`` on no-match or
    any parse/eval error (fail-open so one bad selector never blocks
    rendering).

    ``item_properties`` is expected to be the flat STAC properties dict
    (i.e. the ``item["properties"]`` sub-dict or equivalent flattened dict),
    NOT the full nested STAC JSON item.
    """
    try:
        from pygeofilter.parsers.cql2_json import parse as _parse_cql2_json
        from pygeofilter.backends.native.evaluate import NativeEvaluator

        ast = _parse_cql2_json(selector)
        evaluator = NativeEvaluator(use_getattr=False, allow_nested_attributes=True)
        predicate = evaluator.evaluate(ast)
        return bool(predicate(item_properties))
    except ImportError:
        logger.warning(
            "style-binding: pygeofilter not installed; CQL2 selector skipped"
        )
        return False
    except Exception as exc:
        logger.debug(
            "style-binding: CQL2 selector eval failed (%s); treating as no-match",
            exc,
        )
        return False


# ---------------------------------------------------------------------------
# Pure resolver
# ---------------------------------------------------------------------------

class StyleBindingResolver:
    """Evaluates a ranked binding list against item properties.

    Construction takes no dependencies; all inputs are passed per call.
    """

    def resolve(
        self,
        bindings: "List[StyleBinding]",
        item_properties: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        """Return the winning ``style_id`` or ``None``.

        Iterates ``bindings`` in their given order (callers are responsible for
        sorting by ``priority`` descending before calling).  A binding with
        ``selector=None`` matches every item.  A binding with a CQL2-JSON
        selector matches when the predicate evaluates ``True`` against
        ``item_properties``.

        When ``item_properties`` is ``None`` (e.g. a collection-level
        reference with no item context), only ``selector=None`` entries match.
        """
        if not bindings:
            return None

        for binding in bindings:
            if binding.selector is None:
                # Unconditional match — whole-collection binding.
                return binding.style_id

            if item_properties is None:
                # No item context; skip selector-gated bindings.
                continue

            if _eval_cql2_json_selector(binding.selector, item_properties):
                return binding.style_id

        return None


# ---------------------------------------------------------------------------
# Async loader
# ---------------------------------------------------------------------------

async def resolve_binding_style_id(
    internal_catalog_id: str,
    internal_collection_id: str,
    item_properties: Optional[Dict[str, Any]],
) -> Optional[str]:
    """Async: load both tier configs, evaluate selectors, return winning style_id.

    Loads ``StyleBindingConfig`` at:
    - catalog tier (``catalog_id`` only) — catalog-wide defaults
    - collection tier (``catalog_id`` + ``collection_id``) — collection-specific

    Concatenates their ``bindings`` lists (collection tier appended after
    catalog tier so higher-specificity collection rules can override catalog
    rules when priority is equal), then sorts by ``priority`` descending,
    and delegates to ``StyleBindingResolver``.

    Falls through in order:
    1. Highest-priority matching binding selector (both tiers combined)
    2. Collection ``default_style_id``
    3. Catalog ``default_style_id``
    4. ``None``

    Uses the *internal* (immutable) catalog/collection id so renaming via
    ``external_id`` never shifts the binding.

    Returns ``None`` on any config-load error (fail-open).
    """
    from dynastore.modules.styles.binding_config import StyleBindingConfig
    from dynastore.models.protocols.configs import ConfigsProtocol

    mgr = get_protocol(ConfigsProtocol)
    if mgr is None:
        return None

    # Load collection-tier config (waterfall: collection → catalog → platform)
    try:
        col_cfg: StyleBindingConfig = await mgr.get_config(
            StyleBindingConfig,
            internal_catalog_id,
            internal_collection_id,
        )
    except Exception as exc:
        logger.debug(
            "style-binding: failed to load collection-tier config for %s/%s: %s",
            internal_catalog_id,
            internal_collection_id,
            exc,
        )
        return None

    # Load catalog-tier config (waterfall: catalog → platform)
    try:
        cat_cfg: StyleBindingConfig = await mgr.get_config(
            StyleBindingConfig,
            internal_catalog_id,
        )
    except Exception as exc:
        logger.debug(
            "style-binding: failed to load catalog-tier config for %s: %s",
            internal_catalog_id,
            exc,
        )
        cat_cfg = StyleBindingConfig()

    # Concatenate and sort: collection-specific (higher specificity) after
    # catalog-wide so equal-priority collection rules appear later — the sort
    # is stable, so within the same priority tier the collection rule wins.
    all_bindings = list(cat_cfg.bindings or []) + list(col_cfg.bindings or [])
    all_bindings.sort(key=lambda b: b.priority, reverse=True)

    resolver = StyleBindingResolver()
    matched = resolver.resolve(all_bindings, item_properties)
    if matched is not None:
        return matched

    # No selector matched — fall through to default_style_id cascade.
    if col_cfg.default_style_id:
        return col_cfg.default_style_id
    if cat_cfg.default_style_id:
        return cat_cfg.default_style_id

    return None
