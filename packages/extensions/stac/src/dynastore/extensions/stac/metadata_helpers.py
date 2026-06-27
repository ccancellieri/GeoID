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
STAC metadata helpers for pruning and merging external content.

Provides utilities to:
- Prune platform-managed assets/extensions before storage
- Merge external + managed content on retrieval
- Normalize and localize i18n fields
"""

import logging
from typing import Dict, Any, TYPE_CHECKING
from dynastore.tools.language_utils import (
    resolve_localized_field,
)

if TYPE_CHECKING:
    import pystac

logger = logging.getLogger(__name__)


def normalize_i18n_field(value: Any, default_lang: str = "en") -> Dict[str, Any]:
    """
    Normalize a field to multi-language format.

    Converts:
    - "text" -> {"en": "text"}
    - {"en": "text"} -> {"en": "text"} (unchanged)

    Args:
        value: String or dict to normalize
        default_lang: Default language for string values

    Returns:
        Dict with language keys, or original value if not suitable for normalization
    """
    if value is None:
        return {}

    # Only wrap if it's a plain string.
    # Non-string scalars (int, float, bool) are returned as {default_lang: value}.
    # Dictionaries are ONLY returned as-is if they ALREADY look like language dicts.
    # Otherwise, if it's a dict that is NOT a language dict (like cube:dimensions),
    # we return it as-is (no wrapping) to avoid corruption.
    if isinstance(value, str):
        return {default_lang: value}

    if isinstance(value, dict):
        # Check if it's already a language dict (heuristic: all keys are 2-char strings)
        if all(isinstance(k, str) and len(k) == 2 for k in value.keys() if k):
            return value
        # Not a language dict (e.g. cube:dimensions). Return as-is, NO wrapping.
        return value

    # For other types (int, float, bool), wrap them
    if isinstance(value, (int, float, bool)):
        return {default_lang: value}

    return value


def prune_managed_content_sync(
    item_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Synchronous version of prune_managed_content for use in sidecars.

    No platform-managed assets/extensions exist (the StacExtensionProtocol
    that produced them was retired), so all external content is preserved.
    """
    managed_keys: set = set()
    managed_extensions: set = set()

    # Prune assets
    external_assets = {}
    for key, asset in item_dict.get("assets", {}).items():
        is_managed = asset.get("is_managed", False) or key in managed_keys
        if not is_managed:
            # Normalize i18n fields
            normalized_asset = dict(asset)
            if "title" in normalized_asset:
                normalized_asset["title"] = normalize_i18n_field(
                    normalized_asset["title"]
                )
            if "description" in normalized_asset:
                normalized_asset["description"] = normalize_i18n_field(
                    normalized_asset["description"]
                )
            external_assets[key] = normalized_asset

    # Prune extensions
    external_extensions = [
        ext
        for ext in item_dict.get("stac_extensions", [])
        if ext not in managed_extensions
    ]

    # Extract extension-specific fields (namespace with ":")
    extra_fields = {}

    # 1. From top-level
    for key, value in item_dict.items():
        if ":" in key and key not in ["stac_version", "stac_extensions"]:
            extra_fields[key] = (
                normalize_i18n_field(value) if isinstance(value, (str, dict)) else value
            )

    # Capture source stac_version for provenance round-trip.  The read path always
    # emits the local pystac version unless the original is stored here.
    source_stac_version = item_dict.get("stac_version")
    if source_stac_version is not None:
        extra_fields["stac_version"] = source_stac_version

    # 2. From properties (if not already found)
    properties = item_dict.get("properties", {})
    for key, value in properties.items():
        if ":" in key:
            # Prefer top-level if collision (though unlikely in valid STAC)
            if key not in extra_fields:
                extra_fields[key] = (
                    normalize_i18n_field(value)
                    if isinstance(value, (str, dict))
                    else value
                )

    # Extract per-item metadata from properties
    properties = item_dict.get("properties", {})
    title = (
        normalize_i18n_field(properties.get("title")) if "title" in properties else None
    )
    description = (
        normalize_i18n_field(properties.get("description"))
        if "description" in properties
        else None
    )

    # Keywords are tricky because they can be a list of strings or list of i18n dicts
    keywords = None
    if "keywords" in properties:
        raw_keywords = properties["keywords"]
        if isinstance(raw_keywords, list):
            keywords = [normalize_i18n_field(kw) for kw in raw_keywords]

    return {
        "title": title,
        "description": description,
        "keywords": keywords,
        "external_assets": external_assets,
        "external_extensions": external_extensions,
        "extra_fields": extra_fields,
    }


def prune_stac_managed_properties(properties: Dict[str, Any]) -> None:
    """
    Remove STAC-managed fields from a properties dictionary in-place.
    Used to prevent duplicate storage in generic attributes sidecars.
    """
    # 1. Core fields
    properties.pop("title", None)
    properties.pop("description", None)
    properties.pop("keywords", None)

    # 2. Extension fields (namespace:property)
    for key in list(properties.keys()):
        if ":" in key:
            properties.pop(key, None)


async def prune_managed_content(
    item_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract only external content for storage in sidecar.

    Args:
        item_dict: Full STAC Item dictionary

    Returns:
        Dict with external_assets, external_extensions, title, description, keywords, extra_fields
    """
    # No managed assets/extensions — external content passes through.
    managed_keys: set = set()
    managed_extensions: set = set()

    # Prune assets
    external_assets = {}
    for key, asset in item_dict.get("assets", {}).items():
        is_managed = asset.get("is_managed", False) or key in managed_keys
        if not is_managed:
            # Normalize i18n fields
            normalized_asset = dict(asset)
            if "title" in normalized_asset:
                normalized_asset["title"] = normalize_i18n_field(
                    normalized_asset["title"]
                )
            if "description" in normalized_asset:
                normalized_asset["description"] = normalize_i18n_field(
                    normalized_asset["description"]
                )
            external_assets[key] = normalized_asset

    # Prune extensions
    external_extensions = [
        ext
        for ext in item_dict.get("stac_extensions", [])
        if ext not in managed_extensions
    ]

    # Extract extension-specific fields (namespace with ":")
    extra_fields = {}
    for key, value in item_dict.items():
        if ":" in key:
            extra_fields[key] = (
                normalize_i18n_field(value) if isinstance(value, (str, dict)) else value
            )

    # Extract per-item metadata
    properties = item_dict.get("properties", {})
    title = (
        normalize_i18n_field(properties.get("title")) if "title" in properties else None
    )
    description = (
        normalize_i18n_field(properties.get("description"))
        if "description" in properties
        else None
    )
    keywords = (
        [normalize_i18n_field(kw) for kw in properties.get("keywords", [])]
        if "keywords" in properties
        else None
    )

    return {
        "title": title,
        "description": description,
        "keywords": keywords,
        "external_assets": external_assets,
        "external_extensions": external_extensions,
        "extra_fields": extra_fields,
    }


async def merge_stac_metadata(
    item: "pystac.Item",
    external_metadata: Dict[str, Any],
    lang: str,
) -> "pystac.Item":
    """
    Merge external content into STAC Item.

    Args:
        item: Base STAC Item
        external_metadata: External metadata from sidecar
        lang: Active language for localizing per-item fields

    Returns:
        Enhanced STAC Item
    """
    import pystac

    # 1. Localize per-item metadata
    if external_metadata.get("title"):
        localized_title = resolve_localized_field(external_metadata["title"], lang)
        if localized_title:
            item.properties["title"] = localized_title

    if external_metadata.get("description"):
        localized_desc = resolve_localized_field(external_metadata["description"], lang)
        if localized_desc:
            item.properties["description"] = localized_desc

    if external_metadata.get("keywords"):
        localized_keywords = [
            resolve_localized_field(kw, lang) for kw in external_metadata["keywords"]
        ]
        item.properties["keywords"] = [kw for kw in localized_keywords if kw]

    # 2. Localize external assets
    external_assets = {}
    for key, asset_dict in (external_metadata.get("external_assets") or {}).items():
        localized_asset = dict(asset_dict)
        if "title" in localized_asset:
            localized_asset["title"] = resolve_localized_field(
                localized_asset["title"], lang
            )
        if "description" in localized_asset:
            localized_asset["description"] = resolve_localized_field(
                localized_asset["description"], lang
            )
        external_assets[key] = pystac.Asset.from_dict(localized_asset)

    # 3. Merge external assets onto whatever the item already carries.
    all_assets = {**external_assets, **item.assets}
    item.assets = all_assets

    # 4. Combine extensions (external + the item's existing declarations).
    all_extensions = list(
        set(
            (external_metadata.get("external_extensions") or [])
            + (item.stac_extensions or [])
        )
    )
    item.stac_extensions = all_extensions

    # 6. Merge extra fields back into properties.
    # These were extracted FROM properties during ingest (proj:*, cube:*, etc.) so
    # they must go back into properties, not into item.extra_fields (which is top-level).
    # Dict values are localized ONLY if they look like language dicts.
    for key, value in (external_metadata.get("extra_fields") or {}).items():
        if isinstance(value, dict):
            # Check if it's a language dict before trying to resolve it
            if all(isinstance(k, str) and len(k) == 2 for k in value.keys() if k):
                localized_value = resolve_localized_field(value, lang)
                if localized_value is not None:
                    item.properties[key] = localized_value
            else:
                # Not a language dict (e.g. cube:dimensions), merge as-is
                item.properties[key] = value
        elif value is not None:
            item.properties[key] = value

    return item
