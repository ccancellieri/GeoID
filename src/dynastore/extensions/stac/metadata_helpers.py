"""
STAC metadata helpers for pruning and merging external content.

Provides utilities to:
- Prune platform-managed assets/extensions before storage
- Merge external + managed content on retrieval
- Normalize and localize i18n fields
"""

import logging
from typing import Dict, Any, List, Set, Optional, TYPE_CHECKING
from dynastore.tools.language_utils import (
    resolve_localized_field,
    inject_localized_field,
)

if TYPE_CHECKING:
    import pystac
    from dynastore.extensions.stac.stac_extension_protocol import StacExtensionProtocol, StacExtensionContext

logger = logging.getLogger(__name__)


# Mapping from ``StacPluginConfig.auto_render_extensions`` short names
# to URI substrings that match a provider's ``get_stac_extensions()``.
# Used by :func:`filter_providers_by_short_names` to gate which
# auto-renderers contribute at item-render time.
_SHORT_NAME_URI_MARKERS = {
    "proj":   ("/projection/",),
    "raster": ("/raster/",),
    "vector": ("/vector/", "vector:"),
}


def filter_providers_by_short_names(
    providers: List["StacExtensionProtocol"],
    allowed_short_names: List[str],
) -> List["StacExtensionProtocol"]:
    """Filter ``StacExtensionProtocol`` providers by collection-level
    ``auto_render_extensions``.

    A provider passes if at least one URI it advertises (via
    ``get_stac_extensions()``) matches a short name in
    ``allowed_short_names``.  Empty allowed list → no providers pass
    (passthrough mode: only externally-supplied content surfaces).

    Providers that don't match any known short name (e.g. a custom
    extension plugin) are kept by default — this gate is strictly for
    the proj / raster / vector auto-renderers documented today.
    """
    if not allowed_short_names:
        return []
    allowed_markers: Set[str] = set()
    for short_name in allowed_short_names:
        allowed_markers.update(_SHORT_NAME_URI_MARKERS.get(short_name, ()))

    kept: List["StacExtensionProtocol"] = []
    for p in providers:
        uris = list(p.get_stac_extensions() or [])
        # Keep if any URI matches an allowed marker, OR if no URIs
        # match any *known* marker (custom / unknown extension — out of
        # this gate's scope).
        known_uris = [u for u in uris if any(any(m in u for m in markers) for markers in _SHORT_NAME_URI_MARKERS.values())]
        if not known_uris:
            kept.append(p)
            continue
        if any(any(m in u for m in allowed_markers) for u in uris):
            kept.append(p)
    return kept


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
    item_dict: Dict[str, Any], providers: List["StacExtensionProtocol"]
) -> Dict[str, Any]:
    """
    Synchronous version of prune_managed_content for use in sidecars.
    """
    # Identify managed assets/extensions
    managed_keys = set()
    managed_extensions = set()

    for provider in providers:
        managed_keys.update(provider.get_managed_asset_keys())
        managed_extensions.update(provider.get_stac_extensions())

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


def prune_stac_managed_properties(properties: Dict[str, Any], providers: List) -> None:
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
    item_dict: Dict[str, Any], providers: List
) -> Dict[str, Any]:
    """
    Extract only external content for storage in sidecar.

    Args:
        item_dict: Full STAC Item dictionary
        providers: List of StacExtensionProtocol implementations

    Returns:
        Dict with external_assets, external_extensions, title, description, keywords, extra_fields
    """
    # Identify managed assets/extensions
    managed_keys = set()
    managed_extensions = set()

    for provider in providers:
        managed_keys.update(provider.get_managed_asset_keys())
        managed_extensions.update(provider.get_stac_extensions())

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
    providers: List,
    context: "StacExtensionContext",
) -> "pystac.Item":
    """
    Merge external + managed content into STAC Item.

    Args:
        item: Base STAC Item
        external_metadata: External metadata from sidecar
        providers: List of StacExtensionProtocol implementations
        context: Context for asset generation

    Returns:
        Enhanced STAC Item
    """
    import pystac

    lang = context.lang

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

    # 3. Generate managed assets
    for provider in providers:
        if provider.can_provide_assets("Item", context):
            await provider.add_assets_to_item(item, context)

    # 4. Merge: external first, managed can override
    all_assets = {**external_assets, **item.assets}
    item.assets = all_assets

    # 5. Combine extensions
    all_extensions = list(
        set(
            (external_metadata.get("external_extensions") or [])
            + (item.stac_extensions or [])
            + [ext for p in providers for ext in p.get_stac_extensions()]
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
