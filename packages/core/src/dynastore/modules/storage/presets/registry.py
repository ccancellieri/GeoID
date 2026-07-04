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

"""Preset registry — unified store for all presets.

Plain dict-backed registry. Built-in presets register themselves on
package import (see ``__init__``). Extensions register additional
presets during module bootstrap by importing ``register_preset``.

Every registered preset exposes the unified ``Preset`` interface
(``apply`` / ``revoke`` / ``dry_run``) — build-based routing presets get
it by subclassing ``BundlePreset``. ``CompositePreset.compose`` references
are validated at registration time.
"""
from __future__ import annotations

import importlib.metadata
import logging
from typing import Any, Dict, List, Optional

from .protocol import PresetTier

logger = logging.getLogger(__name__)

# Union type for the registry values; Preset / CompositePreset instances
# (BundlePreset subclasses included) live here.
_AnyPreset = Any  # Preset | CompositePreset

_REGISTRY: Dict[str, _AnyPreset] = {}

_PRESET_ENTRY_POINT_GROUP = "dynastore.presets"


def register_preset(preset: Any) -> None:
    """Register a preset under ``preset.name``.

    Raises ``ValueError`` if a preset with that name is already
    registered — extensions opting in to a name owned by core must use
    a different name. Re-registration to override would mask config
    surprises; explicit error is preferred.

    For ``CompositePreset`` instances, validates that every name declared
    in ``compose`` is already registered at the time of registration.
    Unknown child names raise ``ValueError`` with a clear message so
    authors catch missing dependencies at import time rather than at
    apply time.
    """
    if preset.name in _REGISTRY:
        raise ValueError(
            f"Preset {preset.name!r} already registered "
            f"(by {type(_REGISTRY[preset.name]).__name__})"
        )
    # Validate CompositePreset.compose references.
    compose = getattr(preset, "compose", None)
    if compose:
        missing = [c for c in compose if c not in _REGISTRY]
        if missing:
            raise ValueError(
                f"CompositePreset {preset.name!r} references unknown child "
                f"preset(s): {missing}. Register children first."
            )
    _REGISTRY[preset.name] = preset


def load_preset_entry_points() -> None:
    """Discover and import every ``dynastore.presets`` entry-point (#2601).

    Each entry-point module registers itself via ``register_preset()`` as an
    import side-effect, same as the built-in presets imported above — the
    entry-point's return value is discarded, only the import matters. Mirrors
    the fail-soft handling ``discover_and_load_plugins`` uses for the
    ``dynastore.modules`` / ``dynastore.extensions`` / ``dynastore.tasks``
    groups: a module whose optional dependencies aren't installed
    (``ImportError``) is skipped with a WARN log rather than raising, so one
    missing preset never blocks the others or the process boot that imports
    this package.

    Idempotent per process: Python caches imported modules, so re-running
    this (e.g. a second call from a test) re-imports nothing and does not
    retrigger ``register_preset``'s duplicate-name guard.
    """
    for entry_point in importlib.metadata.entry_points(group=_PRESET_ENTRY_POINT_GROUP):
        try:
            entry_point.load()
            logger.info(
                "Successfully discovered installed %s: %s",
                _PRESET_ENTRY_POINT_GROUP, entry_point.name,
            )
        except ImportError as e:
            logger.warning(
                "Skipping %s plugin '%s': optional dependencies not installed "
                "— %s (expected when SCOPE excludes the plugin's extras; "
                "unexpected if the plugin should be active for this service)",
                _PRESET_ENTRY_POINT_GROUP, entry_point.name, e,
            )
        except Exception:
            logger.error(
                "Failed to load installed plugin '%s' from group '%s'",
                entry_point.name, _PRESET_ENTRY_POINT_GROUP, exc_info=True,
            )


def get_preset(name: str) -> _AnyPreset:
    """Look up a preset by name. Raises ``KeyError`` with the available
    presets in the message for operator self-service."""
    if name not in _REGISTRY:
        raise KeyError(
            f"Unknown preset {name!r}. Known: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]


# Alias used by CompositePreset internals and lifecycle layer.
find_preset = get_preset


def list_presets(tier: Optional[PresetTier] = None) -> List[str]:
    """Sorted list of registered preset names.

    When ``tier`` is given, only presets declaring that tier are
    returned — the registry stays a single flat namespace, the filter is
    applied at read time.
    """
    if tier is None:
        return sorted(_REGISTRY)
    return sorted(
        name
        for name, preset in _REGISTRY.items()
        if getattr(preset, "tier", None) == tier
    )


def search_presets(
    *,
    q: Optional[str] = None,
    name: Optional[str] = None,
    tier: Optional[PresetTier] = None,
    keywords: Optional[List[str]] = None,
    applied_at_scope: Optional[str] = None,
    limit: int = 50,
    cursor: Optional[str] = None,
) -> Dict[str, Any]:
    """Search the registry with optional filters; returns a paginated result.

    All filter fields are AND-combined. ``q`` does a case-insensitive
    substring match against ``name``, ``description``, and ``keywords``.
    ``name`` is an exact or prefix match against the preset name.
    ``keywords`` is an AND match — all supplied keywords must appear in the
    preset's ``keywords`` tuple. ``applied_at_scope`` is reserved for the
    lifecycle layer; the registry itself has no audit knowledge, so the
    caller should filter the result by audit state after receiving it.

    Returns ``{"items": [...], "next_cursor": str | None}``.
    """
    candidates: List[_AnyPreset] = list(_REGISTRY.values())

    if q:
        q_lower = q.lower()
        candidates = [
            p for p in candidates
            if (
                q_lower in p.name.lower()
                or q_lower in getattr(p, "description", "").lower()
                or any(q_lower in kw.lower() for kw in getattr(p, "keywords", ()))
            )
        ]

    if name is not None:
        candidates = [
            p for p in candidates if p.name.startswith(name)
        ]

    if tier is not None:
        candidates = [
            p for p in candidates if getattr(p, "tier", None) == tier
        ]

    if keywords:
        def _has_all_keywords(p: _AnyPreset) -> bool:
            preset_kws = {kw.lower() for kw in getattr(p, "keywords", ())}
            return all(kw.lower() in preset_kws for kw in keywords)

        candidates = [p for p in candidates if _has_all_keywords(p)]

    # Sort by name for stable pagination.
    candidates.sort(key=lambda p: p.name)

    # Keyset pagination on name.
    if cursor is not None:
        candidates = [p for p in candidates if p.name > cursor]

    page = candidates[:limit]
    next_cursor = page[-1].name if len(page) == limit else None

    def _tier_value(tier: Any) -> str:
        if tier is None:
            return ""
        if hasattr(tier, "value"):
            return str(tier.value)
        return str(tier)

    def _entry(p: _AnyPreset) -> Dict[str, Any]:
        params_schema: Optional[Dict[str, Any]] = None
        pm = getattr(p, "params_model", None)
        if pm is not None and hasattr(pm, "model_json_schema"):
            try:
                params_schema = pm.model_json_schema()
            except Exception:  # noqa: BLE001
                params_schema = None
        return {
            "name": p.name,
            "description": getattr(p, "description", ""),
            "keywords": list(getattr(p, "keywords", ())),
            "tier": _tier_value(getattr(p, "tier", None)),
            "catalog_scopable": bool(getattr(p, "catalog_scopable", False)),
            "params_schema": params_schema,
        }

    return {
        "items": [_entry(p) for p in page],
        "next_cursor": next_cursor,
    }
