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

"""Routing preset protocol + bundle dataclass (#847, #972).

A preset is a thin factory that emits a validated ``PresetBundle`` of
config entries. Each entry carries the target config class, instance,
the ``set_config`` scope kwargs, and a human-readable ``slot`` name used
by the admin apply/unapply response payloads. Entries are applied through
the standard ``ConfigsProtocol.set_config(...)`` lifecycle (validate →
apply); presets do NOT bypass validation.

The bundle is intentionally a flat tuple of entries instead of one field
per routing tier so future tiers (platform-, collection-, assets-scoped
presets — #972 PR-2+) compose without per-tier bundle fields. Each entry
brings its own ``scope`` dict so a single bundle can mix tiers (e.g. a
collection-scope preset that also touches a catalog-tier audience).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import (
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    Type,
    runtime_checkable,
)

from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
)


class PresetTier(str, Enum):
    """The URL/scope tier a preset targets.

    URL dispatch in the admin router (#972 PR-2) keys off this value to
    pick which ``/admin/.../presets/{name}`` family the preset is reachable
    from. The single ``CATALOG`` value is shipped here; ``PLATFORM``,
    ``COLLECTION``, ``ITEMS``, ``ASSETS`` arrive with PR-2/3.
    """

    CATALOG = "catalog"


@dataclass(frozen=True)
class PresetBundleEntry:
    """One config to set during preset apply.

    ``slot`` is the operator-visible name surfaced by the apply/unapply
    response payloads — keep it stable across releases since operators
    diff these payloads. ``scope`` is forwarded verbatim to
    ``ConfigsProtocol.set_config`` and ``delete_config``; the admin
    endpoint injects the URL-derived scope (e.g. ``catalog_id``) on top
    of whatever the preset itself emitted, so a preset that scopes
    everything off the URL can leave ``scope`` empty.

    ``rollback_priority`` orders entries for unapply: items templates
    must be deleted before collection templates which must be deleted
    before catalog routing (cascade-validator dependency), and audience
    rows are independent and trail at the end. Lower = unapplied first.
    Defaults match the dependency order shipped in PR #968 / PR #1002 so
    existing presets get the right semantics without per-entry tuning.
    """

    slot: str
    config_cls: Type[PluginConfig]
    instance: PluginConfig
    scope: Mapping[str, str] = field(default_factory=dict)
    rollback_priority: int = 100


@dataclass
class PresetBundle:
    """Sequence of preset entries; iteration is apply order.

    Two construction shapes are accepted for migration ergonomics:

    * **Generic** — ``PresetBundle(entries=(PresetBundleEntry(...), ...))``
      — the canonical form going forward.
    * **Legacy tier-fields** — ``PresetBundle(catalog_routing=...,
      collection_template=..., items_template=..., audience_configs={...})``
      — equivalent to the pre-#972 surface; lifted into ``entries`` at
      construction with the documented apply/rollback ordering. The
      tier-field accessors stay as read-only properties so existing test
      assertions (``bundle.catalog_routing`` / ``bundle.items_template``
      / ``bundle.audience_configs``) keep working.

    The two construction paths cannot be mixed in a single call.
    """

    entries: Tuple[PresetBundleEntry, ...] = ()
    # Legacy convenience args — collapsed into ``entries`` on init.
    catalog_routing: Optional[CatalogRoutingConfig] = None
    collection_template: Optional[CollectionRoutingConfig] = None
    items_template: Optional[ItemsRoutingConfig] = None
    audience_configs: Dict[str, PluginConfig] = field(default_factory=dict)

    def __post_init__(self) -> None:
        legacy_supplied = (
            self.catalog_routing is not None
            or self.collection_template is not None
            or self.items_template is not None
            or bool(self.audience_configs)
        )
        if self.entries and legacy_supplied:
            raise TypeError(
                "PresetBundle: pass either ``entries=`` or the legacy "
                "tier-field args, not both."
            )
        if legacy_supplied:
            self.entries = self._entries_from_legacy_fields()
        # Expose the tier-field accessors as a read-only view onto
        # ``entries`` so back-compat reads after construction reflect the
        # generic source of truth.
        self.catalog_routing = self._first_instance(CatalogRoutingConfig)
        self.collection_template = self._first_instance(CollectionRoutingConfig)
        self.items_template = self._first_instance(ItemsRoutingConfig)
        self.audience_configs = {
            e.slot[len("audience:"):]: e.instance
            for e in self.entries
            if e.slot.startswith("audience:")
        }

    def _first_instance(self, cls: Type[PluginConfig]) -> Optional[PluginConfig]:
        for e in self.entries:
            if e.config_cls is cls:
                return e.instance
        return None

    def _entries_from_legacy_fields(self) -> Tuple[PresetBundleEntry, ...]:
        out: list[PresetBundleEntry] = []
        if self.catalog_routing is not None:
            out.append(PresetBundleEntry(
                slot="catalog_routing",
                config_cls=CatalogRoutingConfig,
                instance=self.catalog_routing,
                rollback_priority=30,
            ))
        if self.collection_template is not None:
            out.append(PresetBundleEntry(
                slot="collection_template",
                config_cls=CollectionRoutingConfig,
                instance=self.collection_template,
                rollback_priority=20,
            ))
        if self.items_template is not None:
            out.append(PresetBundleEntry(
                slot="items_template",
                config_cls=ItemsRoutingConfig,
                instance=self.items_template,
                rollback_priority=10,
            ))
        for plugin_name, cfg in self.audience_configs.items():
            out.append(PresetBundleEntry(
                slot=f"audience:{plugin_name}",
                config_cls=type(cfg),
                instance=cfg,
                rollback_priority=100,
            ))
        return tuple(out)

    def iter_apply(self) -> Iterator[PresetBundleEntry]:
        """Yield entries in apply order (their natural iteration order)."""
        return iter(self.entries)

    def iter_rollback(self) -> Iterable[PresetBundleEntry]:
        """Yield entries in rollback order: lower ``rollback_priority``
        first, ties broken by original insertion order to keep audience
        rows in bundle order for predictable response payloads."""
        return [
            e for _, e in sorted(
                enumerate(self.entries),
                key=lambda pair: (pair[1].rollback_priority, pair[0]),
            )
        ]


@runtime_checkable
class RoutingPreset(Protocol):
    """Structural contract every preset must satisfy.

    ``name`` is the registry key + URL path segment; ``description`` is
    surfaced by ``GET /admin/presets``. ``tier`` keys the admin URL
    family the preset is reachable from (catalog-only today; #972 PR-2+
    expand the enum). ``build(catalog_id)`` is called per apply — preset
    implementations stay stateless so the registry can be a plain dict.
    """

    name: str
    description: str
    tier: ClassVar[PresetTier]

    def build(self, catalog_id: str) -> PresetBundle: ...
