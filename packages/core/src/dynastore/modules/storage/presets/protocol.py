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

"""Preset bundle dataclass (#847, #972).

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

``BundlePreset`` (see ``bundle_preset.py``) is the base class that turns a
``build() -> PresetBundle`` factory into a full apply/revoke/dry_run preset.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Dict,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
)

_C = TypeVar("_C", bound=PluginConfig)


class PresetTier(str, Enum):
    """The URL/scope tier a preset targets.

    URL dispatch in the admin router keys off this value to pick which
    ``/admin/.../presets/{name}`` family the preset is reachable from:

    * ``PLATFORM``   — ``/admin/presets/{name}`` (no scope params; ``build()``)
    * ``CATALOG``    — ``/admin/catalogs/{cat}/presets/{name}``
      (``build(catalog_id)``)
    * ``COLLECTION`` — ``/admin/catalogs/{cat}/collections/{col}/presets/{name}``
      (``build(catalog_id, collection_id)``)
    * ``ITEMS`` / ``ASSETS`` — attachable at the catalog **or** collection
      URL family. Which families are reachable is governed by
      ``catalog_scopable``: a collection-scope items/assets preset reaches
      the collection family only; one with ``catalog_scopable=True`` also
      reaches the catalog family. ``build`` receives whichever scope the
      URL supplies.
    """

    PLATFORM = "platform"
    CATALOG = "catalog"
    COLLECTION = "collection"
    ITEMS = "items"
    ASSETS = "assets"


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

    Construct with ``entries=(PresetBundleEntry(...), ...)`` — the single
    canonical form. The tier-field accessors (``catalog_routing`` /
    ``collection_template`` / ``items_template`` / ``asset_template`` /
    ``audience_configs``) are read-only computed views over ``entries``:
    each returns the first entry instance of its routing-config type (and
    ``audience_configs`` collects the ``audience:*`` slots), so code and
    tests that read a bundle by tier keep working without storing the
    tiers as separate fields.
    """

    entries: Tuple[PresetBundleEntry, ...] = ()

    def _first_instance(self, cls: Type[_C]) -> Optional[_C]:
        for e in self.entries:
            if e.config_cls is cls:
                return cast(_C, e.instance)
        return None

    @property
    def catalog_routing(self) -> Optional[CatalogRoutingConfig]:
        return self._first_instance(CatalogRoutingConfig)

    @property
    def collection_template(self) -> Optional[CollectionRoutingConfig]:
        return self._first_instance(CollectionRoutingConfig)

    @property
    def items_template(self) -> Optional[ItemsRoutingConfig]:
        return self._first_instance(ItemsRoutingConfig)

    @property
    def asset_template(self) -> Optional[AssetRoutingConfig]:
        return self._first_instance(AssetRoutingConfig)

    @property
    def audience_configs(self) -> Dict[str, PluginConfig]:
        return {
            e.slot[len("audience:"):]: e.instance
            for e in self.entries
            if e.slot.startswith("audience:")
        }

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
