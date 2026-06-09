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

"""Schema-id-gated catalog defaults snapshot (#1079 option c).

On catalog creation we capture the resolved platform/code defaults for the
catalog's stable behaviour/shape value-configs into a single blob, so that a
later change to a platform or code default no longer silently re-resolves into
(and diverges) collections that already inherited the old value.

The snapshot is **schema-id-gated**: each entry stores the config class's
``schema_id()`` (sha256 of its canonical JSON schema). On read an entry is used
as the inheritance base only while the stored schema_id still matches the
current class. If the class evolved — any field add/remove/rename mints a new
id — the stale entry is ignored and resolution falls back to the live default,
so an evolved or removed config class never breaks reads (``extra="forbid"``
would otherwise reject a removed field on re-validation).

Driver and routing configs are deliberately **not** snapshotted: they are bound
to driver classes that evolve, and their structure is already ``Immutable``
(``operations`` / ``primary_driver``). Leaving them on live inheritance keeps
platform driver-evolution unblocked — only stable value-configs are frozen.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Type

from dynastore.models.plugin_config import PluginConfig
from dynastore.tools.typed_store.registry import TypedModelRegistry

logger = logging.getLogger(__name__)

# Reserved ``catalog_configs.ref_key`` holding the defaults-snapshot blob. The
# leading ``__`` keeps it out of the per-class config iterators, which resolve
# ref_key -> config class and skip keys that are not registered classes.
SNAPSHOT_REF_KEY = "__catalog_defaults_snapshot__"

# Class-name suffixes excluded from the snapshot: driver/routing configs are
# driver-bound (evolution-sensitive) and already structurally Immutable.
_EXCLUDED_SUFFIXES = ("DriverConfig", "RoutingConfig")


def is_snapshottable_config(cls: Type[PluginConfig]) -> bool:
    """True for catalog-scoped, stable *value* configs (not driver/routing)."""
    if cls.__dict__.get("is_abstract_base", False):
        return False
    # Platform-only configs never cascade to collections — nothing to freeze.
    if getattr(cls, "_freeze_at", None) == "platform":
        return False
    if cls.__name__.endswith(_EXCLUDED_SUFFIXES):
        return False
    return True


def snapshottable_config_classes() -> List[Type[PluginConfig]]:
    """All registered config classes eligible for the catalog defaults snapshot."""
    return [
        cls
        for cls in TypedModelRegistry.subclasses_of(PluginConfig)
        if is_snapshottable_config(cls)
    ]


def serialize_for_snapshot(cfg: PluginConfig) -> Dict[str, Any]:
    """Freeze one resolved config into a schema-id-tagged snapshot entry.

    The resolved value is re-validated through a full dump so every field is
    explicitly set, making the persisted ``data`` a complete shadow of the
    platform/code defaults when later used as the inheritance base.
    """
    cls = type(cfg)
    pinned = cls.model_validate(cfg.model_dump(mode="python"))
    return {
        "schema_id": cls.schema_id(),
        "data": pinned.model_dump(mode="json", context={"secret_mode": "db"}),
    }


def select_snapshot_base(
    snapshot: Optional[Dict[str, Any]],
    cls: Type[PluginConfig],
) -> Optional[PluginConfig]:
    """Return the snapshotted base instance for ``cls`` iff it is still valid.

    The schema-id gate: an entry is honoured only when its stored ``schema_id``
    equals the current class's ``schema_id()``. On mismatch (class evolved) or a
    validation failure (defensive) the caller falls back to the live default.
    """
    if not snapshot:
        return None
    entry = snapshot.get(cls.class_key())
    if not entry:
        return None
    if entry.get("schema_id") != cls.schema_id():
        logger.debug(
            "config snapshot stale for %s (schema_id changed) — using live default",
            cls.class_key(),
        )
        return None
    data = entry.get("data")
    if data is None:
        return None
    try:
        return cls.model_validate(data)
    except Exception:  # noqa: BLE001 — tolerate any stored-shape drift
        logger.warning(
            "config snapshot for %s failed to validate — using live default",
            cls.class_key(),
        )
        return None
