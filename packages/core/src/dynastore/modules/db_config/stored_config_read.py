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

"""Tolerant validation for config rows read back from the store.

Shared by every tier's config service (platform, catalog, collection) so a
field rename/removal degrades gracefully on read instead of bricking config
loading. See :func:`_validate_stored_config` for the rationale.
"""

import logging
from typing import Type

from dynastore.models.plugin_config import PluginConfig

logger = logging.getLogger(__name__)


def _validate_stored_config(cls: Type[PluginConfig], data: dict) -> PluginConfig:
    """Validate a config row read from the persistent store, tolerating keys
    the live model no longer declares.

    ``PersistentModel`` sets ``extra="forbid"`` so a wrong-shape *inbound API
    payload* fails with 422 instead of being silently persisted as ``{}``
    (#918). That strictness is right on write but fatal on read: once a field
    is renamed or removed, every pre-existing stored row raises
    ``extra_forbidden`` and bricks config loading — the scaling publisher hit
    exactly this on a stale ``cooldown_seconds`` row after the cooldown fields
    were split into ``scale_out_/scale_in_cooldown_seconds``. On read we drop
    unknown keys with a warning so a schema evolution degrades gracefully.
    Shared by the platform, catalog, and collection tier read paths.
    """
    known = set(cls.model_fields)
    unknown = [k for k in data if k not in known]
    if unknown:
        logger.warning(
            "Dropping %d legacy key(s) %s from stored %s config; a field was "
            "renamed or removed since this row was written. PATCH the config to "
            "rewrite it cleanly and silence this warning.",
            len(unknown), sorted(unknown), cls.__name__,
        )
        data = {k: v for k, v in data.items() if k in known}
    return cls.model_validate(data)
