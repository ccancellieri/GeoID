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
import types
import typing
from typing import Any, Type, Union

from pydantic import BaseModel

from dynastore.models.plugin_config import PluginConfig

logger = logging.getLogger(__name__)

_UNION_ORIGINS = (Union, types.UnionType)


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

    The tolerance recurses into nested sub-models: several config fields
    (e.g. ``ItemsReadPolicy.feature_type``, ``ItemsWritePolicy.derive``)
    embed ``BaseModel``s that independently set ``extra="forbid"``
    (``dynastore.modules.storage.computed_fields``). Without descending into
    them, drift inside a nested model still hard-fails even though the
    top-level dict looks clean (#2640).
    """
    return cls.model_validate(_strip_extra_forbid(cls, data))


def _strip_extra_forbid(cls: Type[BaseModel], data: dict, *, path: str = "") -> dict:
    """Drop keys ``cls`` no longer declares from ``data`` (only when ``cls``
    itself sets ``extra="forbid"``), then recurse into every declared field
    to reach nested ``extra="forbid"`` sub-models regardless of ``cls``'s own
    extra policy.
    """
    data = dict(data)
    if cls.model_config.get("extra") == "forbid":
        known = set(cls.model_fields)
        unknown = [k for k in data if k not in known]
        if unknown:
            label = cls.__name__ if not path else f"{cls.__name__} (nested field {path!r})"
            logger.warning(
                "Dropping %d legacy key(s) %s from stored %s config; a field was "
                "renamed or removed since this row was written. PATCH the config to "
                "rewrite it cleanly and silence this warning.",
                len(unknown), sorted(unknown), label,
            )
            data = {k: v for k, v in data.items() if k in known}

    for name, field in cls.model_fields.items():
        if name in data:
            field_path = f"{path}.{name}" if path else name
            data[name] = _strip_nested(field.annotation, data[name], path=field_path)
    return data


def _strip_nested(annotation: Any, value: Any, *, path: str) -> Any:
    """Walk ``annotation`` alongside ``value`` and strip unknown keys out of
    any nested ``BaseModel`` dict it finds (directly, through ``Optional``/
    ``Union``, or through ``List``/``Dict`` containers).
    """
    if value is None:
        return value

    origin = typing.get_origin(annotation)

    if origin in _UNION_ORIGINS:
        for arg in typing.get_args(annotation):
            if arg is type(None):
                continue
            return _strip_nested(arg, value, path=path)
        return value

    if origin is list and isinstance(value, list):
        args = typing.get_args(annotation)
        if not args:
            return value
        item_type = args[0]
        return [
            _strip_nested(item_type, item, path=f"{path}[{i}]")
            for i, item in enumerate(value)
        ]

    if origin is dict and isinstance(value, dict):
        args = typing.get_args(annotation)
        if len(args) != 2:
            return value
        value_type = args[1]
        return {
            k: _strip_nested(value_type, v, path=f"{path}.{k}") for k, v in value.items()
        }

    if isinstance(annotation, type) and issubclass(annotation, BaseModel) and isinstance(value, dict):
        return _strip_extra_forbid(annotation, value, path=path)

    return value
