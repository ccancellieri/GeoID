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

"""Tolerant reads must reach nested ``extra="forbid"`` sub-models (#2640).

Follow-up from the top-level tolerant-read work (#2626 platform tier, #2639
catalog/collection tier). ``_validate_stored_config`` stripped unknown keys
only at the root of a stored config dict. Several config fields (e.g.
``ItemsReadPolicy.feature_type``, ``ItemsWritePolicy.derive``) embed
``BaseModel``s that independently set ``extra="forbid"``
(``dynastore.modules.storage.computed_fields``), so a rename/removal inside
one of those nested models still hard-failed with ``extra_forbidden`` even
though the top-level dict looked clean. These tests pin the fix: nested
drift now degrades the same way as top-level drift, and a genuine type
error on a still-declared field keeps raising.
"""

from typing import ClassVar, List, Optional, Tuple

import pytest
from pydantic import BaseModel, ConfigDict, ValidationError

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.db_config.stored_config_read import _validate_stored_config


class _NestedOriginal(BaseModel):
    """Nested sub-model as it existed when a stored row was written."""

    model_config = ConfigDict(extra="forbid")

    x: int = 0
    legacy: int = 0


class _NestedEvolved(BaseModel):
    """Current nested shape: ``legacy`` was removed since the row above was
    written."""

    model_config = ConfigDict(extra="forbid")

    x: int = 0


class _NestedAllow(BaseModel):
    """A nested sub-model that opts into ``extra="allow"``; unrelated to the
    forbid gap but pins that the fix does not strip fields it should keep."""

    model_config = ConfigDict(extra="allow")

    y: int = 0


class _OuterConfig(PluginConfig):
    """Top-level config embedding both a bare nested model and a list of
    nested models, mirroring ``ItemsReadPolicy.feature_type`` /
    ``ItemsWritePolicy.derive.spatial_cells`` shapes."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "_test_outer_config_2640")
    nested: Mutable[_NestedEvolved] = _NestedEvolved()
    nested_list: Mutable[List[_NestedEvolved]] = []
    nested_optional: Mutable[Optional[_NestedEvolved]] = None
    allow_nested: Mutable[_NestedAllow] = _NestedAllow()


def test_nested_extra_forbid_drift_degrades_instead_of_raising(caplog):
    """A stored row whose NESTED sub-model carries a renamed/removed field
    must read tolerantly (drop + warn), not raise ``extra_forbidden``."""
    row = {
        "nested": _NestedOriginal(x=1, legacy=2).model_dump(),
        "nested_list": [],
        "nested_optional": None,
        "allow_nested": {"y": 5},
    }

    with caplog.at_level(
        "WARNING", logger="dynastore.modules.db_config.stored_config_read"
    ):
        cfg = _validate_stored_config(_OuterConfig, row)

    assert isinstance(cfg, _OuterConfig)
    assert cfg.nested.x == 1
    assert not hasattr(cfg.nested, "legacy")
    assert any(
        "legacy key" in r.getMessage() and "'legacy'" in r.getMessage()
        for r in caplog.records
    ), "must warn naming the dropped nested legacy key"


def test_nested_extra_forbid_drift_inside_list_degrades(caplog):
    """The same tolerance applies to a nested model reached through a list
    field (``List[NestedModel]``), not just a bare nested field."""
    row = {
        "nested": _NestedEvolved(x=0).model_dump(),
        "nested_list": [
            _NestedOriginal(x=1, legacy=2).model_dump(),
            _NestedOriginal(x=3, legacy=4).model_dump(),
        ],
        "nested_optional": None,
        "allow_nested": {"y": 5},
    }

    cfg = _validate_stored_config(_OuterConfig, row)

    assert [n.x for n in cfg.nested_list] == [1, 3]
    assert all(not hasattr(n, "legacy") for n in cfg.nested_list)


def test_nested_extra_forbid_drift_through_optional_degrades():
    """The tolerance reaches a nested model behind ``Optional[...]`` too."""
    row = {
        "nested": _NestedEvolved(x=0).model_dump(),
        "nested_list": [],
        "nested_optional": _NestedOriginal(x=9, legacy=1).model_dump(),
        "allow_nested": {"y": 5},
    }

    cfg = _validate_stored_config(_OuterConfig, row)

    assert cfg.nested_optional is not None
    assert cfg.nested_optional.x == 9
    assert not hasattr(cfg.nested_optional, "legacy")


def test_extra_allow_nested_model_keeps_its_extra_keys():
    """A nested model that itself declares ``extra="allow"`` must not have
    its extra keys stripped — only ``extra="forbid"`` sub-models are
    tolerant-stripped; ``allow`` already round-trips unknown keys on
    purpose."""
    row = {
        "nested": _NestedEvolved(x=0).model_dump(),
        "nested_list": [],
        "nested_optional": None,
        "allow_nested": {"y": 5, "kept": "value"},
    }

    cfg = _validate_stored_config(_OuterConfig, row)

    assert cfg.allow_nested.y == 5
    assert cfg.allow_nested.model_extra == {"kept": "value"}


def test_nested_known_field_type_error_still_raises():
    """Stripping unknown keys must not mask genuine corruption: a KNOWN
    nested field with a wrong-typed value still raises."""
    row = {
        "nested": {"x": "not-an-int"},
        "nested_list": [],
        "nested_optional": None,
        "allow_nested": {"y": 5},
    }

    with pytest.raises(ValidationError):
        _validate_stored_config(_OuterConfig, row)
