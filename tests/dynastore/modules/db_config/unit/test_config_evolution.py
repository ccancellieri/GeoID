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

"""Config schema evolution without a persisted schema registry.

JSON schemas are not stored in a table; they are generated on demand from the
registered class, and each config row carries a content-hash ``schema_id`` as a
version tag. Because ``PluginConfig`` is ``extra='forbid'``, evolving a config's
shape (e.g. removing a field) requires a *versioned class*: old rows must keep
deserializing against the class that still declares their fields, dispatched by
the row's stored ``class_key``. These tests pin that contract.
"""

from typing import ClassVar, Tuple

import pytest
from pydantic import ValidationError

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig, resolve_config_class


class EvoConfigOriginal(PluginConfig):
    """Original shape: two fields."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "evo_config_original")
    a: Mutable[int] = 0
    b: Mutable[int] = 0


class EvoConfigEvolved(PluginConfig):
    """Evolved shape for new catalogs: field ``b`` removed."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "evo_config_evolved")
    a: Mutable[int] = 0


def test_extra_forbid_makes_naive_field_removal_break_old_data():
    """A row serialized under the original shape carries ``b``; validating it
    against a class that simply dropped ``b`` fails — extra='forbid'. This is
    why an in-place removal is unsafe and a versioned class is required."""
    old_row = EvoConfigOriginal(a=1, b=2).model_dump()
    with pytest.raises(ValidationError):
        EvoConfigEvolved.model_validate(old_row)


def test_class_override_preserves_old_rows_while_new_use_evolved_shape():
    """The safe path: old rows dispatch by their stored ``class_key`` to the
    class that still declares ``b`` (valid); new catalogs write the evolved
    class. Both resolve through the live registry — no schema table."""
    old_row = EvoConfigOriginal(a=1, b=2).model_dump()

    original = resolve_config_class(EvoConfigOriginal.class_key())
    assert original is EvoConfigOriginal
    assert original.model_validate(old_row).b == 2

    evolved = resolve_config_class(EvoConfigEvolved.class_key())
    assert evolved is EvoConfigEvolved
    assert evolved.model_validate({"a": 9}).a == 9


def test_schema_id_version_tag_distinguishes_shapes():
    """The content-hash ``schema_id`` differs across shapes, so a row's stored
    tag identifies which shape it was serialized under (drift detection)."""
    assert EvoConfigOriginal.schema_id() != EvoConfigEvolved.schema_id()
    assert EvoConfigOriginal.class_key() != EvoConfigEvolved.class_key()
