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

"""Tests for BaseMetadata.get_internal_columns() covering the physical_id entry.

Verifies:
- physical_id is listed as an internal column alongside physical_schema.
- A Catalog model that receives physical_id as an extra field does not
  expose it in model_dump() (which drives API output via STAC serialization).
"""
from __future__ import annotations


def test_physical_id_in_internal_columns():
    """physical_id must be returned by get_internal_columns()."""
    from dynastore.models.shared_models import Catalog

    cols = Catalog.get_internal_columns()
    assert "physical_id" in cols, (
        f"physical_id missing from get_internal_columns(); got: {cols}"
    )


def test_physical_schema_still_in_internal_columns():
    """Existing entry physical_schema must still be present (no regression)."""
    from dynastore.models.shared_models import Catalog

    cols = Catalog.get_internal_columns()
    assert "physical_schema" in cols


def test_internal_columns_returns_set():
    """get_internal_columns() must return a Set[str]."""
    from dynastore.models.shared_models import Catalog

    cols = Catalog.get_internal_columns()
    assert isinstance(cols, set)
    assert all(isinstance(c, str) for c in cols)


def test_physical_id_extra_not_emitted_in_model_dump():
    """A Catalog with physical_id arriving as an extra field must not
    emit it in model_dump() — same rule as physical_schema.

    STAC serialization calls model_dump() and then further filters via
    get_internal_columns(); this test pins the baseline that extra fields
    named in get_internal_columns() are excluded at the API boundary.
    """
    from dynastore.models.shared_models import Catalog

    # Inject physical_id as an extra field (model_config extra="allow")
    cat = Catalog.model_validate(
        {"id": "my_catalog", "physical_id": "s_abc12345"}
    )

    # The extra field must be accessible on the object itself
    # (so resolvers that might read it can do so)
    assert cat.model_extra is not None
    assert "physical_id" in cat.model_extra

    # model_dump with exclude built from get_internal_columns() must strip it
    internal = cat.get_internal_columns()
    dumped = cat.model_dump(exclude=internal)
    assert "physical_id" not in dumped, (
        "physical_id leaked into model_dump() output with internal exclusion applied"
    )


def test_physical_schema_extra_not_emitted_in_model_dump():
    """Regression guard: physical_schema is still excluded (no existing behaviour broken)."""
    from dynastore.models.shared_models import Catalog

    cat = Catalog.model_validate(
        {"id": "my_catalog", "physical_schema": "s_abc12345"}
    )
    internal = cat.get_internal_columns()
    dumped = cat.model_dump(exclude=internal)
    assert "physical_schema" not in dumped
