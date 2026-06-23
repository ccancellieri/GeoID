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

"""Security regression tests for the PG items driver physical-table handling.

Layered defenses after the physical_id unification (retired physical_table JSONB
pin):

- L1: ``resolve_physical_table`` reads from ``CatalogsProtocol.resolve_physical_id``
  and validates the returned identifier before it reaches any SQL expression.
  An unsafe value from the registry (corrupted row) is rejected at this boundary.
- L1a: ``_resolve_schema`` rejects unsafe schema identifiers (unchanged).
- L2: ``ItemsPostgresqlDriverConfig`` no longer carries a ``physical_table``
  field — the physical name is not stored in JSONB.  Callers that previously
  read ``config.physical_table`` must use
  ``CatalogsProtocol.resolve_physical_id``.
- L3a: the collection-init hook never persists a caller-supplied
  ``physical_table`` — the field does not exist on the persisted config.
- L4: the vestigial ``physical_schema`` field remains absent.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver
from dynastore.tools.db import InvalidIdentifierError


# A value that breaks out of `"{schema}"."{table}"` — the double-quote
# terminates the quoted identifier and opens an injection.
INJECTION = 'c__" ; DROP TABLE secret; --'
# A valid c_-prefixed physical_id produced by generate_physical_name("c_").
VALID = "c__abc12345"


# --------------------------------------------------------------------------
# L2 — physical_table field removed from ItemsPostgresqlDriverConfig
# --------------------------------------------------------------------------


class TestPhysicalTableFieldRemoved:
    def test_physical_table_not_in_model_fields(self):
        """physical_table must not appear in ItemsPostgresqlDriverConfig.model_fields.

        The physical name is now authoritative in collections.physical_id and
        resolved via CatalogsProtocol.resolve_physical_id — never stored in
        collection_configs JSONB.
        """
        assert "physical_table" not in ItemsPostgresqlDriverConfig.model_fields, (
            "physical_table must not be a declared field on ItemsPostgresqlDriverConfig"
        )

    def test_physical_schema_not_in_model_fields(self):
        assert "physical_schema" not in ItemsPostgresqlDriverConfig.model_fields


# --------------------------------------------------------------------------
# L1 — SQL-boundary validation in resolve_physical_table
# --------------------------------------------------------------------------


class TestResolvePhysicalTableValidation:
    @pytest.mark.asyncio
    async def test_rejects_unsafe_physical_id_from_registry(self):
        """Even a corrupted registry value must be rejected before it reaches SQL.

        resolve_physical_table delegates to CatalogsProtocol.resolve_physical_id
        and validates the returned string via validate_sql_identifier before
        returning it to any SQL-composing caller.
        """
        driver = ItemsPostgresqlDriver()
        catalogs = MagicMock()
        catalogs.resolve_physical_id = AsyncMock(return_value=INJECTION)
        with patch("dynastore.tools.discovery.get_protocol", return_value=catalogs):
            with pytest.raises(InvalidIdentifierError):
                await driver.resolve_physical_table("cat1", "col1")

    @pytest.mark.asyncio
    async def test_passes_valid_physical_id(self):
        """A valid physical_id that corresponds to an existing hub table is returned.

        resolve_physical_table now verifies hub-table existence after resolving
        the identifier.  The test short-circuits the slow path via the
        process-local _confirmed_active cache.
        """
        driver = ItemsPostgresqlDriver()
        catalogs = MagicMock()
        catalogs.resolve_physical_id = AsyncMock(return_value=VALID)
        from dynastore.modules.catalog.collection_service import (
            _confirmed_active,
            _mark_confirmed_active,
            _unmark_confirmed_active,
        )
        _mark_confirmed_active("cat1", "col1")
        try:
            with patch("dynastore.tools.discovery.get_protocol", return_value=catalogs):
                assert await driver.resolve_physical_table("cat1", "col1") == VALID
        finally:
            _unmark_confirmed_active("cat1", "col1")

    @pytest.mark.asyncio
    async def test_none_when_protocol_absent(self):
        """No CatalogsProtocol registered → None (collection not provisioned)."""
        driver = ItemsPostgresqlDriver()
        with patch("dynastore.tools.discovery.get_protocol", return_value=None):
            assert await driver.resolve_physical_table("cat1", "col1") is None

    @pytest.mark.asyncio
    async def test_none_when_physical_id_absent(self):
        """Registry row exists but physical_id is NULL → None."""
        driver = ItemsPostgresqlDriver()
        catalogs = MagicMock()
        catalogs.resolve_physical_id = AsyncMock(return_value=None)
        with patch("dynastore.tools.discovery.get_protocol", return_value=catalogs):
            assert await driver.resolve_physical_table("cat1", "col1") is None

    @pytest.mark.asyncio
    async def test_none_when_hub_table_absent(self):
        """physical_id set in registry but hub table not yet created → None.

        ensure_storage has not run yet (lazy-activation pending state).
        resolve_physical_table must return None so item_query's pending guard
        fires rather than attempting to SELECT from a non-existent relation.
        """
        driver = ItemsPostgresqlDriver()
        catalogs = MagicMock()
        catalogs.resolve_physical_id = AsyncMock(return_value=VALID)
        catalogs.resolve_physical_schema = AsyncMock(return_value="s_abc12345")
        from dynastore.modules.catalog.collection_service import _unmark_confirmed_active
        _unmark_confirmed_active("cat_pending", "col_pending")
        with (
            patch("dynastore.tools.discovery.get_protocol", return_value=catalogs),
            patch(
                "dynastore.modules.db_config.locking_tools.check_table_exists",
                new=AsyncMock(return_value=False),
            ),
        ):
            assert await driver.resolve_physical_table("cat_pending", "col_pending") is None


# --------------------------------------------------------------------------
# L1a — SQL-boundary validation for the schema resolver (unchanged)
# --------------------------------------------------------------------------


class TestResolveSchemaValidation:
    @pytest.mark.asyncio
    async def test_rejects_unsafe_schema(self):
        driver = ItemsPostgresqlDriver()
        catalogs = MagicMock()
        catalogs.resolve_physical_schema = AsyncMock(return_value='s" ; DROP SCHEMA x; --')
        with patch("dynastore.tools.discovery.get_protocol", return_value=catalogs):
            with pytest.raises(InvalidIdentifierError):
                await driver._resolve_schema("cat1")

    @pytest.mark.asyncio
    async def test_passes_valid_schema(self):
        driver = ItemsPostgresqlDriver()
        catalogs = MagicMock()
        catalogs.resolve_physical_schema = AsyncMock(return_value="s_abc12345")
        with patch("dynastore.tools.discovery.get_protocol", return_value=catalogs):
            assert await driver._resolve_schema("cat1") == "s_abc12345"


# --------------------------------------------------------------------------
# L3a — collection-init hook: no physical_table field on persisted config
# --------------------------------------------------------------------------


class TestInitCollectionHookNoPhysicalTablePin:
    @pytest.mark.asyncio
    async def test_physical_table_in_layer_config_is_extra_only(self):
        """A layer_config dict with only physical_table has no meaningful PG
        fields to persist — the key is absorbed as an extra field (model config
        extra='allow') but is NOT a declared field.  Verify the model behaviour
        so call-sites that strip by declared-fields don't inadvertently persist it.
        """
        from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

        cfg = ItemsPostgresqlDriverConfig.model_validate({"physical_table": VALID})
        # physical_table must NOT appear in declared model_fields
        assert "physical_table" not in ItemsPostgresqlDriverConfig.model_fields, (
            "physical_table must not be a declared field"
        )

    def test_physical_table_field_absent_from_model(self):
        """ItemsPostgresqlDriverConfig.model_fields must not contain physical_table."""
        assert "physical_table" not in ItemsPostgresqlDriverConfig.model_fields
