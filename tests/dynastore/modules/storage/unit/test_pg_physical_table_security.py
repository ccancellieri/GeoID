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

Covers the layered defenses against caller-controlled ``physical_table``
reaching SQL identifiers unvalidated, and against the vestigial
``physical_schema`` override:

- L1: ``resolve_physical_table`` / ``_resolve_schema`` reject unsafe
  identifiers before any value reaches an f-string SQL identifier.
- L2: ``ItemsPostgresqlDriverConfig`` rejects an unsafe ``physical_table``
  charset at validation time.
- L3a: the collection-init hook never persists a caller-supplied
  ``physical_table`` (system-assigned only).
- L4: the vestigial ``physical_schema`` field is gone.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver
from dynastore.tools.db import InvalidIdentifierError


# A value that breaks out of `"{schema}"."{physical_table}"` — the double
# quote terminates the quoted identifier and opens an injection.
INJECTION = 't" ; DROP TABLE secret; --'
# A syntactically valid identifier (would pass charset) used to prove the
# config field accepts well-formed names.
VALID = "t_abc12345"


# --------------------------------------------------------------------------
# L2 — config-level charset validation
# --------------------------------------------------------------------------


class TestConfigPhysicalTableValidation:
    def test_rejects_injection_charset(self):
        with pytest.raises(ValueError):
            ItemsPostgresqlDriverConfig.model_validate({"physical_table": INJECTION})

    def test_rejects_embedded_quote(self):
        with pytest.raises(ValueError):
            ItemsPostgresqlDriverConfig.model_validate({"physical_table": 'a"b'})

    def test_accepts_valid_identifier(self):
        cfg = ItemsPostgresqlDriverConfig.model_validate({"physical_table": VALID})
        assert cfg.physical_table == VALID

    def test_none_is_allowed(self):
        cfg = ItemsPostgresqlDriverConfig.model_validate({})
        assert cfg.physical_table is None


# --------------------------------------------------------------------------
# L4 — vestigial physical_schema removed
# --------------------------------------------------------------------------


class TestPhysicalSchemaRemoved:
    def test_field_absent(self):
        assert "physical_schema" not in ItemsPostgresqlDriverConfig.model_fields

    def test_physical_table_is_computed(self):
        from dynastore.models.mutability import computed_fields, is_computed_field

        fi = ItemsPostgresqlDriverConfig.model_fields["physical_table"]
        assert is_computed_field(fi)
        assert "physical_table" in computed_fields(ItemsPostgresqlDriverConfig)

    def test_physical_table_is_readonly_in_schema(self):
        # Generated/machine-controlled fields must advertise read-only on the wire.
        schema = ItemsPostgresqlDriverConfig.model_json_schema()
        assert schema["properties"]["physical_table"].get("readOnly") is True


# --------------------------------------------------------------------------
# L1 — SQL-boundary validation in the resolvers
# --------------------------------------------------------------------------


class TestResolvePhysicalTableValidation:
    @pytest.mark.asyncio
    async def test_rejects_unsafe_stored_table(self):
        """Even a value that bypassed config validation (legacy row, model_copy)
        must be rejected before it reaches SQL."""
        driver = ItemsPostgresqlDriver()
        bad = ItemsPostgresqlDriverConfig.model_construct(physical_table=INJECTION)
        with patch.object(driver, "get_driver_config", AsyncMock(return_value=bad)):
            with pytest.raises(InvalidIdentifierError):
                await driver.resolve_physical_table("cat1", "col1")

    @pytest.mark.asyncio
    async def test_passes_valid_table(self):
        driver = ItemsPostgresqlDriver()
        good = ItemsPostgresqlDriverConfig.model_construct(physical_table=VALID)
        with patch.object(driver, "get_driver_config", AsyncMock(return_value=good)):
            assert await driver.resolve_physical_table("cat1", "col1") == VALID

    @pytest.mark.asyncio
    async def test_none_passthrough(self):
        driver = ItemsPostgresqlDriver()
        empty = ItemsPostgresqlDriverConfig.model_construct(physical_table=None)
        with patch.object(driver, "get_driver_config", AsyncMock(return_value=empty)):
            assert await driver.resolve_physical_table("cat1", "col1") is None


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
# L3a — collection-init hook strips caller-supplied physical_table
# --------------------------------------------------------------------------


class TestInitCollectionHookStrip:
    @pytest.mark.asyncio
    async def test_caller_physical_table_not_persisted(self):
        from dynastore.modules.storage.drivers import postgresql as pg

        captured = {}

        configs = MagicMock()

        async def _set_config(cls, cfg, **kwargs):
            captured["cfg"] = cfg
            return cfg

        configs.set_config = AsyncMock(side_effect=_set_config)

        def _get_protocol(proto):
            return configs

        with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
            await pg._pg_driver_init_collection(
                conn=MagicMock(),
                schema="s_abc12345",
                catalog_id="cat1",
                collection_id="col1",
                layer_config={"physical_table": VALID},
            )

        # Either nothing was persisted (physical_table was the only field and it
        # was stripped) or, if persisted, physical_table must not carry the
        # caller value.
        if "cfg" in captured:
            assert getattr(captured["cfg"], "physical_table", None) != VALID
