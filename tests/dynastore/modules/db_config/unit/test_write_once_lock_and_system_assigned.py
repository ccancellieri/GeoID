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

"""Tests for two config-write hardening behaviours (GeoID #1135):

- ``enforce_config_immutability`` locks a non-None ``WriteOnce`` value
  regardless of materialization (it previously short-circuited at the
  ``is_materialized`` gate, leaving a pre-first-row PATCH window open).
- ``restore_system_assigned_fields`` discards any caller-supplied value
  for a config's ``_system_assigned_fields`` on the external write path,
  restoring the current persisted value (or unsetting when absent).
"""

from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.db_config.exceptions import ImmutableConfigError
from dynastore.modules.db_config.platform_config_service import (
    enforce_config_immutability,
    restore_system_assigned_fields,
)
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

_IS_MAT = "dynastore.modules.db_config.platform_config_service.is_materialized"


class TestWriteOnceLockedRegardlessOfMaterialization:
    @pytest.mark.asyncio
    async def test_write_once_change_blocked_when_not_materialized(self):
        # engine_ref is WriteOnce; physical_table is system-assigned/WriteOnce.
        current = ItemsPostgresqlDriverConfig.model_construct(engine_ref="eng_a")
        new = ItemsPostgresqlDriverConfig.model_construct(engine_ref="eng_b")
        with patch(_IS_MAT, AsyncMock(return_value=False)):
            with pytest.raises(ImmutableConfigError) as exc:
                await enforce_config_immutability(
                    current, new, catalog_id="c", collection_id="col"
                )
        assert "WriteOnce" in str(exc.value)

    @pytest.mark.asyncio
    async def test_write_once_first_set_allowed(self):
        current = ItemsPostgresqlDriverConfig.model_construct(engine_ref=None)
        new = ItemsPostgresqlDriverConfig.model_construct(engine_ref="eng_b")
        with patch(_IS_MAT, AsyncMock(return_value=False)):
            await enforce_config_immutability(
                current, new, catalog_id="c", collection_id="col"
            )  # no raise — None -> value is the one allowed WriteOnce transition

    @pytest.mark.asyncio
    async def test_immutable_still_editable_when_not_materialized(self):
        # Immutable fields remain editable on an empty resource (PR #738 design):
        # only WriteOnce gains the always-on lock.
        current = ItemsPostgresqlDriverConfig.model_construct(
            partitioning=ItemsPostgresqlDriverConfig().partitioning
        )
        new = current.model_copy(deep=True)
        new.partitioning.enabled = True
        with patch(_IS_MAT, AsyncMock(return_value=False)):
            await enforce_config_immutability(
                current, new, catalog_id="c", collection_id="col"
            )  # no raise


class TestRestoreSystemAssignedFields:
    def test_caller_value_discarded_when_no_current(self):
        new = ItemsPostgresqlDriverConfig.model_validate({"physical_table": "t_caller01"})
        restore_system_assigned_fields(ItemsPostgresqlDriverConfig, new, None)
        assert new.physical_table is None
        assert "physical_table" not in new.model_dump(exclude_unset=True)

    def test_caller_value_replaced_by_current(self):
        current = ItemsPostgresqlDriverConfig.model_construct(physical_table="t_system01")
        new = ItemsPostgresqlDriverConfig.model_validate({"physical_table": "t_caller01"})
        restore_system_assigned_fields(ItemsPostgresqlDriverConfig, new, current)
        assert new.physical_table == "t_system01"

    def test_noop_for_config_without_system_fields(self):
        # A class without _system_assigned_fields must be untouched.
        from dynastore.modules.storage.driver_config import ItemsElasticsearchDriverConfig

        cfg = ItemsElasticsearchDriverConfig()
        restore_system_assigned_fields(ItemsElasticsearchDriverConfig, cfg, None)
