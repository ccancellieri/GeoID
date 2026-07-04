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

"""Unit tests for the retired ES private cascade owner module.

EsItemsIndexOwner has been superseded by RoutingDrivenCascadeOwner which
delegates to ItemsElasticsearchPrivateDriver.drop_storage (which internally
calls _revoke_deny_policy after index deletion).

These tests verify:
- The retired private cascade_owners module's register_owners is now a no-op.
- ItemsElasticsearchPrivateDriver.drop_storage calls _revoke_deny_policy
  (the side-effect parity check that was previously on EsItemsIndexOwner).

Full routing-driven cascade owner tests are in
tests/dynastore/modules/storage/unit/test_routing_driven_cascade_owner.py.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry


class TestRetiredPrivateCascadeOwnerModule:
    """The retired module's register_owners is a no-op — no owners added."""

    def test_register_owners_does_not_add_any_owner(self) -> None:
        from dynastore.modules.storage.drivers.elasticsearch_private import cascade_owners

        reg = CascadeCleanupRegistry()
        cascade_owners.register_owners(reg)
        assert len(reg) == 0, (
            "Retired private cascade_owners.register_owners must not register "
            "any owners — superseded by RoutingDrivenCascadeOwner."
        )

    def test_register_owners_is_idempotent(self) -> None:
        """Calling register_owners twice must not raise."""
        from dynastore.modules.storage.drivers.elasticsearch_private import cascade_owners

        reg = CascadeCleanupRegistry()
        cascade_owners.register_owners(reg)
        cascade_owners.register_owners(reg)  # must not raise ValueError


class TestPrivateDriverDropStorageRevokesPolicy:
    """ItemsElasticsearchPrivateDriver.drop_storage calls _revoke_deny_policy.

    This is the parity assertion for the retired EsItemsIndexOwner.  The DENY
    revoke was previously performed in cleanup_one; it is now performed inside
    driver.drop_storage itself — the routing-driven owner triggers it
    indirectly by calling drop_storage.
    """

    @pytest.mark.asyncio
    async def test_drop_storage_calls_revoke_deny_policy(self) -> None:
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )

        driver = ItemsElasticsearchPrivateDriver()
        mock_es = MagicMock()
        mock_es.indices.delete = AsyncMock(return_value=None)

        with (
            patch.object(
                ItemsElasticsearchPrivateDriver,
                "_get_client",
                return_value=mock_es,
            ),
            patch.object(
                ItemsElasticsearchPrivateDriver,
                "_items_index_name",
                return_value="test-prefix-catalog-private-items",
            ),
            patch.object(
                ItemsElasticsearchPrivateDriver,
                "_revoke_deny_policy",
                new_callable=AsyncMock,
            ) as mock_revoke,
        ):
            await driver.drop_storage("catalog-1")

        mock_es.indices.delete.assert_awaited_once()
        mock_revoke.assert_awaited_once_with("catalog-1")

    @pytest.mark.asyncio
    async def test_drop_storage_soft_raises_not_supported(self) -> None:
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )
        from dynastore.modules.storage.errors import SoftDeleteNotSupportedError

        driver = ItemsElasticsearchPrivateDriver()
        with pytest.raises(SoftDeleteNotSupportedError):
            await driver.drop_storage("catalog-1", soft=True)
