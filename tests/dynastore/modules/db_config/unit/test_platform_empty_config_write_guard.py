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

"""Unit tests for the empty-config write guard in PlatformConfigService (Refs #2524).

A PluginConfig constructed from the code default (no fields explicitly set)
serialises to {} under exclude_unset=True.  {} is falsy in the read-side
waterfall, so persisting it is a no-op that wastes a write transaction and
spuriously invalidates caches.  The guard in set_config and set_config_by_ref
skips the DB upsert for such configs.

Mirrors the catalog-tier guard tests from #2522
(tests/dynastore/modules/catalog/unit/test_empty_config_write_guard.py).
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_service():
    """Build a PlatformConfigService without triggering __init__ or DB connections."""
    from dynastore.modules.db_config.platform_config_service import PlatformConfigService

    svc = PlatformConfigService.__new__(PlatformConfigService)
    svc._engine = AsyncMock()
    svc.get_platform_config_internal_cached = MagicMock()
    return svc


# ---------------------------------------------------------------------------
# Serialisation invariant
# ---------------------------------------------------------------------------


def test_default_items_routing_config_serialises_to_empty_json():
    """ItemsRoutingConfig() carries no explicitly-set fields; exclude_unset=True
    must produce {}.  This is the precondition the platform guard relies on."""
    from dynastore.modules.db_config.platform_config_service import _serialize_config_for_db
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    config = ItemsRoutingConfig()
    assert not config.model_fields_set, "default config must have no explicit fields"
    assert _serialize_config_for_db(config) == "{}", (
        "_serialize_config_for_db with exclude_unset=True must return '{}' "
        "for a default-constructed config"
    )


def test_explicit_items_routing_config_serialises_non_empty():
    """A config built from an explicit payload has model_fields_set populated
    and must serialise to a non-empty JSON string."""
    from dynastore.modules.db_config.platform_config_service import _serialize_config_for_db
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    config = ItemsRoutingConfig.model_validate({"operations": {}})
    assert "operations" in config.model_fields_set
    serialized = _serialize_config_for_db(config)
    assert serialized != "{}", "explicit config must not serialise to '{}'"
    data = json.loads(serialized)
    assert "operations" in data


# ---------------------------------------------------------------------------
# set_config guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_config_skips_empty():
    """set_config must return without opening a transaction when the config
    has no explicitly-set fields (serialises to {})."""
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    svc = _make_service()
    empty = ItemsRoutingConfig()

    with patch(
        "dynastore.modules.db_config.platform_config_service.managed_transaction"
    ) as mock_txn:
        await svc.set_config(ItemsRoutingConfig, empty)

    mock_txn.assert_not_called()


@pytest.mark.asyncio
async def test_set_config_proceeds_with_explicit_fields():
    """set_config must NOT skip when at least one field is explicitly set.
    The guard fires before managed_transaction is entered, so verifying that
    managed_transaction IS called confirms the non-empty path is entered.
    The call raises (no real DB) which is expected."""
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    svc = _make_service()
    explicit = ItemsRoutingConfig.model_validate({"operations": {}})
    assert "operations" in explicit.model_fields_set

    mock_ctx = MagicMock()
    mock_ctx.__aenter__ = AsyncMock(side_effect=RuntimeError("expected — DB not mocked"))
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    with patch(
        "dynastore.modules.db_config.platform_config_service.managed_transaction",
        return_value=mock_ctx,
    ) as mock_txn:
        with pytest.raises(RuntimeError, match="expected — DB not mocked"):
            await svc.set_config(ItemsRoutingConfig, explicit)

    # managed_transaction was called — the guard did NOT short-circuit.
    mock_txn.assert_called_once()


# ---------------------------------------------------------------------------
# set_config_by_ref guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_config_by_ref_skips_empty():
    """set_config_by_ref must return without opening a transaction when the
    config has no explicitly-set fields."""
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    svc = _make_service()
    empty = ItemsRoutingConfig()

    with patch(
        "dynastore.modules.db_config.platform_config_service.managed_transaction"
    ) as mock_txn:
        await svc.set_config_by_ref("my-ref", empty)

    mock_txn.assert_not_called()


@pytest.mark.asyncio
async def test_set_config_by_ref_proceeds_with_explicit_fields():
    """set_config_by_ref must NOT skip when at least one field is explicitly set."""
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    svc = _make_service()
    explicit = ItemsRoutingConfig.model_validate({"operations": {}})
    assert "operations" in explicit.model_fields_set

    mock_ctx = MagicMock()
    mock_ctx.__aenter__ = AsyncMock(side_effect=RuntimeError("expected — DB not mocked"))
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    with patch(
        "dynastore.modules.db_config.platform_config_service.managed_transaction",
        return_value=mock_ctx,
    ) as mock_txn:
        with pytest.raises(RuntimeError, match="expected — DB not mocked"):
            await svc.set_config_by_ref("my-ref", explicit)

    mock_txn.assert_called_once()
