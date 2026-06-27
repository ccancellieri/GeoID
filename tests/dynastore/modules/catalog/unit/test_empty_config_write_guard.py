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

"""Unit tests for the empty-config write guard introduced in #2435.

A PluginConfig constructed from the code default (no fields explicitly set)
serialises to {} under exclude_unset=True.  {} is falsy in the read-side
waterfall, so persisting it is a no-op that wastes a write transaction and
spuriously invalidates caches.  The guard in ConfigService._set_*_config
methods skips the DB upsert for such configs.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_service() -> "ConfigService":
    """Build a ConfigService without triggering __init__ or DB connections."""
    from dynastore.modules.catalog.config_service import ConfigService

    svc = ConfigService.__new__(ConfigService)
    svc.engine = AsyncMock()
    svc._catalogs_service = None
    svc._platform_config_service = None
    return svc


# ---------------------------------------------------------------------------
# Serialisation invariant
# ---------------------------------------------------------------------------


def test_default_items_routing_config_serialises_to_empty_json():
    """ItemsRoutingConfig() carries no explicitly-set fields; exclude_unset=True
    must produce {}.  This is the precondition the guard relies on."""
    from dynastore.modules.catalog.config_service import _serialize_config_for_db
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
    from dynastore.modules.catalog.config_service import _serialize_config_for_db
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    config = ItemsRoutingConfig.model_validate({"operations": {}})
    assert "operations" in config.model_fields_set
    serialized = _serialize_config_for_db(config)
    assert serialized != "{}", "explicit config must not serialise to '{}'"
    data = json.loads(serialized)
    assert "operations" in data


# ---------------------------------------------------------------------------
# _set_collection_config guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_collection_config_skips_empty():
    """_set_collection_config must return without touching the DB when the
    config carries no explicitly-set fields (serialises to {})."""
    from dynastore.modules.catalog.config_service import ConfigService
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    svc = _make_service()
    empty = ItemsRoutingConfig()

    mock_mgr = MagicMock()
    with patch.object(svc, "_get_catalog_manager", return_value=mock_mgr) as mock_get_mgr:
        await svc._set_collection_config(
            "cat1", "col1", ItemsRoutingConfig, empty,
        )

    # The guard fires before _get_catalog_manager() is called, so no DB
    # interaction should occur.
    mock_get_mgr.assert_not_called()


@pytest.mark.asyncio
async def test_set_collection_config_proceeds_with_explicit_fields():
    """_set_collection_config must NOT skip when at least one field is
    explicitly set.  The guard fires before _get_catalog_manager() is called,
    so verifying that _get_catalog_manager IS called is sufficient proof the
    non-empty path is entered.  The call raises (no real DB) which is expected.
    """
    from dynastore.modules.catalog.config_service import ConfigService
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    svc = _make_service()
    explicit = ItemsRoutingConfig.model_validate({"operations": {}})
    assert "operations" in explicit.model_fields_set

    mock_mgr = MagicMock()
    # Provide enough so the code reaches _get_catalog_manager before failing
    # on a real DB call (which we intentionally don't mock deeply here).
    mock_mgr.collections.resolve_collection_ids = AsyncMock(
        side_effect=RuntimeError("expected — DB not mocked")
    )

    with patch.object(svc, "_get_catalog_manager", return_value=mock_mgr) as mock_get_mgr:
        with pytest.raises(RuntimeError, match="expected — DB not mocked"):
            await svc._set_collection_config(
                "cat1", "col1", ItemsRoutingConfig, explicit,
            )

    # _get_catalog_manager was called — the guard did NOT short-circuit.
    mock_get_mgr.assert_called_once()


# ---------------------------------------------------------------------------
# _set_catalog_config guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_catalog_config_skips_empty():
    """_set_catalog_config must return without opening a transaction when the
    config has no explicitly-set fields."""
    from dynastore.modules.catalog.config_service import ConfigService
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    svc = _make_service()
    empty = ItemsRoutingConfig()

    with patch(
        "dynastore.modules.catalog.config_service.managed_transaction"
    ) as mock_txn:
        await svc._set_catalog_config("cat1", ItemsRoutingConfig, empty)

    mock_txn.assert_not_called()


# ---------------------------------------------------------------------------
# set_config public surface
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_config_collection_scope_skips_empty():
    """The public set_config API must honour the guard at collection scope."""
    from dynastore.modules.catalog.config_service import ConfigService
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    svc = _make_service()
    empty = ItemsRoutingConfig()

    mock_mgr = MagicMock()
    with patch.object(svc, "_get_catalog_manager", return_value=mock_mgr) as mock_get_mgr:
        result = await svc.set_config(
            ItemsRoutingConfig, empty,
            catalog_id="cat1", collection_id="col1",
        )

    # No DB interaction, returned config is the same object.
    mock_get_mgr.assert_not_called()
    assert result is empty
