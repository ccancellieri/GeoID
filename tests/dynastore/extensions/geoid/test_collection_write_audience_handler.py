"""Unit tests for CollectionWriteAudienceHandler."""
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_handler_type_is_collection_write_anonymous_allowed():
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    assert handler.type == "collection_write_anonymous_allowed"


@pytest.mark.asyncio
async def test_handler_returns_true_when_collection_allows_via_extras(monkeypatch):
    from dynastore.extensions.geoid.configs import CollectionWriteAudience
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(
        return_value=CollectionWriteAudience(allow_anonymous_create=True),
    )
    monkeypatch.setattr(
        "dynastore.extensions.geoid.conditions.get_protocol",
        lambda _proto: fake_configs,
    )

    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is True
    fake_configs.get_config.assert_awaited_once_with(
        CollectionWriteAudience, catalog_id="cat", collection_id="intake_col",
    )


@pytest.mark.asyncio
async def test_handler_returns_true_when_collection_id_parsed_from_path(monkeypatch):
    """When extras has no collection_id, the handler parses ctx.path."""
    from dynastore.extensions.geoid.configs import CollectionWriteAudience
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(
        return_value=CollectionWriteAudience(allow_anonymous_create=True),
    )
    monkeypatch.setattr(
        "dynastore.extensions.geoid.conditions.get_protocol",
        lambda _proto: fake_configs,
    )

    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {}
    ctx.path = "/stac/catalogs/cat/collections/intake_col/items"
    assert await handler.evaluate({}, ctx) is True
    fake_configs.get_config.assert_awaited_once_with(
        CollectionWriteAudience, catalog_id="cat", collection_id="intake_col",
    )


@pytest.mark.asyncio
async def test_handler_returns_false_when_collection_does_not_allow(monkeypatch):
    from dynastore.extensions.geoid.configs import CollectionWriteAudience
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(
        return_value=CollectionWriteAudience(allow_anonymous_create=False),
    )
    monkeypatch.setattr(
        "dynastore.extensions.geoid.conditions.get_protocol",
        lambda _proto: fake_configs,
    )

    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_returns_false_when_catalog_id_missing(monkeypatch):
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    monkeypatch.setattr(
        "dynastore.extensions.geoid.conditions.get_protocol",
        lambda _proto: MagicMock(),
    )
    ctx = MagicMock()
    ctx.catalog_id = None
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_returns_false_when_collection_id_missing(monkeypatch):
    """No collection_id in extras AND path doesn't match → fail closed."""
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    monkeypatch.setattr(
        "dynastore.extensions.geoid.conditions.get_protocol",
        lambda _proto: MagicMock(),
    )
    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {}
    ctx.path = "/some/unrelated/path"
    assert await handler.evaluate({}, ctx) is False


@pytest.mark.asyncio
async def test_handler_fails_closed_on_get_config_raises(monkeypatch):
    from dynastore.extensions.geoid.conditions import CollectionWriteAudienceHandler

    handler = CollectionWriteAudienceHandler()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "dynastore.extensions.geoid.conditions.get_protocol",
        lambda _proto: fake_configs,
    )
    ctx = MagicMock()
    ctx.catalog_id = "cat"
    ctx.extras = {"collection_id": "intake_col"}
    assert await handler.evaluate({}, ctx) is False
