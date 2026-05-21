"""Unit tests for CollectionWriteAudience PluginConfig."""
from dynastore.extensions.geoid.configs import CollectionWriteAudience


def test_default_disallows_anonymous_create():
    cfg = CollectionWriteAudience()
    assert cfg.allow_anonymous_create is False


def test_allow_anonymous_create_can_be_set_true():
    cfg = CollectionWriteAudience(allow_anonymous_create=True)
    assert cfg.allow_anonymous_create is True


def test_address_is_platform_catalog_collection_write_audience():
    assert CollectionWriteAudience._address == (
        "platform", "catalog", "collection", "write_audience",
    )


def test_freeze_at_is_collection_tier():
    assert CollectionWriteAudience._freeze_at == "collection"


def test_class_key_is_snake_case():
    assert CollectionWriteAudience.class_key() == "collection_write_audience"
