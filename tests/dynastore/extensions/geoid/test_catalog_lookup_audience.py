"""Unit tests for CatalogLookupAudience PluginConfig."""
from dynastore.extensions.geoid.configs import CatalogLookupAudience


def test_default_is_private_audience():
    """Default field value is is_public=False (auth-required, status quo)."""
    cfg = CatalogLookupAudience()
    assert cfg.is_public is False


def test_is_public_can_be_set_true():
    cfg = CatalogLookupAudience(is_public=True)
    assert cfg.is_public is True


def test_address_is_platform_catalog_lookup_audience():
    """Address slot must be ('platform', 'catalog', 'lookup_audience')."""
    assert CatalogLookupAudience._address == ("platform", "catalog", "lookup_audience")


def test_freeze_at_is_catalog_tier():
    """The PluginConfig's immutability gate fires at the catalog tier."""
    assert CatalogLookupAudience._freeze_at == "catalog"


def test_class_key_is_snake_case():
    """plugin_id (== cls.class_key()) is the snake_case class name used in REST URLs."""
    assert CatalogLookupAudience.class_key() == "catalog_lookup_audience"
