"""Cycle E.2 — pin field-level invariants on the privacy configs.

Covers:
- ``CollectionPluginConfig.is_private`` is ``Immutable[bool]`` —
  ``enforce_config_immutability`` rejects flips at apply time.
- ``CatalogPolicyConfig.default_collection_privacy`` defaults to
  ``"public"`` and accepts only the documented Literal values.
- The privacy configs participate in PluginConfig discovery
  (``list_registered_configs``) under the right address keys.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from dynastore.modules.catalog.catalog_config import (
    CatalogPolicyConfig,
    CollectionPluginConfig,
)
from dynastore.modules.db_config.platform_config_service import (
    ImmutableConfigError,
    enforce_config_immutability,
)


# ---------------------------------------------------------------------------
# CollectionPluginConfig.is_private — Immutable[bool]
# ---------------------------------------------------------------------------


def test_is_private_default_is_false():
    cfg = CollectionPluginConfig()
    assert cfg.is_private is False


def test_is_private_can_be_set_true_at_construction():
    cfg = CollectionPluginConfig(is_private=True)
    assert cfg.is_private is True


def test_is_private_immutable_rejects_flip_to_true():
    """Privacy flip on an existing collection requires moving its
    docs across indexes — schema-level operation, not a runtime PATCH.
    The Immutable annotation makes ``enforce_config_immutability``
    refuse the change."""
    current = CollectionPluginConfig(is_private=False)
    new = CollectionPluginConfig(is_private=True)
    with pytest.raises(ImmutableConfigError, match=r"is_private.*Immutable"):
        enforce_config_immutability(current, new)


def test_is_private_immutable_rejects_flip_to_false():
    current = CollectionPluginConfig(is_private=True)
    new = CollectionPluginConfig(is_private=False)
    with pytest.raises(ImmutableConfigError, match=r"is_private.*Immutable"):
        enforce_config_immutability(current, new)


def test_is_private_unchanged_does_not_raise():
    """Re-applying the same config without changing ``is_private``
    is a no-op for the immutability check."""
    current = CollectionPluginConfig(is_private=True)
    new = CollectionPluginConfig(is_private=True)
    enforce_config_immutability(current, new)  # No exception.


# ---------------------------------------------------------------------------
# CatalogPolicyConfig.default_collection_privacy — Literal["public","private"]
# ---------------------------------------------------------------------------


def test_default_collection_privacy_default_is_public():
    cfg = CatalogPolicyConfig()
    assert cfg.default_collection_privacy == "public"


def test_default_collection_privacy_accepts_private():
    cfg = CatalogPolicyConfig(default_collection_privacy="private")
    assert cfg.default_collection_privacy == "private"


def test_default_collection_privacy_rejects_unknown_values():
    """Pydantic Literal validation should reject anything outside
    the documented enum."""
    with pytest.raises(ValidationError):
        CatalogPolicyConfig(default_collection_privacy="hidden")


def test_catalog_policy_address_and_visibility():
    """Pin the address tuple — surfaces the config under
    ``catalog.policy`` in the configs API tree."""
    assert CatalogPolicyConfig._address == ("platform", "catalog", "policy")
    assert CatalogPolicyConfig._visibility == "catalog"
