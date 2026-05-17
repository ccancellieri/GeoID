"""Cycle E.2 / F.0d — pin field-level invariants on the privacy configs.

Covers:
- ``CollectionPrivacy.is_private`` is ``Immutable[bool]`` —
  ``enforce_config_immutability`` rejects flips **once the collection
  has been materialized** (at least one item exists).  Empty
  collections may still flip — see #738 (provisioning-gated
  immutability).
- ``CatalogPrivacy.collection_defaults.is_private`` defaults to
  ``False`` and accepts only bool values.
- The privacy configs participate in PluginConfig discovery
  (``list_registered_configs``) under the right address keys.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from dynastore.modules.catalog.catalog_config import (
    CatalogPrivacy,
    CollectionPrivacy,
    CollectionPrivacyDefaults,
)
from dynastore.modules.db_config.platform_config_service import ImmutableConfigError, enforce_config_immutability

# Patch target: the module-level ``is_materialized`` consumed by
# ``enforce_config_immutability``.  Tests assert the *gated* enforcement —
# materialized=True → Immutable rejection fires; materialized=False → no-op.
_MATERIALIZED = "dynastore.modules.db_config.platform_config_service.is_materialized"


# ---------------------------------------------------------------------------
# CollectionPrivacy.is_private — Immutable[bool]
# ---------------------------------------------------------------------------


def test_is_private_default_is_false():
    cfg = CollectionPrivacy()
    assert cfg.is_private is False


def test_is_private_can_be_set_true_at_construction():
    cfg = CollectionPrivacy(is_private=True)
    assert cfg.is_private is True


async def test_is_private_immutable_rejects_flip_to_true_when_materialized():
    """Privacy flip on a collection that already has items requires
    moving docs across indexes — schema-level operation, not a runtime
    PATCH.  The Immutable annotation makes ``enforce_config_immutability``
    refuse the change once the resource is materialized."""
    current = CollectionPrivacy(is_private=False)
    new = CollectionPrivacy(is_private=True)
    with patch(_MATERIALIZED, AsyncMock(return_value=True)):
        with pytest.raises(ImmutableConfigError, match=r"is_private.*Immutable"):
            await enforce_config_immutability(
                current, new,
                catalog_id="c", collection_id="x", conn=object(),
            )


async def test_is_private_immutable_rejects_flip_to_false_when_materialized():
    current = CollectionPrivacy(is_private=True)
    new = CollectionPrivacy(is_private=False)
    with patch(_MATERIALIZED, AsyncMock(return_value=True)):
        with pytest.raises(ImmutableConfigError, match=r"is_private.*Immutable"):
            await enforce_config_immutability(
                current, new,
                catalog_id="c", collection_id="x", conn=object(),
            )


async def test_is_private_flip_on_empty_collection_does_not_raise():
    """#738 — provisioning-gated immutability.  On an empty collection
    (no items yet) the privacy flag may still be flipped; the
    Immutable enforcement engages only after the first item lands."""
    current = CollectionPrivacy(is_private=False)
    new = CollectionPrivacy(is_private=True)
    with patch(_MATERIALIZED, AsyncMock(return_value=False)):
        await enforce_config_immutability(
            current, new,
            catalog_id="c", collection_id="x", conn=object(),
        )  # No exception.


async def test_is_private_unchanged_does_not_raise():
    """Re-applying the same config without changing ``is_private``
    is a no-op even on a materialized collection."""
    current = CollectionPrivacy(is_private=True)
    new = CollectionPrivacy(is_private=True)
    with patch(_MATERIALIZED, AsyncMock(return_value=True)):
        await enforce_config_immutability(
            current, new,
            catalog_id="c", collection_id="x", conn=object(),
        )  # No exception.


def test_collection_privacy_address_and_visibility():
    """Pin the address tuple — surfaces the config under
    ``platform.catalog.collection.privacy`` in the configs API tree."""
    assert CollectionPrivacy._address == (
        "platform", "catalog", "collection", "privacy",
    )
    assert CollectionPrivacy._visibility == "collection"


# ---------------------------------------------------------------------------
# CatalogPrivacy.collection_defaults — CollectionPrivacyDefaults
# ---------------------------------------------------------------------------


def test_collection_defaults_default_is_public():
    cfg = CatalogPrivacy()
    assert cfg.collection_defaults.is_private is False


def test_collection_defaults_accepts_private():
    cfg = CatalogPrivacy(
        collection_defaults=CollectionPrivacyDefaults(is_private=True),
    )
    assert cfg.collection_defaults.is_private is True


def test_collection_defaults_accepts_dict_form():
    """Pydantic should construct the nested model from a dict payload —
    the wire shape operators PATCH."""
    cfg = CatalogPrivacy.model_validate(
        {"collection_defaults": {"is_private": True}},
    )
    assert cfg.collection_defaults.is_private is True


def test_collection_defaults_rejects_non_bool():
    """Pydantic bool validation should reject non-bool values for
    ``is_private`` — bool is the canonical leaf type."""
    with pytest.raises(ValidationError):
        CatalogPrivacy.model_validate(
            {"collection_defaults": {"is_private": "hidden"}},
        )


def test_catalog_privacy_address_and_visibility():
    """Pin the address tuple — surfaces the config under
    ``platform.catalog.privacy`` in the configs API tree."""
    assert CatalogPrivacy._address == ("platform", "catalog", "privacy")
    assert CatalogPrivacy._visibility == "catalog"
