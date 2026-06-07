"""Unit tests for the write-time STAC validation gate (issue #1884).

Verifies two behaviours:
  (a) Default: ``validate_on_write=False`` — the flag is off by default in
      StacPluginConfig, so no remote network calls are made during ingest.
  (b) Opt-in: ``validate_on_write=True`` — the flag can be set; the validator
      remains lenient (returns warnings, never raises for extension failures).
  (c) STACItem Pydantic parse no longer calls stac_item.validate() (the
      blocking network call removed from the model validator).

These are pure unit tests: no live DB, no network, no pystac remote fetch.
"""

from __future__ import annotations

from unittest.mock import patch

from dynastore.modules.stac.stac_config import StacPluginConfig


# ---------------------------------------------------------------------------
# Tests: StacPluginConfig.validate_on_write field
# ---------------------------------------------------------------------------


def test_validate_on_write_defaults_to_false():
    """Default StacPluginConfig has validate_on_write=False."""
    cfg = StacPluginConfig()
    assert cfg.validate_on_write is False


def test_validate_on_write_can_be_set_to_true():
    """validate_on_write can be enabled explicitly."""
    cfg = StacPluginConfig(validate_on_write=True)
    assert cfg.validate_on_write is True


def test_validate_on_write_field_is_bool():
    """validate_on_write is a plain bool, not nullable."""
    cfg = StacPluginConfig(validate_on_write=False)
    assert isinstance(cfg.validate_on_write, bool)


# ---------------------------------------------------------------------------
# Tests: validate_stac_item remains lenient when enabled
# ---------------------------------------------------------------------------


def test_validate_stac_item_lenient_on_extension_failure():
    """validate_stac_item returns warnings, never raises, for extension failures."""
    import pystac

    from dynastore.extensions.stac.stac_validator import validate_stac_item

    item_dict = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "test-item",
        "geometry": None,
        "properties": {"datetime": "2026-01-01T00:00:00Z"},
        "links": [],
        "assets": {},
        "stac_extensions": ["https://bogus.invalid/schema/does-not-exist.json"],
    }

    def _patched_validate(self):
        raise Exception("connection refused: bogus.invalid")

    with patch.object(pystac.Item, "validate", _patched_validate):
        warnings = validate_stac_item(item_dict, strict=False)

    # Must return a non-empty warning list, not raise
    assert isinstance(warnings, list)
    assert any("pystac validation" in w for w in warnings)


def test_validate_stac_item_does_not_raise_by_default():
    """strict=False (the service default) must never propagate exceptions."""
    import pystac

    from dynastore.extensions.stac.stac_validator import validate_stac_item

    def _boom(self):
        raise Exception("network unreachable")

    with patch.object(pystac.Item, "validate", _boom):
        result = validate_stac_item(
            {
                "type": "Feature",
                "stac_version": "1.1.0",
                "id": "item-x",
                "geometry": None,
                "properties": {"datetime": "2026-01-01T00:00:00Z"},
                "links": [],
                "assets": {},
                "stac_extensions": ["https://x.invalid/schema.json"],
            },
            strict=False,
        )
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Tests: STACItem Pydantic parse does NOT call stac_item.validate()
# ---------------------------------------------------------------------------


def test_stac_item_parse_does_not_call_pystac_validate():
    """Parsing an STACItem must NOT invoke stac_item.validate() (blocking network call)."""
    import pystac

    from dynastore.extensions.stac.stac_models import STACItem

    validate_calls: list[str] = []

    original_validate = pystac.Item.validate

    def _spy_validate(self):
        validate_calls.append(self.id)
        return original_validate(self)

    with patch.object(pystac.Item, "validate", _spy_validate):
        STACItem.model_validate(
            {
                "type": "Feature",
                "stac_version": "1.1.0",
                "id": "no-validate-item",
                "geometry": None,
                "bbox": [-180.0, -90.0, 180.0, 90.0],
                "properties": {"datetime": "2026-01-01T00:00:00Z"},
                "links": [],
                "assets": {},
            }
        )

    assert validate_calls == [], (
        "pystac.Item.validate() must not be called during Pydantic model parse; "
        f"called for items: {validate_calls}"
    )


def test_stac_item_collection_parse_does_not_call_pystac_validate():
    """Parsing an STACItemCollection (batch) must not call pystac.validate()."""
    import pystac

    from dynastore.extensions.stac.stac_models import STACItemCollection

    validate_calls: list[str] = []

    original_validate = pystac.Item.validate

    def _spy_validate(self):
        validate_calls.append(self.id)
        return original_validate(self)

    with patch.object(pystac.Item, "validate", _spy_validate):
        STACItemCollection.model_validate(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "stac_version": "1.1.0",
                        "id": f"item-{i}",
                        "geometry": None,
                        "bbox": [-180.0, -90.0, 180.0, 90.0],
                        "properties": {"datetime": "2026-01-01T00:00:00Z"},
                        "links": [],
                        "assets": {},
                    }
                    for i in range(5)
                ],
            }
        )

    assert validate_calls == [], (
        "pystac.Item.validate() must not be called during batch parse; "
        f"called for items: {validate_calls}"
    )
