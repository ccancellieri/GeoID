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

"""Tests for dynastore.models.model_docs — shared meta/doc extraction helpers."""

from typing import ClassVar, Optional, Tuple

import pytest
from pydantic import Field

from dynastore.models.model_docs import (
    build_meta_block,
    extract_field_docs,
    extract_mutability,
    strip_meta_envelopes,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class _NoDescModel:
    """Model with fields that carry NO description in their schema."""

    @classmethod
    def model_json_schema(cls):
        return {
            "properties": {
                "alpha": {"type": "string"},
                "beta": {"type": "integer"},
            }
        }


class _WithDescModel:
    """Model with fields that carry descriptions in their schema."""

    @classmethod
    def model_json_schema(cls):
        return {
            "properties": {
                "brand_name": {"description": "Display name.", "type": "string"},
                "version":    {"description": "Schema version.", "type": "integer"},
                "no_desc":    {"type": "boolean"},
            }
        }


class _BadSchemaModel:
    """Model whose model_json_schema() raises."""

    @classmethod
    def model_json_schema(cls):
        raise RuntimeError("schema error")


# ---------------------------------------------------------------------------
# extract_field_docs
# ---------------------------------------------------------------------------


def test_extract_field_docs_returns_descriptions():
    docs = extract_field_docs(_WithDescModel)
    assert docs == {"brand_name": "Display name.", "version": "Schema version."}


def test_extract_field_docs_omits_fields_without_description():
    docs = extract_field_docs(_WithDescModel)
    assert "no_desc" not in docs


def test_extract_field_docs_empty_when_no_descriptions():
    docs = extract_field_docs(_NoDescModel)
    assert docs == {}


def test_extract_field_docs_returns_empty_dict_on_schema_error():
    docs = extract_field_docs(_BadSchemaModel)
    assert docs == {}


def test_extract_field_docs_is_cached():
    """extract_field_docs must be lru_cache'd — same object returned on repeat call."""
    r1 = extract_field_docs(_WithDescModel)
    r2 = extract_field_docs(_WithDescModel)
    assert r1 is r2


# ---------------------------------------------------------------------------
# extract_mutability — uses a real PluginConfig subclass with markers
# ---------------------------------------------------------------------------


def test_extract_mutability_with_marker_fields():
    from dynastore.models.mutability import Immutable, Mutable, WriteOnce
    from dynastore.models.plugin_config import PluginConfig

    class _MutFixture(PluginConfig):
        _address: ClassVar[Tuple[str, ...]] = ("platform", "_mut_fixture")
        brand_name: Mutable[str] = Field("X", description="Brand label.")
        engine_ref: WriteOnce[Optional[str]] = Field(None, description="Engine binding.")
        physical_table: Immutable[str] = Field("tbl", description="Backing table.")

    # Ensure cache doesn't bleed from prior calls on same class
    extract_mutability.cache_clear()
    result = extract_mutability(_MutFixture)
    assert result == {
        "brand_name":     "mutable",
        "engine_ref":     "write_once",
        "physical_table": "immutable",
    }


def test_extract_mutability_returns_empty_dict_on_fallback_failure():
    """If mutability introspection raises for an unknown class, return {}."""
    class _NoMutability:
        pass

    extract_mutability.cache_clear()
    result = extract_mutability(_NoMutability)
    assert isinstance(result, dict)


def test_extract_mutability_is_cached():
    from dynastore.models.mutability import Mutable
    from dynastore.models.plugin_config import PluginConfig

    class _CacheFixture(PluginConfig):
        _address: ClassVar[Tuple[str, ...]] = ("platform", "_cache_fixture")
        x: Mutable[str] = Field("y")

    extract_mutability.cache_clear()
    r1 = extract_mutability(_CacheFixture)
    r2 = extract_mutability(_CacheFixture)
    assert r1 is r2


# ---------------------------------------------------------------------------
# build_meta_block
# ---------------------------------------------------------------------------


def test_build_meta_block_mode_none_returns_empty():
    block = build_meta_block(_WithDescModel, mode="none")
    assert block == {}


def test_build_meta_block_mode_none_ignores_tier_source():
    block = build_meta_block(_WithDescModel, tier="catalog", source="platform", mode="none")
    assert block == {}


def test_build_meta_block_mode_field_has_docs():
    extract_field_docs.cache_clear()
    block = build_meta_block(_WithDescModel, tier="catalog", source="default", mode="field")
    assert block["tier"] == "catalog"
    assert block["source"] == "default"
    assert block["docs"] == {"brand_name": "Display name.", "version": "Schema version."}


def test_build_meta_block_mode_field_omits_tier_when_none():
    extract_field_docs.cache_clear()
    block = build_meta_block(_WithDescModel, mode="field")
    assert "tier" not in block
    assert "source" not in block
    assert "docs" in block


def test_build_meta_block_mode_field_omits_mutability_when_empty():
    """When the model has no mutability markers, 'mutability' key is absent."""
    extract_field_docs.cache_clear()
    extract_mutability.cache_clear()
    block = build_meta_block(_WithDescModel, mode="field")
    assert "mutability" not in block


def test_build_meta_block_mode_field_includes_mutability_when_non_empty():
    from dynastore.models.mutability import Mutable
    from dynastore.models.plugin_config import PluginConfig

    class _MetaMutFixture(PluginConfig):
        _address: ClassVar[Tuple[str, ...]] = ("platform", "_meta_mut_fixture")
        x: Mutable[str] = Field("y", description="Some field.")

    extract_field_docs.cache_clear()
    extract_mutability.cache_clear()
    block = build_meta_block(_MetaMutFixture, tier="platform", source="default", mode="field")
    assert "mutability" in block
    assert block["mutability"]["x"] == "mutable"


def test_build_meta_block_mode_schema_has_json_schema():
    block = build_meta_block(_WithDescModel, tier="platform", source="default", mode="schema")
    assert block["tier"] == "platform"
    assert block["source"] == "default"
    schema = block["json_schema"]
    assert "properties" in schema
    assert "docs" not in block
    assert "mutability" not in block


def test_build_meta_block_mode_schema_no_docs_or_mutability():
    block = build_meta_block(_WithDescModel, mode="schema")
    assert "docs" not in block
    assert "mutability" not in block


def test_build_meta_block_tier_omitted_when_none():
    block = build_meta_block(_WithDescModel, tier=None, source="default", mode="field")
    assert "tier" not in block


def test_build_meta_block_source_omitted_when_none():
    block = build_meta_block(_WithDescModel, tier="catalog", source=None, mode="field")
    assert "source" not in block


# ---------------------------------------------------------------------------
# strip_meta_envelopes
# ---------------------------------------------------------------------------


def test_strip_meta_envelopes_removes_meta_and_links():
    body = {"_meta": {"tier": "platform"}, "_links": [], "brand_name": "X"}
    result = strip_meta_envelopes(body)
    assert result == {"brand_name": "X"}


def test_strip_meta_envelopes_leaves_body_unchanged_if_no_envelope_keys():
    body = {"brand_name": "X", "enabled": True}
    result = strip_meta_envelopes(body)
    assert result == body


def test_strip_meta_envelopes_passes_through_non_dict():
    for value in (None, "string", 42, [1, 2, 3]):
        assert strip_meta_envelopes(value) is value


def test_strip_meta_envelopes_only_removes_underscore_keys():
    body = {"_meta": {}, "meta": "keep_me", "_links": [], "data": 1}
    result = strip_meta_envelopes(body)
    assert "meta" in result
    assert "_meta" not in result
    assert "_links" not in result
