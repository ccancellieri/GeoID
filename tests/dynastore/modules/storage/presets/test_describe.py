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

"""Unit tests for presets/describe.py — pure, DB-free."""
from __future__ import annotations

from typing import ClassVar, Tuple
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from dynastore.modules.storage.presets.bundle_preset import BundlePreset
from dynastore.modules.storage.presets.examples import PresetExample
from dynastore.modules.storage.presets.protocol import PresetBundle, PresetBundleEntry, PresetTier
from dynastore.modules.storage.presets.describe import (
    build_preview_bundle,
    describe_preset,
    descriptor_to_html,
    descriptor_to_markdown,
)
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    FailurePolicy,
    Operation,
    OperationDriverEntry,
)


# ---------------------------------------------------------------------------
# Minimal fixture preset with params + examples
# ---------------------------------------------------------------------------

class _FixtureParams(BaseModel):
    """Params for the fixture preset."""

    driver_ref: str = "catalog_postgresql_driver"
    label: str = "default"


class _FixturePreset(BundlePreset):
    """Minimal BundlePreset for describe tests."""

    name = "_fixture_describe_test"
    description = "A fixture preset used only in describe unit tests."
    keywords: ClassVar[Tuple[str, ...]] = ("test", "fixture")
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = False
    params_model = _FixtureParams

    examples: ClassVar[Tuple[PresetExample, ...]] = (
        PresetExample(
            name="basic",
            summary="Basic catalog with PostgreSQL driver",
            params={"driver_ref": "catalog_postgresql_driver", "label": "basic"},
        ),
        PresetExample(
            name="invalid-example",
            summary="Example with invalid params (should not raise)",
            params={"driver_ref": 12345, "label": None},  # type mismatch, coerced or error
        ),
    )

    def _build_bundle(self, params: BaseModel, scope_kwargs: dict) -> PresetBundle:
        if not isinstance(params, _FixtureParams):
            params = _FixtureParams.model_validate(
                params.model_dump() if hasattr(params, "model_dump") else {}
            )
        return PresetBundle(
            entries=(
                PresetBundleEntry(
                    slot="catalog_routing",
                    config_cls=CatalogRoutingConfig,
                    instance=CatalogRoutingConfig(
                        operations={
                            Operation.WRITE: [
                                OperationDriverEntry(
                                    driver_ref=params.driver_ref,
                                    on_failure=FailurePolicy.FATAL,
                                )
                            ],
                        }
                    ),
                    rollback_priority=30,
                ),
            )
        )

    def build(self, catalog_id: str = "", **_scope: str) -> PresetBundle:
        return self._build_bundle(_FixtureParams(), {})


_PRESET = _FixturePreset()


# ---------------------------------------------------------------------------
# Tests: PresetExample model
# ---------------------------------------------------------------------------

class TestPresetExample:
    def test_frozen(self) -> None:
        ex = PresetExample(name="x", summary="y", params={"k": "v"})
        with pytest.raises((TypeError, AttributeError)):
            ex.name = "z"  # type: ignore[misc]

    def test_fields(self) -> None:
        ex = PresetExample(name="my-id", summary="one-liner", params={"a": 1})
        assert ex.name == "my-id"
        assert ex.summary == "one-liner"
        assert ex.params == {"a": 1}


# ---------------------------------------------------------------------------
# Tests: build_preview_bundle
# ---------------------------------------------------------------------------

class TestBuildPreviewBundle:
    def test_returns_bundle_for_valid_params(self) -> None:
        params = _FixtureParams(driver_ref="catalog_postgresql_driver")
        bundle = build_preview_bundle(_PRESET, params)
        assert bundle is not None
        entries = list(bundle.iter_apply())
        assert len(entries) == 1
        assert entries[0].slot == "catalog_routing"

    def test_returns_none_on_build_error(self, monkeypatch) -> None:
        """A preset whose _build_bundle raises returns None without re-raising."""
        def _bad_build(params, scope_kwargs):
            raise RuntimeError("Simulated build failure")

        monkeypatch.setattr(_PRESET, "_build_bundle", _bad_build)
        params = _FixtureParams()
        result = build_preview_bundle(_PRESET, params)
        assert result is None


# ---------------------------------------------------------------------------
# Tests: describe_preset
# ---------------------------------------------------------------------------

class TestDescribePreset:
    def test_required_keys_present(self) -> None:
        desc = describe_preset(_PRESET)
        assert desc["name"] == "_fixture_describe_test"
        assert desc["description"] == "A fixture preset used only in describe unit tests."
        assert desc["tier"] == "catalog"
        assert desc["catalog_scopable"] is False
        assert isinstance(desc["keywords"], list)
        assert "params_schema" in desc
        assert "examples" in desc

    def test_meta_block_in_field_mode(self) -> None:
        desc = describe_preset(_PRESET, mode="field")
        assert "_meta" in desc
        # params_model has fields with descriptions
        assert "docs" in desc["_meta"]

    def test_examples_structure(self) -> None:
        desc = describe_preset(_PRESET)
        examples = desc["examples"]
        assert len(examples) == 2

        first = examples[0]
        assert first["name"] == "basic"
        assert first["summary"] == "Basic catalog with PostgreSQL driver"
        assert "params" in first
        assert "resulting_config" in first

    def test_valid_example_has_resulting_config(self) -> None:
        desc = describe_preset(_PRESET)
        first = desc["examples"][0]
        rc = first["resulting_config"]
        assert rc is not None
        # Should be a list of entry dicts
        assert isinstance(rc, list)
        assert len(rc) == 1
        entry = rc[0]
        assert entry["slot"] == "catalog_routing"
        assert entry["config_cls"] == "CatalogRoutingConfig"
        assert "config" in entry

    def test_invalid_example_sets_error_not_raises(self) -> None:
        """An example with type-incompatible params yields resulting_config=None and an error key."""
        # _FixtureParams coerces driver_ref=12345 to str "12345" (pydantic v2 coercion)
        # but label=None becomes empty str "" or raises — depends on pydantic behavior.
        # The point is: describe_preset must not raise.
        desc = describe_preset(_PRESET)
        # The second example may succeed (coercion) or fail; either way no exception raised.
        second = desc["examples"][1]
        assert "resulting_config" in second
        # If it failed, error key must be present
        if second["resulting_config"] is None:
            assert "error" in second

    def test_no_params_model_omits_meta(self) -> None:
        """A preset with NoParams gets no _meta (or empty)."""
        from dynastore.modules.storage.presets.public_catalog import PublicCatalogPreset
        preset = PublicCatalogPreset()
        desc = describe_preset(preset)
        # _meta is absent or {} when params_model is NoParams
        meta = desc.get("_meta", {})
        # NoParams has no meaningful field docs — docs should be empty or absent
        docs = meta.get("docs", {})
        assert isinstance(docs, dict)

    def test_examples_classvar_default_empty(self) -> None:
        """A BundlePreset that declares no examples has an empty examples list."""

        class _NoExamplePreset(BundlePreset):
            name = "_no_example_describe_test"
            description = "Fixture with no declared examples."
            tier: ClassVar[PresetTier] = PresetTier.CATALOG

            def build(self, catalog_id: str = "", **_scope: str) -> PresetBundle:
                return PresetBundle(entries=())

        desc = describe_preset(_NoExamplePreset())
        assert desc["examples"] == []

    def test_parameterless_preset_example_has_resulting_config(self) -> None:
        """A parameterless built-in (public_catalog) surfaces a worked example
        whose resulting_config is computed from the empty params + synthetic scope."""
        from dynastore.modules.storage.presets.public_catalog import PublicCatalogPreset
        desc = describe_preset(PublicCatalogPreset())
        assert len(desc["examples"]) >= 1
        first = desc["examples"][0]
        assert first["params"] == {}
        assert first["resulting_config"] is not None
        assert isinstance(first["resulting_config"], list)
        assert len(first["resulting_config"]) >= 1


# ---------------------------------------------------------------------------
# Tests: descriptor_to_markdown
# ---------------------------------------------------------------------------

class TestDescriptorToMarkdown:
    def test_contains_preset_name(self) -> None:
        desc = describe_preset(_PRESET)
        md = descriptor_to_markdown(desc)
        assert "_fixture_describe_test" in md

    def test_contains_json_code_fence(self) -> None:
        desc = describe_preset(_PRESET)
        md = descriptor_to_markdown(desc)
        assert "```json" in md

    def test_contains_example_summary(self) -> None:
        desc = describe_preset(_PRESET)
        md = descriptor_to_markdown(desc)
        assert "Basic catalog with PostgreSQL driver" in md

    def test_returns_string(self) -> None:
        desc = describe_preset(_PRESET)
        md = descriptor_to_markdown(desc)
        assert isinstance(md, str)
        assert len(md) > 0


# ---------------------------------------------------------------------------
# Tests: descriptor_to_html
# ---------------------------------------------------------------------------

class TestDescriptorToHtml:
    def test_returns_string_always(self) -> None:
        desc = describe_preset(_PRESET)
        html = descriptor_to_html(desc)
        assert isinstance(html, str)
        assert len(html) > 0

    def test_does_not_raise_when_markdown_missing(self, monkeypatch) -> None:
        """Simulates markdown package being absent — must degrade to <pre> fallback."""
        import builtins
        _real_import = builtins.__import__

        def _mock_import(name, *args, **kwargs):
            if name == "markdown":
                raise ImportError("markdown not installed")
            return _real_import(name, *args, **kwargs)

        desc = describe_preset(_PRESET)
        with patch("builtins.__import__", side_effect=_mock_import):
            # descriptor_to_html must not raise
            html = descriptor_to_html(desc)
        assert isinstance(html, str)
        # Fallback is a <pre> block
        assert "<pre>" in html

    def test_html_contains_preset_name(self) -> None:
        desc = describe_preset(_PRESET)
        html = descriptor_to_html(desc)
        # Either in rendered HTML tags or in escaped pre block
        assert "_fixture_describe_test" in html


# ---------------------------------------------------------------------------
# Ratchet: every named built-in routing preset is self-documenting (#1965)
# ---------------------------------------------------------------------------

def _builtin_routing_presets():
    """Instantiate the built-in routing presets named in #1960 / #1965.

    Imported lazily inside the helper so a collection-time import error in one
    preset module surfaces as a test failure, not a collection error.
    """
    from dynastore.modules.storage.presets.public_catalog import PublicCatalogPreset
    from dynastore.modules.storage.presets.private_catalog import PrivateCatalogPreset
    from dynastore.modules.storage.presets.pg_only_catalog import PgOnlyCatalogPreset
    from dynastore.modules.storage.presets.defaults_postgres import DefaultsPostgresPreset
    from dynastore.modules.storage.presets.items_es_private import ItemsEsPrivatePreset
    from dynastore.modules.storage.presets.assets_local_only import AssetsLocalOnlyPreset
    from dynastore.modules.storage.presets.assets_gcs_uploads import AssetsGcsUploadsPreset
    from dynastore.modules.storage.presets.stac import StacPreset
    from dynastore.modules.storage.presets.file_backed import FileBackedPreset

    return [
        PublicCatalogPreset(),
        PrivateCatalogPreset(),
        PgOnlyCatalogPreset(),
        DefaultsPostgresPreset(),
        ItemsEsPrivatePreset(),
        AssetsLocalOnlyPreset(),
        AssetsGcsUploadsPreset(),
        StacPreset(),
        FileBackedPreset(),
    ]


@pytest.mark.parametrize(
    "preset",
    _builtin_routing_presets(),
    ids=lambda p: p.name,
)
class TestBuiltinPresetSelfDocumentation:
    """Every named built-in routing preset must carry rich keywords + at least
    one worked example whose preview config builds without a DB (#1965)."""

    def test_has_descriptive_keywords(self, preset) -> None:
        # More than the bare default ("routing",) — authored, searchable keywords.
        assert len(preset.keywords) >= 2
        assert "routing" in preset.keywords

    def test_has_at_least_one_example(self, preset) -> None:
        assert len(preset.examples) >= 1
        for ex in preset.examples:
            assert ex.name
            assert ex.summary

    def test_first_example_builds_resulting_config(self, preset) -> None:
        """The first worked example must render a non-None resulting_config —
        proving the documented params actually drive a buildable bundle."""
        desc = describe_preset(preset)
        first = desc["examples"][0]
        assert first["resulting_config"] is not None, (
            f"{preset.name}: first example produced no preview config "
            f"(error={first.get('error')!r})"
        )
        assert isinstance(first["resulting_config"], list)
        assert len(first["resulting_config"]) >= 1

    def test_describe_renders_markdown_with_example(self, preset) -> None:
        md = descriptor_to_markdown(describe_preset(preset))
        assert preset.name in md
        assert "## Examples" in md
