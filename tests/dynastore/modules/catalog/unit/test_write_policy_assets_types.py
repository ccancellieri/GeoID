#    Copyright 2025 FAO
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

"""Pure-Python unit tests for the asset write-policy types.

No DB / no service registry — exercises the ``PluginConfig`` defaults,
``model_validator`` cross-field rules, the ``requires_driver_versioning``
capability hint, and round-trip safety of the new
:class:`AssetIdentityRule` shape.
"""

import pytest
from pydantic import ValidationError

from dynastore.modules.catalog.write_policy_assets import (
    AssetIdentityField,
    AssetIdentityKind,
    AssetIdentityRule,
    AssetsWritePolicy,
    AssetWriteConflictPolicy,
    AssetWritePolicyDefaults,
)


class TestAssetsWritePolicyDefaults:
    def test_zero_arg_instantiation(self) -> None:
        cfg = AssetsWritePolicy()
        assert cfg is not None

    def test_default_on_conflict_is_refuse_fail(self) -> None:
        cfg = AssetsWritePolicy()
        assert cfg.on_conflict == AssetWriteConflictPolicy.REFUSE_FAIL

    def test_default_chain_is_asset_id_then_filename(self) -> None:
        cfg = AssetsWritePolicy()
        # Two single-field rules in declared order: ASSET_ID, then FILENAME.
        assert len(cfg.identity) == 2
        assert cfg.identity[0].match_on[0].kind == AssetIdentityKind.ASSET_ID
        assert cfg.identity[1].match_on[0].kind == AssetIdentityKind.FILENAME
        # Default rules carry no ``on_match`` override — policy-level
        # ``on_conflict`` wins.
        assert all(rule.on_match is None for rule in cfg.identity)

    def test_default_require_filename_is_true(self) -> None:
        cfg = AssetsWritePolicy()
        assert cfg.require_filename is True

    def test_default_skip_if_unchanged_content_hash_is_false(self) -> None:
        cfg = AssetsWritePolicy()
        assert cfg.skip_if_unchanged_content_hash is False

    def test_default_round_trip_preserves_chain(self) -> None:
        """dump → re-validate yields a policy whose chain is byte-identical."""
        original = AssetsWritePolicy()
        dumped = original.model_dump(mode="json")
        restored = AssetsWritePolicy.model_validate(dumped)
        assert restored.model_dump(mode="json") == dumped
        assert restored.identity == original.identity


class TestRequiresDriverVersioning:
    def test_only_new_version_requires_versioning(self) -> None:
        for action in AssetWriteConflictPolicy:
            cfg = AssetsWritePolicy(on_conflict=action)
            expected = action == AssetWriteConflictPolicy.NEW_VERSION
            assert cfg.requires_driver_versioning() is expected, (
                f"unexpected requires_driver_versioning for {action!r}"
            )


class TestAssetIdentityFieldValidator:
    def test_metadata_field_without_path_fails(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            AssetIdentityField(kind=AssetIdentityKind.METADATA_FIELD)
        assert "path" in str(exc_info.value).lower()

    def test_non_metadata_field_with_path_fails(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            AssetIdentityField(kind=AssetIdentityKind.ASSET_ID, path="nope")
        assert "path" in str(exc_info.value).lower()

    def test_metadata_field_with_path_succeeds(self) -> None:
        f = AssetIdentityField(
            kind=AssetIdentityKind.METADATA_FIELD,
            path="iso19115.fileIdentifier",
        )
        assert f.path == "iso19115.fileIdentifier"

    def test_field_is_frozen(self) -> None:
        f = AssetIdentityField(kind=AssetIdentityKind.ASSET_ID)
        with pytest.raises(ValidationError):
            # frozen=True → assignment is rejected
            f.kind = AssetIdentityKind.FILENAME  # type: ignore[misc]

    def test_extra_forbid_on_field(self) -> None:
        with pytest.raises(ValidationError):
            AssetIdentityField(kind=AssetIdentityKind.ASSET_ID, bogus=1)  # type: ignore[call-arg]

    def test_extra_forbid_on_rule(self) -> None:
        with pytest.raises(ValidationError):
            AssetIdentityRule(  # type: ignore[call-arg]
                match_on=[AssetIdentityField(kind=AssetIdentityKind.ASSET_ID)],
                bogus=1,
            )


class TestAssetIdentityRule:
    def test_match_on_must_be_non_empty(self) -> None:
        with pytest.raises(ValidationError):
            AssetIdentityRule(match_on=[])

    def test_metadata_field_rule_carries_path_on_the_field(self) -> None:
        """The dot-path lives on the field, not on the rule or the policy."""
        rule = AssetIdentityRule(
            match_on=[
                AssetIdentityField(
                    kind=AssetIdentityKind.METADATA_FIELD,
                    path="iso19115.fileIdentifier",
                ),
            ],
        )
        assert rule.match_on[0].path == "iso19115.fileIdentifier"

    def test_rule_on_match_override_is_optional(self) -> None:
        # No override
        r1 = AssetIdentityRule(
            match_on=[AssetIdentityField(kind=AssetIdentityKind.ASSET_ID)]
        )
        assert r1.on_match is None
        # With override
        r2 = AssetIdentityRule(
            match_on=[AssetIdentityField(kind=AssetIdentityKind.CONTENT_HASH)],
            on_match=AssetWriteConflictPolicy.REFUSE_RETURN,
        )
        assert r2.on_match == AssetWriteConflictPolicy.REFUSE_RETURN


class TestAssetWritePolicyDefaults:
    def test_zero_arg_instantiation(self) -> None:
        cfg = AssetWritePolicyDefaults()
        assert cfg is not None

    def test_default_posture_is_strict(self) -> None:
        cfg = AssetWritePolicyDefaults()
        assert cfg.on_conflict == AssetWriteConflictPolicy.REFUSE_FAIL
        assert cfg.require_filename is True

    def test_no_field_name_bindings_present(self) -> None:
        # The defaults class must not carry collection-INTRINSIC bindings —
        # those live on AssetsWritePolicy. Asserts the class surface stays
        # posture-only so platform/catalog operators can set defaults
        # without referencing any specific JSON path or identity shape.
        fields = set(AssetWritePolicyDefaults.model_fields.keys())
        assert "metadata_match_path" not in fields
        assert "identity_matchers" not in fields
        assert "identity" not in fields
        assert "skip_if_unchanged_content_hash" not in fields


class TestAssetsWritePolicyShapeMigration:
    """Dropped-field assertions — guard against accidental reintroduction."""

    def test_old_identity_matchers_field_is_gone(self) -> None:
        fields = set(AssetsWritePolicy.model_fields.keys())
        assert "identity_matchers" not in fields
        assert "metadata_match_path" not in fields

    def test_old_matcher_enum_module_export_is_gone(self) -> None:
        from dynastore.modules.catalog import write_policy_assets as mod
        assert not hasattr(mod, "AssetIdentityMatcher")
