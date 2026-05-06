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
``model_validator`` cross-field rules, and the
``requires_driver_versioning`` capability hint.
"""

import pytest
from pydantic import ValidationError

from dynastore.modules.catalog.write_policy_assets import (
    AssetIdentityMatcher,
    AssetWriteConflictPolicy,
    AssetWritePolicyDefaults,
    AssetsWritePolicy,
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
        assert cfg.identity_matchers == [
            AssetIdentityMatcher.ASSET_ID,
            AssetIdentityMatcher.FILENAME,
        ]

    def test_default_require_filename_is_true(self) -> None:
        cfg = AssetsWritePolicy()
        assert cfg.require_filename is True

    def test_default_skip_if_unchanged_content_hash_is_false(self) -> None:
        cfg = AssetsWritePolicy()
        assert cfg.skip_if_unchanged_content_hash is False


class TestRequiresDriverVersioning:
    def test_only_new_version_requires_versioning(self) -> None:
        for action in AssetWriteConflictPolicy:
            cfg = AssetsWritePolicy(on_conflict=action)
            expected = action == AssetWriteConflictPolicy.NEW_VERSION
            assert cfg.requires_driver_versioning() is expected, (
                f"unexpected requires_driver_versioning for {action!r}"
            )


class TestMetadataFieldValidator:
    def test_metadata_field_without_path_fails(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            AssetsWritePolicy(
                identity_matchers=[AssetIdentityMatcher.METADATA_FIELD],
            )
        assert "metadata_match_path" in str(exc_info.value)

    def test_metadata_field_with_path_succeeds(self) -> None:
        cfg = AssetsWritePolicy(
            identity_matchers=[AssetIdentityMatcher.METADATA_FIELD],
            metadata_match_path="iso19115.fileIdentifier",
        )
        assert cfg.metadata_match_path == "iso19115.fileIdentifier"

    def test_metadata_field_in_chain_with_other_matchers_requires_path(
        self,
    ) -> None:
        with pytest.raises(ValidationError):
            AssetsWritePolicy(
                identity_matchers=[
                    AssetIdentityMatcher.ASSET_ID,
                    AssetIdentityMatcher.METADATA_FIELD,
                ],
            )

    def test_path_set_but_matcher_missing_is_allowed(self) -> None:
        # Setting metadata_match_path without using the matcher is harmless
        # — just an unused config value, not a contract violation.
        cfg = AssetsWritePolicy(metadata_match_path="some.path")
        assert cfg.metadata_match_path == "some.path"


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
        # without referencing any specific JSON path.
        fields = set(AssetWritePolicyDefaults.model_fields.keys())
        assert "metadata_match_path" not in fields
        assert "identity_matchers" not in fields
        assert "skip_if_unchanged_content_hash" not in fields
