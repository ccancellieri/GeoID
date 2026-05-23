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
capability hint, and round-trip safety of the bucket + name-reference
:class:`AssetDeriveSpec` / :class:`AssetIdentityRule` shape (mirroring the
items side's ``DeriveSpec`` / ``IdentityRule``).
"""

import pytest
from pydantic import ValidationError

from dynastore.modules.catalog.write_policy_assets import (
    AssetDeriveSpec,
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
        # Two single-name rules in declared order: asset_id, then filename.
        assert len(cfg.identity) == 2
        assert cfg.identity[0].match_on == ["asset_id"]
        assert cfg.identity[1].match_on == ["filename"]
        # Default rules carry no ``on_match`` override — policy-level
        # ``on_conflict`` wins.
        assert all(rule.on_match is None for rule in cfg.identity)

    def test_default_derive_exposes_asset_id_and_filename(self) -> None:
        cfg = AssetsWritePolicy()
        assert cfg.derive.produced_names() == {"asset_id", "filename"}
        assert cfg.derive.asset_id is True
        assert cfg.derive.filename is True
        assert cfg.derive.url is False
        assert cfg.derive.content_hash is False
        assert cfg.derive.metadata_fields == []

    def test_default_require_filename_is_true(self) -> None:
        cfg = AssetsWritePolicy()
        assert cfg.require_filename is True

    def test_skip_if_unchanged_content_hash_field_is_gone(self) -> None:
        # The special-case boolean was collapsed into the unified identity
        # model — an ``AssetIdentityRule(match_on=["content_hash"],
        # on_match=REFUSE_RETURN)`` now expresses the same behaviour. The
        # field must no longer exist (``extra="forbid"`` rejects old configs).
        fields = set(AssetsWritePolicy.model_fields.keys())
        assert "skip_if_unchanged_content_hash" not in fields
        with pytest.raises(ValidationError):
            AssetsWritePolicy(skip_if_unchanged_content_hash=True)  # type: ignore[call-arg]

    def test_default_round_trip_preserves_chain(self) -> None:
        """dump → re-validate yields a policy whose chain is byte-identical."""
        original = AssetsWritePolicy()
        dumped = original.model_dump(mode="json")
        restored = AssetsWritePolicy.model_validate(dumped)
        assert restored.model_dump(mode="json") == dumped
        assert restored.identity == original.identity
        assert restored.derive == original.derive


class TestRequiresDriverVersioning:
    def test_only_new_version_requires_versioning(self) -> None:
        for action in AssetWriteConflictPolicy:
            cfg = AssetsWritePolicy(on_conflict=action)
            expected = action == AssetWriteConflictPolicy.NEW_VERSION
            assert cfg.requires_driver_versioning() is expected, (
                f"unexpected requires_driver_versioning for {action!r}"
            )


class TestAssetDeriveSpec:
    def test_produced_names_includes_all_enabled_dimensions(self) -> None:
        spec = AssetDeriveSpec(
            asset_id=True,
            filename=True,
            url=True,
            content_hash=True,
            metadata_fields=["iso19115.fileIdentifier", "doi"],
        )
        assert spec.produced_names() == {
            "asset_id",
            "filename",
            "url",
            "content_hash",
            "fileIdentifier",
            "doi",
        }

    def test_disabled_dimension_not_produced(self) -> None:
        spec = AssetDeriveSpec(asset_id=False, filename=False, url=True)
        assert spec.produced_names() == {"url"}

    def test_metadata_field_name_is_leaf_segment(self) -> None:
        spec = AssetDeriveSpec(metadata_fields=["a.b.c"])
        assert "c" in spec.produced_names()
        assert spec.metadata_path_for("c") == "a.b.c"

    def test_duplicate_metadata_leaf_names_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            AssetDeriveSpec(metadata_fields=["a.fileId", "b.fileId"])
        assert "unique" in str(exc_info.value).lower()

    def test_empty_metadata_path_rejected(self) -> None:
        with pytest.raises(ValidationError):
            AssetDeriveSpec(metadata_fields=[""])

    def test_spec_is_frozen(self) -> None:
        spec = AssetDeriveSpec()
        with pytest.raises(ValidationError):
            spec.asset_id = False  # type: ignore[misc]

    def test_extra_forbid_on_spec(self) -> None:
        with pytest.raises(ValidationError):
            AssetDeriveSpec(bogus=1)  # type: ignore[call-arg]

    def test_to_field_resolves_self_describing_kinds(self) -> None:
        spec = AssetDeriveSpec(url=True, content_hash=True)
        assert spec.to_field("asset_id").kind == AssetIdentityKind.ASSET_ID
        assert spec.to_field("filename").kind == AssetIdentityKind.FILENAME
        assert spec.to_field("url").kind == AssetIdentityKind.URL
        assert spec.to_field("content_hash").kind == AssetIdentityKind.CONTENT_HASH

    def test_to_field_resolves_metadata_field_with_path(self) -> None:
        spec = AssetDeriveSpec(metadata_fields=["iso19115.fileIdentifier"])
        f = spec.to_field("fileIdentifier")
        assert f.kind == AssetIdentityKind.METADATA_FIELD
        assert f.path == "iso19115.fileIdentifier"

    def test_to_field_rejects_undeclared_name(self) -> None:
        spec = AssetDeriveSpec(url=False)
        with pytest.raises(ValueError) as exc_info:
            spec.to_field("url")
        assert "url" in str(exc_info.value)


class TestAssetIdentityRule:
    def test_match_on_must_be_non_empty(self) -> None:
        with pytest.raises(ValidationError):
            AssetIdentityRule(match_on=[])

    def test_match_on_is_a_list_of_names(self) -> None:
        rule = AssetIdentityRule(match_on=["asset_id", "filename"])
        assert rule.match_on == ["asset_id", "filename"]

    def test_rule_on_match_override_is_optional(self) -> None:
        r1 = AssetIdentityRule(match_on=["asset_id"])
        assert r1.on_match is None
        r2 = AssetIdentityRule(
            match_on=["content_hash"],
            on_match=AssetWriteConflictPolicy.REFUSE_RETURN,
        )
        assert r2.on_match == AssetWriteConflictPolicy.REFUSE_RETURN

    def test_extra_forbid_on_rule(self) -> None:
        with pytest.raises(ValidationError):
            AssetIdentityRule(match_on=["asset_id"], bogus=1)  # type: ignore[call-arg]


class TestAssetsWritePolicyIdentityValidator:
    def test_undeclared_name_rejected(self) -> None:
        """A match_on name not produced by derive fails at config-save."""
        with pytest.raises(ValidationError) as exc_info:
            AssetsWritePolicy(
                derive=AssetDeriveSpec(asset_id=True, filename=False),
                identity=[AssetIdentityRule(match_on=["filename"])],
            )
        msg = str(exc_info.value)
        assert "filename" in msg

    def test_content_hash_requires_derive_optin(self) -> None:
        # content_hash is off by default → referencing it without enabling fails.
        with pytest.raises(ValidationError):
            AssetsWritePolicy(
                identity=[AssetIdentityRule(match_on=["content_hash"])],
            )
        # …enabling it on derive makes the rule legal.
        cfg = AssetsWritePolicy(
            derive=AssetDeriveSpec(content_hash=True),
            identity=[AssetIdentityRule(match_on=["content_hash"])],
        )
        assert cfg.identity[0].match_on == ["content_hash"]

    def test_content_hash_refuse_return_rule_validates_only_with_derive_optin(
        self,
    ) -> None:
        """The unified replacement for ``skip_if_unchanged_content_hash``:
        a content_hash rule pinned to REFUSE_RETURN validates iff
        ``derive.content_hash`` is on, and resolves to the CONTENT_HASH
        engine field carrying the override action."""
        # Without the derive opt-in the reference is unknown → rejected.
        with pytest.raises(ValidationError):
            AssetsWritePolicy(
                identity=[
                    AssetIdentityRule(
                        match_on=["content_hash"],
                        on_match=AssetWriteConflictPolicy.REFUSE_RETURN,
                    ),
                ],
            )
        # With derive.content_hash=True it validates and resolves.
        cfg = AssetsWritePolicy(
            on_conflict=AssetWriteConflictPolicy.UPDATE,
            derive=AssetDeriveSpec(
                asset_id=True, filename=True, content_hash=True
            ),
            identity=[
                AssetIdentityRule(
                    match_on=["content_hash"],
                    on_match=AssetWriteConflictPolicy.REFUSE_RETURN,
                ),
                AssetIdentityRule(match_on=["asset_id"]),
            ],
        )
        resolved = cfg.resolved_identity()
        assert resolved[0].match_on[0].kind == AssetIdentityKind.CONTENT_HASH
        assert resolved[0].on_match == AssetWriteConflictPolicy.REFUSE_RETURN

    def test_metadata_name_resolves(self) -> None:
        cfg = AssetsWritePolicy(
            derive=AssetDeriveSpec(metadata_fields=["iso19115.fileIdentifier"]),
            identity=[AssetIdentityRule(match_on=["fileIdentifier"])],
        )
        resolved = cfg.resolved_identity()
        assert resolved[0].match_on[0].kind == AssetIdentityKind.METADATA_FIELD
        assert resolved[0].match_on[0].path == "iso19115.fileIdentifier"

    def test_resolved_identity_carries_on_match(self) -> None:
        cfg = AssetsWritePolicy(
            identity=[
                AssetIdentityRule(
                    match_on=["asset_id"],
                    on_match=AssetWriteConflictPolicy.UPDATE,
                ),
            ],
        )
        resolved = cfg.resolved_identity()
        assert resolved[0].on_match == AssetWriteConflictPolicy.UPDATE
        assert resolved[0].match_on[0].kind == AssetIdentityKind.ASSET_ID


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
        assert "derive" not in fields
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

    def test_match_on_is_names_not_polymorphic_fields(self) -> None:
        """The AUTHORING surface uses names (List[str]); the polymorphic
        ``List[AssetIdentityField]`` match_on shape is gone."""
        cfg = AssetsWritePolicy()
        for rule in cfg.identity:
            assert all(isinstance(n, str) for n in rule.match_on)

    def test_derive_field_present(self) -> None:
        assert "derive" in set(AssetsWritePolicy.model_fields.keys())


class TestAssetIdentityFieldEngineType:
    """AssetIdentityField is kept as the INTERNAL engine value type behind the
    bucket authoring surface — exactly like the items side keeps ComputedField
    behind DeriveSpec. These pin its engine-facing behaviour."""

    def test_metadata_field_without_path_fails(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            AssetIdentityField(kind=AssetIdentityKind.METADATA_FIELD)
        assert "path" in str(exc_info.value).lower()

    def test_non_metadata_field_with_path_fails(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            AssetIdentityField(kind=AssetIdentityKind.ASSET_ID, path="nope")
        assert "path" in str(exc_info.value).lower()

    def test_resolved_name_for_self_describing(self) -> None:
        assert (
            AssetIdentityField(kind=AssetIdentityKind.ASSET_ID).resolved_name
            == "asset_id"
        )

    def test_resolved_name_for_metadata_is_leaf(self) -> None:
        f = AssetIdentityField(
            kind=AssetIdentityKind.METADATA_FIELD, path="a.b.fileId"
        )
        assert f.resolved_name == "fileId"


def test_default_policy_dump_yields_byte_for_byte_chain() -> None:
    """The default identity chain dumps to a JSON shape that round-trips
    through ``model_validate`` producing the exact same chain dispatch."""
    p1 = AssetsWritePolicy()
    dumped = p1.model_dump(mode="json")
    p2 = AssetsWritePolicy.model_validate(dumped)
    assert [r.match_on for r in p2.identity] == [["asset_id"], ["filename"]]
    assert p2.model_dump(mode="json") == dumped


def test_require_filename_posture_stays_orthogonal_to_identity() -> None:
    """``require_filename`` is a service-layer pre-check, not an identity
    field. Toggling it must not change ``policy.identity`` at all."""
    strict = AssetsWritePolicy(require_filename=True)
    loose = AssetsWritePolicy(require_filename=False)
    assert strict.identity == loose.identity
