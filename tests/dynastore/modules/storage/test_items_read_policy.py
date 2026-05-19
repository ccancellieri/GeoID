#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Phase 3 of #957/#950 — pin :class:`ItemsReadPolicy` registration shape.

These tests guarantee the class loads, the PluginConfig metadata is
correct (collection-scoped, correct address tuple), and the public
attribute set matches the design doc. No driver consumes the policy yet
— that's the read-path rewrite in a later PR.
"""

import pytest
from pydantic import ValidationError

from dynastore.modules.storage import (
    ComputedField,
    ComputedKind,
    FeatureType,
    ItemsReadPolicy,
)


class TestItemsReadPolicyShape:
    def test_default_constructible(self) -> None:
        p = ItemsReadPolicy()
        assert isinstance(p.feature_type, FeatureType)
        assert p.output_transformers == []

    def test_default_feature_type_points_at_write_schema(self) -> None:
        p = ItemsReadPolicy()
        assert p.feature_type.schema_ref == "items_write_policy.schema"
        assert p.feature_type.failure_mode == "best_effort"
        assert p.feature_type.expose == []

    def test_collection_scoped_only(self) -> None:
        assert ItemsReadPolicy._visibility == "collection"

    def test_address_tuple_matches_design(self) -> None:
        assert ItemsReadPolicy._address == (
            "platform",
            "catalog",
            "collection",
            "items",
            "read_policy",
        )

    def test_class_key_is_distinct_from_write_policy(self) -> None:
        from dynastore.modules.storage import ItemsWritePolicy

        assert ItemsReadPolicy.class_key() != ItemsWritePolicy.class_key()

    def test_can_carry_expose_list(self) -> None:
        p = ItemsReadPolicy(
            feature_type=FeatureType(
                expose=["area_m2", "h3_7"], failure_mode="strict"
            ),
            output_transformers=["private_entity", "locale_resolver"],
        )
        assert p.feature_type.expose == ["area_m2", "h3_7"]
        assert p.feature_type.failure_mode == "strict"
        assert p.output_transformers == ["private_entity", "locale_resolver"]

    def test_unknown_failure_mode_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ItemsReadPolicy(
                feature_type=FeatureType(failure_mode="loud")  # type: ignore[arg-type]
            )

    def test_extra_field_rejected_on_feature_type(self) -> None:
        with pytest.raises(ValidationError):
            FeatureType(unknown="x")  # type: ignore[call-arg]


def test_reexports_present() -> None:
    """The new model classes must be reachable from the package root."""
    from dynastore.modules.storage import (
        ComputedField,
        ComputedKind,
        FeatureType,
        IdentityRule,
        ItemsReadPolicy,
    )

    # Smoke-construct one of each so the imports do real work.
    cf = ComputedField(kind=ComputedKind.AREA)
    assert cf.resolved_name == "area"
    ft = FeatureType()
    assert ft.schema_ref
    rp = ItemsReadPolicy()
    assert rp.output_transformers == []
