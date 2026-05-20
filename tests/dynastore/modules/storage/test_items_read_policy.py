#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Pin :class:`ItemsReadPolicy` registration shape (#957/#950, #1065).

These tests guarantee the class loads, the PluginConfig metadata is
correct (collection-scoped, correct address tuple), and the public
attribute set matches the design. The PG read path consumes
``feature_type`` (expose merge + ``external_id_as_feature_id``); transformer
wiring lives on the routing config, not here (#950).
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

    def test_default_feature_type(self) -> None:
        p = ItemsReadPolicy()
        assert p.feature_type.failure_mode == "best_effort"
        assert p.feature_type.expose == []
        assert p.feature_type.external_id_as_feature_id is True

    def test_output_transformers_not_a_field(self) -> None:
        # Transformer wiring lives on the routing config (#950), not here.
        assert "output_transformers" not in ItemsReadPolicy.model_fields

    def test_schema_ref_not_a_field(self) -> None:
        # The read shape derives from items_schema unconditionally (#1065).
        assert "schema_ref" not in FeatureType.model_fields

    def test_external_id_as_feature_id_can_be_disabled(self) -> None:
        p = ItemsReadPolicy(
            feature_type=FeatureType(external_id_as_feature_id=False)
        )
        assert p.feature_type.external_id_as_feature_id is False

    def test_collection_scoped_only(self) -> None:
        assert ItemsReadPolicy._visibility == "collection"

    def test_address_grouped_under_items_policy(self) -> None:
        # Grouped with ItemsWritePolicy under ``items.policy``; the composer
        # keys each leaf by class_key, so this nests as
        # ``items.policy.items_read_policy``. Storage/lookup is by class_key.
        from dynastore.modules.storage import ItemsWritePolicy

        assert ItemsReadPolicy._address == (
            "platform",
            "catalog",
            "collection",
            "items",
            "policy",
        )
        assert ItemsReadPolicy._address == ItemsWritePolicy._address

    def test_class_key_is_distinct_from_write_policy(self) -> None:
        from dynastore.modules.storage import ItemsWritePolicy

        assert ItemsReadPolicy.class_key() != ItemsWritePolicy.class_key()

    def test_can_carry_expose_list(self) -> None:
        p = ItemsReadPolicy(
            feature_type=FeatureType(
                expose=["area_m2", "h3_7"], failure_mode="strict"
            ),
        )
        assert p.feature_type.expose == ["area_m2", "h3_7"]
        assert p.feature_type.failure_mode == "strict"

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
    assert ft.expose == []
    rp = ItemsReadPolicy()
    assert isinstance(rp.feature_type, FeatureType)
