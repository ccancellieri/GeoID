"""ItemsWritePolicy: geometry_hash identity rule default + extra-forbid.

Two contracts:

1. Authoring ``derive.content_hashes=["geometry"]`` with the default
   identity chain auto-appends an
   ``IdentityRule(match_on=["geometry_hash"], on_match=REFUSE_RETURN)``
   rule. The legacy ``geometries.skip_if_unchanged_geometry_hash`` boolean
   is gone (hard cut, no backward-compat) and is rejected by Pydantic as an
   unknown field.

2. An explicit identity list is respected verbatim — the auto-append fires
   only when the identity list is the unmodified single-rule default.
"""

from typing import cast

import pytest
from pydantic import ValidationError

from dynastore.modules.storage.computed_fields import (
    DeriveSpec,
    IdentityRule,
)
from dynastore.modules.storage.driver_config import (
    ItemsWritePolicy,
    WriteConflictPolicy,
)


class TestGeometryHashIdentityDefault:
    def test_default_identity_is_external_id_only_without_content_hash(self):
        policy = ItemsWritePolicy()
        assert policy.identity == [IdentityRule(match_on=["external_id"])]

    def test_content_hashes_geometry_appends_geometry_hash_rule(self):
        policy = ItemsWritePolicy(
            derive=DeriveSpec(content_hashes=["geometry"]),
        )
        assert policy.identity == [
            IdentityRule(match_on=["external_id"]),
            IdentityRule(
                match_on=["geometry_hash"],
                on_match=WriteConflictPolicy.REFUSE_RETURN,
            ),
        ]

    def test_explicit_identity_overrides_auto_append(self):
        explicit = [
            IdentityRule(
                match_on=["external_id"],
                on_match=WriteConflictPolicy.NEW_VERSION,
            ),
        ]
        policy = ItemsWritePolicy(
            derive=DeriveSpec(content_hashes=["geometry"]),
            identity=explicit,
        )
        assert policy.identity == explicit

    def test_attributes_only_content_hash_does_not_append(self):
        policy = ItemsWritePolicy(
            derive=DeriveSpec(content_hashes=["attributes"]),
        )
        assert policy.identity == [IdentityRule(match_on=["external_id"])]


class TestSkipIfUnchangedGeometryHashRejected:
    """The legacy boolean is hard-cut: submitting it must raise."""

    def test_pydantic_rejects_skip_if_unchanged_geometry_hash(self):
        with pytest.raises(ValidationError) as exc_info:
            ItemsWritePolicy.model_validate(
                {
                    "geometries": {
                        "skip_if_unchanged_geometry_hash": True,
                    },
                }
            )
        # Pydantic's extra="forbid" surfaces the unknown field; the error
        # message must name it so an operator migrating an old config gets
        # an actionable signal.
        msg = str(exc_info.value)
        assert "skip_if_unchanged_geometry_hash" in msg

    def test_field_not_present_on_geometries_write_behavior(self):
        from dynastore.modules.storage.driver_config import GeometriesWriteBehavior

        assert "skip_if_unchanged_geometry_hash" not in cast(
            dict, GeometriesWriteBehavior.model_fields
        )
