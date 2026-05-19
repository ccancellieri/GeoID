"""Unit tests for the per-rule conflict-action override on
:class:`ItemsWritePolicy.identity` (phase 2 of #957/#950).

The previous shape (``matcher_actions: Dict[IdentityMatcher, WriteConflictPolicy]``)
collapsed into ``identity[*].on_match``: each :class:`IdentityRule` declares
its own conflict action; absent ``on_match`` falls back to the policy-level
``on_conflict``.
"""

from dynastore.modules.storage import (
    ComputedField,
    ComputedKind,
    IdentityRule,
    ItemsWritePolicy,
)
from dynastore.modules.storage.driver_config import WriteConflictPolicy


def _rule(*kinds: ComputedKind, on_match: WriteConflictPolicy | None = None) -> IdentityRule:
    return IdentityRule(
        match_on=[ComputedField(kind=k) for k in kinds],
        on_match=on_match,
    )


def test_default_identity_is_single_external_id_rule() -> None:
    """The default ``identity`` field is one rule matching on EXTERNAL_ID with
    no per-rule action override (falls back to ``on_conflict``)."""
    p = ItemsWritePolicy()
    assert len(p.identity) == 1
    assert [cf.kind for cf in p.identity[0].match_on] == [ComputedKind.EXTERNAL_ID]
    assert p.identity[0].on_match is None


def test_rule_on_match_overrides_policy_on_conflict() -> None:
    """When a rule declares ``on_match``, that overrides the policy default."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    rule = _rule(ComputedKind.GEOMETRY_HASH, on_match=WriteConflictPolicy.REFUSE_RETURN)
    p = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.UPDATE,
        identity=[rule],
    )
    assert _select_effective_on_conflict(p, rule) == WriteConflictPolicy.REFUSE_RETURN


def test_rule_without_on_match_falls_back_to_policy() -> None:
    """``on_match=None`` collapses to ``policy.on_conflict``."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    rule = _rule(ComputedKind.EXTERNAL_ID)
    p = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.REFUSE_FAIL,
        identity=[rule],
    )
    assert _select_effective_on_conflict(p, rule) == WriteConflictPolicy.REFUSE_FAIL


def test_multi_rule_chain_each_with_distinct_on_match() -> None:
    """Each rule carries its own ``on_match`` — the winning rule's action wins."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    rule_a = _rule(ComputedKind.EXTERNAL_ID, on_match=WriteConflictPolicy.REFUSE_FAIL)
    rule_b = _rule(ComputedKind.GEOMETRY_HASH, on_match=WriteConflictPolicy.REFUSE_RETURN)
    p = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.UPDATE,
        identity=[rule_a, rule_b],
    )
    assert _select_effective_on_conflict(p, rule_a) == WriteConflictPolicy.REFUSE_FAIL
    assert _select_effective_on_conflict(p, rule_b) == WriteConflictPolicy.REFUSE_RETURN


def test_no_policy_returns_default_update() -> None:
    """``None`` policy preserves the historical default."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    assert _select_effective_on_conflict(None, None) == WriteConflictPolicy.UPDATE


def test_composite_rule_ands_within_match_on() -> None:
    """A single rule can declare AND across multiple ComputedFields."""
    rule = IdentityRule(
        match_on=[
            ComputedField(kind=ComputedKind.GEOMETRY_HASH),
            ComputedField(kind=ComputedKind.ATTRIBUTES_HASH),
        ],
        on_match=WriteConflictPolicy.REFUSE_RETURN,
    )
    p = ItemsWritePolicy(identity=[rule])
    assert len(p.identity) == 1
    assert [cf.kind for cf in p.identity[0].match_on] == [
        ComputedKind.GEOMETRY_HASH,
        ComputedKind.ATTRIBUTES_HASH,
    ]


def test_legacy_field_names_rejected() -> None:
    """``identity_matchers`` / ``matcher_actions`` / ``geohash_precision`` /
    ``external_id_field`` / ``require_external_id`` are gone — Pydantic rejects them."""
    import pytest
    from pydantic import ValidationError

    for kwargs in (
        {"identity_matchers": []},
        {"matcher_actions": {}},
        {"geohash_precision": 6},
        {"external_id_field": "properties.code"},
        {"require_external_id": True},
    ):
        with pytest.raises(ValidationError):
            ItemsWritePolicy(**kwargs)  # type: ignore[arg-type]
