"""Unit tests for the per-rule conflict-action override on
:class:`ItemsWritePolicy.identity` (phase 2 of #957/#950).

The previous shape (``matcher_actions: Dict[IdentityMatcher, WriteConflictPolicy]``)
collapsed into ``identity[*].on_match``: each :class:`IdentityRule` declares
its own conflict action; absent ``on_match`` falls back to the policy-level
``on_conflict``. Identity rules now reference declared ``derive`` outputs by
name (``match_on: List[str]``) rather than re-declaring ComputedFields.
"""

from dynastore.modules.storage import (
    ComputedKind,
    DeriveSpec,
    IdentityRule,
    ItemsWritePolicy,
)
from dynastore.modules.storage.driver_config import WriteConflictPolicy


def _rule(*names: str, on_match: WriteConflictPolicy | None = None) -> IdentityRule:
    return IdentityRule(match_on=list(names), on_match=on_match)


def test_default_identity_is_single_external_id_rule() -> None:
    """The default ``identity`` field is one rule matching on ``external_id``
    with no per-rule action override (falls back to ``on_conflict``)."""
    p = ItemsWritePolicy()
    assert len(p.identity) == 1
    assert p.identity[0].match_on == ["external_id"]
    assert p.identity[0].on_match is None
    # resolves to the EXTERNAL_ID engine field even without an extraction path
    assert [cf.kind for cf in p.resolved_identity()[0].match_on] == [
        ComputedKind.EXTERNAL_ID
    ]


def test_rule_on_match_overrides_policy_on_conflict() -> None:
    """When a rule declares ``on_match``, that overrides the policy default."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    rule = _rule("geometry_hash", on_match=WriteConflictPolicy.REFUSE_RETURN)
    p = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.UPDATE,
        derive=DeriveSpec(content_hashes=["geometry"]),
        identity=[rule],
    )
    matched = p.resolved_identity()[0]
    assert _select_effective_on_conflict(p, matched) == WriteConflictPolicy.REFUSE_RETURN


def test_rule_without_on_match_falls_back_to_policy() -> None:
    """``on_match=None`` collapses to ``policy.on_conflict``."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    rule = _rule("external_id")
    p = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.REFUSE_FAIL,
        identity=[rule],
    )
    matched = p.resolved_identity()[0]
    assert _select_effective_on_conflict(p, matched) == WriteConflictPolicy.REFUSE_FAIL


def test_multi_rule_chain_each_with_distinct_on_match() -> None:
    """Each rule carries its own ``on_match`` — the winning rule's action wins."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    rule_a = _rule("external_id", on_match=WriteConflictPolicy.REFUSE_FAIL)
    rule_b = _rule("geometry_hash", on_match=WriteConflictPolicy.REFUSE_RETURN)
    p = ItemsWritePolicy(
        on_conflict=WriteConflictPolicy.UPDATE,
        derive=DeriveSpec(content_hashes=["geometry"]),
        identity=[rule_a, rule_b],
    )
    resolved_a, resolved_b = p.resolved_identity()
    assert _select_effective_on_conflict(p, resolved_a) == WriteConflictPolicy.REFUSE_FAIL
    assert _select_effective_on_conflict(p, resolved_b) == WriteConflictPolicy.REFUSE_RETURN


def test_no_policy_returns_default_update() -> None:
    """``None`` policy preserves the historical default."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    assert _select_effective_on_conflict(None, None) == WriteConflictPolicy.UPDATE


def test_composite_rule_ands_within_match_on() -> None:
    """A single rule can declare AND across multiple referenced derivations."""
    rule = IdentityRule(
        match_on=["geometry_hash", "attributes_hash"],
        on_match=WriteConflictPolicy.REFUSE_RETURN,
    )
    p = ItemsWritePolicy(
        derive=DeriveSpec(content_hashes=["geometry", "attributes"]),
        identity=[rule],
    )
    assert len(p.identity) == 1
    assert p.identity[0].match_on == ["geometry_hash", "attributes_hash"]
    assert [cf.kind for cf in p.resolved_identity()[0].match_on] == [
        ComputedKind.GEOMETRY_HASH,
        ComputedKind.ATTRIBUTES_HASH,
    ]


def test_identity_ref_to_undeclared_field_rejected() -> None:
    """A ``match_on`` name not produced by ``derive`` is rejected at save."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ItemsWritePolicy(identity=[IdentityRule(match_on=["geometry_hash"])])


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
