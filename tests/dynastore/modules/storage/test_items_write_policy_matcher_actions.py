"""Unit tests for ItemsWritePolicy.matcher_actions (per-matcher conflict action)."""
from dynastore.modules.storage.driver_config import (
    IdentityMatcher,
    ItemsWritePolicy,
    WriteConflictPolicy,
)


def test_matcher_actions_default_is_none():
    """When unspecified, matcher_actions is None and global on_conflict is authoritative."""
    p = ItemsWritePolicy()
    assert p.matcher_actions is None


def test_matcher_actions_accepts_dict_of_matcher_to_action():
    """matcher_actions accepts a dict keyed by IdentityMatcher with WriteConflictPolicy values."""
    p = ItemsWritePolicy(
        identity_matchers=[IdentityMatcher.EXTERNAL_ID, IdentityMatcher.GEOMETRY_HASH],
        matcher_actions={
            IdentityMatcher.EXTERNAL_ID: WriteConflictPolicy.REFUSE_FAIL,
            IdentityMatcher.GEOMETRY_HASH: WriteConflictPolicy.REFUSE_RETURN,
        },
    )
    assert p.matcher_actions is not None
    assert p.matcher_actions[IdentityMatcher.EXTERNAL_ID] == WriteConflictPolicy.REFUSE_FAIL
    assert p.matcher_actions[IdentityMatcher.GEOMETRY_HASH] == WriteConflictPolicy.REFUSE_RETURN


def test_matcher_actions_partial_override_is_allowed():
    """matcher_actions can specify only some matchers; others fall back to on_conflict."""
    p = ItemsWritePolicy(
        identity_matchers=[IdentityMatcher.EXTERNAL_ID, IdentityMatcher.GEOMETRY_HASH],
        on_conflict=WriteConflictPolicy.UPDATE,
        matcher_actions={IdentityMatcher.GEOMETRY_HASH: WriteConflictPolicy.REFUSE_RETURN},
    )
    assert p.on_conflict == WriteConflictPolicy.UPDATE
    assert p.matcher_actions == {IdentityMatcher.GEOMETRY_HASH: WriteConflictPolicy.REFUSE_RETURN}


def test_matcher_actions_serialises_to_string_keys_in_json():
    """JSON round-trip uses the StrEnum value of the matcher key."""
    p = ItemsWritePolicy(
        matcher_actions={IdentityMatcher.EXTERNAL_ID: WriteConflictPolicy.REFUSE_FAIL},
    )
    payload = p.model_dump(mode="json")
    assert payload["matcher_actions"] == {"external_id": "refuse_fail"}


def test_effective_on_conflict_uses_matcher_actions_when_set():
    """The per-matcher action wins when matcher_actions is populated."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    policy = ItemsWritePolicy(
        identity_matchers=[IdentityMatcher.EXTERNAL_ID, IdentityMatcher.GEOMETRY_HASH],
        on_conflict=WriteConflictPolicy.UPDATE,
        matcher_actions={
            IdentityMatcher.EXTERNAL_ID: WriteConflictPolicy.REFUSE_FAIL,
            IdentityMatcher.GEOMETRY_HASH: WriteConflictPolicy.REFUSE_RETURN,
        },
    )
    assert _select_effective_on_conflict(policy, IdentityMatcher.EXTERNAL_ID) == WriteConflictPolicy.REFUSE_FAIL
    assert _select_effective_on_conflict(policy, IdentityMatcher.GEOMETRY_HASH) == WriteConflictPolicy.REFUSE_RETURN


def test_effective_on_conflict_falls_back_to_on_conflict_for_unspecified_matcher():
    """When matcher_actions has no entry for the winning matcher, fall back to on_conflict."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    policy = ItemsWritePolicy(
        identity_matchers=[IdentityMatcher.EXTERNAL_ID, IdentityMatcher.GEOMETRY_HASH],
        on_conflict=WriteConflictPolicy.UPDATE,
        matcher_actions={IdentityMatcher.GEOMETRY_HASH: WriteConflictPolicy.REFUSE_RETURN},
    )
    assert _select_effective_on_conflict(policy, IdentityMatcher.EXTERNAL_ID) == WriteConflictPolicy.UPDATE


def test_effective_on_conflict_falls_back_when_matcher_actions_is_none():
    """When matcher_actions is unset, on_conflict is always used."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    policy = ItemsWritePolicy(
        identity_matchers=[IdentityMatcher.EXTERNAL_ID],
        on_conflict=WriteConflictPolicy.REFUSE_FAIL,
    )
    assert _select_effective_on_conflict(policy, IdentityMatcher.EXTERNAL_ID) == WriteConflictPolicy.REFUSE_FAIL


def test_effective_on_conflict_handles_none_policy():
    """No policy at all → default UPDATE semantics preserved."""
    from dynastore.modules.catalog.item_distributed import _select_effective_on_conflict

    assert _select_effective_on_conflict(None, IdentityMatcher.EXTERNAL_ID) == WriteConflictPolicy.UPDATE
