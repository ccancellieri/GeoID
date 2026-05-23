"""Unit tests for the batch-level conflict guard on :class:`ItemsWritePolicy`.

``on_batch_conflict`` (type :class:`BatchConflictPolicy`) is the all-or-nothing
pre-flight guard: before any item is processed, the incoming ingest batch is
rejected in full if any feature collides with an existing feature's identity.
It is distinct from ``AssetsWritePolicy.on_conflict`` (asset/file
re-registration) and from the per-item ``on_conflict`` action.
"""

from dynastore.modules.storage import BatchConflictPolicy, ItemsWritePolicy
from dynastore.modules.storage.driver_config import (
    BatchConflictPolicy as DriverConfigBatchConflictPolicy,
)


def test_enum_value_is_refuse_batch() -> None:
    """The single REFUSE member serialises to ``"refuse_batch"``."""
    assert BatchConflictPolicy.REFUSE == "refuse_batch"
    assert BatchConflictPolicy.REFUSE.value == "refuse_batch"
    # The storage package re-export is the same object as the module symbol.
    assert BatchConflictPolicy is DriverConfigBatchConflictPolicy


def test_default_on_batch_conflict_is_none() -> None:
    """No batch-level check unless explicitly opted in."""
    assert ItemsWritePolicy().on_batch_conflict is None


def test_on_batch_conflict_round_trips() -> None:
    """The renamed field round-trips through model_dump / model_validate and
    accepts the raw ``"refuse_batch"`` string."""
    policy = ItemsWritePolicy(on_batch_conflict=BatchConflictPolicy.REFUSE)
    assert policy.on_batch_conflict == BatchConflictPolicy.REFUSE

    dumped = policy.model_dump(mode="json")
    assert dumped["on_batch_conflict"] == "refuse_batch"

    revived = ItemsWritePolicy.model_validate({"on_batch_conflict": "refuse_batch"})
    assert revived.on_batch_conflict == BatchConflictPolicy.REFUSE
