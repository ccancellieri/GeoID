"""Pin the post-#200 invariants on conflict-vs-other IntegrityError mapping.

Pre-fix behaviour
-----------------
``conflict_to_409`` greedily downcast EVERY unmapped IntegrityError to
``DuplicateObjectError`` → bogus 409. ``is_conflict_error`` returned True
for any IntegrityError, so the ``ConflictExceptionHandler`` claimed
NOT NULL / CHECK violations and emitted misleading 409s — masking the
real cause (issue #200).

Post-fix behaviour pinned here
------------------------------
* ``is_conflict_error`` returns True ONLY for ``UniqueViolationError`` /
  ``ForeignKeyViolationError`` / ``DuplicateObjectError`` instances and
  for ``IntegrityError`` with conflict-class pgcodes (23505, 23503, 42710).
* It returns False for IntegrityError with NOT NULL (23502) / CHECK
  (23514) pgcodes so the chain proceeds to the new
  ``ConstraintViolationExceptionHandler`` → 422.
* ``conflict_to_409`` raises ``ValueError`` if a caller invokes it with
  an IntegrityError carrying a non-conflict pgcode — loud bug rather
  than a silent 409.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError as SqlIntegrityError

from dynastore.extensions.tools.conflict_handler import conflict_to_409
from dynastore.modules.db_config.exceptions import (
    CheckViolationError,
    DuplicateObjectError,
    ForeignKeyViolationError,
    NotNullViolationError,
    UniqueViolationError,
    is_conflict_error,
)


def _mk_integrity_error(pgcode: str | None) -> SqlIntegrityError:
    """Build a SQLAlchemy IntegrityError carrying a pgcode-bearing .orig."""
    orig = MagicMock()
    orig.pgcode = pgcode
    return SqlIntegrityError(statement="INSERT INTO ...", params={}, orig=orig)


# ---------------------------------------------------------------------------
# is_conflict_error — predicate behaviour
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("pgcode", ["23505", "23503", "42710"])
def test_is_conflict_error_true_for_conflict_pgcodes(pgcode: str) -> None:
    assert is_conflict_error(_mk_integrity_error(pgcode))


@pytest.mark.parametrize("pgcode", ["23502", "23514", "42501", "08000", None])
def test_is_conflict_error_false_for_non_conflict_pgcodes(pgcode: str | None) -> None:
    assert not is_conflict_error(_mk_integrity_error(pgcode))


def test_is_conflict_error_true_for_typed_classes() -> None:
    assert is_conflict_error(UniqueViolationError("dup"))
    assert is_conflict_error(ForeignKeyViolationError("fk"))
    assert is_conflict_error(DuplicateObjectError("dup obj"))


def test_is_conflict_error_false_for_typed_non_conflict() -> None:
    assert not is_conflict_error(NotNullViolationError("missing col"))
    assert not is_conflict_error(CheckViolationError("range"))
    assert not is_conflict_error(RuntimeError("unrelated"))


# ---------------------------------------------------------------------------
# conflict_to_409 — direct invocation
# ---------------------------------------------------------------------------

def test_conflict_to_409_maps_unique_violation_to_409() -> None:
    err = _mk_integrity_error("23505")
    response = conflict_to_409(err, resource_name="Catalog", resource_id="cat_x")
    assert response.status_code == 409
    assert "Catalog" in response.detail
    assert "cat_x" in response.detail


def test_conflict_to_409_maps_fk_violation_to_409() -> None:
    err = _mk_integrity_error("23503")
    response = conflict_to_409(err, resource_name="Asset", resource_id="a_y")
    assert response.status_code == 409
    assert "referenced resource does not exist" in response.detail


def test_conflict_to_409_raises_on_not_null_pgcode() -> None:
    """NOT NULL pgcode (23502) maps to NotNullViolationError which is NOT
    a conflict class — conflict_to_409 must raise rather than silently
    bake it into a bogus 409.
    """
    err = _mk_integrity_error("23502")
    with pytest.raises(ValueError, match="non-conflict-class exception"):
        conflict_to_409(err, resource_name="Resource")


def test_conflict_to_409_raises_on_unmapped_pgcode() -> None:
    err = _mk_integrity_error("99999")  # unknown pgcode
    with pytest.raises(ValueError, match="not a conflict-class pgcode"):
        conflict_to_409(err, resource_name="Resource")


def test_conflict_to_409_raises_on_missing_pgcode() -> None:
    err = _mk_integrity_error(None)
    with pytest.raises(ValueError, match="not a conflict-class pgcode"):
        conflict_to_409(err, resource_name="Resource")


def test_conflict_to_409_raises_on_non_conflict_typed_class() -> None:
    """Direct invocation with a typed non-conflict class (operator misuse)
    must raise rather than silently 409 (closes the message-builder hole)."""
    with pytest.raises(ValueError, match="non-conflict-class exception"):
        conflict_to_409(NotNullViolationError("col 'x'"), resource_name="Resource")
    with pytest.raises(ValueError, match="non-conflict-class exception"):
        conflict_to_409(CheckViolationError("range"), resource_name="Resource")


# ---------------------------------------------------------------------------
# Handler-chain dispatch — is_conflict_error gates ConflictExceptionHandler
# ---------------------------------------------------------------------------

def test_handler_chain_routes_unique_to_conflict_handler() -> None:
    from dynastore.extensions.tools.exception_handlers import _global_registry

    err = _mk_integrity_error("23505")
    result = _global_registry.handle(err, context={}, reraise_unhandled=False)
    # Conflict handler returns a 409 HTTPException for this case.
    assert getattr(result, "status_code", None) == 409


def test_handler_chain_routes_not_null_to_constraint_handler() -> None:
    """Pre-#200 this would have been claimed by ConflictExceptionHandler →
    bogus 409. Post-fix: ConstraintViolationExceptionHandler emits 422.
    """
    from dynastore.extensions.tools.exception_handlers import _global_registry

    err = NotNullViolationError("column 'description' violates not-null")
    result = _global_registry.handle(err, context={}, reraise_unhandled=False)
    assert getattr(result, "status_code", None) == 422
    detail = getattr(result, "detail", None)
    assert isinstance(detail, dict)
    assert detail.get("constraint_kind") == "NOT NULL"


def test_handler_chain_routes_check_to_constraint_handler() -> None:
    from dynastore.extensions.tools.exception_handlers import _global_registry

    err = CheckViolationError("value out of range for 'lanes'")
    result = _global_registry.handle(err, context={}, reraise_unhandled=False)
    assert getattr(result, "status_code", None) == 422
    detail = getattr(result, "detail", None)
    assert isinstance(detail, dict)
    assert detail.get("constraint_kind") == "CHECK"
