"""Code-review guard: no deprecated, naive-UTC datetime constructors.

``datetime.utcnow()`` and ``datetime.utcfromtimestamp()`` return a **naive**
datetime (no tzinfo) that *looks* like UTC but compares/serialises wrongly
against tz-aware datetimes — a recurring source of off-by-timezone bugs. Both
are also **deprecated as of Python 3.12** and slated for removal.

Use the tz-aware forms: ``datetime.now(timezone.utc)`` and
``datetime.fromtimestamp(ts, tz=timezone.utc)``.

The codebase is clean today (all timestamps are tz-aware) — this guard locks
that in.
"""
from __future__ import annotations

import ast

from tests._repo_paths import CORE_SRC, EXTENSIONS_ROOTS

_FORBIDDEN_ATTRS = frozenset({"utcnow", "utcfromtimestamp"})


def _iter_source_files():
    for root in (CORE_SRC, *EXTENSIONS_ROOTS):
        for p in root.rglob("*.py"):
            if "__pycache__" not in p.parts:
                yield p


def test_no_naive_utc_datetime_constructors() -> None:
    offenders: list[str] = []
    for path in _iter_source_files():
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        for node in ast.walk(tree):
            # match `<anything>.utcnow(...)` / `<anything>.utcfromtimestamp(...)`
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in _FORBIDDEN_ATTRS
            ):
                offenders.append(f"{path}:{node.lineno} .{node.func.attr}()")

    assert not offenders, (
        "Naive/deprecated UTC datetime constructor. Use "
        "`datetime.now(timezone.utc)` / `datetime.fromtimestamp(ts, tz=timezone.utc)`:\n  "
        + "\n  ".join(offenders)
    )
