"""
CI guard: no driver-class isinstance checks in service/extension code.

Invariant: zero isinstance checks on concrete driver classes inside the
catalog module or any extension's service code.

Any new isinstance check on a concrete driver class in extensions or catalog
services is an architecture violation. Use Capability constants instead.
"""

import re

from tests._repo_paths import CORE_SRC, EXTENSIONS_ROOTS, REPO_ROOT

# Directories where driver isinstance leakage is forbidden:
#   - catalog module (single tree, lives in core)
#   - every extension's source tree (one root per packages/extensions/<name>)
_GUARDED_DIRS = [
    CORE_SRC / "modules" / "catalog",
    *EXTENSIONS_ROOTS,
]

# Pattern: isinstance(something, <DriverName>)
_PATTERN = r"isinstance\([^,]+,\s*(?:Collection|Asset|Metadata)(?:Postgresql|Elasticsearch|ElasticsearchPrivate|Iceberg|Duckdb)Driver\b"


def _grep_for_violations() -> list[str]:
    """Return list of 'file:line: match' strings for each violation."""
    violations = []
    for target in _GUARDED_DIRS:
        if not target.exists():
            continue
        for py_file in target.rglob("*.py"):
            text = py_file.read_text(encoding="utf-8")
            for lineno, line in enumerate(text.splitlines(), start=1):
                if re.search(_PATTERN, line):
                    violations.append(f"{py_file.relative_to(REPO_ROOT)}:{lineno}: {line.strip()}")
    return violations


def test_no_driver_class_isinstance_in_services():
    """No isinstance check on a concrete driver class in extensions or catalog modules."""
    violations = _grep_for_violations()
    assert violations == [], (
        "Driver isinstance leak(s) found — use Capability constants instead:\n"
        + "\n".join(f"  {v}" for v in violations)
    )
