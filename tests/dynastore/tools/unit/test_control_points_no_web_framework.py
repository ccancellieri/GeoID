"""Code-review guard: framework control points stay runner-agnostic.

`tools/async_utils.py`, `modules/concurrency.py`, and the cache modules are the
platform's concurrency/caching **control points**. They must not import a web
framework (FastAPI / Starlette): doing so couples the runner-agnostic core to a
specific serving stack and creates a cross-layer contamination that breaks reuse
from sync workers, task runners, and tests.

This pins the rule from the project memory
(`feedback_central_control_points_runner_agnostic`). Currently clean — the guard
keeps it that way.
"""
from __future__ import annotations

import ast

from tests._repo_paths import CORE_SRC

# Relative-to-CORE_SRC control-point modules.
_CONTROL_POINTS = (
    "tools/async_utils.py",
    "modules/concurrency.py",
    "tools/cache.py",
    "tools/cache_valkey.py",
    "modules/cache/cache_module.py",
    "modules/cache/cache_config.py",
)

# Web-framework top-level package names that must never appear in a control point.
_FORBIDDEN_ROOTS = frozenset({"fastapi", "starlette"})


def _forbidden_imports(tree: ast.Module) -> list[str]:
    hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                if root in _FORBIDDEN_ROOTS:
                    hits.append(f"import {alias.name}")
        elif isinstance(node, ast.ImportFrom) and node.module:
            root = node.module.split(".")[0]
            if root in _FORBIDDEN_ROOTS:
                hits.append(f"from {node.module} import ...")
    return hits


def test_control_points_have_no_web_framework_imports() -> None:
    offenders: list[str] = []
    for rel in _CONTROL_POINTS:
        path = CORE_SRC / rel
        if not path.is_file():
            # A control point was renamed/moved — fail loudly so this guard
            # cannot silently scan nothing (cf. tests/_repo_paths rationale).
            offenders.append(f"{rel}: MISSING (update _CONTROL_POINTS)")
            continue
        for hit in _forbidden_imports(ast.parse(path.read_text(encoding="utf-8"))):
            offenders.append(f"{rel}: {hit}")

    assert not offenders, (
        "Framework control points must stay runner-agnostic (no FastAPI/Starlette "
        "imports). Move web-framework concerns to the extension/route layer:\n  "
        + "\n  ".join(offenders)
    )
