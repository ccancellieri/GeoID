#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

#!/usr/bin/env python3
"""Guard: the framework-free core must not import the web framework.

`packages/core/src/dynastore/modules/` (which includes `modules/tasks/`) is the
framework-free core. Modules are imported by tasks and by extensions and run in
contexts that have no HTTP request — background runners, Cloud Run jobs, CLI.
They must therefore NEVER import `fastapi` or `starlette`. Extensions own all
HTTP; a module signals failure by raising a domain exception which the extension
boundary maps to a response via `extensions/tools/exception_handlers.py`.

This rule is the core's own long-standing convention (see e.g.
`modules/iam/authorization/iam_authorizer.py` "must stay framework-free: no
FastAPI") that eroded over time; #1969 removed the accumulated leak and this
guard makes the regression unrepeatable. Sub-task of the #1504 cleanup campaign.

The check is AST-based, so comments, docstrings and string literals that merely
mention the framework names are ignored — only real `import` statements count.

Pure stdlib, Python 3.12+. Exit 0 if the core is clean, exit 1 otherwise.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# The framework-free pillar. `modules/tasks/` lives under this tree, so a single
# recursive scan covers both the modules and tasks pillars.
CORE_ROOT = REPO_ROOT / "packages/core/src/dynastore/modules"

# Top-level package names a module may never import (any submodule too).
BANNED_ROOTS = ("fastapi", "starlette")


def _banned(module: str | None) -> bool:
    """True if a dotted module path is rooted at a banned top-level package."""
    if not module:
        return False
    root = module.split(".", 1)[0]
    return root in BANNED_ROOTS


def _violations_in(path: Path) -> list[tuple[int, str]]:
    """Return (lineno, statement) for every banned import in one file."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (SyntaxError, UnicodeDecodeError) as exc:  # pragma: no cover
        print(f"  ! could not parse {path}: {exc}", file=sys.stderr)
        return []

    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _banned(alias.name):
                    found.append((node.lineno, f"import {alias.name}"))
        elif isinstance(node, ast.ImportFrom):
            # Relative imports (level > 0) have no external root — never banned.
            if node.level == 0 and _banned(node.module):
                names = ", ".join(a.name for a in node.names)
                found.append((node.lineno, f"from {node.module} import {names}"))
    return found


def main() -> int:
    if not CORE_ROOT.is_dir():
        print(f"core path not found: {CORE_ROOT}", file=sys.stderr)
        return 1

    offenders: list[tuple[Path, int, str]] = []
    for py in sorted(CORE_ROOT.rglob("*.py")):
        for lineno, stmt in _violations_in(py):
            offenders.append((py.relative_to(REPO_ROOT), lineno, stmt))

    if not offenders:
        print(
            "OK: no fastapi/starlette imports under "
            f"{CORE_ROOT.relative_to(REPO_ROOT)} (core stays framework-free)."
        )
        return 0

    print(
        "FAIL: the framework-free core imports a web framework. Modules and "
        "tasks must never depend on fastapi/starlette (#1969). Move the HTTP "
        "code to an extension; raise a domain exception instead of "
        "HTTPException and let extensions/tools/exception_handlers.py map it.\n",
        file=sys.stderr,
    )
    for rel, lineno, stmt in offenders:
        print(f"  {rel}:{lineno}: {stmt}", file=sys.stderr)
    print(f"\n{len(offenders)} violation(s).", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
