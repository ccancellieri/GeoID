"""Code-review guard: don't lump asyncio.CancelledError with Exception and swallow.

``except (asyncio.CancelledError, Exception): pass`` is an antipattern:

* ``CancelledError`` is the *expected* outcome of awaiting a task you just
  ``.cancel()``-ed — swallowing it is fine, but lumping it with ``Exception``
  also silently eats genuine shutdown/teardown errors, hiding real failures.
* it suppresses cooperative cancellation propagation when the *outer* task is
  cancelled while sitting in the ``await``.

The correct shape separates the two — swallow the expected cancellation, log
(or handle) the unexpected error::

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass  # expected — we just cancelled it
    except Exception:
        logger.warning("... errored during shutdown", exc_info=True)

This guard flags only the unambiguous silent form: a single handler catching
both a cancellation type and ``Exception``/``BaseException`` whose body is just
``pass``. Handlers that catch both but *log/handle* (non-``pass`` body) are not
flagged — separating them is still preferable but not silently dangerous.
"""
from __future__ import annotations

import ast

from tests._repo_paths import CORE_SRC, EXTENSIONS_ROOTS

# Intentional, justified exceptions — keep EMPTY.
_ALLOWLIST: frozenset[str] = frozenset()


def _caught_names(node: ast.expr | None) -> set[str]:
    if node is None:
        return set()
    elts = node.elts if isinstance(node, ast.Tuple) else [node]
    names: set[str] = set()
    for e in elts:
        if isinstance(e, ast.Attribute):
            names.add(e.attr)
        elif isinstance(e, ast.Name):
            names.add(e.id)
    return names


def _body_is_only_pass(handler: ast.ExceptHandler) -> bool:
    body = [
        s
        for s in handler.body
        if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))
    ]
    return len(body) == 1 and isinstance(body[0], ast.Pass)


def test_no_cancel_plus_exception_swallow() -> None:
    """No ``except (CancelledError, Exception): pass`` across packages."""
    offenders: list[str] = []
    roots = [CORE_SRC, *EXTENSIONS_ROOTS]
    for root in roots:
        for path in sorted(p for p in root.rglob("*.py") if "__pycache__" not in p.parts):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ExceptHandler):
                    continue
                names = _caught_names(node.type)
                if "CancelledError" not in names:
                    continue
                if not ({"Exception", "BaseException"} & names):
                    continue
                if not _body_is_only_pass(node):
                    continue
                try:
                    rel = path.relative_to(CORE_SRC)
                except ValueError:
                    rel = path.name
                if f"{rel}:{node.lineno}" in _ALLOWLIST:
                    continue
                offenders.append(f"{path}:{node.lineno}")

    assert not offenders, (
        "`except (asyncio.CancelledError, Exception): pass` silently eats both "
        "cancellation and real errors. Split the handler: swallow CancelledError, "
        "log the Exception (see tools/async_utils + the docstring of this test):\n  "
        + "\n  ".join(offenders)
    )
