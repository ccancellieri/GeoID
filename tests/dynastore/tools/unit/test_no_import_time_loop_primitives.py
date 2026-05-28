"""Code-review guard: no event-loop-bound asyncio primitives at import time.

An ``asyncio.Lock()`` (or ``Event``/``Queue``/``Semaphore``/``Condition``)
constructed at **module-level or class-body scope** is created at import time,
before any event loop exists. It then implicitly binds to whichever loop first
awaits it and raises ``RuntimeError`` if reused from another loop. Consequences:

* breaks under multiple event loops — tests spin up a fresh loop per case, and
  multi-worker processes likewise;
* a single primitive object is silently shared across every instance/loop,
  giving false cross-instance safety in our distributed-stateless runtime where
  only PG/ES/Valkey are shared (an in-process lock never coordinates pods).

This is the generalisation of the ``IamModule.__init__`` lock removed in #1501.
The correct pattern is a per-running-loop primitive created lazily on first use
— see ``dynastore.tools.async_utils.LoopLocalLock`` (drop-in for ``async with``)
and ``SignalBus`` (per-loop signal registry).

Function-local primitives (created inside an ``async def`` / method while a loop
is running) are fine and NOT flagged — only module-level and class-body
constructions are.
"""
from __future__ import annotations

import ast
import pathlib

from tests._repo_paths import CORE_SRC, EXTENSIONS_ROOTS

# Loop-bound asyncio synchronisation primitives whose construction must not
# happen at import time.
_PRIMITIVES = frozenset(
    {
        "Lock",
        "Event",
        "Condition",
        "Semaphore",
        "BoundedSemaphore",
        "Queue",
        "PriorityQueue",
        "LifoQueue",
    }
)

# Intentional, justified exceptions — keep EMPTY. If a genuine import-time
# primitive is ever required, add ``"<path-relative-to-package-root>:<lineno>"``
# here with a comment explaining why it is loop-safe.
_ALLOWLIST: frozenset[str] = frozenset()


def _iter_source_files() -> list[pathlib.Path]:
    roots = [CORE_SRC, *EXTENSIONS_ROOTS]
    files: list[pathlib.Path] = []
    for root in roots:
        files.extend(p for p in root.rglob("*.py") if "__pycache__" not in p.parts)
    return sorted(files)


def _asyncio_aliases(tree: ast.Module) -> set[str]:
    """Names bound by ``from asyncio import Lock [as X]`` (so a bare ``Lock()``
    construction is also caught, not just the ``asyncio.Lock()`` form)."""
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "asyncio":
            for a in node.names:
                if a.name in _PRIMITIVES:
                    aliases.add(a.asname or a.name)
    return aliases


def _is_primitive_construction(value: ast.AST, aliases: set[str]) -> str | None:
    if not isinstance(value, ast.Call):
        return None
    func = value.func
    # asyncio.Lock(...)
    if (
        isinstance(func, ast.Attribute)
        and func.attr in _PRIMITIVES
        and isinstance(func.value, ast.Name)
        and func.value.id == "asyncio"
    ):
        return func.attr
    # bare Lock(...) when imported from asyncio
    if isinstance(func, ast.Name) and func.id in aliases:
        return func.id
    return None


def _scan(tree: ast.Module, aliases: set[str]) -> list[tuple[int, str, str]]:
    """Return (lineno, scope, primitive) for module-level and class-body
    assignments that construct a loop-bound primitive."""
    found: list[tuple[int, str, str]] = []

    def check(stmt: ast.stmt, scope: str) -> None:
        value = getattr(stmt, "value", None)
        if value is None:
            return
        prim = _is_primitive_construction(value, aliases)
        if prim is not None:
            found.append((stmt.lineno, scope, prim))

    for stmt in tree.body:
        if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
            check(stmt, "module")
        elif isinstance(stmt, ast.ClassDef):
            for inner in stmt.body:
                if isinstance(inner, (ast.Assign, ast.AnnAssign)):
                    check(inner, f"class {stmt.name}")
    return found


def test_no_import_time_loop_primitives() -> None:
    """No module-level / class-body asyncio loop primitives across packages."""
    offenders: list[str] = []
    for path in _iter_source_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        aliases = _asyncio_aliases(tree)
        for lineno, scope, prim in _scan(tree, aliases):
            try:
                rel = path.relative_to(CORE_SRC)
            except ValueError:
                rel = path.name
            key = f"{rel}:{lineno}"
            if key in _ALLOWLIST:
                continue
            offenders.append(f"{path}:{lineno} [{scope}] asyncio.{prim}()")

    assert not offenders, (
        "Event-loop-bound asyncio primitives constructed at import time "
        "(module-level or class-body). These bind to the first loop that "
        "awaits them and break across loops/instances. Use "
        "dynastore.tools.async_utils.LoopLocalLock (or a per-loop lazy "
        "pattern) instead:\n  " + "\n  ".join(offenders)
    )
