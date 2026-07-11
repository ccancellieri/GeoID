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

"""Code-review guards for dangerous Python antipatterns.

1. **Mutable default arguments** (``def f(x=[])`` / ``={}`` / ``=set()``) — the
   default is created once and shared across every call, so any mutation leaks
   between calls (a classic latent-state bug; ruff/flake8-bugbear B006).
2. **``time.sleep()`` inside ``async def``** — blocks the whole event loop,
   stalling every coroutine on that loop. Use ``await asyncio.sleep()``.
3. **Blocking ``requests.*`` calls inside ``async def``** — the synchronous
   ``requests`` client blocks the event loop for the duration of the network
   round-trip, same failure mode as (2) (see the provisioner-hook heartbeat
   starvation this guards against). Route it through ``run_in_thread``/
   ``asyncio.to_thread`` or an async HTTP client instead.

All three are scanned across all package source and are clean today (no
allowlist). The sidecar ``get_ddl`` mutable defaults that were previously
grandfathered here have been normalised to a ``None`` sentinel (#1561); any
mutable default now fails the build.
"""
from __future__ import annotations

import ast

from tests._repo_paths import CORE_SRC, EXTENSIONS_ROOTS

# No grandfathered sites: the sidecar get_ddl mutable defaults were normalised
# to a None sentinel (#1561). Any mutable default now fails the build.
_MUTABLE_DEFAULT_ALLOWLIST: frozenset[str] = frozenset()


def _iter_source_files():
    for root in (CORE_SRC, *EXTENSIONS_ROOTS):
        for p in root.rglob("*.py"):
            if "__pycache__" not in p.parts:
                yield p


def _has_mutable_default(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    defaults = list(fn.args.defaults) + [d for d in fn.args.kw_defaults if d is not None]
    return any(isinstance(d, (ast.List, ast.Dict, ast.Set)) for d in defaults)


def test_no_new_mutable_default_arguments() -> None:
    offenders: list[str] = []
    for path in _iter_source_files():
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _has_mutable_default(node):
                key = f"{path.name}:{node.name}"
                if key in _MUTABLE_DEFAULT_ALLOWLIST:
                    continue
                offenders.append(f"{path}:{node.lineno} def {node.name}(...=[] / {{}})")
    assert not offenders, (
        "Mutable default argument(s) — the default is shared across calls. Use a "
        "None sentinel and normalise inside the body:\n  " + "\n  ".join(offenders)
    )


def test_no_blocking_time_sleep_in_async() -> None:
    offenders: list[str] = []

    class _V(ast.NodeVisitor):
        def __init__(self, path):
            self.path = path
            self.depth = 0

        def visit_AsyncFunctionDef(self, node):
            self.depth += 1
            self.generic_visit(node)
            self.depth -= 1

        def visit_FunctionDef(self, node):
            # a sync def nested in an async def resets the loop-blocking context
            saved, self.depth = self.depth, 0
            self.generic_visit(node)
            self.depth = saved

        def visit_Call(self, node):
            if self.depth > 0:
                f = node.func
                if (
                    isinstance(f, ast.Attribute)
                    and f.attr == "sleep"
                    and isinstance(f.value, ast.Name)
                    and f.value.id == "time"
                ):
                    offenders.append(f"{self.path}:{node.lineno} time.sleep() in async def")
            self.generic_visit(node)

    for path in _iter_source_files():
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        _V(path).visit(tree)

    assert not offenders, (
        "time.sleep() inside an async function blocks the event loop. "
        "Use `await asyncio.sleep(...)`:\n  " + "\n  ".join(offenders)
    )


_BLOCKING_REQUESTS_METHODS = frozenset(
    {"get", "post", "put", "delete", "patch", "head", "request"}
)


def test_no_blocking_requests_call_in_async() -> None:
    offenders: list[str] = []

    class _V(ast.NodeVisitor):
        def __init__(self, path):
            self.path = path
            self.depth = 0

        def visit_AsyncFunctionDef(self, node):
            self.depth += 1
            self.generic_visit(node)
            self.depth -= 1

        def visit_FunctionDef(self, node):
            # a sync def nested in an async def resets the loop-blocking context
            saved, self.depth = self.depth, 0
            self.generic_visit(node)
            self.depth = saved

        def visit_Call(self, node):
            if self.depth > 0:
                f = node.func
                if (
                    isinstance(f, ast.Attribute)
                    and f.attr in _BLOCKING_REQUESTS_METHODS
                    and isinstance(f.value, ast.Name)
                    and f.value.id == "requests"
                ):
                    offenders.append(f"{self.path}:{node.lineno} requests.{f.attr}() in async def")
            self.generic_visit(node)

    for path in _iter_source_files():
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        _V(path).visit(tree)

    assert not offenders, (
        "requests.*() inside an async function blocks the event loop for the "
        "network round-trip. Offload via `await run_in_thread(...)` or use an "
        "async HTTP client:\n  " + "\n  ".join(offenders)
    )
