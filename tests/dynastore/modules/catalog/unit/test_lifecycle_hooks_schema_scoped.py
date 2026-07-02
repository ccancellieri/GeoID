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

"""Guard test pinning the #2749 wedge invariant for catalog lifecycle hooks.

#2749 was a production wedge: a lifecycle hook ran ``CREATE TABLE ...
PARTITION OF`` against a table OUTSIDE the tenant's own physical schema
(a shared parent table), on a connection that stayed open for the rest of
the caller's transaction — an ACCESS EXCLUSIVE lock held on shared state,
blocking every other tenant. The mechanism is gone (#2763), but nothing
stopped a future hook from reintroducing the same class of mistake.

Every hook registered via ``sync_catalog_initializer`` / ``sync_collection_initializer``
(and siblings — post-create, destroyers, hard-destroyers, asset hooks) under
``modules/catalog/`` receives the tenant's own physical ``schema`` as an
argument specifically so it never needs to touch anything else. This test
statically scans every such hook's source for a hardcoded (non-tenant)
schema-qualified table reference — the shape ``"some_schema"."table"`` with
a literal schema name — as opposed to the tenant-scoped ``"{schema}".table``
f-string qualifier every hook is expected to use.

Static/source-level rather than runtime: it catches the mistake at review
time without needing a live Postgres to exercise every hook.
"""
from __future__ import annotations

import ast
import pathlib
import re
from typing import Iterator, Tuple

_HOOK_DECORATOR_NAMES = {
    "sync_catalog_initializer",
    "sync_collection_initializer",
    "sync_catalog_post_create",
    "sync_catalog_destroyer",
    "sync_collection_destroyer",
    "sync_collection_hard_destroyer",
    "sync_asset_initializer",
    "sync_asset_destroyer",
}

# A hardcoded, double-quoted "schema"."table" reference — the shape every
# raw-SQL schema qualifier in this codebase uses for a *literal* schema
# name. The tenant-scoped qualifier every hook is expected to use instead
# is an f-string placeholder, e.g. ``f'"{schema}".assets'`` — its raw source
# text is ``"{schema}"``, where the character right after the opening quote
# is ``{``, not a letter/underscore, so it does NOT match this pattern.
_HARDCODED_SCHEMA_QUALIFIER_RE = re.compile(r'"[A-Za-z_][A-Za-z0-9_]*"\s*\.\s*"')


def _iter_catalog_module_py_files() -> Iterator[pathlib.Path]:
    import dynastore.modules.catalog as catalog_pkg

    root = pathlib.Path(catalog_pkg.__file__).parent
    yield from root.rglob("*.py")


def _iter_lifecycle_hook_functions() -> Iterator[Tuple[pathlib.Path, str, str]]:
    """Yield ``(file, function_name, source)`` for every function decorated
    with a lifecycle-hook registration decorator anywhere under
    ``modules/catalog/``."""
    for path in _iter_catalog_module_py_files():
        src = path.read_text()
        try:
            tree = ast.parse(src, filename=str(path))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for deco in node.decorator_list:
                deco_target = deco.func if isinstance(deco, ast.Call) else deco
                deco_name = getattr(deco_target, "attr", None) or getattr(
                    deco_target, "id", None
                )
                if deco_name in _HOOK_DECORATOR_NAMES:
                    func_src = ast.get_source_segment(src, node) or ""
                    yield path, node.name, func_src
                    break


def test_catalog_lifecycle_hooks_are_registered_and_discoverable():
    """Sanity check for the scanner itself: modules/catalog/ registers at
    least one lifecycle hook today (``_pg_asset_driver_init_tenant`` in
    ``drivers/pg_asset_driver.py``). If this starts failing, the discovery
    logic below is broken and the guard test would be silently vacuous —
    not that the invariant itself holds."""
    hooks = list(_iter_lifecycle_hook_functions())
    assert hooks, (
        "expected at least one lifecycle hook decorated with "
        f"{sorted(_HOOK_DECORATOR_NAMES)} under modules/catalog/ — the "
        "discovery logic is broken if this list is empty."
    )


def test_catalog_lifecycle_hooks_never_hardcode_a_non_tenant_schema():
    """Every catalog lifecycle hook must schema-qualify tables with the
    tenant ``schema`` argument it receives — never a hardcoded schema name.
    Reintroducing a hardcoded schema-qualified table reference in one of
    these hooks is exactly the #2749 wedge class."""
    violations = []
    for path, func_name, func_src in _iter_lifecycle_hook_functions():
        for match in _HARDCODED_SCHEMA_QUALIFIER_RE.finditer(func_src):
            violations.append(f"{path.name}:{func_name} — {match.group(0)!r}")

    assert not violations, (
        "lifecycle hook(s) under modules/catalog/ schema-qualify a table "
        "with a hardcoded (non-tenant) schema name instead of the `schema` "
        "argument they are handed. This is the #2749 wedge class: DDL/DML "
        "against a table outside the caller's own tenant schema, held open "
        f"on the caller's transaction. Violations: {violations}"
    )
