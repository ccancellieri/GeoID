"""Sync-tier import-isolation regression for ``db_config/``.

The ``db_config`` module tree is shared between async services and sync-only
worker images. ``dynastore.tools.discovery.register_plugin`` wraps each
entry-point load in ``try/except ImportError`` and silently skips on
failure — so any top-level import of a dep that the sync image does not
install causes ``db_config`` to disappear from sync-image discovery,
``app_state.db_config`` goes unset, and the sync ``DatastoreModule.lifespan``
crashes on ``get_config(app_state)`` with ``AttributeError`` masquerading as
a foundational-module startup failure.

See ``docs/architecture/database.md`` "Import-time isolation" + #909.
PR #849 (2026-05-16) introduced the exact regression this test pins.

Two complementary layers:

1. **Static AST scan** — pins ``asyncpg`` specifically, since that is the
   one dep sync images never install and the one that fired in production.
   Other async-tier imports (``sqlalchemy.ext.asyncio``, etc.) are fine to
   appear at top level because base ``sqlalchemy`` ships them in any venv
   that has sqlalchemy at all.
2. **Dynamic import** of ``DBConfigModule`` with ``asyncpg`` hidden from
   ``sys.modules`` — catches anything else that would block the import
   chain in a sync-only image, even if a future dep becomes the new
   regressor.
"""
from __future__ import annotations

import ast
import pathlib
import subprocess
import sys
import textwrap

import pytest

from tests._repo_paths import CORE_SRC as _CORE_SRC


_DB_CONFIG_DIR = _CORE_SRC / "modules" / "db_config"

# Deps that the sync-only worker image does NOT install. A top-level import
# of these in any db_config/ module causes silent entry-point drop at
# discovery time. Function-local imports inside async-tier-only code paths
# are allowed (query_executor.py already does this for asyncpg.exceptions).
_FORBIDDEN_TOP_LEVEL_IMPORTS = {
    "asyncpg",
}


def _iter_db_config_modules() -> list[pathlib.Path]:
    return sorted(p for p in _DB_CONFIG_DIR.rglob("*.py") if "__pycache__" not in p.parts)


def _top_level_imports(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


@pytest.mark.parametrize("path", _iter_db_config_modules(), ids=lambda p: str(p.relative_to(_DB_CONFIG_DIR)))
def test_db_config_module_has_no_async_only_top_level_import(path: pathlib.Path) -> None:
    """Each .py under db_config/ must not top-level-import async-only deps.

    Sync-only worker images do not install ``asyncpg``; a top-level import
    would make the discovery loader drop the whole module silently.
    """
    tree = ast.parse(path.read_text())
    top_level = _top_level_imports(tree)
    forbidden_hits = {
        name for name in top_level
        if any(name == forbidden or name.startswith(f"{forbidden}.") for forbidden in _FORBIDDEN_TOP_LEVEL_IMPORTS)
    }
    assert not forbidden_hits, (
        f"{path.relative_to(_CORE_SRC)} has top-level async-only imports {forbidden_hits}; "
        f"guard with try/except ImportError or move to a function-local import. "
        f"See docs/architecture/database.md 'Import-time isolation'."
    )


def test_dbconfig_module_imports_without_asyncpg() -> None:
    """``DBConfigModule`` must import successfully in a venv lacking ``asyncpg``.

    Runs in a fresh subprocess so the sys.modules patching can't pollute
    sibling tests in the same xdist worker. The subprocess installs an
    import-hook that makes every ``import asyncpg`` raise ``ImportError``
    — the same shape a pip-uninstalled asyncpg produces in a sync-only
    worker image.
    """
    probe = textwrap.dedent(
        """
        import importlib.abc
        import importlib.machinery
        import sys

        class _BlockAsyncpg(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path, target=None):
                if fullname == "asyncpg" or fullname.startswith("asyncpg."):
                    raise ImportError(f"asyncpg blocked (sync-only image simulation): {fullname}")
                return None

        sys.meta_path.insert(0, _BlockAsyncpg())
        # Evict any pre-cached asyncpg modules so the hook actually fires.
        for name in [n for n in sys.modules if n == "asyncpg" or n.startswith("asyncpg.")]:
            del sys.modules[name]

        from dynastore.modules.db_config.db_config_service import DBConfigModule
        assert DBConfigModule is not None
        print("OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        f"DBConfigModule failed to import without asyncpg.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert result.stdout.strip().endswith("OK")
