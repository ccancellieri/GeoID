"""Guard: load-bearing cross-package re-exports must keep resolving.

Some symbols are defined in one module but imported and re-exported by another,
then consumed elsewhere via the re-exporting module's path (``module.name``).
These are invisible to a per-file F401 check — the symbol genuinely isn't
referenced inside the file that re-exports it — so an ``unused-import`` sweep
(``ruff --fix``, the #1547 campaign, or its CI ratchet) will silently strip
them. That removal is only observed at runtime, as an ``AttributeError`` /
``ImportError`` on a code path the sweep never exercised.

This already bit us twice: PR #1705 (refs #1547) removed
``maintenance_tools.acquire_startup_lock`` (6 module lifespans broke at startup)
and ``db_config.tools.managed_transaction`` (the OGC Features extension dropped
out of discovery), both fixed in PR #1745. This test is the standing tripwire
for that class of regression (#1747).

Two complementary guards:

1. **Runtime resolution** — every protected path below must import and resolve.
   Catches removal regardless of cause (F401 sweep, a moved definition, a
   renamed consumer path).
2. **``__all__`` membership** — where a re-exporting module declares ``__all__``,
   each protected symbol must appear in it. A name in ``__all__`` is never
   reported as F401 and never stripped by ``ruff --fix`` (verified behaviour),
   so this is the durable, machine-checkable protection that survives an edit
   accidentally dropping the per-line suppression directive.

``PROTECTED_REEXPORTS`` is the single registry of these load-bearing paths —
add to it whenever a new cross-package re-export is introduced.
"""
from __future__ import annotations

import importlib

import pytest

# module path -> symbols that MUST resolve via ``getattr(module, symbol)``.
#
# Keep this list focused on genuine *cross-package* re-exports (defined in one
# module, surfaced from another) that downstream code reaches via the
# re-exporting path — not every public name.
PROTECTED_REEXPORTS: dict[str, tuple[str, ...]] = {
    # The core plugin-discovery API, re-exported from dynastore.tools.discovery
    # and consumed everywhere as ``dynastore.modules.get_protocol(...)``.
    "dynastore.modules": ("get_protocol", "get_protocols"),
    # Startup-lock + cron helpers defined in locking_tools, plus the query
    # primitives re-exported back into locking_tools; consumed as
    # ``maintenance_tools.<name>`` by module lifespans and tenant cron callers.
    "dynastore.modules.db_config.maintenance_tools": (
        "acquire_startup_lock",
        "check_cron_job_exists",
        "DDLQuery",
        "DQLQuery",
        "DbResource",
        "ResultHandler",
    ),
    # Canonical task models defined in dynastore.models.tasks, re-exported here
    # for backward compatibility and imported via this path across the codebase.
    "dynastore.modules.tasks.models": (
        "TaskStatusEnum",
        "TaskExecutionMode",
        "TaskExecutionScope",
        "TaskPayload",
        "TaskBase",
        "TaskCreate",
        "TaskUpdate",
        "Task",
    ),
    # managed_transaction is defined in query_executor; the OGC Features
    # extension (and many modules) import it from this public path. Guarding the
    # path documents it as load-bearing even though it lives where it is defined.
    "dynastore.modules.db_config.query_executor": ("managed_transaction",),
}


def _params() -> list[tuple[str, str]]:
    return [(mod, sym) for mod, syms in PROTECTED_REEXPORTS.items() for sym in syms]


@pytest.mark.parametrize("module_path, symbol", _params())
def test_reexport_resolves(module_path: str, symbol: str) -> None:
    """The protected re-export path imports and the symbol is present."""
    module = importlib.import_module(module_path)
    assert hasattr(module, symbol), (
        f"{module_path}.{symbol} no longer resolves — a load-bearing "
        f"cross-package re-export was removed (likely an F401/unused-import "
        f"sweep). Restore the import (and keep it in __all__); see #1747."
    )


@pytest.mark.parametrize("module_path", list(PROTECTED_REEXPORTS))
def test_protected_symbols_in_all_when_declared(module_path: str) -> None:
    """Where a re-exporting module declares ``__all__``, every protected symbol
    must be listed in it — this is what makes the re-export immune to
    ``ruff --fix`` removing it.
    """
    module = importlib.import_module(module_path)
    declared = getattr(module, "__all__", None)
    if declared is None:
        pytest.skip(f"{module_path} does not declare __all__ (runtime guard only)")
    missing = [s for s in PROTECTED_REEXPORTS[module_path] if s not in declared]
    assert not missing, (
        f"{module_path} declares __all__ but is missing protected re-export(s) "
        f"{missing}; add them so an F401 sweep cannot strip them (#1747)."
    )
