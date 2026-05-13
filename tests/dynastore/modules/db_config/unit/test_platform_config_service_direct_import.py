"""Pin the circular-import latent failure on direct
``import dynastore.modules.db_config.platform_config_service`` (#686).

Pytest collection naturally imports through ``dynastore.models.protocols``
first, which warms its ``__init__.py`` in an order that masks the cycle.
External entry points — operator scripts, notebooks, ad-hoc REPL —
trigger the failure path by importing ``platform_config_service``
directly.

The cycle:

  platform_config_service line 74
    -> models.protocols.platform_configs
    -> models.protocols.__init__ line 105
    -> authorization.py (which holds the misplaced ``IamRolesConfig``)
    -> exposure_mixin.py
    -> back to ``platform_config_service.Mutable`` (not yet defined)

The architectural fix is moving ``IamRolesConfig`` + ``RoleSeed`` out of
``models/protocols/authorization.py`` into a proper IAM home so the
Protocol-contracts module stops pulling ``Mutable`` / ``PluginConfig``
into a load-order-sensitive position.  See #686 for the full plan.

This test runs a subprocess with a fresh interpreter so the in-process
module cache from pytest collection can't mask the cycle.  Marked
``xfail(strict=True)``: it stays failing until #686 lands, at which
point the strict flag flips the result and the marker can be removed.
"""

import subprocess
import sys

import pytest


@pytest.mark.xfail(
    strict=True,
    reason=(
        "#686: direct `import platform_config_service` triggers circular "
        "ImportError via IamRolesConfig in models/protocols/authorization.py. "
        "Remove this xfail once #686 ships."
    ),
)
def test_platform_config_service_imports_cleanly_from_fresh_interpreter():
    """Spawn ``python -c 'import platform_config_service'`` and assert exit 0."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import dynastore.modules.db_config.platform_config_service",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Direct import failed (exit {result.returncode}). stderr tail:\n"
        f"{result.stderr[-2000:]}"
    )
