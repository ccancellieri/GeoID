"""Guard: the CI test image must install EVERY extension.

The root `all` extra is the union of the three runtime services
(api_full + geoid_service + worker_service), not every extension. Building the
test image with SCOPE=all therefore left the standalone OGC API extensions that
no standard service composes (edr, dggs, moving_features, connected_systems,
proxy, template, httpx, notebooks) uninstalled. Their test modules then failed
to *collect* with ModuleNotFoundError, cascading into mass per-job red on the
nightly run.

The fix builds the test image with SCOPE=everything (pyproject `everything`
extra = `all` + every `extension_*` extra). This guard turns any future drift
into a single clear failure naming the missing distributions instead of a
cryptic flood of collection errors: when DYNASTORE_ASSERT_ALL_EXTENSIONS is set
(the CI test runs), every distribution under packages/extensions/ must be
installed.

The companion guard ``test_every_declared_entrypoint_is_loadable`` catches the
*stale editable install* shape (#1366): a developer venv whose
``dynastore-ext-<x>`` distribution metadata exists but whose
``[project.entry-points."dynastore.extensions"]`` declaration was added after
the venv was last fully synced — so ``entry_points(group="dynastore.extensions")``
omits the name and ``enable_extensions("<x>")`` markers silently no-op, leaving
tests to hit 404 on routes that the extension would have mounted.
"""
from __future__ import annotations

import os
import tomllib
from importlib.metadata import PackageNotFoundError, entry_points, version

import pytest

from tests._repo_paths import REPO_ROOT

_EXT_ENTRYPOINT_GROUP = "dynastore.extensions"


def _expected_ext_dists() -> dict[str, str]:
    """Map each extension distribution name → its packages/extensions/<dir>."""
    out: dict[str, str] = {}
    for pyproject in sorted((REPO_ROOT / "packages" / "extensions").glob("*/pyproject.toml")):
        data = tomllib.loads(pyproject.read_text())
        out[data["project"]["name"]] = pyproject.parent.name
    return out


def _declared_entrypoints() -> dict[str, str]:
    """Map each declared ``dynastore.extensions`` entry-point name →
    packages/extensions/<dir> where it is declared.
    """
    out: dict[str, str] = {}
    for pyproject in sorted((REPO_ROOT / "packages" / "extensions").glob("*/pyproject.toml")):
        data = tomllib.loads(pyproject.read_text())
        ep_section = (
            data.get("project", {})
            .get("entry-points", {})
            .get(_EXT_ENTRYPOINT_GROUP, {})
        )
        for name in ep_section:
            out[name] = pyproject.parent.name
    return out


def _missing(expected: dict[str, str]) -> list[str]:
    missing = []
    for dist, dirname in expected.items():
        try:
            version(dist)
        except PackageNotFoundError:
            missing.append(f"{dist} (packages/extensions/{dirname})")
    return sorted(missing)


def test_every_extension_is_installed() -> None:
    expected = _expected_ext_dists()
    assert expected, "no extensions discovered under packages/extensions/"

    missing = _missing(expected)

    if not os.environ.get("DYNASTORE_ASSERT_ALL_EXTENSIONS"):
        if missing:
            pytest.skip(
                "partial extension set (set DYNASTORE_ASSERT_ALL_EXTENSIONS=1 to "
                f"enforce); missing {len(missing)}/{len(expected)}: {missing}"
            )
        return

    assert not missing, (
        "Test image is missing extension packages — pytest will fail to collect "
        "their test modules with ModuleNotFoundError. Build the test image with "
        "SCOPE=everything (pyproject `everything` extra). "
        f"Missing {len(missing)}/{len(expected)}: {missing}"
    )


def test_every_declared_entrypoint_is_loadable() -> None:
    """Every declared ``dynastore.extensions`` entry-point must surface in the
    active interpreter's ``entry_points(group=...)``.

    Catches the stale-editable-install drift (#1366): the extension's
    distribution metadata is present (so ``test_every_extension_is_installed``
    is happy) but the entry-point was added to ``pyproject.toml`` after the
    venv was last fully synced. Tests then fail with a confusing 404 on routes
    the extension would have mounted — ``enable_extensions("<name>")`` has
    nothing to enable. Failing here, at collection, points straight at the
    fix instead.
    """
    declared = _declared_entrypoints()
    assert declared, (
        f"no [project.entry-points.\"{_EXT_ENTRYPOINT_GROUP}\"] declarations "
        "found under packages/extensions/*/pyproject.toml — repo layout drift"
    )

    installed = {ep.name for ep in entry_points(group=_EXT_ENTRYPOINT_GROUP)}
    missing = sorted(
        f"{name} (packages/extensions/{dirname})"
        for name, dirname in declared.items()
        if name not in installed
    )

    if not missing:
        return

    if not os.environ.get("DYNASTORE_ASSERT_ALL_EXTENSIONS"):
        # Local dev: surface the drift as a skip with the exact fix commands so
        # the developer doesn't chase a 404 inside an unrelated test.
        fix_lines = "\n".join(
            f"  .venv/bin/pip install -e packages/extensions/{dirname} --no-deps"
            for _, dirname in (
                (name, declared[name])
                for name in declared
                if name not in installed
            )
        )
        pytest.skip(
            f"{len(missing)} extension(s) declared but missing from "
            f"entry_points(group='{_EXT_ENTRYPOINT_GROUP}'): {missing}.\n"
            f"Re-sync the venv (uv sync --all-extras) or install editable:\n"
            f"{fix_lines}"
        )

    pytest.fail(
        f"{len(missing)} declared extension entry-point(s) are not surfaced by "
        f"the active interpreter: {missing}. The venv is stale; re-sync with "
        "`uv sync --all-extras` or `pip install -e packages/extensions/<name> "
        "--no-deps` per extension. See #1366."
    )
