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
"""
from __future__ import annotations

import os
import tomllib
from importlib.metadata import PackageNotFoundError, version

import pytest

from tests._repo_paths import REPO_ROOT


def _expected_ext_dists() -> dict[str, str]:
    """Map each extension distribution name → its packages/extensions/<dir>."""
    out: dict[str, str] = {}
    for pyproject in sorted((REPO_ROOT / "packages" / "extensions").glob("*/pyproject.toml")):
        data = tomllib.loads(pyproject.read_text())
        out[data["project"]["name"]] = pyproject.parent.name
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
