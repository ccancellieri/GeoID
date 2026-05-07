"""Drift detector for the per-service dev-compose config dirs.

``packages/core/src/dynastore/docker/config/<svc>/defaults/`` is duplicated across the
five local-dev services because Docker bind mounts cannot overlay a sub-
directory inside a ``:ro`` parent (see
``packages/core/src/dynastore/docker/config/example/README.md`` "Pitfall" section). This
test catches drift between the copies — every ``defaults/*.json`` file in
every service dir must hold identical content. Without this, a routing
change to one service silently makes the other four wrong, and the gate
behaves inconsistently across deployments.

Per-service ``instance.json`` files differ on purpose (each declares its
own ``service_name``) and are not checked here.
"""
from __future__ import annotations

from pathlib import Path

import pytest

# Repo-relative path: this test lives at tests/dynastore/, parents[2] is the repo root.
_CONFIG_ROOT = Path(__file__).resolve().parents[2] / "src" / "dynastore" / "docker" / "config"
_SERVICES = ("catalog", "geoid", "maps", "tools", "worker")


def _read_defaults_tree(svc: str) -> dict[str, str]:
    """Map filename → content for every JSON file under ``<svc>/defaults/``."""
    defaults_dir = _CONFIG_ROOT / svc / "defaults"
    if not defaults_dir.is_dir():
        return {}
    return {
        p.name: p.read_text()
        for p in sorted(defaults_dir.iterdir())
        if p.is_file() and p.suffix == ".json"
    }


def test_all_dev_service_dirs_have_defaults():
    missing = [
        svc for svc in _SERVICES
        if not (_CONFIG_ROOT / svc / "defaults").is_dir()
    ]
    assert not missing, (
        f"docker/config/<svc>/defaults/ missing for: {missing}. "
        "Each dev service needs a self-contained config tree — see "
        "packages/core/src/dynastore/docker/config/example/README.md (Pitfall section)."
    )


def test_all_dev_service_dirs_have_instance_json():
    for svc in _SERVICES:
        instance_path = _CONFIG_ROOT / svc / "instance.json"
        assert instance_path.is_file(), f"{svc}/instance.json is missing"


@pytest.mark.parametrize("svc", _SERVICES)
def test_instance_json_service_name_matches_dir(svc: str):
    """instance.json must declare the same name as its containing directory."""
    import json

    instance = json.loads((_CONFIG_ROOT / svc / "instance.json").read_text())
    assert instance.get("service_name") == svc, (
        f"{svc}/instance.json service_name={instance.get('service_name')!r} "
        f"does not match dir name {svc!r}"
    )


def test_defaults_content_identical_across_services():
    """Every ``defaults/*.json`` must be byte-identical across all five
    service dirs. Catalog is the canonical reference; differences are
    reported as service → diverging filenames so a CI failure points
    straight at the file that drifted.
    """
    canonical = _read_defaults_tree("catalog")
    drift: dict[str, list[str]] = {}
    for svc in _SERVICES:
        if svc == "catalog":
            continue
        svc_tree = _read_defaults_tree(svc)
        diverged = sorted(
            name for name in (set(canonical) | set(svc_tree))
            if canonical.get(name) != svc_tree.get(name)
        )
        if diverged:
            drift[svc] = diverged
    assert not drift, (
        f"docker/config/<svc>/defaults/*.json has drifted from catalog/: {drift}. "
        "Re-sync by copying catalog/defaults/<file> over the others, or update "
        "catalog/defaults/<file> to be the new canonical version."
    )
