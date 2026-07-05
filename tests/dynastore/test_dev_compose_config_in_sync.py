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

import pytest

from tests._repo_paths import CORE_SRC

_CONFIG_ROOT = CORE_SRC / "docker" / "config"
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
    """Every ``defaults/<name>.json`` shared by two or more config dirs must
    be byte-identical across all the dirs that carry it.

    Not every filename is expected in every dir — e.g.
    ``task-routing-config.json`` only ships to ``worker`` — so this compares
    content per-filename across whichever dirs happen to hold it, rather than
    requiring the full set of filenames to match everywhere. ``example`` is
    included: it is docs-only (never copied into an image) but the README
    explicitly claims its ``defaults/`` is covered by this drift check.

    ``idp-config.json`` is intentionally absent from every dir here: geoid's
    tracked onprem baseline ships no IdP override (the neutral ``IdpConfig``
    pydantic default is unconfigured). The compose-local Keycloak values live
    in ``config/local/defaults/idp-config.json`` instead, covered by
    ``tests/dynastore/modules/iam/unit/test_idp_config_dev_seed.py``.
    """
    dirs = _SERVICES + ("example",)
    trees = {d: _read_defaults_tree(d) for d in dirs}
    all_names = {name for tree in trees.values() for name in tree}

    drift: dict[str, list[str]] = {}
    for name in sorted(all_names):
        contents = {d: tree[name] for d, tree in trees.items() if name in tree}
        if len(set(contents.values())) > 1:
            drift[name] = sorted(contents)
    assert not drift, (
        f"docker/config/<dir>/defaults/ has diverging copies of: {drift}. "
        "Every dir sharing a defaults/<file>.json must keep it byte-identical "
        "— re-sync from whichever copy is canonical."
    )
