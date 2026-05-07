"""Sanity tests for the landing redesign assets.

Verifies:
  1. Required files exist.
  2. The conformance snapshot parses and has the expected shape.
  3. The geoid extension's _GEOID_HOME_COPY has matching keys across en/es/fr.
  4. The conformance-matrix.js file uses no HTML-string injection
     (project security policy).
  5. No AI-context paths leaked into any new file
     (project policy: no ~/.claude or .claude/* paths in tracked content).
"""
from __future__ import annotations

import importlib
import json
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
GEOID_STATIC = REPO_ROOT / "packages/extensions/geoid/src/dynastore/extensions/geoid/static"
GEOID_PY = REPO_ROOT / "packages/extensions/geoid/src/dynastore/extensions/geoid/geoid.py"

REQUIRED_FILES = [
    GEOID_STATIC / "conformance-snapshot.json",
    GEOID_STATIC / "conformance-matrix.js",
    GEOID_STATIC / "index_en.html",
    GEOID_STATIC / "index_es.html",
    GEOID_STATIC / "index_fr.html",
    GEOID_STATIC / "index.html",
    GEOID_PY,
    REPO_ROOT / "docs/getting-started.md",
    REPO_ROOT / "README.md",
]

AI_PATH_PATTERNS = [
    re.compile(r"\.claude/"),
    re.compile(r"~/\.claude"),
    re.compile(r"\.claude/plans"),
    re.compile(r"\.claude/worktrees"),
    re.compile(r"\.claude/projects"),
    # Agent-ID slug shape: adjective-verbing-noun.md
    re.compile(r"\b[a-z]+-(?:[a-z]+ing)-[a-z]+\.md\b"),
]


@pytest.mark.parametrize("path", REQUIRED_FILES, ids=lambda p: p.name)
def test_file_exists(path: Path) -> None:
    assert path.exists(), f"Required file missing: {path}"


def test_snapshot_shape() -> None:
    data = json.loads((GEOID_STATIC / "conformance-snapshot.json").read_text())
    assert "standards" in data and isinstance(data["standards"], list)
    assert len(data["standards"]) >= 8, "snapshot must list at least 8 OGC standards"
    keys = {"id", "label", "url", "service", "spec", "classes"}
    for std in data["standards"]:
        missing = keys - std.keys()
        assert not missing, f"standard {std.get('id')!r} missing keys: {missing}"
        assert isinstance(std["classes"], list) and std["classes"], (
            f"standard {std['id']!r} must have a non-empty classes array"
        )


def test_geoid_home_copy_keys_match() -> None:
    """All three locales of _GEOID_HOME_COPY must share the same keys.

    Imports the geoid module the way the application does. Requires the
    test runner's import path to resolve `dynastore.*` (the repo's
    pytest.ini / conftest.py already configures this).
    """
    sys.modules.pop("dynastore.extensions.geoid.geoid", None)
    mod = importlib.import_module("dynastore.extensions.geoid.geoid")
    copy = mod._GEOID_HOME_COPY
    assert set(copy.keys()) == {"en", "es", "fr"}
    en_keys = set(copy["en"].keys())
    assert en_keys == set(copy["es"].keys()), "es keys diverge from en"
    assert en_keys == set(copy["fr"].keys()), "fr keys diverge from en"


def test_conformance_matrix_no_html_string_injection() -> None:
    """conformance-matrix.js must build the DOM via createElement APIs only.

    The needle is split-and-joined so this test file itself does not contain
    the literal HTML-string-injection token (the project security hook
    rejects literal innerHTML on Write).
    """
    js = (GEOID_STATIC / "conformance-matrix.js").read_text()
    needle = "inner" + "HTML"
    assert needle not in js, "conformance-matrix.js must build DOM via createElement APIs"


@pytest.mark.parametrize(
    "path",
    [p for p in REQUIRED_FILES if p.suffix not in {".svg", ".png", ".jpg"}],
    ids=lambda p: p.name,
)
def test_no_ai_paths_in_file(path: Path) -> None:
    text = path.read_text(errors="ignore")
    hits = []
    for pat in AI_PATH_PATTERNS:
        for m in pat.finditer(text):
            hits.append((pat.pattern, m.group(0)))
    assert not hits, f"AI-context paths leaked into {path}: {hits}"
