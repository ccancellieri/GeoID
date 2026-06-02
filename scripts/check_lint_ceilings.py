#!/usr/bin/env python3
"""Ratcheting lint gate for the antipattern-cleanup campaign (refs #1547, #1504).

Enforces a per-rule CEILING on a curated set of antipattern / readability
rules that the cleanup campaign has been driving down. The gate FAILS only
when a rule's count *increases* above its committed ceiling — it blocks **new**
violations without requiring the existing backlog to be zero. This is the
standard "ratchet" pattern: the numbers only ever go down.

When a PR reduces a rule below its ceiling, lower the number in ``CEILINGS``
in the same PR so the ground stays won (the gate prints a reminder).

Scope: all first-party source (``packages/core/src`` + ``packages/extensions/*/src``),
excluding notebooks. ruff is pinned to PINNED_RUFF — rule behaviour drifts
across releases, so CI installs that exact version (see
``.github/workflows/lint-ratchet.yml``). Local runs with a different ruff are
advisory; CI is the source of truth.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

PINNED_RUFF = "0.15.15"

# Current counts as of the gate's introduction. Ratchet DOWN as rules improve;
# never raise a ceiling to make a regression pass.
CEILINGS: dict[str, int] = {
    "F401": 233,    # unused-import
    "F811": 11,     # redefined-while-unused (duplicate imports)
    "F841": 35,     # unused-variable
    "B904": 23,     # raise-without-from (broken exception chain)
    "RUF012": 0,    # mutable-class-default (unannotated shared class state) — cleared #1598; ClassVar / ConfigDict / default_factory
    "SIM118": 17,   # `key in d.keys()` -> `key in d`
}

REPO_ROOT = Path(__file__).resolve().parents[1]


def _source_roots() -> list[str]:
    roots = sorted(REPO_ROOT.glob("packages/core/src"))
    roots += sorted(REPO_ROOT.glob("packages/extensions/*/src"))
    if not roots:
        sys.exit("check_lint_ceilings: no source roots found under packages/ — layout changed.")
    return [str(p) for p in roots]


def _ruff_cmd() -> list[str]:
    if shutil.which("ruff"):
        return ["ruff"]
    if shutil.which("uv"):
        return ["uv", "tool", "run", f"ruff@{PINNED_RUFF}"]
    sys.exit("check_lint_ceilings: ruff not found (install ruff or uv).")


def _check_version(ruff: list[str]) -> None:
    try:
        out = subprocess.run([*ruff, "--version"], capture_output=True, text=True, check=True)
        version = out.stdout.strip().split()[-1]
    except Exception:
        return
    if version != PINNED_RUFF:
        print(
            f"::warning::ruff {version} != pinned {PINNED_RUFF}; counts are advisory. "
            f"CI installs {PINNED_RUFF}.",
            file=sys.stderr,
        )


def _count(ruff: list[str], roots: list[str], rule: str) -> int:
    cmd = [
        *ruff, "check", *roots,
        "--select", rule,
        "--exclude", "**/notebooks/**",
        "--output-format", "json",
        "--no-cache",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    # ruff exits 1 when violations exist — expected; rely on stdout JSON.
    try:
        return len(json.loads(proc.stdout or "[]"))
    except json.JSONDecodeError:
        sys.exit(f"check_lint_ceilings: could not parse ruff output for {rule}:\n{proc.stderr}")


def main() -> int:
    ruff = _ruff_cmd()
    _check_version(ruff)
    roots = _source_roots()

    print("Lint ratchet gate — scope: packages/core/src + packages/extensions/*/src (no notebooks)\n")
    print(f"  {'rule':<8} {'count':>6} {'ceiling':>8}  status")
    print(f"  {'-'*8} {'-'*6} {'-'*8}  {'-'*10}")

    regressions: list[tuple[str, int, int]] = []
    ratchets: list[tuple[str, int, int]] = []
    for rule, ceiling in sorted(CEILINGS.items()):
        n = _count(ruff, roots, rule)
        if n > ceiling:
            status = "REGRESSION"
            regressions.append((rule, n, ceiling))
        elif n < ceiling:
            status = "improved"
            ratchets.append((rule, n, ceiling))
        else:
            status = "ok"
        print(f"  {rule:<8} {n:>6} {ceiling:>8}  {status}")

    if ratchets:
        print("\nThese rules dropped below their ceiling — lower them in scripts/check_lint_ceilings.py:")
        for rule, n, ceiling in ratchets:
            print(f"  {rule}: {ceiling} -> {n}")

    if regressions:
        print("\nFAIL: new lint violations were introduced above the ratchet ceiling:")
        for rule, n, ceiling in regressions:
            print(f"  {rule}: {n} > {ceiling}")
            print(
                f"    locate: ruff check packages/core/src packages/extensions/*/src "
                f"--select {rule} --exclude '**/notebooks/**'"
            )
        print(
            "\nFix the new violation(s), or — if intentional — justify and adjust the ceiling "
            "with the rationale in the PR description."
        )
        return 1

    print("\nPASS: no rule exceeds its ratchet ceiling.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
