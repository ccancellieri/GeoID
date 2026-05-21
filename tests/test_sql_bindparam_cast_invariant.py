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

"""Static guard against the ``:name::cast`` bind-parameter anti-pattern (#1178).

SQLAlchemy ``text()`` does NOT recognise a named bind parameter that is
immediately followed by the ``::`` cast operator: in ``:foo::jsonb`` the
``:foo`` placeholder is silently dropped (or mangled to a bogus name), so the
statement either reaches the database with a bare ``::`` and raises
``syntax error at or near ":"``, or binds the wrong value:

    >>> from sqlalchemy import text
    >>> sorted(text("SELECT :foo::jsonb").compile().params)   # doctest: +SKIP
    []                          # ':foo' dropped — NOT bound

The correct, unambiguous form is ``CAST(:foo AS jsonb)``.

These defects are invisible to mocked unit tests (the bind only resolves at
execution against a real driver), so this pure-static scan is the cheapest
durable guardrail. If a legitimate exception ever arises, add it to
``_WAIVERS`` with a comment — but the right fix is almost always ``CAST(...)``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterator

from tests._repo_paths import CORE_SRC, EXTENSIONS_ROOTS

# A named bind param (``:ident``) immediately followed by the ``::`` cast
# operator and a type name (a letter). This matches the broken SQL forms
# (``:foo::jsonb``, ``:dt::timestamptz``, ``:ids::uuid[]``) while excluding:
#   - literal casts: ``'{}'::jsonb``, ``ST_AsGeoJSON(g)::jsonb`` (no ``:ident``);
#   - OGC CRS URN string literals: ``urn:ogc:def:crs:EPSG::4326`` /
#     ``EPSG::{srid}`` (``::`` followed by a digit or ``{``, not a type name).
# A SQL cast type always starts with a letter, so requiring ``::<letter>``
# keeps the scan precise to the actual defect.
_BIND_CAST_RE = re.compile(r":[a-zA-Z_][a-zA-Z0-9_]*::[a-zA-Z_]")

# Package-relative paths (POSIX) that are knowingly exempt. Empty by design —
# the fix for any hit is to switch to CAST(:name AS type).
_WAIVERS: frozenset[str] = frozenset()


def _iter_py() -> Iterator[Path]:
    for root in (CORE_SRC, *EXTENSIONS_ROOTS):
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            yield path


def _rel(path: Path) -> str:
    for root in (CORE_SRC, *EXTENSIONS_ROOTS):
        try:
            return path.relative_to(root.parent).as_posix()
        except ValueError:
            continue
    return path.as_posix()


def test_no_named_bindparam_adjacent_to_cast_operator():
    """No source file may use the ``:name::cast`` form — it silently drops the
    bind parameter under SQLAlchemy ``text()`` (#1178). Use ``CAST(:name AS
    type)`` instead."""
    offenders: list[str] = []
    for path in _iter_py():
        rel = _rel(path)
        if rel in _WAIVERS:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if "::" not in text:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            for m in _BIND_CAST_RE.finditer(line):
                offenders.append(f"{rel}:{lineno}: {m.group(0)}…  ({line.strip()})")

    assert not offenders, (
        "Found `:name::cast` bind-parameter anti-pattern (#1178) — SQLAlchemy "
        "text() drops the placeholder. Use CAST(:name AS type) instead:\n  "
        + "\n  ".join(offenders)
    )
