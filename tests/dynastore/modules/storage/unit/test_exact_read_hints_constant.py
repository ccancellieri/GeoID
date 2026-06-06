#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Pin the EXACT_READ_HINTS constant and its relationship with Hint.GEOMETRY_EXACT.

Verifies:
- EXACT_READ_HINTS is a frozenset containing exactly Hint.GEOMETRY_EXACT.
- It is distinct from the empty frozenset (no accidental no-op).
- It is a strict subset of a driver surface that includes GEOMETRY_EXACT.
- It is NOT a subset of a driver surface that only has GEOMETRY_SIMPLIFIED.
"""

from dynastore.modules.storage.hints import EXACT_READ_HINTS, Hint


def test_exact_read_hints_is_frozenset():
    assert isinstance(EXACT_READ_HINTS, frozenset)


def test_exact_read_hints_contains_geometry_exact():
    assert Hint.GEOMETRY_EXACT in EXACT_READ_HINTS


def test_exact_read_hints_is_singleton():
    assert len(EXACT_READ_HINTS) == 1


def test_exact_read_hints_not_empty():
    assert EXACT_READ_HINTS != frozenset()


def test_exact_read_hints_matches_exact_capable_driver_surface():
    # A driver advertising GEOMETRY_EXACT satisfies the hint set.
    driver_surface = frozenset({Hint.GEOMETRY_EXACT, Hint.FEATURES})
    assert EXACT_READ_HINTS.issubset(driver_surface)


def test_exact_read_hints_does_not_match_simplified_only_surface():
    # A driver that only carries GEOMETRY_SIMPLIFIED cannot serve the exact hint.
    driver_surface = frozenset({Hint.GEOMETRY_SIMPLIFIED, Hint.SEARCH})
    assert not EXACT_READ_HINTS.issubset(driver_surface)


def test_exact_read_hints_string_equality():
    # StrEnum: "geometry_exact" string must equal the Hint member.
    assert "geometry_exact" in EXACT_READ_HINTS
