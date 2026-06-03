#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Pure-unit tests for the GET /admin/conditions/types endpoint.

No database, no app lifespan, no HTTP stack. The tests verify:

1. ``known_condition_types()`` returns a set that includes the core
   built-in types and the audience-condition types registered by the
   geoid audience handlers.
2. The ``AdminService.list_condition_types`` handler returns
   ``{"types": sorted(known_condition_types())}`` — a sorted list
   consistent with the registry.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# known_condition_types — pure registry coverage
# ---------------------------------------------------------------------------


class TestKnownConditionTypes:
    def _types(self) -> set[str]:
        from dynastore.modules.iam.conditions import known_condition_types

        return known_condition_types()

    def test_contains_rate_limit(self):
        assert "rate_limit" in self._types()

    def test_contains_max_count(self):
        assert "max_count" in self._types()

    def test_contains_time_window(self):
        assert "time_window" in self._types()

    def test_contains_expiration(self):
        assert "expiration" in self._types()

    def test_contains_match(self):
        assert "match" in self._types()

    def test_contains_lookup_only_search(self):
        assert "lookup_only_search" in self._types()

    def test_contains_catalog_lookup_public_allowed(self):
        assert "catalog_lookup_public_allowed" in self._types()

    def test_contains_collection_write_anonymous_allowed(self):
        assert "collection_write_anonymous_allowed" in self._types()

    def test_returns_set_of_strings(self):
        types = self._types()
        assert isinstance(types, set)
        assert all(isinstance(t, str) for t in types)

    def test_non_empty(self):
        assert len(self._types()) > 0


# ---------------------------------------------------------------------------
# AdminService.list_condition_types handler — no PG, no app lifespan
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_condition_types_handler_returns_sorted_list():
    """The handler must return ``{"types": sorted(known_condition_types())}``
    — a sorted list with at least the four audience / core types required
    by the UI."""
    from dynastore.extensions.admin.admin_service import AdminService
    from dynastore.modules.iam.conditions import known_condition_types

    result = await AdminService.list_condition_types()

    assert "types" in result
    types = result["types"]
    assert isinstance(types, list)

    # Result must be sorted.
    assert types == sorted(types), "types list must be sorted"

    # Must include the four types the task spec requires.
    required = {
        "rate_limit",
        "catalog_lookup_public_allowed",
        "collection_write_anonymous_allowed",
        "lookup_only_search",
    }
    missing = required - set(types)
    assert not missing, f"missing required types: {missing}"

    # Must be consistent with the registry.
    assert set(types) == known_condition_types()
