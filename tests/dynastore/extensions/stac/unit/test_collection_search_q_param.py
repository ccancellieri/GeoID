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

"""Unit tests for collection free-text search via the ``q`` parameter.

``collection_core.title`` and ``collection_core.description`` are JSONB columns
that store localized text as ``{"en": "...", "fr": "..."}``.  Applying
``lower()`` directly to a jsonb expression raises an
``UndefinedFunctionError`` in Postgres because there is no ``lower(jsonb)``
overload.  The fix extracts the English text via ``->>'en'`` before calling
``lower()``.

These tests are DB-free — they capture the generated SQL and assert the correct
column accessors are emitted without executing against a real database.
"""

from __future__ import annotations

import pytest

import dynastore.extensions.stac.search as stac_search
from dynastore.extensions.stac.search import CollectionSearchRequest, ResultHandler


class _FakeCatalogs:
    async def resolve_physical_schema(self, _cid, ctx=None):
        return "phys_test"


@pytest.mark.asyncio
async def test_q_param_uses_text_accessor_not_bare_jsonb(monkeypatch):
    """The ``q`` WHERE clause must use ``->>'en'`` on title and description,
    not bare ``lower(mc.title)`` / ``lower(mc.description)`` which fail on
    jsonb columns with ``UndefinedFunctionError``.
    """
    captured: list[str] = []

    class _DQLStub:
        def __init__(self, sql, result_handler=None, **_kw):
            self.sql = sql
            self.result_handler = result_handler
            captured.append(sql)

        async def execute(self, *_a, **_kw):
            if self.result_handler == ResultHandler.SCALAR_ONE_OR_NONE:
                return 1
            return []

    async def _none(*_a, **_kw):
        return None

    import dynastore.models.protocols.visibility as visibility

    monkeypatch.setattr(stac_search, "DQLQuery", _DQLStub)
    monkeypatch.setattr(stac_search, "get_protocol", lambda _proto: _FakeCatalogs())
    monkeypatch.setattr(visibility, "resolve_catalog_listing_ids", _none)
    monkeypatch.setattr(visibility, "resolve_collection_listing_ids", _none)

    req = CollectionSearchRequest(catalog_id="cat", q=["cropland"], limit=10, offset=0)
    collections, total = await stac_search.search_collections(None, req)

    assert collections == [] and total == 1
    assert len(captured) == 2, captured

    for sql in captured:
        # Must use the ->> text accessor, not bare jsonb column reference
        assert "mc.title->>'en'" in sql, (
            "Expected mc.title->>'en' in SQL but got:\n" + sql
        )
        assert "mc.description->>'en'" in sql, (
            "Expected mc.description->>'en' in SQL but got:\n" + sql
        )
        # Must NOT call lower() directly on the bare jsonb column
        assert "lower(mc.title)" not in sql, (
            "lower(mc.title) applies lower() directly to jsonb — will 500:\n" + sql
        )
        assert "lower(mc.description)" not in sql, (
            "lower(mc.description) applies lower() directly to jsonb — will 500:\n" + sql
        )
        # Must not cast the whole blob to text (was a workaround for title, wrong for description)
        assert "mc.title::text" not in sql, (
            "mc.title::text casts the whole jsonb blob — use ->>'en' instead:\n" + sql
        )


@pytest.mark.asyncio
async def test_q_param_multi_term_all_terms_present(monkeypatch):
    """Each term in ``q`` produces an independent AND-ed condition block."""
    captured: list[str] = []

    class _DQLStub:
        def __init__(self, sql, result_handler=None, **_kw):
            self.sql = sql
            self.result_handler = result_handler
            captured.append(sql)

        async def execute(self, *_a, **_kw):
            if self.result_handler == ResultHandler.SCALAR_ONE_OR_NONE:
                return 2
            return []

    async def _none(*_a, **_kw):
        return None

    import dynastore.models.protocols.visibility as visibility

    monkeypatch.setattr(stac_search, "DQLQuery", _DQLStub)
    monkeypatch.setattr(stac_search, "get_protocol", lambda _proto: _FakeCatalogs())
    monkeypatch.setattr(visibility, "resolve_catalog_listing_ids", _none)
    monkeypatch.setattr(visibility, "resolve_collection_listing_ids", _none)

    req = CollectionSearchRequest(
        catalog_id="cat", q=["cropland", "africa"], limit=10, offset=0
    )
    collections, total = await stac_search.search_collections(None, req)

    assert total == 2
    for sql in captured:
        # Both terms must appear as distinct bind-parameter placeholders
        assert "_q_term_0" in sql
        assert "_q_term_1" in sql
        # Each term block must use the text accessor
        assert sql.count("mc.title->>'en'") >= 2
        assert sql.count("mc.description->>'en'") >= 2


@pytest.mark.asyncio
async def test_q_param_case_insensitive_pattern(monkeypatch):
    """The search pattern is lowercased in Python and compared against
    ``lower(col)`` so matching is case-insensitive end-to-end."""
    captured_params: list[dict] = []

    class _DQLStub:
        def __init__(self, sql, result_handler=None, **_kw):
            self.sql = sql
            self.result_handler = result_handler

        async def execute(self, *_a, **_kw):
            captured_params.append(_kw)
            if self.result_handler == ResultHandler.SCALAR_ONE_OR_NONE:
                return 1
            return []

    async def _none(*_a, **_kw):
        return None

    import dynastore.models.protocols.visibility as visibility

    monkeypatch.setattr(stac_search, "DQLQuery", _DQLStub)
    monkeypatch.setattr(stac_search, "get_protocol", lambda _proto: _FakeCatalogs())
    monkeypatch.setattr(visibility, "resolve_catalog_listing_ids", _none)
    monkeypatch.setattr(visibility, "resolve_collection_listing_ids", _none)

    req = CollectionSearchRequest(
        catalog_id="cat", q=["CropLand"], limit=10, offset=0
    )
    await stac_search.search_collections(None, req)

    # The bound parameter value must be lowercased with leading/trailing wildcards
    for kw in captured_params:
        if "_q_term_0" in kw:
            assert kw["_q_term_0"] == "%cropland%", (
                f"Expected '%cropland%' but got {kw['_q_term_0']!r}"
            )
