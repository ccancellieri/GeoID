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

"""Unit tests for ``ElasticsearchLogBackend`` write/read paths (#2797/#2798).

Mocks the ``opensearchpy`` client per the convention in
``test_bulk_error_surfacing.py`` — no live cluster required.

``ElasticsearchLogBackend`` is instantiated via a function-local import
(``_make_backend``) rather than a module-level one: ``test_log_backend_
optional.py`` (collected earlier in this same directory) deliberately
``sys.modules.pop``s and reimports ``dynastore.modules.elasticsearch.
log_backend`` to test import-safety, which rebinds the module object. A
module-level ``from ... import ElasticsearchLogBackend`` here would freeze
a reference to whichever module object existed at collection time, so its
methods would keep closing over that object's (unpatched) ``get_client`` —
a function-local import always resolves the live ``sys.modules`` entry,
matching what ``unittest.mock.patch`` targets.
"""
from __future__ import annotations

import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.log_manager import LogEntryCreate


def _make_backend():
    from dynastore.modules.elasticsearch.log_backend import ElasticsearchLogBackend

    return ElasticsearchLogBackend()


def _mock_es() -> MagicMock:
    es = MagicMock()
    es.indices = MagicMock()
    es.indices.exists = AsyncMock(return_value=True)
    es.indices.create = AsyncMock()
    es.bulk = AsyncMock(return_value={"errors": False, "items": []})
    es.search = AsyncMock(return_value={"hits": {"hits": []}})
    return es


def _entry(**kwargs) -> LogEntryCreate:
    defaults = dict(catalog_id="cat1", event_type="test.event", message="hello")
    defaults.update(kwargs)
    return LogEntryCreate(**defaults)


# ---------------------------------------------------------------------------
# write_batch — monthly index targeting
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_write_batch_targets_current_month_index():
    es = _mock_es()
    backend = _make_backend()

    with (
        patch("dynastore.modules.elasticsearch.log_backend.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.log_backend.get_index_prefix", return_value="dynastore"),
    ):
        result = await backend.write_batch([_entry()])

    assert result["status"] == "success"
    bulk_body = es.bulk.call_args.kwargs["body"]
    action = bulk_body[0]["index"]
    assert re.fullmatch(r"dynastore-logs-\d{4}\.\d{2}", action["_index"])


@pytest.mark.asyncio
async def test_ensure_index_is_tracked_per_index_name():
    """A second write for a different (e.g. rolled-over) month re-checks
    existence for THAT index instead of trusting the previous month's cache."""
    es = _mock_es()
    backend = _make_backend()

    await backend._ensure_index(es, "dynastore-logs-2026.06")
    await backend._ensure_index(es, "dynastore-logs-2026.06")  # cached, no re-check
    await backend._ensure_index(es, "dynastore-logs-2026.07")  # new month, re-checked

    assert es.indices.exists.await_count == 2
    assert backend._ensured_indices == {"dynastore-logs-2026.06", "dynastore-logs-2026.07"}


# ---------------------------------------------------------------------------
# write_batch — #2798 stacktrace / request_context extraction + scrub
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_write_batch_persists_stacktrace_from_details():
    es = _mock_es()
    backend = _make_backend()
    entry = _entry(details={"stacktrace": "Traceback (most recent call last): boom"})

    with (
        patch("dynastore.modules.elasticsearch.log_backend.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.log_backend.get_index_prefix", return_value="dynastore"),
    ):
        await backend.write_batch([entry])

    doc = es.bulk.call_args.kwargs["body"][1]
    assert doc["stacktrace"] == "Traceback (most recent call last): boom"


@pytest.mark.asyncio
async def test_write_batch_accepts_legacy_traceback_key():
    """generic_exception_handler still uses details['traceback'] — must not be dropped."""
    es = _mock_es()
    backend = _make_backend()
    entry = _entry(details={"traceback": "legacy traceback text"})

    with (
        patch("dynastore.modules.elasticsearch.log_backend.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.log_backend.get_index_prefix", return_value="dynastore"),
    ):
        await backend.write_batch([entry])

    doc = es.bulk.call_args.kwargs["body"][1]
    assert doc["stacktrace"] == "legacy traceback text"


@pytest.mark.asyncio
async def test_write_batch_persists_request_context_from_details():
    es = _mock_es()
    backend = _make_backend()
    entry = _entry(
        details={
            "request_context": {
                "method": "POST",
                "path": "/catalogs/foo/items",
                "status": 500,
                "caller_id": "user-123",
            }
        }
    )

    with (
        patch("dynastore.modules.elasticsearch.log_backend.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.log_backend.get_index_prefix", return_value="dynastore"),
    ):
        await backend.write_batch([entry])

    doc = es.bulk.call_args.kwargs["body"][1]
    assert doc["request_context"]["method"] == "POST"
    assert doc["request_context"]["path"] == "/catalogs/foo/items"
    assert doc["request_context"]["caller_id"] == "user-123"


@pytest.mark.asyncio
async def test_write_batch_omits_stacktrace_and_request_context_when_absent():
    es = _mock_es()
    backend = _make_backend()

    with (
        patch("dynastore.modules.elasticsearch.log_backend.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.log_backend.get_index_prefix", return_value="dynastore"),
    ):
        await backend.write_batch([_entry()])

    doc = es.bulk.call_args.kwargs["body"][1]
    assert "stacktrace" not in doc
    assert "request_context" not in doc


@pytest.mark.asyncio
async def test_write_batch_scrubs_pii_in_stacktrace():
    es = _mock_es()
    backend = _make_backend()
    entry = _entry(details={"stacktrace": "failed for user jane.doe@example.com"})

    with (
        patch("dynastore.modules.elasticsearch.log_backend.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.log_backend.get_index_prefix", return_value="dynastore"),
    ):
        await backend.write_batch([entry])

    doc = es.bulk.call_args.kwargs["body"][1]
    assert "jane.doe@example.com" not in doc["stacktrace"]
    assert "[redacted]" in doc["stacktrace"]


@pytest.mark.asyncio
async def test_write_batch_scrubs_pii_nested_in_request_context():
    es = _mock_es()
    backend = _make_backend()
    entry = _entry(
        details={
            "request_context": {
                "headers": {"x-forwarded-for-email": "someone@example.com"},
                "query_params": {"contact": "jane.doe@example.com"},
            }
        }
    )

    with (
        patch("dynastore.modules.elasticsearch.log_backend.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.log_backend.get_index_prefix", return_value="dynastore"),
    ):
        await backend.write_batch([entry])

    doc = es.bulk.call_args.kwargs["body"][1]
    flat = str(doc["request_context"])
    assert "someone@example.com" not in flat
    assert "jane.doe@example.com" not in flat
    assert "[redacted]" in flat


# ---------------------------------------------------------------------------
# search_logs / get_log — read over the monthly wildcard target
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_logs_targets_read_index_pattern_ignoring_unavailable():
    es = _mock_es()
    backend = _make_backend()

    with (
        patch("dynastore.modules.elasticsearch.log_backend.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.log_backend.get_index_prefix", return_value="dynastore"),
    ):
        await backend.search_logs(catalog_id="cat1")

    call = es.search.call_args
    assert call.kwargs["index"] == "dynastore-logs,dynastore-logs-*"
    assert call.kwargs["params"] == {"ignore_unavailable": "true"}


@pytest.mark.asyncio
async def test_search_logs_returns_stacktrace_and_request_context_from_source():
    es = _mock_es()
    es.search = AsyncMock(
        return_value={
            "hits": {
                "hits": [
                    {
                        "_id": "abc123",
                        "_source": {
                            "catalog_id": "cat1",
                            "message": "boom",
                            "stacktrace": "Traceback: boom",
                            "request_context": {"method": "GET"},
                        },
                    }
                ]
            }
        }
    )
    backend = _make_backend()

    with (
        patch("dynastore.modules.elasticsearch.log_backend.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.log_backend.get_index_prefix", return_value="dynastore"),
    ):
        results = await backend.search_logs(catalog_id="cat1")

    assert results[0]["stacktrace"] == "Traceback: boom"
    assert results[0]["request_context"] == {"method": "GET"}


@pytest.mark.asyncio
async def test_get_log_uses_ids_query_over_read_index_target():
    es = _mock_es()
    backend = _make_backend()

    with (
        patch("dynastore.modules.elasticsearch.log_backend.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.log_backend.get_index_prefix", return_value="dynastore"),
    ):
        result = await backend.get_log("missing-id")

    call = es.search.call_args
    assert call.kwargs["index"] == "dynastore-logs,dynastore-logs-*"
    assert call.kwargs["body"]["query"] == {"ids": {"values": ["missing-id"]}}
    assert call.kwargs["params"] == {"ignore_unavailable": "true"}
    assert result is None


@pytest.mark.asyncio
async def test_get_log_returns_hit_when_found():
    es = _mock_es()
    es.search = AsyncMock(
        return_value={
            "hits": {
                "hits": [
                    {
                        "_id": "abc123",
                        "_source": {
                            "catalog_id": "cat1",
                            "message": "boom",
                            "stacktrace": "trace",
                            "request_context": {"path": "/x"},
                        },
                    }
                ]
            }
        }
    )
    backend = _make_backend()

    with (
        patch("dynastore.modules.elasticsearch.log_backend.get_client", return_value=es),
        patch("dynastore.modules.elasticsearch.log_backend.get_index_prefix", return_value="dynastore"),
    ):
        result = await backend.get_log("abc123")

    assert result["id"] == "abc123"
    assert result["stacktrace"] == "trace"
    assert result["request_context"] == {"path": "/x"}
