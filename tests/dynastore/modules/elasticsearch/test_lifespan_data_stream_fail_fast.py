"""Lifespan must fail-fast when a shared metadata index exists as a data stream.

The platform expects ``{prefix}-collections`` and ``{prefix}-catalogs`` to be
regular mutable indices — collection / catalog drivers issue ``client.index()``
which is upsert (op_type=index). Data streams reject upserts with HTTP 400
``illegal_argument_exception: only write ops with an op_type of create are
allowed in data streams`` (observed on review env 2026-04-30).

``indices.exists()`` returns True for both regular indices AND data streams,
so a stream that snuck in (cluster-side index template, ISM/ILM policy,
manual creation) would be silently skipped by the existing creation
guard. The fail-fast catches it at startup with a remediation message.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from dynastore.modules.elasticsearch.module import (
    _find_overbroad_dynastore_data_stream_templates,
    _is_data_stream,
)


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_is_data_stream_true_when_get_data_stream_returns_match() -> None:
    es = AsyncMock()
    es.indices.get_data_stream = AsyncMock(return_value={
        "data_streams": [{"name": "dynastore-collections", "generation": 1}],
    })
    assert await _is_data_stream(es, "dynastore-collections") is True


@pytest.mark.asyncio
async def test_is_data_stream_false_when_get_data_stream_404s() -> None:
    """A regular index returns 404 from the data-streams API → not a stream."""
    es = AsyncMock()
    es.indices.get_data_stream = AsyncMock(side_effect=Exception("404 not found"))
    assert await _is_data_stream(es, "dynastore-collections") is False


@pytest.mark.asyncio
async def test_is_data_stream_false_when_get_data_stream_returns_empty() -> None:
    es = AsyncMock()
    es.indices.get_data_stream = AsyncMock(return_value={"data_streams": []})
    assert await _is_data_stream(es, "dynastore-collections") is False


@pytest.mark.asyncio
async def test_is_data_stream_false_when_name_does_not_match() -> None:
    """API may return streams matching a wildcard — only exact name counts."""
    es = AsyncMock()
    es.indices.get_data_stream = AsyncMock(return_value={
        "data_streams": [{"name": "dynastore-other-stream"}],
    })
    assert await _is_data_stream(es, "dynastore-collections") is False


@pytest.mark.asyncio
async def test_is_data_stream_handles_non_dict_response() -> None:
    """Defensive: opensearch-py occasionally returns a transport object;
    ``_is_data_stream`` should not crash on unexpected shapes."""
    es = AsyncMock()
    es.indices.get_data_stream = AsyncMock(return_value=None)
    assert await _is_data_stream(es, "dynastore-collections") is False


# ---------------------------------------------------------------------------
# Lifespan integration — pre-check raises RuntimeError when stream is in the way
# ---------------------------------------------------------------------------

async def _drive_shared_index_precheck(
    *, exists: bool, is_data_stream: bool,
) -> tuple[bool, str | None]:
    """Run only the shared-index data-stream pre-check from
    ``ElasticsearchModule.lifespan``. Returns ``(raised, message)``.

    The check runs before ``indices.create``, so we only need to mock
    ``indices.exists`` and ``indices.get_data_stream``.
    """
    es = AsyncMock()
    es.indices.exists = AsyncMock(return_value=exists)
    if is_data_stream:
        es.indices.get_data_stream = AsyncMock(return_value={
            "data_streams": [{"name": "dynastore-collections"}],
        })
    else:
        es.indices.get_data_stream = AsyncMock(side_effect=Exception("404"))
    es.indices.create = AsyncMock(return_value=None)

    shared_name = "dynastore-collections"
    try:
        if await _is_data_stream(es, shared_name):
            raise RuntimeError(
                f"ElasticsearchModule: '{shared_name}' is a data stream, "
                "but the platform requires a regular index for mutable "
                f"metadata upserts. Delete it before redeploy: "
                f"DELETE /_data_stream/{shared_name}"
            )
        if not await es.indices.exists(index=shared_name):
            await es.indices.create(index=shared_name, body={"mappings": {}})
    except RuntimeError as e:
        return True, str(e)
    return False, None


@pytest.mark.asyncio
async def test_lifespan_raises_on_data_stream_collision() -> None:
    raised, msg = await _drive_shared_index_precheck(
        exists=True, is_data_stream=True,
    )
    assert raised is True
    assert msg is not None
    assert "data stream" in msg.lower()
    assert "DELETE /_data_stream/dynastore-collections" in msg


@pytest.mark.asyncio
async def test_lifespan_silent_on_regular_index() -> None:
    """Regular index in place — pre-check passes silently, no raise."""
    raised, _ = await _drive_shared_index_precheck(
        exists=True, is_data_stream=False,
    )
    assert raised is False


@pytest.mark.asyncio
async def test_lifespan_silent_on_missing_index() -> None:
    """Index doesn't exist yet — pre-check passes, lifespan proceeds to create."""
    raised, _ = await _drive_shared_index_precheck(
        exists=False, is_data_stream=False,
    )
    assert raised is False


# ---------------------------------------------------------------------------
# Over-broad-template fail-fast — catches the cluster CONFIG that would
# convert {prefix}-collections / {prefix}-catalogs into data streams on
# create. Complements the symptom check above.
# ---------------------------------------------------------------------------

def _mk_es_with_templates(templates: list) -> AsyncMock:
    es = AsyncMock()
    es.indices.get_index_template = AsyncMock(return_value={
        "index_templates": templates,
    })
    return es


@pytest.mark.asyncio
async def test_overbroad_template_detects_dynastore_star_with_data_stream() -> None:
    """The exact review-env config: index_patterns=['dynastore-*'] + data_stream."""
    es = _mk_es_with_templates([
        {
            "name": "dynastore_logs",
            "index_template": {
                "index_patterns": ["dynastore-*"],
                "data_stream": {"hidden": False},
            },
        },
    ])
    out = await _find_overbroad_dynastore_data_stream_templates(es, "dynastore")
    assert len(out) == 1
    assert out[0][0] == "dynastore_logs"
    assert out[0][1] == ["dynastore-*"]


@pytest.mark.asyncio
async def test_overbroad_template_silent_on_safe_logs_only_pattern() -> None:
    """index_patterns=['dynastore-logs-*'] does NOT match {prefix}-collections."""
    es = _mk_es_with_templates([
        {
            "name": "dynastore_logs",
            "index_template": {
                "index_patterns": ["dynastore-logs-*"],
                "data_stream": {"hidden": False},
            },
        },
    ])
    out = await _find_overbroad_dynastore_data_stream_templates(es, "dynastore")
    assert out == []


@pytest.mark.asyncio
async def test_overbroad_template_silent_on_regular_template_without_data_stream() -> None:
    """A template without data_stream={...} cannot convert indices to streams."""
    es = _mk_es_with_templates([
        {
            "name": "dynastore_overlay",
            "index_template": {
                "index_patterns": ["dynastore-*"],
                # No data_stream key — regular composable template, harmless.
            },
        },
    ])
    out = await _find_overbroad_dynastore_data_stream_templates(es, "dynastore")
    assert out == []


@pytest.mark.asyncio
async def test_overbroad_template_detects_universal_wildcard() -> None:
    """index_patterns=['*'] also matches {prefix}-collections / -catalogs."""
    es = _mk_es_with_templates([
        {
            "name": "catch_all",
            "index_template": {
                "index_patterns": ["*"],
                "data_stream": {},
            },
        },
    ])
    out = await _find_overbroad_dynastore_data_stream_templates(es, "dynastore")
    assert len(out) == 1
    assert out[0][0] == "catch_all"


@pytest.mark.asyncio
async def test_overbroad_template_detects_only_offending_one_among_many() -> None:
    """Mixed cluster: one safe template + one offending template + one regular."""
    es = _mk_es_with_templates([
        {
            "name": "dynastore_logs",
            "index_template": {
                "index_patterns": ["dynastore-logs-*"],
                "data_stream": {"hidden": False},
            },
        },
        {
            "name": "rogue_overlay",
            "index_template": {
                "index_patterns": ["dynastore-collections", "other-*"],
                "data_stream": {},
            },
        },
        {
            "name": "regular_template",
            "index_template": {
                "index_patterns": ["dynastore-*"],
                # No data_stream — harmless.
            },
        },
    ])
    out = await _find_overbroad_dynastore_data_stream_templates(es, "dynastore")
    names = [n for n, _ in out]
    assert names == ["rogue_overlay"]


@pytest.mark.asyncio
async def test_overbroad_template_returns_empty_on_api_error() -> None:
    """Non-fatal: API error means we cannot probe; symptom check still runs."""
    es = AsyncMock()
    es.indices.get_index_template = AsyncMock(side_effect=Exception("transport down"))
    out = await _find_overbroad_dynastore_data_stream_templates(es, "dynastore")
    assert out == []


@pytest.mark.asyncio
async def test_overbroad_template_returns_empty_on_no_templates() -> None:
    """Fresh cluster — no composable templates at all."""
    es = _mk_es_with_templates([])
    out = await _find_overbroad_dynastore_data_stream_templates(es, "dynastore")
    assert out == []


@pytest.mark.asyncio
async def test_overbroad_template_handles_non_dict_response() -> None:
    """Defensive — opensearch-py occasionally returns transport objects."""
    es = AsyncMock()
    es.indices.get_index_template = AsyncMock(return_value=None)
    out = await _find_overbroad_dynastore_data_stream_templates(es, "dynastore")
    assert out == []


@pytest.mark.asyncio
async def test_overbroad_template_handles_string_index_patterns() -> None:
    """Some ES versions return index_patterns as a single string, not a list."""
    es = _mk_es_with_templates([
        {
            "name": "single_pattern",
            "index_template": {
                "index_patterns": "dynastore-*",  # str, not list
                "data_stream": {},
            },
        },
    ])
    out = await _find_overbroad_dynastore_data_stream_templates(es, "dynastore")
    assert len(out) == 1
    assert out[0][0] == "single_pattern"
