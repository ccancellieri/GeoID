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

"""Unit tests for i18n link-title resolution on the remaining job routes.

Covers the newly-converted routes (list_jobs*, dismiss_job*, update_job*,
create_job*, start_job* / execute_process*) via their shared serialization
path: ``_localize_status_info`` applied to a ``StatusInfo`` with titled
``Link`` objects.

Each group checks:
- default ``en`` resolution produces a plain string on the wire
- unknown language code falls back to ``en`` (still a plain string)
- ``lang='*'`` keeps the full multi-language dict on the wire

The fixture ``_make_status_info_with_links`` intentionally mirrors the output
of ``_task_to_status_info`` + ``_get_job_links`` (self + optional results link)
so the tests exercise the real wire shape.
"""
from __future__ import annotations

import json
import uuid

from dynastore.modules.processes import models
from dynastore.extensions.processes.processes_service import (
    _localize_status_info,
)
from dynastore.tools.json import CustomJSONEncoder


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_status_info_with_links(
    *,
    include_results_link: bool = True,
) -> models.StatusInfo:
    """StatusInfo that matches what _task_to_status_info + _get_job_links
    produce for a completed job."""
    job_id = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
    links: list[models.Link] = [
        models.Link(
            href=f"http://example.com/jobs/{job_id}",
            rel="self",
            type="application/json",
            title="This document",  # type: ignore[arg-type]
        ),
    ]
    if include_results_link:
        links.append(
            models.Link(
                href=f"http://example.com/jobs/{job_id}/results",
                rel="http://www.opengis.net/def/rel/ogc/1.0/results",
                type="application/json",
                title="Job results",  # type: ignore[arg-type]
            )
        )
    return models.StatusInfo(
        jobID=job_id,
        status="successful",
        type="process",
        links=links,
    )


def _link_titles(result: dict) -> list:
    return [lnk["title"] for lnk in result.get("links", []) if "title" in lnk]


# ---------------------------------------------------------------------------
# STEP 1: prove the leak before localization
# ---------------------------------------------------------------------------


def test_status_info_link_title_raw_is_dict():
    """Without localization, model_dump exposes a dict title — the known leak."""
    si = _make_status_info_with_links()
    dump = si.model_dump(by_alias=True, exclude_none=True)
    for lnk in dump["links"]:
        if "title" in lnk:
            assert isinstance(lnk["title"], dict), (
                f"Expected raw dict before localization, got {type(lnk['title'])!r}"
            )
            assert "en" in lnk["title"]


# ---------------------------------------------------------------------------
# list_jobs / list_jobs_catalog / list_jobs_collection
# (all three scopes share the same map-over-list pattern)
# ---------------------------------------------------------------------------


def test_list_jobs_wire_en_titles_are_plain_strings():
    """list_jobs: default 'en' produces plain string titles in each item."""
    si = _make_status_info_with_links()
    result = _localize_status_info(si, "en")
    for title in _link_titles(result):
        assert isinstance(title, str), f"Expected str, got {type(title)!r}: {title!r}"


def test_list_jobs_wire_en_title_content():
    """list_jobs: the resolved English title matches the expected values."""
    si = _make_status_info_with_links()
    result = _localize_status_info(si, "en")
    titles = _link_titles(result)
    assert "This document" in titles
    assert "Job results" in titles


def test_list_jobs_wire_unknown_lang_falls_back_to_en():
    """list_jobs: unknown language code falls back to 'en' (plain string)."""
    si = _make_status_info_with_links()
    result = _localize_status_info(si, "xx")
    for title in _link_titles(result):
        assert isinstance(title, str), f"Expected str fallback, got {type(title)!r}: {title!r}"


def test_list_jobs_wire_star_preserves_full_dict():
    """list_jobs: lang='*' keeps the full multi-language dict on the wire."""
    si = _make_status_info_with_links()
    result = _localize_status_info(si, "*")
    for title in _link_titles(result):
        assert isinstance(title, dict), f"Expected dict for lang='*', got {type(title)!r}: {title!r}"
        assert "en" in title


# ---------------------------------------------------------------------------
# dismiss_job / dismiss_job_catalog / dismiss_job_collection
# ---------------------------------------------------------------------------


def test_dismiss_job_wire_en_titles_are_plain_strings():
    """dismiss_job: default 'en' resolves link titles to plain strings."""
    si = _make_status_info_with_links(include_results_link=False)
    result = _localize_status_info(si, "en")
    for title in _link_titles(result):
        assert isinstance(title, str), f"Expected str, got {type(title)!r}: {title!r}"


def test_dismiss_job_wire_unknown_lang_falls_back_to_en():
    """dismiss_job: unknown language falls back to 'en' string."""
    si = _make_status_info_with_links(include_results_link=False)
    result = _localize_status_info(si, "de")
    for title in _link_titles(result):
        assert isinstance(title, str)


def test_dismiss_job_wire_star_preserves_full_dict():
    """dismiss_job: lang='*' keeps the full dict on the wire."""
    si = _make_status_info_with_links(include_results_link=False)
    result = _localize_status_info(si, "*")
    for title in _link_titles(result):
        assert isinstance(title, dict)
        assert "en" in title


# ---------------------------------------------------------------------------
# update_job / update_job_catalog / update_job_collection
# ---------------------------------------------------------------------------


def test_update_job_wire_en_titles_are_plain_strings():
    """update_job: default 'en' resolves link titles to plain strings."""
    si = _make_status_info_with_links(include_results_link=False)
    result = _localize_status_info(si, "en")
    for title in _link_titles(result):
        assert isinstance(title, str)


def test_update_job_wire_star_preserves_full_dict():
    """update_job: lang='*' keeps the full dict on the wire."""
    si = _make_status_info_with_links(include_results_link=False)
    result = _localize_status_info(si, "*")
    for title in _link_titles(result):
        assert isinstance(title, dict)
        assert "en" in title


# ---------------------------------------------------------------------------
# create_job* — uses json.dumps(_localize_status_info(...))
# ---------------------------------------------------------------------------


def _wire_round_trip(si: models.StatusInfo, language: str) -> dict:
    """Simulate the create_job* / start_job* wire path: json.dumps with
    CustomJSONEncoder (handles UUID) then json.loads."""
    raw = json.dumps(_localize_status_info(si, language), cls=CustomJSONEncoder)
    return json.loads(raw)


def test_create_job_wire_json_dumps_en_titles_are_plain_strings():
    """create_job: serialized body with 'en' gives plain string link titles."""
    si = _make_status_info_with_links(include_results_link=False)
    wire = _wire_round_trip(si, "en")
    for title in _link_titles(wire):
        assert isinstance(title, str), f"Expected str, got {type(title)!r}: {title!r}"


def test_create_job_wire_json_dumps_star_preserves_full_dict():
    """create_job: serialized body with lang='*' keeps full multi-language dict."""
    si = _make_status_info_with_links(include_results_link=False)
    wire = _wire_round_trip(si, "*")
    for title in _link_titles(wire):
        assert isinstance(title, dict)
        assert "en" in title


def test_create_job_wire_required_fields_present():
    """create_job: the serialized response always carries the mandatory OGC fields."""
    si = _make_status_info_with_links(include_results_link=False)
    wire = _wire_round_trip(si, "en")
    for key in ("jobID", "status", "type", "links"):
        assert key in wire, f"Missing required field {key!r}"


# ---------------------------------------------------------------------------
# start_job* / execute_process* — go via _handle_execution_result(ASYNC path)
# which also uses json.dumps(_localize_status_info(...)) after the fix
# ---------------------------------------------------------------------------


def test_start_job_async_wire_en_titles_are_plain_strings():
    """start_job (ASYNC path): localized body has plain string titles."""
    si = _make_status_info_with_links()
    wire = _wire_round_trip(si, "en")
    for title in _link_titles(wire):
        assert isinstance(title, str)


def test_start_job_async_wire_star_preserves_full_dict():
    """start_job (ASYNC path): lang='*' preserves the full dict."""
    si = _make_status_info_with_links()
    wire = _wire_round_trip(si, "*")
    for title in _link_titles(wire):
        assert isinstance(title, dict)
        assert "en" in title


def test_start_job_async_wire_unknown_lang_falls_back_to_en():
    """start_job (ASYNC path): unknown language falls back to 'en'."""
    si = _make_status_info_with_links()
    wire = _wire_round_trip(si, "fr")
    for title in _link_titles(wire):
        assert isinstance(title, str)
