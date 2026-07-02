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

"""Monthly ES log index naming contract (#2797).

Covers:
  - ``get_log_index_name`` — monthly suffix, computed at write time, default
    to "now" when no explicit timestamp is passed.
  - ``get_log_index_pattern`` — the bare monthly wildcard used by retention.
  - ``get_log_read_index_target`` — read-side multi-target that also covers
    the pre-#2797 flat index.
  - The extended ``LOG_MAPPING`` fields added by #2798.
"""
from __future__ import annotations

from datetime import datetime, timezone

from dynastore.modules.elasticsearch.mappings import (
    LOG_MAPPING,
    get_log_index_name,
    get_log_index_pattern,
    get_log_read_index_target,
)


PREFIX = "dynastore"


# ---------------------------------------------------------------------------
# get_log_index_name — monthly suffix
# ---------------------------------------------------------------------------


def test_get_log_index_name_uses_monthly_suffix():
    when = datetime(2026, 7, 2, 12, 0, tzinfo=timezone.utc)
    assert get_log_index_name(PREFIX, when=when) == "dynastore-logs-2026.07"


def test_get_log_index_name_pads_single_digit_month():
    when = datetime(2026, 1, 15, tzinfo=timezone.utc)
    assert get_log_index_name(PREFIX, when=when) == "dynastore-logs-2026.01"


def test_get_log_index_name_rolls_over_across_year_boundary():
    dec = datetime(2025, 12, 31, 23, 59, tzinfo=timezone.utc)
    jan = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert get_log_index_name(PREFIX, when=dec) == "dynastore-logs-2025.12"
    assert get_log_index_name(PREFIX, when=jan) == "dynastore-logs-2026.01"


def test_get_log_index_name_defaults_to_now():
    # No explicit `when` — must resolve to *some* well-formed current-month
    # index name rather than raising or falling back to the old flat name.
    name = get_log_index_name(PREFIX)
    now = datetime.now(timezone.utc)
    assert name == f"dynastore-logs-{now.strftime('%Y.%m')}"


# ---------------------------------------------------------------------------
# get_log_index_pattern / get_log_read_index_target
# ---------------------------------------------------------------------------


def test_get_log_index_pattern_is_bare_monthly_wildcard():
    assert get_log_index_pattern(PREFIX) == "dynastore-logs-*"


def test_get_log_read_index_target_includes_flat_and_wildcard():
    target = get_log_read_index_target(PREFIX)
    parts = target.split(",")
    assert "dynastore-logs" in parts
    assert "dynastore-logs-*" in parts


def test_flat_index_name_does_not_match_monthly_pattern():
    # Sanity check for the migration-note assumption in mappings.py: the
    # pre-#2797 flat name has no trailing "-YYYY.MM", so a naive prefix
    # membership check on the wildcard alone would miss it.
    pattern = get_log_index_pattern(PREFIX)
    flat_name = f"{PREFIX}-logs"
    assert not flat_name.startswith(pattern.rstrip("*"))


# ---------------------------------------------------------------------------
# LOG_MAPPING — #2798 field completeness
# ---------------------------------------------------------------------------


def test_log_mapping_has_stacktrace_as_unindexed_text():
    field = LOG_MAPPING["properties"]["stacktrace"]
    assert field["type"] == "text"
    assert field["index"] is False


def test_log_mapping_has_request_context_as_flattened():
    field = LOG_MAPPING["properties"]["request_context"]
    assert field["type"] == "flattened"


def test_log_mapping_scope_fields_are_filterable():
    props = LOG_MAPPING["properties"]
    assert props["catalog_id"]["type"] == "keyword"
    assert props["collection_id"]["type"] == "keyword"
    assert props["event_type"]["type"] == "keyword"
    assert props["level"]["type"] == "keyword"
    assert props["is_system"]["type"] == "boolean"


def test_log_mapping_is_strict_dynamic_false():
    assert LOG_MAPPING["dynamic"] is False
