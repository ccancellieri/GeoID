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

"""``BucketService.generate_bucket_name`` produces project-id-prefixed,
length-safe bucket names.

The previous opaque-hash scheme produced names like ``d88971-test-...``
that were error-prone to type or recognise (e.g. the ``dd88971`` typo
in production).  The new scheme uses the project ID directly so the
bucket name is human-recognisable in logs / consoles / report paths.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dynastore.modules.gcp.bucket_service import BucketService


def _make_bm(project_id: str) -> BucketService:
    return BucketService(
        engine=None,
        config_service=None,
        storage_client=MagicMock(),
        project_id=project_id,
        region="europe-west1",
    )


def test_basic_naming_uses_project_id_prefix():
    bm = _make_bm("my-test-project")
    assert bm.generate_bucket_name("test_catalog_19") == \
        "my-test-project-test-catalog-19"


def test_physical_schema_preferred_over_catalog_id():
    bm = _make_bm("my-test-project")
    assert bm.generate_bucket_name("any-id", physical_schema="s_2ka8fbc3") == \
        "my-test-project-s-2ka8fbc3"


def test_underscores_normalised_to_dashes():
    bm = _make_bm("my-test-project")
    assert bm.generate_bucket_name("with_under_scores") == \
        "my-test-project-with-under-scores"


def test_uppercase_lowercased():
    bm = _make_bm("My-Test-Project")
    name = bm.generate_bucket_name("TEST_CATALOG_19")
    assert name == name.lower()
    assert name.startswith("my-test-project-")


def test_within_63_char_limit_keeps_full_identifier():
    bm = _make_bm("my-test-project")
    # Identifier picked so total length is exactly 63
    pad = "x" * (63 - len("my-test-project-"))  # 37 chars
    name = bm.generate_bucket_name(pad)
    assert name == f"my-test-project-{pad}"
    assert len(name) == 63


def test_overflow_truncates_identifier_and_appends_hash():
    bm = _make_bm("my-test-project")
    long_id = "x" * 100
    name = bm.generate_bucket_name(long_id)
    assert len(name) <= 63
    assert name.startswith("my-test-project-")
    # Suffix should be 8 hex chars after a dash
    assert name[-9] == "-"
    assert all(c in "0123456789abcdef" for c in name[-8:])


def test_overflow_is_deterministic_for_same_identifier():
    bm = _make_bm("my-test-project")
    long_id = "y" * 200
    n1 = bm.generate_bucket_name(long_id)
    n2 = bm.generate_bucket_name(long_id)
    assert n1 == n2


def test_overflow_differentiates_distinct_identifiers():
    bm = _make_bm("my-test-project")
    n1 = bm.generate_bucket_name("z" * 100)
    n2 = bm.generate_bucket_name("z" * 99 + "a")  # different last char
    assert n1 != n2  # the appended hash makes them distinct


def test_short_project_id_works():
    bm = _make_bm("p")
    name = bm.generate_bucket_name("test_catalog_19")
    assert name == "p-test-catalog-19"


def test_missing_project_id_raises():
    bm = _make_bm("")
    with pytest.raises(RuntimeError, match="GCP Project ID not available"):
        bm.generate_bucket_name("test_catalog_19")


def test_long_identifier_keeps_full_project_name():
    bm = _make_bm("my-test-project")  # 25 chars
    name = bm.generate_bucket_name("x" * 100)
    assert name.startswith("my-test-project-")  # prefix preserved
    assert len(name) <= 63
    assert name[-9] == "-"  # 8-char hash after dash
    assert all(c in "0123456789abcdef" for c in name[-8:])


def test_long_project_id_truncated_with_hash():
    # Project ID > 2/3 of 63 (42) triggers Path B truncation.
    long_proj = "a" * 50  # exceeds project_min*2 (42)
    bm = _make_bm(long_proj)
    name = bm.generate_bucket_name("test_catalog_19")
    assert len(name) <= 63
    # Project segment should be ~1/3 budget = 21 chars: 12 readable + '-' + 8 hex
    assert name.startswith("a" * 12 + "-")
    proj_hash = name[:21].rsplit("-", 1)[1]
    assert len(proj_hash) == 8
    assert all(c in "0123456789abcdef" for c in proj_hash)


def test_long_project_and_long_identifier_split_one_third_two_thirds():
    long_proj = "p" * 50
    bm = _make_bm(long_proj)
    name = bm.generate_bucket_name("i" * 100)
    assert len(name) <= 63
    # Truncated project section ~21 chars (1/3 budget), identifier section ~42 chars (2/3)
    project_section = name[:21]
    assert project_section.startswith("p" * 12 + "-")  # 12 head + '-' + 8 hash
    # Trailing 8 chars of full bucket are the identifier hash
    assert name[-9] == "-"
    assert all(c in "0123456789abcdef" for c in name[-8:])


def test_distinct_long_identifiers_never_collide():
    bm = _make_bm("my-test-project")
    n1 = bm.generate_bucket_name("a" * 200)
    n2 = bm.generate_bucket_name("a" * 199 + "b")
    assert n1 != n2


def test_empty_identifier_raises():
    bm = _make_bm("my-test-project")
    # Both arguments empty (catalog_id="" and physical_schema=None default) → raise
    with pytest.raises(ValueError, match="both physical_schema and catalog_id are empty"):
        bm.generate_bucket_name("")


def test_empty_physical_schema_falls_back_to_catalog_id():
    # Legacy `(physical_schema or catalog_id)` semantic: empty physical_schema
    # is treated as "not provided" and the catalog_id is used.
    bm = _make_bm("my-test-project")
    name = bm.generate_bucket_name("test_catalog_19", physical_schema="")
    assert name == "my-test-project-test-catalog-19"


def test_invalid_chars_raise():
    bm = _make_bm("my-test-project")
    for bad_id in ["with space", "with/slash", "with:colon", "with@at", "with!bang"]:
        with pytest.raises(ValueError, match="invalid for GCS bucket names"):
            bm.generate_bucket_name(bad_id)


def test_google_substring_raises():
    bm = _make_bm("my-test-project")
    for bad_id in ["google-imagery", "my-google-data", "g00gle-cache", "g0ogle-test"]:
        with pytest.raises(ValueError, match="GCS-reserved substring"):
            bm.generate_bucket_name(bad_id)


def test_truncation_strips_trailing_dash():
    # Identifier ending with dash that lands at truncation boundary must
    # not produce a double-dash before the hash suffix.
    bm = _make_bm("my-test-project")
    name = bm.generate_bucket_name("a-" * 50)  # 100 chars, ends in '-'
    assert len(name) <= 63
    assert "--" not in name
    body = name[:-9]  # strip "-{hash}"
    assert not body.endswith("-")
    assert not body.endswith(".")
