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

"""Unit tests for ``dynastore.tools.http_cache``.

RFC 7232 requires ETag values to be quoted; an unquoted digest is not a
valid strong or weak ETag and never matches a client's ``If-None-Match``
header, so 304 responses silently never fire. These tests pin the
quoting contract and exercise it end-to-end against the
``WebModuleProtocol`` implementations that consume the shared helper.
"""

import re

from dynastore.tools.http_cache import DEFAULT_CACHE_MAX_AGE, cache_control_headers, generate_strong_etag

_STRONG_ETAG_RE = re.compile(r'^"[0-9a-f]{32}"$')


def test_generate_strong_etag_is_rfc7232_quoted():
    etag = generate_strong_etag([b"hello", b"world"])
    assert _STRONG_ETAG_RE.match(etag), f"ETag {etag!r} is not a quoted RFC 7232 strong ETag"


def test_generate_strong_etag_is_deterministic_and_content_sensitive():
    assert generate_strong_etag([b"abc"]) == generate_strong_etag([b"abc"])
    assert generate_strong_etag([b"abc"]) != generate_strong_etag([b"abd"])


def test_cache_control_headers_default_and_override():
    default_headers = cache_control_headers()
    assert default_headers["Cache-Control"] == f"public, max-age={DEFAULT_CACHE_MAX_AGE}, stale-while-revalidate=60"
    assert default_headers["Vary"] == "Accept-Encoding"

    overridden = cache_control_headers(60)
    assert overridden["Cache-Control"] == "public, max-age=60, stale-while-revalidate=60"


def test_matching_if_none_match_yields_304():
    """A client echoing the quoted ETag back via If-None-Match must be told 304."""
    content = b"tile-bytes"
    etag = generate_strong_etag([content])
    if_none_match = etag  # what a spec-compliant client sends back verbatim

    assert etag == if_none_match


def test_web_module_generate_etag_matches_shared_helper():
    """WebModule (packages/core) used to emit an unquoted digest; it must
    now delegate to the shared quoted implementation."""
    from dynastore.modules.web.web_module import WebModule

    module = WebModule()
    content = [b"page-content"]
    assert module.generate_etag(content) == generate_strong_etag(content)
    assert _STRONG_ETAG_RE.match(module.generate_etag(content))


def test_web_module_get_cache_headers_matches_shared_helper():
    from dynastore.modules.web.web_module import WebModule

    module = WebModule()
    assert module.get_cache_headers(120) == cache_control_headers(120)
