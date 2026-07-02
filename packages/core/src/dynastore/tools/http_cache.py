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

"""Shared HTTP caching helpers: strong ETags (RFC 7232) and Cache-Control headers.

Consolidates the ``generate_etag``/``get_cache_headers`` pairs that used to be
hand-rolled per module. RFC 7232 requires ETag values to be quoted; an
unquoted value is not a valid strong or weak ETag, so it never matches a
client's ``If-None-Match`` header and 304 responses never fire.
"""

from __future__ import annotations

import hashlib
from typing import Dict, List, Optional

DEFAULT_CACHE_MAX_AGE = 3600


def generate_strong_etag(content_parts: List[bytes]) -> str:
    """Generate a quoted RFC 7232 strong ETag from *content_parts*.

    Each part is hashed in sequence (MD5) and the result is returned
    wrapped in double quotes, e.g. ``"d41d8cd98f00b204e9800998ecf8427e"``.
    """
    hasher = hashlib.md5()
    for part in content_parts:
        hasher.update(part)
    return f'"{hasher.hexdigest()}"'


def cache_control_headers(max_age: Optional[int] = None) -> Dict[str, str]:
    """Return standard ``Cache-Control``/``Vary`` headers for *max_age* seconds.

    Falls back to :data:`DEFAULT_CACHE_MAX_AGE` when *max_age* is ``None``.
    """
    eff_max_age = max_age if max_age is not None else DEFAULT_CACHE_MAX_AGE
    return {
        "Cache-Control": f"public, max-age={eff_max_age}, stale-while-revalidate=60",
        "Vary": "Accept-Encoding",
    }
