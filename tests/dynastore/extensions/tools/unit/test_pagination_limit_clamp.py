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

"""Unit tests for :func:`resolve_page_limit` — the shared OGC ``limit``
resolution/clamp helper (OGC API - Features Part 1 Core,
``/req/core/fc-limit-response-1``: an over-max ``limit`` is clamped, never
rejected).
"""

from dynastore.extensions.tools.pagination import resolve_page_limit


def test_omitted_limit_falls_back_to_default():
    assert resolve_page_limit(None, default_limit=10, max_limit=1000) == 10


def test_in_range_limit_passes_through_unchanged():
    assert resolve_page_limit(250, default_limit=10, max_limit=1000) == 250


def test_over_max_limit_clamps_to_max_not_rejected():
    # This is the load-bearing case for fc-limit-response-1: an over-max
    # ``limit`` must be capped, not raise/422.
    assert resolve_page_limit(2000, default_limit=10, max_limit=1000) == 1000


def test_limit_equal_to_max_passes_through():
    assert resolve_page_limit(1000, default_limit=10, max_limit=1000) == 1000


def test_zero_or_negative_limit_floors_to_one():
    assert resolve_page_limit(0, default_limit=10, max_limit=1000) == 1
    assert resolve_page_limit(-5, default_limit=10, max_limit=1000) == 1


def test_default_above_max_still_clamps():
    # Defensive: a misconfigured default (default_limit > max_limit) must
    # not leak an over-max page either.
    assert resolve_page_limit(None, default_limit=5000, max_limit=1000) == 1000
