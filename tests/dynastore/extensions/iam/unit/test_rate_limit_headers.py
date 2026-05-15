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

"""Unit tests for the rate-limit header projection helper.

The full middleware pipeline (auth + policy + condition evaluation +
response writing) is covered by integration tests against the dev
compose. This module verifies the small pure function that turns a
list of inspect() payloads into the right X-RateLimit-* / Retry-After
header set.
"""

from __future__ import annotations

import time

from dynastore.extensions.iam.middleware import _build_rate_limit_headers


class TestEmpty:
    def test_no_inspections_yields_no_headers(self):
        assert _build_rate_limit_headers([], deny=False) == {}

    def test_unrelated_inspection_types_ignored(self):
        # time_window / expiration entries don't carry rate-limit fields.
        assert _build_rate_limit_headers(
            [{"type": "time_window", "ok": True}], deny=False
        ) == {}


class TestProjection:
    def test_single_rate_limit_emits_canonical_headers(self):
        reset = int(time.time()) + 30
        headers = _build_rate_limit_headers(
            [
                {
                    "type": "rate_limit",
                    "limit": 10,
                    "used": 3,
                    "remaining": 7,
                    "window_seconds": 60,
                    "reset_at": reset,
                }
            ],
            deny=False,
        )
        assert headers["X-RateLimit-Limit"] == "10"
        assert headers["X-RateLimit-Remaining"] == "7"
        assert headers["X-RateLimit-Reset"] == str(reset)
        assert "Retry-After" not in headers  # success — no backoff hint

    def test_most_restrictive_wins(self):
        reset_a = int(time.time()) + 30
        reset_b = int(time.time()) + 120
        headers = _build_rate_limit_headers(
            [
                {
                    "type": "rate_limit",
                    "limit": 100,
                    "remaining": 50,
                    "reset_at": reset_a,
                },
                {
                    "type": "rate_limit",
                    "limit": 10,
                    "remaining": 1,  # tighter — wins
                    "reset_at": reset_b,
                },
            ],
            deny=False,
        )
        assert headers["X-RateLimit-Limit"] == "10"
        assert headers["X-RateLimit-Remaining"] == "1"
        assert headers["X-RateLimit-Reset"] == str(reset_b)

    def test_max_count_inspection_projects_without_reset(self):
        headers = _build_rate_limit_headers(
            [
                {
                    "type": "max_count",
                    "limit": 5,
                    "used": 2,
                    "remaining": 3,
                }
            ],
            deny=False,
        )
        assert headers["X-RateLimit-Limit"] == "5"
        assert headers["X-RateLimit-Remaining"] == "3"
        assert "X-RateLimit-Reset" not in headers


class TestDeny:
    def test_rate_limit_deny_sets_retry_after_from_reset(self):
        reset = int(time.time()) + 42
        headers = _build_rate_limit_headers(
            [
                {
                    "type": "rate_limit",
                    "limit": 10,
                    "remaining": 0,
                    "window_seconds": 60,
                    "reset_at": reset,
                }
            ],
            deny=True,
        )
        # Retry-After is bounded; allow ±1 for clock-skew between the
        # capture above and the helper's own time.time() call.
        retry = int(headers["Retry-After"])
        assert 40 <= retry <= 44

    def test_rate_limit_deny_falls_back_to_window_when_no_reset(self):
        headers = _build_rate_limit_headers(
            [
                {
                    "type": "rate_limit",
                    "limit": 5,
                    "remaining": 0,
                    "window_seconds": 30,
                    # no reset_at — operator policy stored without one
                }
            ],
            deny=True,
        )
        assert headers["Retry-After"] == "30"

    def test_max_count_deny_does_not_emit_retry_after(self):
        # Lifetime quota — no automatic renewal, no Retry-After hint.
        headers = _build_rate_limit_headers(
            [
                {
                    "type": "max_count",
                    "limit": 3,
                    "remaining": 0,
                }
            ],
            deny=True,
        )
        assert "Retry-After" not in headers

    def test_negative_remaining_clamps_to_zero(self):
        # Some backends might report a snapshot just over the cap.
        headers = _build_rate_limit_headers(
            [
                {
                    "type": "rate_limit",
                    "limit": 10,
                    "remaining": -3,
                    "window_seconds": 60,
                    "reset_at": int(time.time()) + 30,
                }
            ],
            deny=True,
        )
        assert headers["X-RateLimit-Remaining"] == "0"
