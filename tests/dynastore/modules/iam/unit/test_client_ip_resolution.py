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

"""Unit tests for the client-IP resolver used by ``scope=client_ip``.

GeoID deployments always run behind a proxy/LB (Cloud Run + LB per
the deployed topology), so ``request.client.host`` is the proxy and
``X-Forwarded-For`` carries the real caller. The resolver must
prefer XFF and fall back gracefully when the header is absent.
"""

from __future__ import annotations

from types import SimpleNamespace

from dynastore.modules.iam.conditions import _client_ip_from_request


def _req(headers=None, client_host=None):
    """Build a minimal request-like object matching Starlette's shape."""
    h = SimpleNamespace(get=lambda k, default=None: (headers or {}).get(k.lower(), default))
    client = SimpleNamespace(host=client_host) if client_host else None
    return SimpleNamespace(headers=h, client=client)


class TestXffPreferred:
    def test_xff_single_ip_wins_over_client_host(self):
        req = _req(headers={"x-forwarded-for": "203.0.113.7"}, client_host="10.0.0.1")
        assert _client_ip_from_request(req) == "203.0.113.7"

    def test_xff_chain_takes_leftmost(self):
        # Proxy chain: client → edge LB → app → me. The original caller
        # is leftmost; intermediates are appended on each hop.
        req = _req(
            headers={"x-forwarded-for": "203.0.113.7, 10.1.0.2, 10.1.0.3"},
            client_host="10.0.0.1",
        )
        assert _client_ip_from_request(req) == "203.0.113.7"

    def test_xff_strips_whitespace(self):
        req = _req(headers={"x-forwarded-for": "  198.51.100.4  ,  10.0.0.1"})
        assert _client_ip_from_request(req) == "198.51.100.4"


class TestFallback:
    def test_no_xff_falls_back_to_client_host(self):
        req = _req(headers={}, client_host="10.0.0.1")
        assert _client_ip_from_request(req) == "10.0.0.1"

    def test_empty_xff_falls_back_to_client_host(self):
        req = _req(headers={"x-forwarded-for": ""}, client_host="10.0.0.1")
        assert _client_ip_from_request(req) == "10.0.0.1"

    def test_whitespace_only_xff_falls_back_to_client_host(self):
        # A header that's literally "  ,  " has no usable leftmost token.
        req = _req(headers={"x-forwarded-for": "  ,  "}, client_host="10.0.0.1")
        assert _client_ip_from_request(req) == "10.0.0.1"


class TestNullSafety:
    def test_no_request_returns_none(self):
        assert _client_ip_from_request(None) is None

    def test_no_headers_no_client_returns_none(self):
        req = SimpleNamespace(headers=None, client=None)
        assert _client_ip_from_request(req) is None

    def test_headers_present_but_no_xff_no_client(self):
        # Request with headers but neither XFF nor a client tuple.
        req = _req(headers={}, client_host=None)
        assert _client_ip_from_request(req) is None
