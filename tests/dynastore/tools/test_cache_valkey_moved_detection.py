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

"""MOVED-on-standalone detection (#2812 follow-up).

The #2812 outage was a standalone client pointed at a cluster-mode Valkey
endpoint — every command failed with ``MOVED``, and nothing named the
endpoint or called out the mode mismatch until an operator dug through
Cloud Logging by hand. ``ValkeyCacheBackend._record_failure`` now inspects
the failing exception: a ``MOVED`` ``ResponseError`` on a standalone client
logs one hard WARNING naming the resolved endpoint, rate-limited to once
per backend instance (not per-command spam). Detection only — no client
rebuild, no config mutation (that is #2743's scope).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from valkey.exceptions import ResponseError

from dynastore.tools.cache_valkey import ValkeyCacheBackend


def _standalone_client(target: str = "10.0.0.1:6379 (standalone)") -> MagicMock:
    """A client with no ``nodes_manager`` / ``get_primaries`` — the
    standalone shape ``ValkeyCacheBackend`` duck-types against."""
    client = MagicMock(spec=["get", "_ds_resolved_target"])
    client._ds_resolved_target = target
    return client


def _cluster_client(target: str = "10.0.0.9:6379 (cluster)") -> MagicMock:
    """A client shaped like ``ValkeyCluster`` (has ``nodes_manager`` +
    ``get_primaries``)."""
    client = MagicMock()
    client._ds_resolved_target = target
    client.nodes_manager = MagicMock()
    client.get_primaries = MagicMock(return_value=[])
    return client


def _patched_manager():
    manager = MagicMock()
    manager.unregister_backend = MagicMock()
    return patch("dynastore.tools.cache.get_cache_manager", return_value=manager)


async def test_moved_on_standalone_logs_hard_warning_naming_endpoint(caplog):
    client = _standalone_client("10.0.0.1:6379 (standalone)")
    client.get = AsyncMock(side_effect=ResponseError("MOVED 3999 10.0.0.9:6379"))
    backend = ValkeyCacheBackend(client=client, owns_client=False)

    with _patched_manager(), caplog.at_level("WARNING"):
        result = await backend.get("k1")

    assert result is None
    mismatch_lines = [
        r for r in caplog.records if "CACHE ENDPOINT MISMATCH" in r.getMessage()
    ]
    assert len(mismatch_lines) == 1
    msg = mismatch_lines[0].getMessage()
    assert "10.0.0.1:6379 (standalone)" in msg
    assert "CLUSTER" in msg


async def test_moved_on_standalone_rate_limited_to_once_per_instance(caplog):
    """A MOVED storm across many commands must warn exactly once, not spam."""
    client = _standalone_client()
    client.get = AsyncMock(side_effect=ResponseError("MOVED 3999 10.0.0.9:6379"))
    backend = ValkeyCacheBackend(client=client, owns_client=False)

    with _patched_manager(), caplog.at_level("WARNING"):
        for _ in range(5):
            await backend.get("k1")

    mismatch_lines = [
        r for r in caplog.records if "CACHE ENDPOINT MISMATCH" in r.getMessage()
    ]
    assert len(mismatch_lines) == 1


async def test_moved_on_cluster_client_does_not_warn(caplog):
    """A cluster-mode client is expected to see MOVED transiently during
    resharding — no mismatch signal should fire."""
    client = _cluster_client()
    client.get = AsyncMock(side_effect=ResponseError("MOVED 3999 10.0.0.9:6379"))
    backend = ValkeyCacheBackend(client=client, owns_client=False)

    with _patched_manager(), caplog.at_level("WARNING"):
        await backend.get("k1")

    mismatch_lines = [
        r for r in caplog.records if "CACHE ENDPOINT MISMATCH" in r.getMessage()
    ]
    assert mismatch_lines == []


async def test_non_moved_failure_on_standalone_does_not_warn(caplog):
    """Ordinary connection errors must not trigger the mismatch signal."""
    client = _standalone_client()
    client.get = AsyncMock(side_effect=ConnectionError("boom"))
    backend = ValkeyCacheBackend(client=client, owns_client=False)

    with _patched_manager(), caplog.at_level("WARNING"):
        await backend.get("k1")

    mismatch_lines = [
        r for r in caplog.records if "CACHE ENDPOINT MISMATCH" in r.getMessage()
    ]
    assert mismatch_lines == []


def test_bare_record_failure_call_stays_inert() -> None:
    """``_record_failure()`` with no exception (existing circuit-breaker
    call sites) must not raise or attempt MOVED detection."""
    client = _standalone_client()
    backend = ValkeyCacheBackend(client=client, owns_client=False)

    with _patched_manager():
        backend._record_failure()  # must not raise

    assert backend._moved_warned is False
