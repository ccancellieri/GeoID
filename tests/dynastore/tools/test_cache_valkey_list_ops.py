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

"""Unit tests for ``ValkeyCacheBackend``'s ``ListCacheBackend`` extension
(#2833) — ``rpush_trimmed`` / ``lpop_many``, backing the Valkey-buffered
log producer/drainer. A fake ``valkey.asyncio`` client stands in for the
live server; end-to-end RPUSH/LTRIM/LPOP semantics belong in the
integration suite.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.models.protocols.cache import ListCacheBackend
from dynastore.tools.cache_valkey import ValkeyCacheBackend


def _backend() -> ValkeyCacheBackend:
    client = MagicMock()
    return ValkeyCacheBackend(client=client, owns_client=False)


class TestProtocolConformance:
    def test_valkey_backend_satisfies_list_cache_backend(self) -> None:
        assert isinstance(_backend(), ListCacheBackend)

    def test_protocol_is_runtime_checkable(self) -> None:
        assert getattr(ListCacheBackend, "_is_runtime_protocol", False) is True


class TestRpushTrimmed:
    @pytest.mark.asyncio
    async def test_rpush_under_cap_drops_nothing(self) -> None:
        backend = _backend()
        backend._client.rpush = AsyncMock(return_value=3)
        backend._client.ltrim = AsyncMock()

        dropped = await backend.rpush_trimmed("q", [b"a", b"b", b"c"], max_len=100)

        assert dropped == 0
        backend._client.rpush.assert_awaited_once_with("ds:q", b"a", b"b", b"c")
        backend._client.ltrim.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rpush_over_cap_trims_and_reports_dropped(self) -> None:
        backend = _backend()
        backend._client.rpush = AsyncMock(return_value=105)
        backend._client.ltrim = AsyncMock()

        dropped = await backend.rpush_trimmed("q", [b"x"], max_len=100)

        assert dropped == 5
        backend._client.ltrim.assert_awaited_once_with("ds:q", -100, -1)

    @pytest.mark.asyncio
    async def test_empty_values_is_a_noop(self) -> None:
        backend = _backend()
        backend._client.rpush = AsyncMock()

        dropped = await backend.rpush_trimmed("q", [], max_len=100)

        assert dropped == 0
        backend._client.rpush.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rpush_failure_raises_not_swallowed(self) -> None:
        """Producer needs to observe the failure to fall back to direct
        dispatch — unlike get/set, this must not swallow the exception."""
        backend = _backend()
        backend._client.rpush = AsyncMock(side_effect=ConnectionError("boom"))

        with pytest.raises(ConnectionError):
            await backend.rpush_trimmed("q", [b"a"], max_len=100)

    @pytest.mark.asyncio
    async def test_ltrim_failure_does_not_raise(self) -> None:
        """The push itself already succeeded; a follow-up LTRIM failure is
        logged, not propagated — the entries are safely queued either way."""
        backend = _backend()
        backend._client.rpush = AsyncMock(return_value=105)
        backend._client.ltrim = AsyncMock(side_effect=ConnectionError("boom"))

        dropped = await backend.rpush_trimmed("q", [b"x"], max_len=100)
        assert dropped == 5  # still reported, even though the trim itself failed


class TestLpopMany:
    @pytest.mark.asyncio
    async def test_lpop_many_returns_popped_entries(self) -> None:
        backend = _backend()
        backend._client.lpop = AsyncMock(return_value=[b"a", b"b"])

        result = await backend.lpop_many("q", 10)

        assert result == [b"a", b"b"]
        backend._client.lpop.assert_awaited_once_with("ds:q", 10)

    @pytest.mark.asyncio
    async def test_lpop_many_empty_list_returns_empty(self) -> None:
        backend = _backend()
        backend._client.lpop = AsyncMock(return_value=None)

        result = await backend.lpop_many("q", 10)

        assert result == []

    @pytest.mark.asyncio
    async def test_lpop_many_failure_raises(self) -> None:
        backend = _backend()
        backend._client.lpop = AsyncMock(side_effect=ConnectionError("boom"))

        with pytest.raises(ConnectionError):
            await backend.lpop_many("q", 10)
