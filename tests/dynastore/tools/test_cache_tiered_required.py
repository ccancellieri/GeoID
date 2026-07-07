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

from __future__ import annotations

from typing import Optional

import pytest

from dynastore.tools.cache import LocalAsyncCacheBackend, TieredAsyncBackend


class _RequiredFailingBackend:
    name = "valkey"
    priority = 100
    required = True

    async def get(self, key: str) -> Optional[bytes]:
        raise ConnectionError("valkey down")

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ttl: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        return True

    async def clear(self, *, key=None, namespace=None, tags=None) -> bool:
        return True

    async def exists(self, key: str) -> bool:
        return False

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_required_l2_get_failure_does_not_fall_back_to_l1() -> None:
    l1 = LocalAsyncCacheBackend()
    await l1.set("k", b'{"__v": 1, "__d": "stale"}')
    tiered = TieredAsyncBackend([l1, _RequiredFailingBackend()])

    with pytest.raises(ConnectionError, match="valkey down"):
        await tiered.get("k")
