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

"""Identity-checked unregister on CacheManager.

Backend names are class-level constants (every ValkeyCacheBackend is
named "valkey"), so a stale instance's late circuit-breaker trip must not
remove a healthy replacement registered under the same name by a live
reconnect.
"""

import pytest

from dynastore.tools import cache as cache_tools
from dynastore.tools.cache import CacheManager


class _NamedAsyncBackend:
    name = "valkey"
    priority = 100

    async def get(self, key):
        return None

    async def set(self, key, value, *, ttl=None, exist=None):
        return True

    async def clear(self, *, key=None, namespace=None, tags=None):
        return False

    async def exists(self, key):
        return False

    async def close(self):
        return None


def test_unregister_removes_current_instance():
    manager = CacheManager()
    backend = _NamedAsyncBackend()
    manager.register_backend(backend)
    manager.unregister_backend(backend)
    with pytest.raises(KeyError):
        manager.get_async_backend("valkey")


def test_unregister_ignores_superseded_instance():
    manager = CacheManager()
    stale = _NamedAsyncBackend()
    replacement = _NamedAsyncBackend()
    manager.register_backend(stale)
    manager.register_backend(replacement)  # same name, new instance

    manager.unregister_backend(stale)  # late circuit-breaker trip

    assert manager.get_async_backend("valkey") is replacement


def test_ignored_unregister_does_not_bump_generation():
    manager = CacheManager()
    stale = _NamedAsyncBackend()
    replacement = _NamedAsyncBackend()
    manager.register_backend(stale)
    manager.register_backend(replacement)

    before = cache_tools._backend_generation
    manager.unregister_backend(stale)
    assert cache_tools._backend_generation == before

    manager.unregister_backend(replacement)
    assert cache_tools._backend_generation == before + 1
