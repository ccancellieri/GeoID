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

"""Unit tests for ``cache_clear_prefix`` on ``@cached`` functions.

Covers:
- ``cache_clear_prefix`` is attached to every async ``@cached`` function.
- Local backend: entries matching the sub-namespace prefix are evicted;
  unrelated entries are preserved (validated via call-count tracking).
- The trailing ``|`` in the prefix ensures ``coll_a`` does not match
  ``coll_ab`` — repr-wrapped IDs are naturally quoted so the delimiter
  is unambiguous.
- ``invalidate_collection_config_cache`` busts all class-key entries for a
  ``(catalog, collection)`` pair while leaving other collections intact.
- Lifecycle transition triggers the config-cache bust.
"""

from typing import Any, Dict, List

import pytest

from dynastore.tools.cache import cached


# ---------------------------------------------------------------------------
#  cache_clear_prefix: attribute presence
# ---------------------------------------------------------------------------


class TestCacheClearPrefixPresence:
    """``cache_clear_prefix`` is attached to every async ``@cached`` function."""

    @pytest.mark.asyncio
    async def test_attribute_is_attached(self):
        """Every async @cached function exposes cache_clear_prefix."""

        @cached(maxsize=64, namespace="test_ns_attr", distributed=False)
        async def my_fn(a: str, b: str) -> str:
            return f"{a}:{b}"

        assert callable(getattr(my_fn, "cache_clear_prefix", None))

    def test_sync_cached_does_not_break(self):
        """The sync ``@cached`` path must not raise on import."""

        @cached(maxsize=16, namespace="sync_test_presence", distributed=False)
        def sync_fn(x: str) -> str:
            return x

        # The sync wrapper gets cache_invalidate / cache_clear — no error expected.
        assert callable(getattr(sync_fn, "cache_invalidate", None))
        assert callable(getattr(sync_fn, "cache_clear", None))


# ---------------------------------------------------------------------------
#  cache_clear_prefix: local backend behaviour
# ---------------------------------------------------------------------------


class TestCacheClearPrefixLocal:
    """``cache_clear_prefix`` removes the right keys on a local ``@cached`` function."""

    @pytest.mark.asyncio
    async def test_prefix_clear_forces_refetch_for_target_collection(self):
        """After clearing a collection's sub-namespace, the next call re-invokes the fn."""
        call_counts: Dict[str, int] = {}

        @cached(maxsize=64, namespace="col_cfg_refetch", distributed=False)
        async def fn(cat: str, coll: str, cls_key: str) -> str:
            key = f"{cat}/{coll}/{cls_key}"
            call_counts[key] = call_counts.get(key, 0) + 1
            return key

        # Populate two class_key entries for coll_a and one for coll_b.
        await fn("cat1", "coll_a", "routing")
        await fn("cat1", "coll_a", "tiles")
        await fn("cat1", "coll_b", "routing")

        assert call_counts.get("cat1/coll_a/routing") == 1
        assert call_counts.get("cat1/coll_a/tiles") == 1
        assert call_counts.get("cat1/coll_b/routing") == 1

        # Sub-namespace prefix for coll_a: matches every key that has
        # repr('cat1') + "|" + repr('coll_a') as its second and third segment.
        sub_ns = f"col_cfg_refetch|{repr('cat1')}|{repr('coll_a')}"
        fn.cache_clear_prefix(sub_ns)  # type: ignore[attr-defined]

        # Next reads for coll_a must hit the function again.
        await fn("cat1", "coll_a", "routing")
        await fn("cat1", "coll_a", "tiles")

        assert call_counts.get("cat1/coll_a/routing") == 2, "routing re-fetched after clear"
        assert call_counts.get("cat1/coll_a/tiles") == 2, "tiles re-fetched after clear"

        # coll_b must still be cached (call count unchanged).
        await fn("cat1", "coll_b", "routing")
        assert call_counts.get("cat1/coll_b/routing") == 1, "coll_b entry still cached"

    @pytest.mark.asyncio
    async def test_prefix_clear_no_match_is_noop(self):
        """Clearing a sub-namespace that matches no keys is a safe no-op."""
        call_counts: Dict[str, int] = {}

        @cached(maxsize=64, namespace="col_cfg_noop", distributed=False)
        async def fn2(cat: str, coll: str) -> str:
            call_counts[(cat, coll)] = call_counts.get((cat, coll), 0) + 1
            return f"{cat}:{coll}"

        await fn2("cat1", "coll_a")
        assert call_counts.get(("cat1", "coll_a")) == 1

        # Clear a non-existent sub-namespace — must not raise and must not evict.
        fn2.cache_clear_prefix(f"col_cfg_noop|{repr('cat1')}|{repr('nonexistent')}")  # type: ignore[attr-defined]

        await fn2("cat1", "coll_a")
        assert call_counts.get(("cat1", "coll_a")) == 1, "entry must still be cached"

    @pytest.mark.asyncio
    async def test_partial_prefix_does_not_clear_longer_match(self):
        """``coll_a`` sub-namespace does not evict ``coll_ab`` entries.

        The sub-namespace for 'coll_a' ends with ``|repr('coll_a')|``.
        Since repr() wraps the id in quotes, the key for 'coll_ab' starts
        with ``...|repr('coll_ab')|`` — a different string — so no overlap.
        """
        call_counts: Dict[str, int] = {}

        @cached(maxsize=64, namespace="col_cfg_partial", distributed=False)
        async def fn3(cat: str, coll: str, cls_key: str) -> str:
            key = f"{cat}/{coll}/{cls_key}"
            call_counts[key] = call_counts.get(key, 0) + 1
            return key

        await fn3("cat1", "coll_a", "routing")
        await fn3("cat1", "coll_ab", "routing")

        sub_ns = f"col_cfg_partial|{repr('cat1')}|{repr('coll_a')}"
        fn3.cache_clear_prefix(sub_ns)  # type: ignore[attr-defined]

        # coll_a was cleared.
        await fn3("cat1", "coll_a", "routing")
        assert call_counts.get("cat1/coll_a/routing") == 2, "coll_a must be refetched"

        # coll_ab was NOT cleared.
        await fn3("cat1", "coll_ab", "routing")
        assert call_counts.get("cat1/coll_ab/routing") == 1, "coll_ab must still be cached"

    @pytest.mark.asyncio
    async def test_prefix_clear_is_catalog_scoped(self):
        """Clearing (cat_a, coll) does not evict (cat_b, coll) entries."""
        call_counts: Dict[str, int] = {}

        @cached(maxsize=64, namespace="col_cfg_catscope", distributed=False)
        async def fn4(cat: str, coll: str, cls_key: str) -> str:
            key = f"{cat}/{coll}/{cls_key}"
            call_counts[key] = call_counts.get(key, 0) + 1
            return key

        await fn4("cat_a", "shared_coll", "routing")
        await fn4("cat_b", "shared_coll", "routing")

        # Clear only cat_a/shared_coll.
        sub_ns = f"col_cfg_catscope|{repr('cat_a')}|{repr('shared_coll')}"
        fn4.cache_clear_prefix(sub_ns)  # type: ignore[attr-defined]

        await fn4("cat_a", "shared_coll", "routing")
        assert call_counts.get("cat_a/shared_coll/routing") == 2, "cat_a entry refetched"

        await fn4("cat_b", "shared_coll", "routing")
        assert call_counts.get("cat_b/shared_coll/routing") == 1, "cat_b still cached"


# ---------------------------------------------------------------------------
#  invalidate_collection_config_cache
# ---------------------------------------------------------------------------


class TestInvalidateCollectionConfigCache:
    """``invalidate_collection_config_cache`` clears all class-key variants for one collection.

    Tests patch ``_collection_config_cache`` in-module with a local @cached
    stand-in that uses the same key structure (same ``namespace``, same
    ``ignore=["engine", "catalog_manager"]`` so ignored args are excluded from
    the cache key, matching the production key format).
    """

    @pytest.mark.asyncio
    async def test_busts_all_class_keys_for_target_collection(self):
        """All class-key entries for the target (catalog, collection) are cleared."""
        call_log: List[tuple] = []

        # Mirror of the real _collection_config_cache: same namespace, same ignore list.
        @cached(maxsize=256, namespace="collection_config", ignore=["engine", "catalog_manager"], distributed=False)
        async def _fake_coll_config(engine: Any, catalog_manager: Any, cat: str, coll: str, cls_key: str) -> str:
            entry = (cat, coll, cls_key)
            call_log.append(entry)
            return f"{cat}/{coll}/{cls_key}"

        from dynastore.modules.catalog import config_service as _cs

        original = _cs._collection_config_cache
        _cs._collection_config_cache = _fake_coll_config  # type: ignore[attr-defined]

        try:
            sentinel_engine = object()
            sentinel_mgr = object()

            # Prime: two class_key entries for target_coll and one for other_coll.
            await _fake_coll_config(sentinel_engine, sentinel_mgr, "mycat", "target_coll", "routing")
            await _fake_coll_config(sentinel_engine, sentinel_mgr, "mycat", "target_coll", "tiles")
            await _fake_coll_config(sentinel_engine, sentinel_mgr, "mycat", "other_coll", "routing")
            assert len(call_log) == 3

            # Invalidate only target_coll.
            from dynastore.modules.catalog.config_service import invalidate_collection_config_cache
            invalidate_collection_config_cache("mycat", "target_coll")

            call_log.clear()

            # target_coll entries must be re-fetched (cache miss → function called).
            await _fake_coll_config(sentinel_engine, sentinel_mgr, "mycat", "target_coll", "routing")
            await _fake_coll_config(sentinel_engine, sentinel_mgr, "mycat", "target_coll", "tiles")

            assert ("mycat", "target_coll", "routing") in call_log, "routing must have been re-fetched"
            assert ("mycat", "target_coll", "tiles") in call_log, "tiles must have been re-fetched"

            # other_coll entry must still be cached (not in call_log).
            await _fake_coll_config(sentinel_engine, sentinel_mgr, "mycat", "other_coll", "routing")
            assert ("mycat", "other_coll", "routing") not in call_log, "other_coll must still be cached"

        finally:
            _cs._collection_config_cache = original  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_invalidate_is_catalog_scoped(self):
        """Invalidating (cat_a, coll) does not evict (cat_b, coll) entries."""
        call_log: List[tuple] = []

        @cached(maxsize=64, namespace="collection_config", ignore=["engine", "catalog_manager"], distributed=False)
        async def _fake2(engine: Any, catalog_manager: Any, cat: str, coll: str, cls_key: str) -> str:
            entry = (cat, coll, cls_key)
            call_log.append(entry)
            return f"{cat}/{coll}/{cls_key}"

        from dynastore.modules.catalog import config_service as _cs

        original = _cs._collection_config_cache
        _cs._collection_config_cache = _fake2  # type: ignore[attr-defined]

        try:
            sentinel = object()
            await _fake2(sentinel, sentinel, "cat_a", "shared_coll", "routing")
            await _fake2(sentinel, sentinel, "cat_b", "shared_coll", "routing")
            assert len(call_log) == 2
            call_log.clear()

            from dynastore.modules.catalog.config_service import invalidate_collection_config_cache
            invalidate_collection_config_cache("cat_a", "shared_coll")

            # cat_a entry re-fetched.
            await _fake2(sentinel, sentinel, "cat_a", "shared_coll", "routing")
            assert ("cat_a", "shared_coll", "routing") in call_log, "cat_a must be re-fetched"

            # cat_b entry still cached.
            await _fake2(sentinel, sentinel, "cat_b", "shared_coll", "routing")
            assert ("cat_b", "shared_coll", "routing") not in call_log, "cat_b must still be cached"

        finally:
            _cs._collection_config_cache = original  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
#  _invalidate_collection_lifecycle_caches wires config bust
# ---------------------------------------------------------------------------


class TestLifecycleInvalidationIncludesConfigCache:
    """``_invalidate_collection_lifecycle_caches`` delegates to ``invalidate_collection_config_cache``."""

    def test_lifecycle_calls_config_bust_with_correct_args(self):
        """Smoke test: the fan-out function now includes the config-cache clear."""
        from dynastore.modules.catalog.collection_service import (
            _invalidate_collection_lifecycle_caches,
        )
        from dynastore.modules.catalog import config_service as _cs
        from dynastore.modules.storage import router as _router
        import dynastore.modules.catalog.collection_service as _col_svc

        called_with: List[tuple] = []

        def fake_config_bust(cat: str, coll: str) -> None:
            called_with.append(("config", cat, coll))

        def fake_router_bust(cat: str, coll: str) -> None:
            called_with.append(("router", cat, coll))

        def fake_model_bust(cat: str, coll: str) -> None:
            called_with.append(("model", cat, coll))

        original_config = _cs.invalidate_collection_config_cache
        original_router = _router.invalidate_router_cache
        original_model = _col_svc._invalidate_collection_model_cache

        _cs.invalidate_collection_config_cache = fake_config_bust  # type: ignore[attr-defined]
        _router.invalidate_router_cache = fake_router_bust  # type: ignore[attr-defined]
        _col_svc._invalidate_collection_model_cache = fake_model_bust  # type: ignore[attr-defined]

        try:
            _invalidate_collection_lifecycle_caches("the_catalog", "the_collection")

            tags = {t[0] for t in called_with}
            assert "config" in tags, "Config cache bust was not called"
            assert "router" in tags, "Router cache bust was not called"
            assert "model" in tags, "Model cache bust was not called"

            config_call = next(t for t in called_with if t[0] == "config")
            assert config_call == ("config", "the_catalog", "the_collection")

        finally:
            _cs.invalidate_collection_config_cache = original_config  # type: ignore[attr-defined]
            _router.invalidate_router_cache = original_router  # type: ignore[attr-defined]
            _col_svc._invalidate_collection_model_cache = original_model  # type: ignore[attr-defined]
