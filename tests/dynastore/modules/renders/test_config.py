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

"""Unit tests for RenderCachingConfig and build_render_cache_key.

Pure: no DB, no HTTP, no I/O.
"""

import pytest
from pydantic import ValidationError

from dynastore.modules.renders.config import (
    RenderCachingConfig,
    build_render_cache_key,
)


class TestBuildRenderCacheKey:
    def test_default_prefix(self):
        key = build_render_cache_key(
            "renders/collections",
            "c_internal_123",
            "sld_ndvi",
            "WebMercatorQuad",
            5, 16, 10,
            "png",
        )
        assert key == (
            "renders/collections/c_internal_123/sld_ndvi/WebMercatorQuad/5/16/10.png"
        )

    def test_webp_extension(self):
        key = build_render_cache_key(
            "renders/collections",
            "c_abc",
            "fire",
            "WebMercatorQuad",
            8, 100, 200,
            "webp",
        )
        assert key.endswith(".webp")

    def test_custom_prefix(self):
        key = build_render_cache_key(
            "custom/prefix",
            "coll01",
            "style01",
            "WebMercatorQuad",
            1, 0, 0,
            "png",
        )
        assert key.startswith("custom/prefix/")

    def test_internal_id_in_key_not_external(self):
        # The INTERNAL id must be in the key, not the external label.
        # This test asserts that callers supplying the internal id produce
        # a deterministic key shape.
        internal = "c_internal_000"
        external = "my-readable-name"
        key = build_render_cache_key(
            "renders/collections", internal, "s", "WebMercatorQuad", 0, 0, 0, "png"
        )
        assert internal in key
        assert external not in key


class TestRenderCachingConfig:
    def test_defaults(self):
        cfg = RenderCachingConfig()
        assert cfg.cache_enabled is True
        assert cfg.key_prefix == "renders/collections"
        assert cfg.ttl_seconds == 31536000
        assert cfg.render_budget_seconds == 55

    def test_render_budget_seconds_must_be_positive(self):
        with pytest.raises(ValidationError):
            RenderCachingConfig(render_budget_seconds=0)

    def test_invalid_key_prefix_too_short(self):
        with pytest.raises(ValidationError):
            RenderCachingConfig(key_prefix="x")

    def test_invalid_key_prefix_bad_char(self):
        with pytest.raises(ValidationError):
            # Leading slash is not allowed by the pattern
            RenderCachingConfig(key_prefix="/renders")

    def test_ttl_zero_allowed(self):
        cfg = RenderCachingConfig(ttl_seconds=0)
        assert cfg.ttl_seconds == 0

    def test_ttl_above_max_rejected(self):
        with pytest.raises(ValidationError):
            RenderCachingConfig(ttl_seconds=31536001)
