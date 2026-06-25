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

"""Unit tests for RenderPreseedConfig — pure, no DB, no I/O."""

import pytest
from pydantic import ValidationError

from dynastore.modules.renders.config import RenderPreseedConfig


class TestRenderPreseedConfig:
    def test_disabled_by_default(self):
        cfg = RenderPreseedConfig()
        assert cfg.enabled is False

    def test_default_zoom_range(self):
        cfg = RenderPreseedConfig()
        assert cfg.min_zoom == 0
        assert cfg.max_zoom == 6
        assert cfg.min_zoom <= cfg.max_zoom

    def test_default_tms(self):
        cfg = RenderPreseedConfig()
        assert cfg.tms_ids == ["WebMercatorQuad"]

    def test_seed_raster_on_by_default(self):
        cfg = RenderPreseedConfig()
        assert cfg.seed_raster is True

    def test_seed_vector_off_by_default(self):
        cfg = RenderPreseedConfig()
        assert cfg.seed_vector is False

    def test_style_id_defaults_to_none(self):
        cfg = RenderPreseedConfig()
        assert cfg.style_id is None

    def test_max_zoom_below_min_zoom_raises(self):
        with pytest.raises(ValidationError):
            RenderPreseedConfig(min_zoom=5, max_zoom=3)

    def test_equal_min_max_zoom_valid(self):
        cfg = RenderPreseedConfig(min_zoom=4, max_zoom=4)
        assert cfg.min_zoom == cfg.max_zoom == 4

    def test_explicit_enable(self):
        cfg = RenderPreseedConfig(enabled=True, min_zoom=0, max_zoom=8)
        assert cfg.enabled is True
        assert cfg.max_zoom == 8

    def test_address(self):
        assert RenderPreseedConfig._address == (
            "platform", "modules", "renders", "preseed"
        )

    def test_negative_zoom_rejected(self):
        with pytest.raises(ValidationError):
            RenderPreseedConfig(min_zoom=-1)
