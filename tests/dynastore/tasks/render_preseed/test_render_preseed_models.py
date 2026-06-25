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

"""Unit tests for RenderPreseedInputs payload model — pure, no I/O."""

import pytest
from pydantic import ValidationError

from dynastore.tasks.render_preseed.models import RenderPreseedInputs


class TestRenderPreseedInputs:
    def test_raster_roundtrip(self):
        raw = {
            "catalog_id": "cat1",
            "collection_id": "col1",
            "asset_id": "a1",
            "producer_kind": "raster",
            "min_zoom": 0,
            "max_zoom": 6,
            "tms_ids": ["WebMercatorQuad"],
            "style_id": "sld_fire",
        }
        inputs = RenderPreseedInputs.model_validate(raw)
        assert inputs.producer_kind == "raster"
        assert inputs.min_zoom == 0
        assert inputs.max_zoom == 6
        assert inputs.style_id == "sld_fire"
        assert "WebMercatorQuad" in inputs.tms_ids

    def test_vector_kind_accepted(self):
        inputs = RenderPreseedInputs(
            catalog_id="c",
            collection_id="co",
            asset_id="a",
            producer_kind="vector",
            min_zoom=2,
            max_zoom=8,
        )
        assert inputs.producer_kind == "vector"

    def test_invalid_producer_kind_rejected(self):
        with pytest.raises(ValidationError):
            RenderPreseedInputs(
                catalog_id="c",
                collection_id="co",
                asset_id="a",
                producer_kind="pmtiles",  # not a valid literal
                min_zoom=0,
                max_zoom=4,
            )

    def test_negative_min_zoom_rejected(self):
        with pytest.raises(ValidationError):
            RenderPreseedInputs(
                catalog_id="c",
                collection_id="co",
                asset_id="a",
                producer_kind="raster",
                min_zoom=-1,
                max_zoom=4,
            )

    def test_style_id_defaults_to_default(self):
        inputs = RenderPreseedInputs(
            catalog_id="c",
            collection_id="co",
            asset_id="a",
            producer_kind="raster",
            min_zoom=0,
            max_zoom=4,
        )
        assert inputs.style_id == "default"

    def test_tms_ids_defaults_to_webmercator(self):
        inputs = RenderPreseedInputs(
            catalog_id="c",
            collection_id="co",
            asset_id="a",
            producer_kind="raster",
            min_zoom=0,
            max_zoom=4,
        )
        assert inputs.tms_ids == ["WebMercatorQuad"]

    def test_zoom_bound_enforced_in_task_run(self):
        """min_zoom > max_zoom is stored (model allows it); task run logic enforces it."""
        # The model itself does NOT enforce zoom order — that is the task's
        # responsibility so a mis-configured obligation can still be observed
        # in the queue and is not silently dropped by the validator.
        inputs = RenderPreseedInputs(
            catalog_id="c",
            collection_id="co",
            asset_id="a",
            producer_kind="raster",
            min_zoom=8,
            max_zoom=2,  # inverted
        )
        assert inputs.min_zoom == 8
        assert inputs.max_zoom == 2

    def test_json_roundtrip(self):
        inputs = RenderPreseedInputs(
            catalog_id="my-cat",
            collection_id="my-col",
            asset_id="my-asset",
            producer_kind="raster",
            min_zoom=0,
            max_zoom=5,
            tms_ids=["WebMercatorQuad", "WorldCRS84Quad"],
            style_id="fire",
        )
        dumped = inputs.model_dump()
        reloaded = RenderPreseedInputs.model_validate(dumped)
        assert reloaded == inputs
