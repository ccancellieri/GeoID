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

import pytest


def test_format_param_resolves_default():
    from dynastore.extensions.coverages.coverages_service import _resolve_format
    assert _resolve_format("geotiff") == "geotiff"
    assert _resolve_format("GeoTIFF") == "geotiff"
    assert _resolve_format(None) == "geotiff"


def test_format_param_rejects_unknown():
    from fastapi import HTTPException
    from dynastore.extensions.coverages.coverages_service import _resolve_format
    with pytest.raises(HTTPException) as exc:
        _resolve_format("webp")
    assert exc.value.status_code == 415


def test_resolve_coverage_asset_returns_href_and_media_type():
    from dynastore.extensions.coverages.coverages_service import _resolve_coverage_asset
    item = {
        "assets": {
            "data": {"href": "gs://bucket/data.zarr", "type": "application/vnd+zarr"},
        }
    }
    href, media_type = _resolve_coverage_asset(item)
    assert href == "gs://bucket/data.zarr"
    assert media_type == "application/vnd+zarr"


def test_resolve_coverage_asset_defaults_media_type_when_untyped():
    from dynastore.extensions.coverages.coverages_service import _resolve_coverage_asset
    item = {"assets": {"data": {"href": "gs://bucket/data.tif"}}}
    href, media_type = _resolve_coverage_asset(item)
    assert href == "gs://bucket/data.tif"
    assert media_type == ""


def test_require_reader_accepts_known_media_type():
    from dynastore.extensions.coverages.coverages_service import _require_reader
    _require_reader("image/tiff; application=geotiff")  # does not raise


def test_require_reader_rejects_unsupported_media_type():
    from fastapi import HTTPException
    from dynastore.extensions.coverages.coverages_service import _require_reader
    with pytest.raises(HTTPException) as exc:
        _require_reader("application/octet-stream")
    assert exc.value.status_code == 415
