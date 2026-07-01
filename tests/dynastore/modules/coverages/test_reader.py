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

import importlib.util
import pytest

_HAS_RASTERIO = importlib.util.find_spec("rasterio") is not None
skipif_no_rasterio = pytest.mark.skipif(not _HAS_RASTERIO, reason="rasterio not installed")


def test_module_importable_without_rasterio():
    from dynastore.modules.coverages import reader
    assert hasattr(reader, "read_window_iter")


def test_reader_for_dispatches_cog_and_zarr_to_gdal_opener():
    """Per-asset media types resolve to a registered reader (COG and Zarr
    alike currently open through GDAL, but the lookup is explicit)."""
    from dynastore.modules.coverages.reader import _open_via_gdal, reader_for

    assert reader_for("image/tiff; application=geotiff; profile=cloud-optimized") is _open_via_gdal
    assert reader_for("application/vnd+zarr") is _open_via_gdal
    assert reader_for("application/x-netcdf") is _open_via_gdal


def test_reader_for_defaults_empty_media_type_to_gdal():
    from dynastore.modules.coverages.reader import _open_via_gdal, reader_for
    assert reader_for("") is _open_via_gdal


def test_reader_for_rejects_unknown_media_type():
    from dynastore.modules.coverages.reader import UnsupportedReaderMediaType, reader_for
    with pytest.raises(UnsupportedReaderMediaType):
        reader_for("application/octet-stream")


@pytest.mark.xfail(reason="#514 — rasterio: TIFF block dims must be multiples of 16; fixture rebuild needed.", strict=False)
@skipif_no_rasterio
def test_read_window_iter_emits_block_tiles(tmp_path):
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin
    from dynastore.modules.coverages.reader import read_window_iter
    from dynastore.modules.coverages.window import WindowBox

    path = tmp_path / "tiny.tif"
    data = np.arange(64, dtype="uint8").reshape(8, 8)
    with rasterio.open(
        path, "w", driver="GTiff", height=8, width=8, count=1,
        dtype="uint8", transform=from_origin(0, 8, 1, 1),
        tiled=True, blockxsize=4, blockysize=4,
    ) as dst:
        dst.write(data, 1)

    with rasterio.open(path) as ds:
        tiles = list(read_window_iter(ds, WindowBox(0, 0, 8, 8), band=1, block=4))
    assert len(tiles) == 4
    assert all(t.shape == (4, 4) for t in tiles)
