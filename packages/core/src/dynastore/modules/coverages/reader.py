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

"""Windowed raster reader that yields blocks without loading the whole image.

Lazy-imports rasterio. Callers must supply an open rasterio dataset
(``open_raster_vsi`` from ``modules/gdal/service``).
"""

from __future__ import annotations

from typing import Any, Iterator, Optional, Tuple

from dynastore.modules.coverages.window import WindowBox


def read_window_iter(ds, box: WindowBox, band: int = 1, block: int = 512) -> Iterator:
    """Yield numpy arrays block-by-block covering ``box``."""
    import numpy as np
    from rasterio.windows import Window

    if box.width == 0 or box.height == 0:
        return

    for row in range(box.row_off, box.row_off + box.height, block):
        rh = min(block, box.row_off + box.height - row)
        for col in range(box.col_off, box.col_off + box.width, block):
            cw = min(block, box.col_off + box.width - col)
            arr = ds.read(band, window=Window(col, row, cw, rh))  # type: ignore
            yield np.asarray(arr)


def read_scaled(
    ds,
    box: WindowBox,
    band: int = 1,
    out_shape: Optional[Tuple[int, int]] = None,
) -> Any:
    """Read the raster window in a single call, optionally downsampling.

    ``out_shape`` is ``(out_height, out_width)`` in output pixels. When
    ``None`` the native resolution of ``box`` is used.  Uses rasterio's
    built-in ``out_shape`` resampling (Lanczos for downsampling, nearest
    otherwise) so the full down-sample is GPU/GDAL-accelerated and does not
    buffer the native-resolution pixels in Python.
    """
    import numpy as np
    from rasterio.enums import Resampling
    from rasterio.windows import Window

    win = Window(box.col_off, box.row_off, box.width, box.height)
    if out_shape is None:
        out_shape = (box.height, box.width)

    arr = ds.read(  # type: ignore[union-attr]
        band,
        window=win,
        out_shape=out_shape,
        resampling=Resampling.lanczos,
    )
    return np.asarray(arr)
