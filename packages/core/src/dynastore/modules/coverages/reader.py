"""Windowed raster reader that yields blocks without loading the whole image.

Lazy-imports rasterio. Callers must supply an open rasterio dataset
(``open_raster_vsi`` from ``modules/gdal/service``).
"""

from __future__ import annotations

from typing import Iterator

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
