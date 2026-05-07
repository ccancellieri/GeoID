"""Streaming GeoTIFF writer backed by a rasterio MemoryFile."""

from __future__ import annotations

from typing import Any, Iterable, Iterator, Tuple


def write_geotiff(
    *,
    width: int,
    height: int,
    transform,
    crs,
    dtype: str,
    band_count: int,
    tiles: Iterable[Tuple[int, int, Any]],
) -> Iterator[bytes]:
    import rasterio

    with rasterio.MemoryFile() as mem:
        with mem.open(
            driver="GTiff",
            width=width, height=height, count=band_count,
            dtype=dtype, transform=transform, crs=crs,
            tiled=True, blockxsize=256, blockysize=256, compress="deflate",
        ) as dst:
            for col, row, arr in tiles:
                h, w = arr.shape[-2], arr.shape[-1]
                dst.write(arr, 1, window=rasterio.windows.Window(col, row, w, h))  # type: ignore
        yield mem.read()
