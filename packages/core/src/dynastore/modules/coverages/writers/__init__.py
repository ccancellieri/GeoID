"""Writer dispatch by output format."""

from __future__ import annotations

from typing import Callable, Dict


MEDIA_TYPE_FOR: Dict[str, str] = {
    "geotiff": "image/tiff;application=geotiff",
    "netcdf":  "application/x-netcdf",
    "zarr":    "application/x-zarr",
    "covjson": "application/prs.coverage+json",
}


def writer_for(fmt: str) -> Callable:
    if fmt == "covjson":
        from .coveragejson import write_coveragejson
        return write_coveragejson
    if fmt == "geotiff":
        from .geotiff import write_geotiff
        return write_geotiff
    if fmt == "netcdf":
        from .netcdf import write_netcdf
        return write_netcdf
    if fmt == "zarr":
        from .zarr import write_zarr
        return write_zarr
    raise ValueError(f"Unknown coverage output format: {fmt!r}")
