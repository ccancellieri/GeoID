"""PNG → JPEG/GeoTIFF output conversion for OGC API — Maps.

Extracted from ``maps_service`` so the helper can be tested without
importing the full renderer (which pulls in osgeo/GDAL and is only
installed in the production base image).

Supported output formats on the OGC API - Maps response:

- ``png``      — ``image/png`` (default; passthrough)
- ``jpeg``     — ``image/jpeg`` (flattens alpha on white)
- ``geotiff``  — ``image/tiff;application=geotiff`` (requires ``bbox`` + ``crs``)
"""

from __future__ import annotations

from typing import List, Optional

from fastapi import HTTPException

SUPPORTED_MAP_FORMATS = {"png", "jpeg", "geotiff"}
FORMAT_MEDIA_TYPES = {
    "png": "image/png",
    "jpeg": "image/jpeg",
    "geotiff": "image/tiff;application=geotiff",
}


def convert_png_to_format(
    png_bytes: bytes,
    fmt: str,
    *,
    bbox: Optional[List[float]] = None,
    crs: Optional[str] = None,
) -> bytes:
    """Convert PNG bytes to the requested output format.

    Returns the converted bytes. Raises ``HTTPException(415)`` for
    unsupported formats. ``bbox`` and ``crs`` are required for
    ``geotiff`` so the output carries valid georeferencing; otherwise a
    400 is raised.
    """
    if fmt == "png":
        return png_bytes
    if fmt == "jpeg":
        from io import BytesIO

        from PIL import Image

        with Image.open(BytesIO(png_bytes)) as img:
            # Flatten alpha on white so JPEG (no alpha) stays opaque.
            if img.mode in ("RGBA", "LA"):
                background = Image.new("RGB", img.size, (255, 255, 255))
                background.paste(img, mask=img.split()[-1])
                rgb = background
            else:
                rgb = img.convert("RGB")
            buf = BytesIO()
            rgb.save(buf, format="JPEG", quality=85, optimize=True)
            return buf.getvalue()
    if fmt == "geotiff":
        if bbox is None or crs is None:
            raise HTTPException(
                status_code=400,
                detail="GeoTIFF output requires bbox and crs.",
            )
        from io import BytesIO

        import numpy as np
        import rasterio
        from PIL import Image
        from rasterio.transform import from_bounds

        with Image.open(BytesIO(png_bytes)) as img:
            arr = np.array(img.convert("RGBA"))  # (H, W, 4)
        bands = arr.transpose(2, 0, 1)  # (4, H, W)
        height, width = bands.shape[1:]
        transform = from_bounds(*bbox, width, height)
        with rasterio.MemoryFile() as mem:
            with mem.open(
                driver="GTiff",
                count=4,
                dtype=bands.dtype,
                width=width,
                height=height,
                transform=transform,
                crs=crs,
                photometric="RGB",
                alpha="unassociated",
            ) as dst:
                dst.write(bands)
            return mem.read()
    raise HTTPException(status_code=415, detail=f"Unsupported map format: {fmt!r}")
