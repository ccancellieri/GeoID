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

"""Unit tests for Terrain-RGB encoding and hillshade computation.

Pure NumPy: no GDAL, no rio-tiler, no DB, no HTTP.  Only the private helpers
and pure-NumPy logic paths in engine.py are exercised here.
"""

from __future__ import annotations

import numpy as np
import pytest

from dynastore.modules.renders.engine import (
    RioColormap,
    _apply_colormap_hillshade,
    _compute_hillshade,
    _elevation_to_terrain_rgb,
)


# ---------------------------------------------------------------------------
# _elevation_to_terrain_rgb
# ---------------------------------------------------------------------------


class TestElevationToTerrainRgb:
    """Validate the Mapbox Terrain-RGB encoding against the specification."""

    def _decode(self, rgb: np.ndarray, r: int, g: int, b: int) -> float:
        """Inverse of the Terrain-RGB encoding."""
        packed = r * 65536 + g * 256 + b
        return -10000 + packed * 0.1

    def test_sea_level_zero(self):
        """0 m should encode to the packed value for 10 000 / 0.1 = 100 000."""
        elev = np.array([[0.0]], dtype=np.float32)
        rgb = _elevation_to_terrain_rgb(elev)
        assert rgb.shape == (3, 1, 1)
        r, g, b = int(rgb[0, 0, 0]), int(rgb[1, 0, 0]), int(rgb[2, 0, 0])
        assert self._decode(rgb, r, g, b) == pytest.approx(0.0, abs=0.1)

    def test_positive_elevation(self):
        """A representative 1000 m elevation should round-trip within ±0.1 m."""
        elev = np.array([[1000.0]], dtype=np.float32)
        rgb = _elevation_to_terrain_rgb(elev)
        r, g, b = int(rgb[0, 0, 0]), int(rgb[1, 0, 0]), int(rgb[2, 0, 0])
        assert self._decode(rgb, r, g, b) == pytest.approx(1000.0, abs=0.1)

    def test_negative_elevation(self):
        """Below-sea-level elevation (e.g. Dead Sea −430 m) should encode correctly."""
        elev = np.array([[-430.0]], dtype=np.float32)
        rgb = _elevation_to_terrain_rgb(elev)
        r, g, b = int(rgb[0, 0, 0]), int(rgb[1, 0, 0]), int(rgb[2, 0, 0])
        assert self._decode(rgb, r, g, b) == pytest.approx(-430.0, abs=0.1)

    def test_minimum_clamp(self):
        """Elevation below −10 000 m clips to (0, 0, 0)."""
        elev = np.array([[-11000.0]], dtype=np.float32)
        rgb = _elevation_to_terrain_rgb(elev)
        assert rgb[0, 0, 0] == 0
        assert rgb[1, 0, 0] == 0
        assert rgb[2, 0, 0] == 0

    def test_maximum_clamp(self):
        """Elevation above ~1 677 721 m clips to (255, 255, 255)."""
        elev = np.array([[2_000_000.0]], dtype=np.float32)
        rgb = _elevation_to_terrain_rgb(elev)
        assert rgb[0, 0, 0] == 255
        assert rgb[1, 0, 0] == 255
        assert rgb[2, 0, 0] == 255

    def test_output_dtype_uint8(self):
        """Output must be uint8 — required by PNG encoding."""
        elev = np.zeros((4, 4), dtype=np.float32)
        rgb = _elevation_to_terrain_rgb(elev)
        assert rgb.dtype == np.uint8

    def test_output_shape(self):
        """Output shape must be (3, H, W)."""
        elev = np.zeros((16, 16), dtype=np.float32)
        rgb = _elevation_to_terrain_rgb(elev)
        assert rgb.shape == (3, 16, 16)

    def test_batch_shapes(self):
        """All cells in a batch should encode independently."""
        elev = np.array([[0.0, 100.0], [500.0, -100.0]], dtype=np.float32)
        rgb = _elevation_to_terrain_rgb(elev)
        assert rgb.shape == (3, 2, 2)
        # Each cell must round-trip.
        for row in range(2):
            for col in range(2):
                r, g, b = int(rgb[0, row, col]), int(rgb[1, row, col]), int(rgb[2, row, col])
                got = self._decode(rgb, r, g, b)
                assert got == pytest.approx(float(elev[row, col]), abs=0.15)


# ---------------------------------------------------------------------------
# _compute_hillshade
# ---------------------------------------------------------------------------


class TestComputeHillshade:
    """Validate hillshade output range, shape, and boundary conditions."""

    def test_output_range_0_1(self):
        """All hillshade values must lie in [0, 1]."""
        rng = np.random.default_rng(42)
        elev = rng.uniform(0, 3000, (64, 64)).astype(np.float32)
        shade = _compute_hillshade(elev)
        assert shade.min() >= 0.0
        assert shade.max() <= 1.0

    def test_output_shape_matches_input(self):
        elev = np.zeros((32, 32), dtype=np.float32)
        shade = _compute_hillshade(elev)
        assert shade.shape == (32, 32)

    def test_flat_terrain_uniform_shade(self):
        """Perfectly flat terrain — every interior pixel has identical shade."""
        elev = np.full((10, 10), 100.0, dtype=np.float32)
        shade = _compute_hillshade(elev)
        interior = shade[1:-1, 1:-1]
        assert np.allclose(interior, interior[0, 0], atol=1e-6)

    def test_boundary_pixels_match_flat_shade(self):
        """Boundary pixels use dx=dy=0 (same as flat terrain interior).

        The finite-difference kernel only writes interior pixels; boundary
        pixels keep dx=dy=0 (the initial np.zeros_like fill).  For a flat
        surface dx=dy=0 everywhere, so boundary and interior values should
        be identical — equal to sin(altitude).
        """
        elev = np.full((6, 6), 50.0, dtype=np.float32)
        shade = _compute_hillshade(elev, altitude=45.0)
        # All pixels (boundary included) should equal sin(45°) ≈ 0.7071
        expected = np.sin(np.deg2rad(45.0))
        assert np.allclose(shade, expected, atol=1e-6)

    def test_azimuth_affects_shade(self):
        """Different azimuths produce different hillshade maps."""
        elev = np.linspace(0, 1000, 64).reshape(8, 8).astype(np.float32)
        shade_nw = _compute_hillshade(elev, azimuth=315.0)
        shade_se = _compute_hillshade(elev, azimuth=135.0)
        assert not np.allclose(shade_nw, shade_se)

    def test_altitude_affects_shade(self):
        """Different sun altitudes produce different hillshade maps."""
        elev = np.linspace(0, 1000, 64).reshape(8, 8).astype(np.float32)
        shade_low = _compute_hillshade(elev, altitude=10.0)
        shade_high = _compute_hillshade(elev, altitude=80.0)
        assert not np.allclose(shade_low, shade_high)


# ---------------------------------------------------------------------------
# _apply_colormap_hillshade
# ---------------------------------------------------------------------------


class TestApplyColormapHillshade:
    """Validate colormap-hillshade blending output shape and values."""

    _SIMPLE_CMAP: RioColormap = {
        0: (0, 0, 255, 255),     # blue below 500 m
        500: (0, 255, 0, 255),   # green 500–2000 m
        2000: (255, 255, 255, 255),  # white above 2000 m
    }

    def test_output_shape(self):
        elev = np.zeros((8, 8), dtype=np.float32)
        shade = np.ones((8, 8), dtype=np.float64)
        out = _apply_colormap_hillshade(elev, shade, self._SIMPLE_CMAP)
        assert out.shape == (4, 8, 8)

    def test_output_dtype_uint8(self):
        elev = np.zeros((4, 4), dtype=np.float32)
        shade = np.ones((4, 4), dtype=np.float64)
        out = _apply_colormap_hillshade(elev, shade, self._SIMPLE_CMAP)
        assert out.dtype == np.uint8

    def test_full_sun_equals_colormap_values(self):
        """With shade=1.0 everywhere the output should equal the colormap RGB."""
        elev = np.full((4, 4), 100.0, dtype=np.float32)  # in [0, 500) → blue
        shade = np.ones((4, 4), dtype=np.float64)
        out = _apply_colormap_hillshade(elev, shade, self._SIMPLE_CMAP)
        assert np.all(out[0] == 0)     # R
        assert np.all(out[1] == 0)     # G
        assert np.all(out[2] == 255)   # B
        assert np.all(out[3] == 255)   # A

    def test_zero_shade_makes_black(self):
        """shade=0 should produce black RGB (hillshade in full shadow)."""
        elev = np.full((4, 4), 100.0, dtype=np.float32)
        shade = np.zeros((4, 4), dtype=np.float64)
        out = _apply_colormap_hillshade(elev, shade, self._SIMPLE_CMAP)
        assert np.all(out[0] == 0)
        assert np.all(out[1] == 0)
        assert np.all(out[2] == 0)

    def test_high_elevation_uses_highest_class(self):
        """Pixels ≥ 2000 m should use the last colormap class (white)."""
        elev = np.full((4, 4), 3000.0, dtype=np.float32)
        shade = np.ones((4, 4), dtype=np.float64)
        out = _apply_colormap_hillshade(elev, shade, self._SIMPLE_CMAP)
        assert np.all(out[0] == 255)
        assert np.all(out[1] == 255)
        assert np.all(out[2] == 255)

    def test_empty_colormap_returns_greyscale(self):
        """An empty colormap falls back to greyscale hillshade."""
        elev = np.full((4, 4), 500.0, dtype=np.float32)
        shade = np.full((4, 4), 0.5, dtype=np.float64)
        out = _apply_colormap_hillshade(elev, shade, {})
        # All three channels should be equal (greyscale)
        assert np.all(out[0] == out[1])
        assert np.all(out[1] == out[2])

    def test_interval_colormap_matches_discrete_equivalent(self):
        """SLD ramp/intervals parses now yield interval colormaps; the
        hillshade blender must treat them like the equivalent discrete dict."""
        interval_cmap: RioColormap = [
            ((0.0, 500.0), (0, 0, 255, 255)),
            ((500.0, 2000.0), (0, 255, 0, 255)),
            ((2000.0, float("inf")), (255, 255, 255, 255)),
        ]
        elev = np.array([[100.0, 900.0], [2500.0, -50.0]], dtype=np.float32)
        shade = np.ones((2, 2), dtype=np.float64)
        out = _apply_colormap_hillshade(elev, shade, interval_cmap)
        expected = _apply_colormap_hillshade(elev, shade, self._SIMPLE_CMAP)
        assert np.array_equal(out, expected)
