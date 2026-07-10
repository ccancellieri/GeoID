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

"""Unit tests for the SLD ColorMap → rio-tiler colormap parser.

Pure: no DB, no HTTP, no rio-tiler required.
"""

import pytest

from dynastore.modules.renders.colormap import (
    _opacity_to_alpha,
    _parse_hex_color,
    extract_sld_body,
    parse_sld_colormap,
)


# ---------------------------------------------------------------------------
# _parse_hex_color
# ---------------------------------------------------------------------------


class TestParseHexColor:
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param("#000000", (0, 0, 0), id="black"),
            pytest.param("#ffffff", (255, 255, 255), id="white"),
            pytest.param("#1a2b3c", (0x1A, 0x2B, 0x3C), id="mixed"),
            pytest.param("#AABBCC", (0xAA, 0xBB, 0xCC), id="uppercase"),
            pytest.param("  #0A0B0C  ", (0x0A, 0x0B, 0x0C), id="leading_whitespace"),
        ],
    )
    def test_parse_hex_color(self, value, expected):
        assert _parse_hex_color(value) == expected

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("0A0B0C", id="no_hash"),
            pytest.param("#ABC", id="short"),
            pytest.param("#ZZZZZZ", id="non_hex"),
        ],
    )
    def test_parse_hex_color_invalid(self, value):
        with pytest.raises(ValueError):
            _parse_hex_color(value)


# ---------------------------------------------------------------------------
# _opacity_to_alpha
# ---------------------------------------------------------------------------


class TestOpacityToAlpha:
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(None, 255, id="none_defaults_to_255"),
            pytest.param("1.0", 255, id="one_is_255"),
            pytest.param("0.0", 0, id="zero_is_0"),
            pytest.param("0.5", 128, id="half"),
            pytest.param("1.5", 255, id="clamp_over_1"),
            pytest.param("-0.1", 0, id="clamp_negative"),
            pytest.param("not-a-float", 255, id="invalid_string_defaults_255"),
        ],
    )
    def test_opacity_to_alpha(self, value, expected):
        assert _opacity_to_alpha(value) == expected


# ---------------------------------------------------------------------------
# parse_sld_colormap — happy paths
# ---------------------------------------------------------------------------

_SLD_SE_NS = (
    'http://www.opengis.net/se'
)

_VALID_SLD_SE = """<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.1.0"
    xmlns:se="http://www.opengis.net/se">
  <NamedLayer>
    <se:Name>single_band</se:Name>
    <UserStyle>
      <se:FeatureTypeStyle>
        <se:Rule>
          <se:RasterSymbolizer>
            <se:ColorMap type="values">
              <se:ColorMapEntry quantity="0"   color="#000000" opacity="0.0"/>
              <se:ColorMapEntry quantity="100" color="#ff0000" opacity="1.0"/>
              <se:ColorMapEntry quantity="200" color="#00ff00" opacity="0.5"/>
              <se:ColorMapEntry quantity="255" color="#ffffff"/>
            </se:ColorMap>
          </se:RasterSymbolizer>
        </se:Rule>
      </se:FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
"""

_VALID_SLD_NO_NS = """<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor>
  <NamedLayer>
    <Name>bare</Name>
    <UserStyle>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap type="values">
              <ColorMapEntry quantity="10" color="#0a0b0c" opacity="1.0"/>
              <ColorMapEntry quantity="20" color="#ffffff"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
"""


class TestParseSldColormap:
    def test_valid_se_namespace(self):
        cmap = parse_sld_colormap(_VALID_SLD_SE)
        assert cmap[0] == (0, 0, 0, 0)
        assert cmap[100] == (255, 0, 0, 255)
        assert cmap[200] == (0, 255, 0, 128)
        assert cmap[255] == (255, 255, 255, 255)

    def test_valid_no_namespace(self):
        cmap = parse_sld_colormap(_VALID_SLD_NO_NS)
        assert cmap[10] == (0x0A, 0x0B, 0x0C, 255)
        assert cmap[20] == (255, 255, 255, 255)

    def test_returns_empty_when_no_colormap_element(self):
        sld = """<StyledLayerDescriptor xmlns:se="http://www.opengis.net/se">
            <se:NamedLayer/>
        </StyledLayerDescriptor>"""
        cmap = parse_sld_colormap(sld)
        assert cmap == {}

    def test_skips_non_integer_quantity(self):
        sld = """<StyledLayerDescriptor>
            <ColorMap type="values">
              <ColorMapEntry quantity="not_an_int" color="#ff0000"/>
              <ColorMapEntry quantity="5" color="#0000ff"/>
            </ColorMap>
        </StyledLayerDescriptor>"""
        cmap = parse_sld_colormap(sld)
        assert 5 in cmap
        assert len(cmap) == 1

    def test_skips_entry_missing_quantity(self):
        sld = """<StyledLayerDescriptor>
            <ColorMap type="values">
              <ColorMapEntry color="#ff0000"/>
              <ColorMapEntry quantity="3" color="#00ff00"/>
            </ColorMap>
        </StyledLayerDescriptor>"""
        cmap = parse_sld_colormap(sld)
        assert 3 in cmap
        assert len(cmap) == 1

    def test_skips_entry_with_bad_color(self):
        sld = """<StyledLayerDescriptor>
            <ColorMap type="values">
              <ColorMapEntry quantity="1" color="notacolor"/>
              <ColorMapEntry quantity="2" color="#aabbcc"/>
            </ColorMap>
        </StyledLayerDescriptor>"""
        cmap = parse_sld_colormap(sld)
        assert 2 in cmap
        assert len(cmap) == 1

    def test_invalid_xml_raises_value_error(self):
        with pytest.raises(ValueError, match="XML parse failed"):
            parse_sld_colormap("not xml at all <<<")

    def test_empty_colormap_returns_empty_dict(self):
        sld = """<StyledLayerDescriptor>
            <ColorMap type="values"/>
        </StyledLayerDescriptor>"""
        cmap = parse_sld_colormap(sld)
        assert cmap == {}

    def test_values_accepts_float_quantities(self):
        """gismgr emits quantity="0.0"-style floats even for integral values."""
        sld = """<StyledLayerDescriptor>
            <ColorMap type="values">
              <ColorMapEntry quantity="7.0" color="#ff0000"/>
              <ColorMapEntry quantity="0.5" color="#0000ff"/>
            </ColorMap>
        </StyledLayerDescriptor>"""
        cmap = parse_sld_colormap(sld)
        assert isinstance(cmap, dict)
        assert cmap[7] == (255, 0, 0, 255)  # integral float → int key
        assert cmap[0.5] == (0, 0, 255, 255)  # true float stays float


# ---------------------------------------------------------------------------
# parse_sld_colormap — ramp (SLD default) and intervals types
# ---------------------------------------------------------------------------

# Live SLD served by gismgr for C3S/AGERA5-RH2M (relative humidity, viridis).
# No ColorMap `type` attribute → SLD default `ramp`; float quantities; a
# -9999 fully-transparent nodata entry. This exact document previously parsed
# to {} (every entry skipped as "non-integer") and rendered grayscale tiles.
_GISMGR_RAMP_SLD = """<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc" xmlns:sld="http://www.opengis.net/sld" version="1.0.0">
  <UserLayer>
    <sld:LayerFeatureConstraints>
      <sld:FeatureTypeConstraint/>
    </sld:LayerFeatureConstraints>
    <sld:UserStyle>
      <sld:Name>C3S_AGERA5-RH2M</sld:Name>
      <sld:Title>Relative Humidity</sld:Title>
      <sld:FeatureTypeStyle>
        <sld:Rule>
          <sld:RasterSymbolizer>
            <sld:ColorMap>
              <sld:ColorMapEntry color="#ffffff" opacity="0.0" quantity="-9999.0" label="no data"/>
              <sld:ColorMapEntry color="#440154" opacity="1.0" quantity="0.0" label="0 %"/>
              <sld:ColorMapEntry color="#48196b" opacity="1.0" quantity="7.0" label="7 %"/>
              <sld:ColorMapEntry color="#462e7d" opacity="1.0" quantity="14.0" label="14 %"/>
              <sld:ColorMapEntry color="#404387" opacity="1.0" quantity="21.0" label="21 %"/>
              <sld:ColorMapEntry color="#39568c" opacity="1.0" quantity="28.0" label="28 %"/>
              <sld:ColorMapEntry color="#31688e" opacity="1.0" quantity="35.0" label="35 %"/>
              <sld:ColorMapEntry color="#29788e" opacity="1.0" quantity="42.0" label="42 %"/>
              <sld:ColorMapEntry color="#23888e" opacity="1.0" quantity="49.0" label="49 %"/>
              <sld:ColorMapEntry color="#1e988b" opacity="1.0" quantity="56.0" label="56 %"/>
              <sld:ColorMapEntry color="#22a884" opacity="1.0" quantity="63.0" label="63 %"/>
              <sld:ColorMapEntry color="#35b779" opacity="1.0" quantity="70.0" label="70 %"/>
              <sld:ColorMapEntry color="#54c668" opacity="1.0" quantity="77.0" label="77 %"/>
              <sld:ColorMapEntry color="#7ad251" opacity="1.0" quantity="84.0" label="84 %"/>
              <sld:ColorMapEntry color="#a5db35" opacity="1.0" quantity="91.0" label="91 %"/>
              <sld:ColorMapEntry color="#d3e21b" opacity="1.0" quantity="98.0" label="98 %"/>
              <sld:ColorMapEntry color="#fde725" opacity="1.0" quantity="105.0" label="105 %"/>
            </sld:ColorMap>
          </sld:RasterSymbolizer>
        </sld:Rule>
      </sld:FeatureTypeStyle>
    </sld:UserStyle>
  </UserLayer>
</StyledLayerDescriptor>
"""


def _intervals(sld_body):
    """Parse ``sld_body`` and assert the result is an interval colormap."""
    cmap = parse_sld_colormap(sld_body)
    assert isinstance(cmap, list)
    return cmap


def _color_at(intervals, value):
    """Return the RGBA the interval colormap assigns to ``value``."""
    for (lo, hi), rgba in intervals:
        if lo <= value < hi:
            return rgba
    return None


def _color_at_or_fail(intervals, value):
    rgba = _color_at(intervals, value)
    assert rgba is not None, f"no interval covers {value}"
    return rgba


class TestParseSldColormapRamp:
    def test_gismgr_sld_returns_interval_colormap(self):
        cmap = parse_sld_colormap(_GISMGR_RAMP_SLD)
        assert isinstance(cmap, list)
        assert cmap, "live gismgr SLD must not parse to an empty colormap"

    def test_gismgr_sld_covers_the_full_value_line(self):
        intervals = _intervals(_GISMGR_RAMP_SLD)
        # Contiguous: every interval starts exactly where the previous ends.
        for ((_, prev_hi), _), ((lo, _), _) in zip(intervals, intervals[1:]):
            assert lo == prev_hi
        assert intervals[0][0][0] == float("-inf")
        assert intervals[-1][0][1] == float("inf")

    def test_gismgr_nodata_below_first_entry_is_transparent(self):
        intervals = _intervals(_GISMGR_RAMP_SLD)
        assert _color_at_or_fail(intervals, -9999.0)[3] == 0
        assert _color_at_or_fail(intervals, -1e12)[3] == 0

    def test_gismgr_entry_quantities_get_entry_colors(self):
        intervals = _intervals(_GISMGR_RAMP_SLD)
        # Values exactly on an entry quantity get that entry's exact color.
        assert _color_at_or_fail(intervals, 0.0) == (0x44, 0x01, 0x54, 255)
        assert _color_at_or_fail(intervals, 105.0) == (0xFD, 0xE7, 0x25, 255)
        assert _color_at_or_fail(intervals, 106.0) == (0xFD, 0xE7, 0x25, 255)  # clamped

    def test_gismgr_midpoint_interpolates_between_entries(self):
        intervals = _intervals(_GISMGR_RAMP_SLD)
        # 52.5 is halfway between 49 (#23888e) and 56 (#1e988b): each channel
        # must land between the two entry colors.
        rgba = _color_at_or_fail(intervals, 52.5)
        for got, lo_c, hi_c in zip(rgba, (0x23, 0x88, 0x8E, 255), (0x1E, 0x98, 0x8B, 255)):
            assert min(lo_c, hi_c) <= got <= max(lo_c, hi_c)

    def test_explicit_ramp_type_matches_default(self):
        sld_default = """<StyledLayerDescriptor>
            <ColorMap>
              <ColorMapEntry quantity="0" color="#000000"/>
              <ColorMapEntry quantity="10" color="#ffffff"/>
            </ColorMap>
        </StyledLayerDescriptor>"""
        sld_ramp = sld_default.replace("<ColorMap>", '<ColorMap type="ramp">')
        assert parse_sld_colormap(sld_default) == parse_sld_colormap(sld_ramp)

    def test_single_entry_ramp_is_constant(self):
        sld = """<StyledLayerDescriptor>
            <ColorMap>
              <ColorMapEntry quantity="5" color="#102030"/>
            </ColorMap>
        </StyledLayerDescriptor>"""
        intervals = _intervals(sld)
        assert _color_at(intervals, -100) == (0x10, 0x20, 0x30, 255)
        assert _color_at(intervals, 100) == (0x10, 0x20, 0x30, 255)


class TestParseSldColormapIntervals:
    _SLD = """<StyledLayerDescriptor>
        <ColorMap type="intervals">
          <ColorMapEntry quantity="0" color="#ff0000"/>
          <ColorMapEntry quantity="50" color="#00ff00"/>
          <ColorMapEntry quantity="100" color="#0000ff"/>
        </ColorMap>
    </StyledLayerDescriptor>"""

    def test_entry_quantity_is_lower_bound(self):
        intervals = _intervals(self._SLD)
        assert _color_at(intervals, 0) == (255, 0, 0, 255)
        assert _color_at(intervals, 49.9) == (255, 0, 0, 255)
        assert _color_at(intervals, 50) == (0, 255, 0, 255)
        assert _color_at(intervals, 100) == (0, 0, 255, 255)
        assert _color_at(intervals, 5000) == (0, 0, 255, 255)

    def test_below_first_entry_is_unrendered(self):
        intervals = _intervals(self._SLD)
        assert _color_at(intervals, -1) is None

    def test_no_interpolation_within_interval(self):
        intervals = _intervals(self._SLD)
        assert _color_at(intervals, 25) == _color_at(intervals, 0)


class TestRioTilerAcceptsParsedColormaps:
    """End-to-end: rio-tiler must accept both parser output shapes."""

    def test_apply_cmap_on_gismgr_ramp_output(self):
        numpy = pytest.importorskip("numpy")
        rio_colormap = pytest.importorskip("rio_tiler.colormap")

        intervals = parse_sld_colormap(_GISMGR_RAMP_SLD)
        data = numpy.array([[[-9999.0, 0.0, 52.5, 105.0, 200.0]]])
        rgb, alpha = rio_colormap.apply_cmap(data, intervals)

        assert alpha[0, 0] == 0  # nodata transparent
        assert alpha[0, 1] > 0  # 0 % rendered
        assert tuple(rgb[:, 0, 4]) == (0xFD, 0xE7, 0x25)  # clamped above


# ---------------------------------------------------------------------------
# extract_sld_body
# ---------------------------------------------------------------------------


class TestExtractSldBody:
    """Tests for the module-level extract_sld_body helper.

    Uses lightweight stub objects rather than the real styles models so this
    test runs without the styles extension installed.
    """

    def test_returns_none_when_no_stylesheets(self):
        class _StyleObj:
            stylesheets = []

        assert extract_sld_body(_StyleObj()) is None

    def test_returns_none_when_stylesheets_is_none(self):
        class _StyleObj:
            stylesheets = None

        assert extract_sld_body(_StyleObj()) is None

    def test_returns_none_for_object_without_stylesheets(self):
        assert extract_sld_body(object()) is None

    def test_extracts_sld_body_from_sld_content_instance(self):
        """When a stylesheet carries an SLDContent instance, the sld_body is returned."""
        try:
            from dynastore.modules.styles.models import SLDContent
        except ImportError:
            pytest.skip("styles models not available in this test environment")

        sld_content = SLDContent(sld_body="<sld>body</sld>")

        class _Sheet:
            content = sld_content

        class _StyleObj:
            stylesheets = [_Sheet()]

        result = extract_sld_body(_StyleObj())
        assert result == "<sld>body</sld>"

    def test_extracts_sld_body_from_dict_content(self):
        """When a stylesheet.content is a dict with SLD format, the sld_body is returned."""
        try:
            from dynastore.modules.styles.models import StyleFormatEnum
        except ImportError:
            pytest.skip("styles models not available in this test environment")

        class _Sheet:
            content = {
                "format": StyleFormatEnum.SLD_1_1,
                "sld_body": "<sld>from dict</sld>",
            }

        class _StyleObj:
            stylesheets = [_Sheet()]

        result = extract_sld_body(_StyleObj())
        assert result == "<sld>from dict</sld>"

    def test_skips_sheets_without_content(self):
        class _SheetNoContent:
            content = None

        class _StyleObj:
            stylesheets = [_SheetNoContent()]

        assert extract_sld_body(_StyleObj()) is None
