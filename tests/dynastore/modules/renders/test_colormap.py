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
    parse_sld_colormap,
)


# ---------------------------------------------------------------------------
# _parse_hex_color
# ---------------------------------------------------------------------------


class TestParseHexColor:
    def test_black(self):
        assert _parse_hex_color("#000000") == (0, 0, 0)

    def test_white(self):
        assert _parse_hex_color("#ffffff") == (255, 255, 255)

    def test_mixed(self):
        assert _parse_hex_color("#1a2b3c") == (0x1A, 0x2B, 0x3C)

    def test_uppercase(self):
        assert _parse_hex_color("#AABBCC") == (0xAA, 0xBB, 0xCC)

    def test_leading_whitespace(self):
        assert _parse_hex_color("  #0A0B0C  ") == (0x0A, 0x0B, 0x0C)

    def test_invalid_no_hash(self):
        with pytest.raises(ValueError):
            _parse_hex_color("0A0B0C")

    def test_invalid_short(self):
        with pytest.raises(ValueError):
            _parse_hex_color("#ABC")

    def test_invalid_non_hex(self):
        with pytest.raises(ValueError):
            _parse_hex_color("#ZZZZZZ")


# ---------------------------------------------------------------------------
# _opacity_to_alpha
# ---------------------------------------------------------------------------


class TestOpacityToAlpha:
    def test_none_defaults_to_255(self):
        assert _opacity_to_alpha(None) == 255

    def test_one_is_255(self):
        assert _opacity_to_alpha("1.0") == 255

    def test_zero_is_0(self):
        assert _opacity_to_alpha("0.0") == 0

    def test_half(self):
        assert _opacity_to_alpha("0.5") == 128

    def test_clamp_over_1(self):
        assert _opacity_to_alpha("1.5") == 255

    def test_clamp_negative(self):
        assert _opacity_to_alpha("-0.1") == 0

    def test_invalid_string_defaults_255(self):
        assert _opacity_to_alpha("not-a-float") == 255


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
