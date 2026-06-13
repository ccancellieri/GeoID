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

"""Tests for modules/volumes/color_ramp.py."""

from __future__ import annotations

import pytest

from dynastore.modules.volumes.color_ramp import (
    DEFAULT_HEIGHT_RAMP_HEX,
    interpolate_ramp,
    parse_hex,
    parse_ramp,
)


# ---------------------------------------------------------------------------
# parse_hex
# ---------------------------------------------------------------------------


def test_parse_hex_with_hash():
    assert parse_hex("#2c7bb6") == (44, 123, 182)


def test_parse_hex_without_hash():
    assert parse_hex("ffffff") == (255, 255, 255)


def test_parse_hex_rejects_short_string():
    with pytest.raises(ValueError):
        parse_hex("#abc")


# ---------------------------------------------------------------------------
# parse_ramp
# ---------------------------------------------------------------------------


def test_parse_ramp_sorts_by_stop():
    stops = parse_ramp([(40.0, "#000000"), (0.0, "#ffffff")])
    assert [s[0] for s in stops] == [0.0, 40.0]
    assert stops[0][1] == (255, 255, 255)


def test_parse_ramp_empty_yields_empty():
    assert parse_ramp([]) == []


def test_default_ramp_parses():
    stops = parse_ramp(DEFAULT_HEIGHT_RAMP_HEX)
    assert len(stops) == len(DEFAULT_HEIGHT_RAMP_HEX)
    assert stops[0][0] == 0.0
    assert stops[-1][0] == 80.0


# ---------------------------------------------------------------------------
# interpolate_ramp
# ---------------------------------------------------------------------------


_RAMP = [(0.0, (0, 0, 0)), (10.0, (100, 100, 100)), (20.0, (200, 200, 200))]


def test_interpolate_below_range_clamps_to_first():
    assert interpolate_ramp(-5.0, _RAMP) == (0, 0, 0)


def test_interpolate_above_range_clamps_to_last():
    assert interpolate_ramp(999.0, _RAMP) == (200, 200, 200)


def test_interpolate_exact_stop():
    assert interpolate_ramp(10.0, _RAMP) == (100, 100, 100)


def test_interpolate_midpoint_linear():
    # Halfway between 0 and 10 → halfway between (0,0,0) and (100,100,100).
    assert interpolate_ramp(5.0, _RAMP) == (50, 50, 50)


def test_interpolate_quarter_point_rounds():
    # 2.5 / 10 = 0.25 of the way to (100,...) → 25.
    assert interpolate_ramp(2.5, _RAMP) == (25, 25, 25)


def test_interpolate_empty_ramp_raises():
    with pytest.raises(ValueError):
        interpolate_ramp(5.0, [])


def test_interpolate_single_stop_returns_that_color():
    assert interpolate_ramp(5.0, [(0.0, (7, 8, 9))]) == (7, 8, 9)
