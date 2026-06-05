"""Unit tests for WFS -> OGC parameter translation helpers.

WFS expresses sort order as ``property [ASC|DESC]`` and lets clients request a
namespace wildcard (``attributes.*``) in ``PROPERTYNAME``. The shared OGC
query parser/projection layer understands neither, so the WFS service
normalises both before delegating. These guard that normalisation.
"""

import pytest

pytest.importorskip("pyproj")  # SCOPE gate: wfs_service imports pyproj at module load

from dynastore.extensions.wfs.wfs_service import (  # noqa: E402
    wfs_property_names,
    wfs_sortby_to_ogc,
)


# --- wfs_sortby_to_ogc -------------------------------------------------------

def test_sortby_field_with_asc_becomes_bare_field():
    # ``geoid ASC`` (the value behind ``sortBy=geoid+ASC``) must not reach the
    # OGC parser verbatim — it would be read as a single unknown field.
    assert wfs_sortby_to_ogc("geoid ASC") == "geoid"


def test_sortby_field_with_desc_becomes_minus_prefixed():
    assert wfs_sortby_to_ogc("geoid DESC") == "-geoid"


def test_sortby_supports_a_d_abbreviations():
    assert wfs_sortby_to_ogc("geoid D") == "-geoid"
    assert wfs_sortby_to_ogc("geoid A") == "geoid"


def test_sortby_bare_field_defaults_to_ascending():
    assert wfs_sortby_to_ogc("geoid") == "geoid"


def test_sortby_multiple_clauses_are_preserved_in_order():
    assert wfs_sortby_to_ogc("geoid ASC,date DESC") == "geoid,-date"


def test_sortby_already_ogc_form_round_trips():
    # A value already in OGC ``[+-]field`` form survives untouched.
    assert wfs_sortby_to_ogc("-date") == "-date"
    assert wfs_sortby_to_ogc("+geoid") == "geoid"


def test_sortby_empty_or_none_is_none():
    assert wfs_sortby_to_ogc(None) is None
    assert wfs_sortby_to_ogc("") is None
    assert wfs_sortby_to_ogc("  ,  ") is None


# --- wfs_property_names ------------------------------------------------------

def test_property_names_wildcard_returns_none():
    # ``geoid,attributes.*`` -> "return everything", i.e. no projection.
    assert wfs_property_names("geoid,attributes.*") is None


def test_property_names_bare_star_returns_none():
    assert wfs_property_names("*") is None


def test_property_names_concrete_list_is_passed_through():
    assert wfs_property_names("geoid,NAME,CODE") == ["geoid", "NAME", "CODE"]


def test_property_names_strips_whitespace():
    assert wfs_property_names(" geoid , NAME ") == ["geoid", "NAME"]


def test_property_names_empty_or_none_is_none():
    assert wfs_property_names(None) is None
    assert wfs_property_names("") is None
    assert wfs_property_names("  ,  ") is None
