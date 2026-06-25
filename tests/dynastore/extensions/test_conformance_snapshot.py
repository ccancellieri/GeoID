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

"""
Pass 1 conformance snapshot test.

Pins the set of OGC API conformance URIs Pass 1 declares, so a careless
refactor that drops a URI trips the test loudly. The test reads each
extension's conformance-URI constants directly — no server boot required.

The full ``/conformance`` HTTP endpoint is covered by existing
integration tests; this snapshot is the unit-level guard.
"""

from __future__ import annotations


EXPECTED_PASS1_RECORDS_URIS = {
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/geojson",  # T5
}

EXPECTED_PASS1_WEB_URIS = {
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas31",  # T6
}

EXPECTED_PASS1_MAPS_URIS = {
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/jpeg",       # T7
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/tiff",       # T7 (Maps uses tiff; geotiff is a Coverages class)
}

EXPECTED_PASS1_STYLES_URIS = {
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/manage-styles",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/style-info",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/mapbox-style",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/sld-10",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/sld-11",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/html",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/json",
}


def test_records_declares_geojson_conformance():
    from dynastore.extensions.records.records_service import OGC_API_RECORDS_URIS
    declared = set(OGC_API_RECORDS_URIS)
    missing = EXPECTED_PASS1_RECORDS_URIS - declared
    assert not missing, f"Records extension missing Pass 1 URIs: {sorted(missing)}"


def test_web_declares_oas31_conformance():
    from dynastore.extensions.web.web import WEB_CONFORMANCE_URIS
    declared = set(WEB_CONFORMANCE_URIS)
    missing = EXPECTED_PASS1_WEB_URIS - declared
    assert not missing, f"Web extension missing Pass 1 URIs: {sorted(missing)}"
    # OAS 3.0 must remain declared (Pass 1 adds 3.1 alongside, not in place of).
    assert "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30" in declared


def _read_uri_list_from_source(module_path: str, var_name: str) -> set:
    """Read a URI list constant from a source file without importing.

    Some modules depend on heavy native libraries (``osgeo`` for maps,
    ``lxml`` for styles) that ship from the production base image but
    aren't installed in local dev venvs. Parsing the module's AST avoids
    the heavy import while still exercising the actual source-of-truth.
    """
    import ast

    from tests._repo_paths import find_in_packages

    src = find_in_packages(module_path)
    tree = ast.parse(src.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == var_name:
                    if isinstance(node.value, ast.List):
                        return {
                            elt.value for elt in node.value.elts
                            if isinstance(elt, ast.Constant)
                        }
    raise AssertionError(f"{var_name} not found in {module_path}")


def _read_maps_conformance_uris_from_source() -> set:
    return _read_uri_list_from_source(
        "extensions/maps/maps_service.py", "OGC_API_MAPS_URIS",
    )


def _read_styles_conformance_uris_from_source() -> set:
    return _read_uri_list_from_source(
        "extensions/styles/styles_service.py", "OGC_API_STYLES_URIS",
    )


def test_maps_declares_jpeg_and_tiff_conformance():
    declared = _read_maps_conformance_uris_from_source()
    missing = EXPECTED_PASS1_MAPS_URIS - declared
    assert not missing, f"Maps extension missing Pass 1 URIs: {sorted(missing)}"
    # PNG must remain declared (Pass 1 adds alongside, never replaces).
    assert "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/png" in declared


def test_styles_declares_full_conformance_set():
    declared = _read_styles_conformance_uris_from_source()
    missing = EXPECTED_PASS1_STYLES_URIS - declared
    assert not missing, f"Styles extension missing Pass 1 URIs: {sorted(missing)}"


def test_full_pass1_uri_set_is_declared_somewhere():
    """Belt-and-suspenders: aggregate all Pass 1 URIs and assert none are missing."""
    from dynastore.extensions.records.records_service import OGC_API_RECORDS_URIS
    from dynastore.extensions.web.web import WEB_CONFORMANCE_URIS

    all_declared = (
        set(OGC_API_RECORDS_URIS)
        | set(WEB_CONFORMANCE_URIS)
        | _read_maps_conformance_uris_from_source()
        | _read_styles_conformance_uris_from_source()
    )
    all_expected = (
        EXPECTED_PASS1_RECORDS_URIS
        | EXPECTED_PASS1_WEB_URIS
        | EXPECTED_PASS1_MAPS_URIS
        | EXPECTED_PASS1_STYLES_URIS
    )
    missing = all_expected - all_declared
    assert not missing, (
        f"Pass 1 conformance regression — these URIs are no longer declared: "
        f"{sorted(missing)}"
    )


# Coverages is OGC 19-087 (DRAFT); these track the current axis-typed taxonomy.
# Spatial-only: no temporal subsetting/scaling is implemented, so no
# subsetting-temporal / scaling-temporal / *-general is declared.
EXPECTED_PASS2_COVERAGES_URIS = {
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/subsetting-spatial",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/scaling-spatial",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/geotiff",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/netcdf",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coveragejson",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/html",
}


def _read_coverages_conformance_uris_from_source() -> set:
    return _read_uri_list_from_source(
        "extensions/coverages/coverages_service.py", "OGC_API_COVERAGES_URIS",
    )


def test_coverages_declares_pass2_conformance_set():
    declared = _read_coverages_conformance_uris_from_source()
    missing = EXPECTED_PASS2_COVERAGES_URIS - declared
    assert not missing, f"Coverages missing Pass 2 URIs: {sorted(missing)}"
    assert "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/core" in declared


EXPECTED_PASS5_VOLUMES_URIS = {
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/3dtiles",
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/tileset",
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/spatialquery",
}


def _read_volumes_conformance_uris_from_source() -> set:
    return _read_uri_list_from_source(
        "extensions/volumes/volumes_service.py", "OGC_API_VOLUMES_URIS",
    )


def test_volumes_declares_pass5_conformance_set():
    declared = _read_volumes_conformance_uris_from_source()
    missing = EXPECTED_PASS5_VOLUMES_URIS - declared
    assert not missing, f"Volumes missing Pass 5 URIs: {sorted(missing)}"


def test_geovolumes_uris_classify_into_their_own_family():
    """The conformance summary must bucket GeoVolumes URIs under GeoVolumes.

    Declaring the URIs (test above) is not enough — without a pattern in
    ``_STANDARD_PATTERNS`` the summary classifier drops them into the
    catch-all "Other / OGC Common" bucket and reports "OGC API 3D
    GeoVolumes" as not-implemented on the home page, even though Pass 5
    (#715) shipped it. This guards the classification, not just the
    declaration.
    """
    import re

    from dynastore.extensions.tools.conformance import _STANDARD_PATTERNS

    def classify(uri: str) -> str | None:
        for name, pattern in _STANDARD_PATTERNS.items():
            if re.search(pattern, uri):
                return name
        return None

    for uri in EXPECTED_PASS5_VOLUMES_URIS:
        assert classify(uri) == "OGC API 3D GeoVolumes", (
            f"{uri} must classify as 'OGC API 3D GeoVolumes', "
            f"got {classify(uri)!r} — add an ogcapi-3d-geovolumes pattern"
        )


EXPECTED_PASS4_JOINS_URIS = {
    "http://www.opengis.net/spec/ogcapi-joins-1/0.0/conf/core",
}


def _read_joins_conformance_uris_from_source() -> set:
    return _read_uri_list_from_source(
        "extensions/joins/joins_service.py", "OGC_API_JOINS_URIS",
    )


def test_joins_declares_pass4_conformance_set():
    declared = _read_joins_conformance_uris_from_source()
    missing = EXPECTED_PASS4_JOINS_URIS - declared
    assert not missing, f"Joins missing Pass 4 URIs: {sorted(missing)}"
