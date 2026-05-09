#    Copyright 2025 FAO
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

# dynastore/tools/conformance.py

import re
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from pydantic import BaseModel

from dynastore.modules import get_protocols
from dynastore.models.protocols.conformance import ConformanceContributor

logger = logging.getLogger(__name__)

# --- Data Model ---

# Canonical Conformance is defined in ogc_common_models; re-export for compatibility.
from dynastore.extensions.tools.ogc_common_models import Conformance


class StandardSummary(BaseModel):
    name: str
    implemented: int
    uris: List[str]
    status: str
    doc_url: Optional[str] = None


class RoadmapEntry(BaseModel):
    name: str
    doc_url: Optional[str] = None


class ConformanceSummary(BaseModel):
    timestamp: str
    total_conformance_classes: int
    standards: List[StandardSummary]
    not_implemented: List[str]
    roadmap: List[RoadmapEntry] = []


# --- Pattern map: standard display name -> regex matching its conformance URIs
# Each entry must classify URIs from a single published OGC / STAC standard.
# Research proposals (e.g. FAO's paginated datacube-dimensions extension) are
# deliberately NOT listed here — they are not OGC standards and surfacing
# them as peer entries on the conformance matrix would overstate the
# platform's standards posture.
_STANDARD_PATTERNS: Dict[str, str] = {
    "OGC API Features": r"ogcapi-features",
    "STAC API": r"stacspec\.org|stac-api",
    "OGC API Processes": r"ogcapi-processes",
    "OGC API Records": r"ogcapi-records",
    "OGC API Tiles": r"ogcapi-tiles",
    "OGC API Maps": r"ogcapi-maps",
    "OGC API Coverages": r"ogcapi-coverages",
    "OGC API DGGS": r"ogcapi-dggs",
    "OGC API Connected Systems": r"ogcapi-connectedsystems",
    "OGC API Moving Features": r"ogcapi-movingfeatures",
    "OGC API Styles": r"ogcapi-styles",
    "OGC API EDR": r"ogcapi-edr",
}

# Standards in the OGC REST API family that DynaStore could potentially
# implement. Items absent from `_STANDARD_PATTERNS` show up under "Roadmap"
# on the home page until at least one extension declares a matching
# `conformance_uris` entry. This list is for the home-page roadmap pills
# only — every entry MUST be a published OGC standard or a recognised STAC
# spec; research proposals do not belong here.
# Documentation URLs for the OGC / STAC family — surfaced as clickable links
# from the home-page coverage panel so visitors can dig into a standard
# directly from the platform without leaving the landing page.
# Source: OGC_COMPATIBILITY_STUDY.md (2026-05).
_STANDARD_DOC_URLS: Dict[str, str] = {
    "OGC API Common": "https://ogcapi.ogc.org/common/",
    "OGC API Features": "https://ogcapi.ogc.org/features/",
    "OGC API Tiles": "https://ogcapi.ogc.org/tiles/",
    "OGC API Maps": "https://ogcapi.ogc.org/maps/",
    "OGC API Processes": "https://ogcapi.ogc.org/processes/",
    "OGC API Records": "https://ogcapi.ogc.org/records/",
    "OGC API Coverages": "https://ogcapi.ogc.org/coverages/",
    "OGC API DGGS": "https://ogcapi.ogc.org/dggs/",
    "OGC API Connected Systems": "https://ogcapi.ogc.org/connectedsystems/",
    "OGC API Moving Features": "https://ogcapi.ogc.org/movingfeatures/",
    "OGC API Styles": "https://ogcapi.ogc.org/styles/",
    "OGC API EDR": "https://ogcapi.ogc.org/edr/",
    "OGC API Routes": "https://ogcapi.ogc.org/routes/",
    "OGC API Joins": "https://github.com/opengeospatial/ogcapi-joins",
    "OGC API 3D GeoVolumes": "https://ogcapi.ogc.org/geovolumes/",
    "SensorThings API": "https://www.ogc.org/standard/sensorthings/",
    "STAC API": "https://github.com/radiantearth/stac-api-spec",
    # Aggregate bucket — link to the OGC API hub so unclassified URIs still
    # have somewhere meaningful to point.
    "Other / OGC Common": "https://ogcapi.ogc.org/common/",
}


_ALL_OGC_STANDARDS = [
    "OGC API Common",
    "OGC API Features",
    "OGC API Tiles",
    "OGC API Maps",
    "OGC API Processes",
    "OGC API Records",
    "OGC API Coverages",
    "OGC API DGGS",
    "OGC API Connected Systems",
    "OGC API Moving Features",
    "OGC API Styles",
    "OGC API EDR",
    "OGC API Routes",
    "OGC API Joins",
    "OGC API 3D GeoVolumes",
    "SensorThings API",
    "STAC API",
]

# --- Public API ---

def _collect_uris() -> Set[str]:
    """Gather conformance URIs from every registered `ConformanceContributor`."""
    uris: Set[str] = set()
    for contributor in get_protocols(ConformanceContributor):
        declared = getattr(contributor, "conformance_uris", None) or []
        uris.update(declared)
    return uris


def get_active_conformance() -> Conformance:
    """Dynamically-built list of all conformance classes contributed by loaded extensions."""
    return Conformance(conformsTo=sorted(_collect_uris()))


def get_conformance_summary() -> ConformanceSummary:
    """
    Returns a structured summary of OGC API compliance grouped by standard family.
    Classifies each contributed conformance URI into its parent standard using
    regex patterns, computes per-standard counts, and lists standards with
    zero coverage as not_implemented.
    """
    all_uris = sorted(_collect_uris())

    # Bucket URIs into standard families
    buckets: Dict[str, List[str]] = {name: [] for name in _STANDARD_PATTERNS}
    unclassified: List[str] = []

    for uri in all_uris:
        matched = False
        for name, pattern in _STANDARD_PATTERNS.items():
            if re.search(pattern, uri):
                buckets[name].append(uri)
                matched = True
                break
        if not matched:
            unclassified.append(uri)

    # Build per-standard summaries
    standards: List[StandardSummary] = []
    implemented_names: Set[str] = set()

    for name, uris in sorted(buckets.items()):
        if uris:
            implemented_names.add(name)
            standards.append(StandardSummary(
                name=name,
                implemented=len(uris),
                uris=uris,
                status="implemented",
                doc_url=_STANDARD_DOC_URLS.get(name),
            ))

    # Add unclassified URIs as a bucket if any exist
    if unclassified:
        standards.append(StandardSummary(
            name="Other / OGC Common",
            implemented=len(unclassified),
            uris=unclassified,
            status="implemented",
            doc_url=_STANDARD_DOC_URLS.get("Other / OGC Common"),
        ))
        implemented_names.add("OGC API Common")

    # Determine which standards have zero coverage
    not_implemented = sorted(
        s for s in _ALL_OGC_STANDARDS if s not in implemented_names
    )
    roadmap = [
        RoadmapEntry(name=n, doc_url=_STANDARD_DOC_URLS.get(n))
        for n in not_implemented
    ]

    return ConformanceSummary(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_conformance_classes=len(all_uris),
        standards=standards,
        not_implemented=not_implemented,
        roadmap=roadmap,
    )
