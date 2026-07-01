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
    # Standards with shipped code that require an optional dependency (e.g. BQ)
    # and are therefore absent from the live conformance registry in deployments
    # that don't install that dependency. Shown separately from pure roadmap items.
    scope_conditional: List[RoadmapEntry] = []


# --- Pattern map: standard display name -> regex matching its conformance URIs
# Each entry must classify URIs from a single published OGC / STAC standard.
# Research proposals (e.g. FAO's paginated datacube-dimensions extension) are
# deliberately NOT listed here — they are not OGC standards and surfacing
# them as peer entries on the conformance matrix would overstate the
# platform's standards posture.
_STANDARD_PATTERNS: Dict[str, str] = {
    "OGC API Common": r"ogcapi-common",
    "OGC API Features": r"ogcapi-features",
    "STAC API": r"stacspec\.org|stac-api",
    "OGC API Processes": r"ogcapi-processes",
    "OGC API Records": r"ogcapi-records",
    "OGC API Tiles": r"ogcapi-tiles",
    "OGC API Maps": r"ogcapi-maps",
    "OGC API Coverages": r"ogcapi-coverages",
    "OGC API DGGS": r"ogcapi-dggs",
    "OGC API ConnectedSystems": r"ogcapi-connectedsystems",
    "OGC API Moving Features": r"ogcapi-movingfeatures",
    "OGC API Styles": r"ogcapi-styles",
    "OGC API EDR": r"ogcapi-edr",
    "OGC API Joins": r"ogcapi-joins",
    "OGC API Routes": r"ogcapi-routes",
    "OGC API 3D GeoVolumes": r"ogcapi-3d-geovolumes",
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
    # SensorThings API is a deliberate non-goal — superseded by OGC API
    # Connected Systems for the IoT / observation domain (see
    # docs/roadmap.md). Kept out of the roadmap pills so the home-page
    # surface only advertises standards we may still implement.
    "STAC API",
]

# Standards that ARE fully implemented as extension code but require an
# optional SCOPE dependency to load (e.g. google-cloud-bigquery for Joins).
# When such a standard is absent from the live conformance registry it means
# the dependency is not installed in this deployment — NOT that the standard
# is unimplemented. The home-page renders these with a distinct "available,
# optional dependency" label rather than the "Roadmap" label used for
# standards that have no implementation at all.
_SCOPE_CONDITIONAL_STANDARDS: Set[str] = {
    "OGC API Joins",  # dynastore-ext-joins; requires google-cloud-bigquery
}

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


def get_ogc_service_prefixes() -> List[str]:
    """Router prefixes (e.g. ``"/maps"``) for every loaded OGC service.

    Walks every registered ``ConformanceContributor`` (which every
    ``OGCServiceMixin`` subclass is, structurally) and collects each
    contributor's ``prefix`` ClassVar. Non-OGC services don't inherit the
    mixin and are therefore not returned. Result is deduplicated and
    sorted for deterministic output.

    Used by the catalog-privacy DENY policy emitter so the resource
    pattern self-maintains as new OGC protocols come online — no
    hardcoded protocol list to keep in sync.
    """
    prefixes: Set[str] = set()
    for contributor in get_protocols(ConformanceContributor):
        prefix = getattr(contributor, "prefix", "") or ""
        if prefix.startswith("/"):
            prefixes.add(prefix.lstrip("/"))
    return sorted(prefixes)


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

    # Determine which standards have zero coverage and split into two buckets:
    # - scope_conditional: shipped code, absent because optional dep not installed
    # - roadmap: no implementation yet
    not_implemented = sorted(
        s for s in _ALL_OGC_STANDARDS if s not in implemented_names
    )
    scope_conditional = [
        RoadmapEntry(name=n, doc_url=_STANDARD_DOC_URLS.get(n))
        for n in not_implemented
        if n in _SCOPE_CONDITIONAL_STANDARDS
    ]
    roadmap = [
        RoadmapEntry(name=n, doc_url=_STANDARD_DOC_URLS.get(n))
        for n in not_implemented
        if n not in _SCOPE_CONDITIONAL_STANDARDS
    ]

    return ConformanceSummary(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_conformance_classes=len(all_uris),
        standards=standards,
        not_implemented=not_implemented,
        roadmap=roadmap,
        scope_conditional=scope_conditional,
    )
