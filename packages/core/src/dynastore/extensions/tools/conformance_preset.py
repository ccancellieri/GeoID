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

"""Preset: anonymous read access to OGC conformance and landing-page endpoints.

OGC API Common Part 1 §7.4 requires ``/conformance`` to be accessible
without credentials; automated conformance suites (TEAM Engine / ETS) probe
it unauthenticated before any other call.  This preset applies a single
GET-only policy that covers:

* ``/{svc}/conformance`` — every loaded OGC service's conformance endpoint.
* ``/{svc}/`` and ``/{svc}`` — the corresponding landing pages.
* ``/conformance`` — a platform-level conformance aggregator if one is
  ever registered.

The landing/conformance grants are scoped to an explicit allowlist of OGC
service prefixes (``_OGC_SERVICE_PREFIXES``) — the prefixes of the
``OGCServiceMixin`` subclasses.  An earlier revision used a blanket
``^/[^/]+/?$`` which matched *any* single path segment and so silently
granted anonymous read to non-OGC roots (``/admin``, ``/iam``, ``/auth``,
``/configs``, ``/dwh``, …).  Those surfaces are protected by deny-by-default
(narrow role-scoped ALLOWs, no explicit DENY), so a blanket anonymous ALLOW
overrode their protection — a privilege/metadata leak.  The allowlist below
makes the grant reach OGC services only; non-OGC roots stay deny-by-default.

The policy is GET-only.  Data surfaces (items, collections, coverage data,
…) are untouched.

Registered as the ``ogc_conformance_public`` preset.  Applied at boot by
``extensions/web/web.py`` when ``ConformanceExposureConfig.public`` is
``True`` (the default).
"""

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset

# Policy id is stable — the revoke descriptor stores it and uses it to
# delete exactly what apply wrote.
_POLICY_ID = "ogc_conformance_public_access"

# OGC service prefixes = the path prefixes of the ``OGCServiceMixin``
# subclasses (the only extensions that expose a spec landing page +
# ``/conformance``).  Keep in sync with the ``OGCServiceMixin`` subclasses;
# the unit test ``test_ogc_conformance_preset`` locks the security-critical
# property that NON-OGC roots (/admin, /iam, /auth, /configs, /dwh, /tasks,
# /events, /gcp, /logs, /me, /notebooks, /proxy, /search, /stats, /template)
# are never matched.  A new OGC service merely omitted here loses anonymous
# conformance (a functional follow-up) — it can never widen the grant.
_OGC_SERVICE_PREFIXES = (
    "features",
    "stac",
    "records",
    "coverages",
    "edr",
    "tiles",
    "maps",
    "styles",
    "dggs",
    "processes",
    "volumes",
    "movingfeatures",
    "consys",
    "join",
    "dimensions",
    "wfs",
    "assets",
    "web",
)

# Anchored alternation of the OGC prefixes, e.g. ``(features|stac|…)``.
_OGC_ALT = "|".join(_OGC_SERVICE_PREFIXES)


def _conformance_policies() -> list:
    """Pure data: the anonymous-read policy for conformance + landing routes.

    Resource patterns (all anchored at start by ``re.match``):

    * ``/conformance$`` — platform-level aggregator (safe no-op if absent).
    * ``^/({alt})/conformance$`` — ``/{svc}/conformance`` for OGC services only.
    * ``^/({alt})/?$`` — ``/{svc}/`` or ``/{svc}`` landing for OGC services only.

    The ``{alt}`` is the explicit OGC-prefix allowlist (``_OGC_SERVICE_PREFIXES``):
    a single OGC service segment with an optional trailing slash.  It does NOT
    match ``/{svc}/collections/…`` (data paths) nor any non-OGC root.  The
    platform root ``/`` stays with ``web_public_access`` (login-page
    reachability), not this preset.
    """
    return [
        Policy(
            id=_POLICY_ID,
            description=(
                "Allows anonymous GET access to OGC API conformance declarations "
                "(/{svc}/conformance) and service landing pages (/{svc}/, /{svc}) "
                "for OGC services only, plus the platform root landing page. "
                "OGC API Common Part 1 requires these to be publicly readable so "
                "automated conformance suites (ETS/TEAM Engine) can run without "
                "credentials. Data surfaces and non-OGC roots are unaffected."
            ),
            actions=["GET", "OPTIONS"],
            resources=[
                "/conformance$",
                f"^/({_OGC_ALT})/conformance$",
                f"^/({_OGC_ALT})/?$",
            ],
            effect="ALLOW",
        ),
    ]


def _conformance_role_bindings() -> list:
    return [
        Role(
            name=IamRolesConfig().anonymous_role_name,
            policies=[_POLICY_ID],
        ),
    ]


class _ConformancePublicContributor:
    def get_policies(self) -> list:
        return _conformance_policies()

    def get_role_bindings(self) -> list:
        return _conformance_role_bindings()


register_preset(PolicyContributorPreset(
    name="ogc_conformance_public",
    description=(
        "Anonymous GET access to OGC conformance endpoints and service landing "
        "pages.  Default-public; can be skipped by setting "
        "ConformanceExposureConfig.public=False."
    ),
    keywords=("iam", "ogc", "conformance", "platform"),
    contributor_factory=_ConformancePublicContributor,
))
