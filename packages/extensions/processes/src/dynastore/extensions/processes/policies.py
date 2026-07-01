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

"""Pure declarations of processes extension authorization policies.

The IAM module's ``PolicyContributor`` consumer reads these declarations
through the preset registered in ``presets/__init__.py`` and forwards them
to ``PermissionProtocol``.  This file never calls ``register_policy``
directly.

Four policies:

``processes_public_read``
    ALLOW ``GET`` on the platform-scope process registry and job surfaces
    (process listing/description, conformance, and the unscoped job
    status/results/logs routes).  No condition — deliberately public per
    the OGC API - Processes discovery model: browsing available processes
    and job status is not privileged, only execution is.

``processes_read``
    ALLOW ``GET`` on the catalog/collection-scoped process listing and job
    read surfaces, gated by ``catalog_membership_required`` so any
    principal with a grant on the catalog can browse.  Fails closed for
    anonymous requests.

``processes_system_execute``
    ALLOW ``POST`` on the platform-scope execution route.  No condition —
    scoped to sysadmin role only via the role binding.

``processes_admin``
    ALLOW ``POST`` on the catalog- and collection-scope execution routes,
    gated by ``catalog_admin_required``.

These policies only take effect where ``IamMiddleware`` is actually mounted
(any SCOPE that pulls in ``auth_grp``, e.g. ``api_catalog``, ``geoid_service``,
``scope_geoid``). The ``maps`` service (``api_maps_open`` / ``scope_maps``)
does not include ``auth_grp``, so no ``AuthenticatorProtocol`` /
``PermissionProtocol`` is registered there and the middleware runs in
pass-through mode — these policies are inert on that deployment. The
unauthenticated execution surface on ``maps`` is covered today by the
network/load-balancer ingress boundary; moving that service onto the
auth-bearing variant is a separate, tracked follow-up.
"""
from typing import List, Optional

from dynastore.models.auth import Condition, Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig


def processes_policies() -> List[Policy]:
    """Pure declaration of the processes extension's policies."""
    return [
        # ----------------------------------------------------------------
        # processes_public_read: platform-scope registry + job surfaces
        # Routes: get_processes_conformance, list_processes,
        #         get_process_description, list_jobs, get_job_status,
        #         get_job_results, get_job_logs
        # ----------------------------------------------------------------
        Policy(
            id="processes_public_read",
            description=(
                "Allows any caller (including anonymous) to reach the "
                "platform-scope process registry and job read surfaces "
                "(GET /processes/conformance, GET /processes/processes"
                "[/{process_id}], GET /processes/jobs[/{job_id}"
                "[/results|/logs]]). Listing available processes and job "
                "status is deliberately public — only execution is gated."
            ),
            actions=["GET"],
            resources=[
                r"^/processes/(conformance|processes(/[^/]+)?"
                r"|jobs(/[^/]+(/(results|logs))?)?)$",
            ],
            effect="ALLOW",
            conditions=[],
        ),
        # ----------------------------------------------------------------
        # processes_read: catalog/collection GET surfaces
        # Routes: list_processes_catalog, list_processes_collection,
        #         list_jobs_catalog, get_job_status_catalog,
        #         get_job_results_catalog, get_job_logs_catalog,
        #         list_jobs_collection, get_job_status_collection,
        #         get_job_results_collection, get_job_logs_collection
        # ----------------------------------------------------------------
        Policy(
            id="processes_read",
            description=(
                "Allows catalog members (any principal with a grant on the "
                "catalog) to reach the catalog/collection-scoped process "
                "listing and job read surfaces (GET /processes/catalogs/{id}"
                "[/collections/{col}]/processes and the matching /jobs "
                "surfaces). Gated by catalog_membership_required, which "
                "fails closed for anonymous requests."
            ),
            actions=["GET"],
            resources=[
                r"^/processes/catalogs/[^/]+(/collections/[^/]+)?"
                r"/(processes|jobs(/[^/]+(/(results|logs))?)?)$",
            ],
            effect="ALLOW",
            conditions=[
                Condition(type="catalog_membership_required", config={}),
            ],
        ),
        # ----------------------------------------------------------------
        # processes_system_execute: platform-scope execution
        # Routes: execute_process
        # ----------------------------------------------------------------
        Policy(
            id="processes_system_execute",
            description=(
                "Allows sysadmin principals to execute platform-scope "
                "processes (POST /processes/processes/{process_id}"
                "/execution). No condition — effective audience is "
                "sysadmin only via the role binding."
            ),
            actions=["POST"],
            resources=[
                r"^/processes/processes/[^/]+/execution$",
            ],
            effect="ALLOW",
            conditions=[],
        ),
        # ----------------------------------------------------------------
        # processes_admin: catalog/collection-scope execution
        # Routes: execute_process_catalog, execute_process_collection
        # ----------------------------------------------------------------
        Policy(
            id="processes_admin",
            description=(
                "Per-catalog admin access for process execution: POST "
                "/processes/catalogs/{id}[/collections/{col}]/processes"
                "/{process_id}/execution. Gated by catalog_admin_required "
                "so only catalog-tier admins and above may trigger "
                "execution."
            ),
            actions=["POST"],
            resources=[
                r"^/processes/catalogs/[^/]+(/collections/[^/]+)?"
                r"/processes/[^/]+/execution$",
            ],
            effect="ALLOW",
            conditions=[
                Condition(
                    type="catalog_admin_required",
                    config={"required_roles": [IamRolesConfig().admin_role_name]},
                )
            ],
        ),
    ]


def processes_role_bindings(
    sysadmin_role_name: Optional[str] = None,
    admin_role_name: Optional[str] = None,
) -> List[Role]:
    """Pure declaration of the processes extension's role bindings.

    ``processes_public_read`` and ``processes_read`` are bound to the
    universal base role so every principal can reach the read surfaces;
    ``catalog_membership_required`` on ``processes_read`` is the actual
    access gate for catalog/collection scope (fails closed for anonymous).

    ``processes_system_execute`` is sysadmin-only (platform-scope
    execution). ``processes_admin`` is bound to sysadmin + admin so both
    privileged tiers can trigger execution on catalogs they administer.
    """
    cfg = IamRolesConfig()
    sysadmin_role_name = sysadmin_role_name or cfg.sysadmin_role_name
    admin_role_name = admin_role_name or cfg.admin_role_name
    return [
        # Read surfaces: reachable by every request; processes_public_read
        # is unconditionally open, processes_read's catalog_membership_required
        # is the actual access control for catalog/collection scope.
        Role(name=cfg.anonymous_role_name, policies=["processes_public_read", "processes_read"]),
        # Platform-scope execution: sysadmin only.
        Role(name=sysadmin_role_name, policies=["processes_system_execute"]),
        # Catalog/collection-scope execution: sysadmin and admin.
        Role(name=sysadmin_role_name, policies=["processes_admin"]),
        Role(name=admin_role_name, policies=["processes_admin"]),
    ]
