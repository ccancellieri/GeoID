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

"""Unit tests for environment-aware Cloud Run job discovery in get_job_config().

Dev and review environments share GCP project fao-aip-geospatial-review.
Jobs from both fleets carry the same APP=dynastore marker and often the same
TASK_TYPE — so without filtering, the last enumerated job wins and dev can end
up dispatching review's containers.

The fix: when the service's own ENVIRONMENT env var is set, only jobs carrying
the same ENVIRONMENT value are kept.  Jobs with a missing or different
ENVIRONMENT tag are skipped.  When ENVIRONMENT is unset the old behaviour is
preserved for on-prem/local/single-env deployments.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_job(name: str, env: dict) -> SimpleNamespace:
    """Build a minimal job object that gcp_module's async-for loop iterates."""
    env_vars = [SimpleNamespace(name=k, value=v) for k, v in env.items()]
    container = SimpleNamespace(env=env_vars)
    template_inner = SimpleNamespace(containers=[container])
    template_outer = SimpleNamespace(template=template_inner)
    return SimpleNamespace(
        name=f"projects/proj/locations/europe-west1/jobs/{name}",
        template=template_outer,
    )


async def _async_iter(items):
    """Wrap a plain list into an async iterable."""
    for item in items:
        yield item


def _make_module(jobs: List[SimpleNamespace]):
    """Return a thin stand-in whose get_job_config() is the real implementation
    from GCPModule, bound onto a lightweight object.

    get_job_config() only reads self through three accessors:
      - self.get_project_id()
      - self.get_region()
      - self.get_jobs_client()

    We bind the real unbound method to a SimpleNamespace that supplies those
    three callables, avoiding a full GCPModule construction (which requires
    app_state, DB wiring, and GCP credentials).
    """
    from dynastore.modules.gcp import gcp_module as _gcp_mod

    mock_client = MagicMock()
    mock_client.list_jobs = AsyncMock(return_value=_async_iter(jobs))

    fake_self = SimpleNamespace(
        get_project_id=lambda: "fao-aip-geospatial-review",
        get_region=lambda: "europe-west1",
        get_jobs_client=lambda: mock_client,
    )
    fake_self.get_job_config = _gcp_mod.GCPModule.get_job_config.__get__(fake_self)
    return fake_self


def _run_v2_stub() -> MagicMock:
    """A minimal run_v2 stub: only ListJobsRequest is used by get_job_config."""
    stub = MagicMock()
    stub.ListJobsRequest = MagicMock
    return stub


# ---------------------------------------------------------------------------
# (a) ENVIRONMENT=dev → only dev-tagged jobs mapped; review/untagged excluded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_env_filter_keeps_own_env_jobs_only(monkeypatch):
    """When ENVIRONMENT=dev, only the job tagged ENVIRONMENT=dev is accepted.
    The review job and the untagged job must be excluded even though both carry
    APP=dynastore and a valid TASK_TYPE.
    """
    monkeypatch.setenv("ENVIRONMENT", "dev")

    jobs = [
        _make_job("dev-dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion", "ENVIRONMENT": "dev",
        }),
        _make_job("dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion", "ENVIRONMENT": "review",
        }),
        _make_job("dynastore-export-job", {
            "APP": "dynastore", "TASK_TYPE": "export",
            # no ENVIRONMENT tag — must also be excluded in strict mode
        }),
    ]
    module = _make_module(jobs)

    with patch("dynastore.modules.gcp.gcp_module.run_v2", _run_v2_stub()), \
         patch("dynastore.modules.gcp.tools.jobs.set_job_extras"):
        result = await module.get_job_config()

    assert result == {"ingestion": "dev-dynastore-ingestion-job"}


@pytest.mark.asyncio
async def test_env_filter_excludes_untagged_jobs_in_strict_mode(monkeypatch):
    """An untagged job (no ENVIRONMENT set) is excluded when the service is
    tagged, even if it is the only APP=dynastore job in the region.  This
    prevents a legacy review job that has not yet been re-deployed with the
    new tag from leaking into the dev fleet.
    """
    monkeypatch.setenv("ENVIRONMENT", "dev")

    jobs = [
        _make_job("dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion",
            # ENVIRONMENT absent
        }),
    ]
    module = _make_module(jobs)

    with patch("dynastore.modules.gcp.gcp_module.run_v2", _run_v2_stub()), \
         patch("dynastore.modules.gcp.tools.jobs.set_job_extras"):
        result = await module.get_job_config()

    assert result == {}


@pytest.mark.asyncio
async def test_env_filter_review_service_keeps_review_jobs_only(monkeypatch):
    """Review-side symmetry: ENVIRONMENT=review keeps only review-tagged jobs."""
    monkeypatch.setenv("ENVIRONMENT", "review")

    jobs = [
        _make_job("dev-dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion", "ENVIRONMENT": "dev",
        }),
        _make_job("dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion", "ENVIRONMENT": "review",
        }),
    ]
    module = _make_module(jobs)

    with patch("dynastore.modules.gcp.gcp_module.run_v2", _run_v2_stub()), \
         patch("dynastore.modules.gcp.tools.jobs.set_job_extras"):
        result = await module.get_job_config()

    assert result == {"ingestion": "dynastore-ingestion-job"}


# ---------------------------------------------------------------------------
# (b) ENVIRONMENT unset → all APP=dynastore jobs mapped (back-compat)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_env_var_passes_all_dynastore_jobs(monkeypatch):
    """When ENVIRONMENT is not set (on-prem / local / single-env deployments),
    all APP=dynastore jobs are accepted regardless of their ENVIRONMENT tag.
    This preserves the existing behaviour for operators who have not yet
    adopted the env tagging convention.
    """
    monkeypatch.delenv("ENVIRONMENT", raising=False)

    jobs = [
        _make_job("dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion", "ENVIRONMENT": "review",
        }),
        _make_job("dev-dynastore-export-job", {
            "APP": "dynastore", "TASK_TYPE": "export", "ENVIRONMENT": "dev",
        }),
        _make_job("dynastore-reindex-job", {
            "APP": "dynastore", "TASK_TYPE": "elasticsearch_indexer",
            # no ENVIRONMENT tag — also accepted in unfiltered mode
        }),
    ]
    module = _make_module(jobs)

    with patch("dynastore.modules.gcp.gcp_module.run_v2", _run_v2_stub()), \
         patch("dynastore.modules.gcp.tools.jobs.set_job_extras"):
        result = await module.get_job_config()

    assert set(result.keys()) == {"ingestion", "export", "elasticsearch_indexer"}


@pytest.mark.asyncio
async def test_empty_environment_string_treated_as_unset(monkeypatch):
    """An explicitly empty ENVIRONMENT='' is treated the same as absent — the
    full unfiltered list is returned so a misconfigured deploy does not
    silently break job discovery.
    """
    monkeypatch.setenv("ENVIRONMENT", "")

    jobs = [
        _make_job("dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion", "ENVIRONMENT": "review",
        }),
    ]
    module = _make_module(jobs)

    with patch("dynastore.modules.gcp.gcp_module.run_v2", _run_v2_stub()), \
         patch("dynastore.modules.gcp.tools.jobs.set_job_extras"):
        result = await module.get_job_config()

    assert result == {"ingestion": "dynastore-ingestion-job"}


# ---------------------------------------------------------------------------
# (c) TASK_TYPE collision resolves to the own-environment job
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collision_resolves_to_own_env(monkeypatch):
    """When dev and review jobs carry the same TASK_TYPE, the filtered result
    contains exactly the job from the service's own environment.  The review
    job must not overwrite the dev job in the map regardless of enumeration
    order.
    """
    monkeypatch.setenv("ENVIRONMENT", "dev")

    # Simulate review job appearing first in the list_jobs response — if
    # filtering were absent, it would be overwritten by the dev job only when
    # the dev job happens to come second.  With filtering the review job is
    # never added at all.
    jobs = [
        _make_job("dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion", "ENVIRONMENT": "review",
        }),
        _make_job("dev-dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion", "ENVIRONMENT": "dev",
        }),
    ]
    module = _make_module(jobs)

    with patch("dynastore.modules.gcp.gcp_module.run_v2", _run_v2_stub()), \
         patch("dynastore.modules.gcp.tools.jobs.set_job_extras"):
        result = await module.get_job_config()

    assert result == {"ingestion": "dev-dynastore-ingestion-job"}


@pytest.mark.asyncio
async def test_collision_own_env_listed_first(monkeypatch):
    """Enumeration order must not matter: own-env job listed first, review job
    listed second — result must still be the dev job only.
    """
    monkeypatch.setenv("ENVIRONMENT", "dev")

    jobs = [
        _make_job("dev-dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion", "ENVIRONMENT": "dev",
        }),
        _make_job("dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion", "ENVIRONMENT": "review",
        }),
    ]
    module = _make_module(jobs)

    with patch("dynastore.modules.gcp.gcp_module.run_v2", _run_v2_stub()), \
         patch("dynastore.modules.gcp.tools.jobs.set_job_extras"):
        result = await module.get_job_config()

    assert result == {"ingestion": "dev-dynastore-ingestion-job"}


# ---------------------------------------------------------------------------
# (d) TASK_TYPE as a comma-separated list — one job hosting several task
# types (#2622's generalized async_writer job).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_type_comma_list_maps_every_type_to_the_same_job(monkeypatch):
    """A single job advertising TASK_TYPE="a,b,c" maps every one of a/b/c to
    that job's name — the mechanism that lets one Cloud Run Job host several
    task types (the generalized async_writer job drains storage_drain +
    event_drain alongside the elasticsearch_indexer bulk-reindex backstop)."""
    monkeypatch.delenv("ENVIRONMENT", raising=False)

    jobs = [
        _make_job("dynastore-async-writer-job", {
            "APP": "dynastore",
            "TASK_TYPE": "elasticsearch_indexer,storage_drain,event_drain",
        }),
    ]
    module = _make_module(jobs)

    with patch("dynastore.modules.gcp.gcp_module.run_v2", _run_v2_stub()), \
         patch("dynastore.modules.gcp.tools.jobs.set_job_extras"):
        result = await module.get_job_config()

    assert result == {
        "elasticsearch_indexer": "dynastore-async-writer-job",
        "storage_drain": "dynastore-async-writer-job",
        "event_drain": "dynastore-async-writer-job",
    }


@pytest.mark.asyncio
async def test_task_type_single_value_unaffected_by_list_support(monkeypatch):
    """A plain single-value TASK_TYPE (the common case, no commas) still maps
    to exactly one entry — the list-parsing change must not alter the
    existing single-value contract."""
    monkeypatch.delenv("ENVIRONMENT", raising=False)

    jobs = [
        _make_job("dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion",
        }),
    ]
    module = _make_module(jobs)

    with patch("dynastore.modules.gcp.gcp_module.run_v2", _run_v2_stub()), \
         patch("dynastore.modules.gcp.tools.jobs.set_job_extras"):
        result = await module.get_job_config()

    assert result == {"ingestion": "dynastore-ingestion-job"}


# ---------------------------------------------------------------------------
# Non-dynastore jobs are never included (existing guard — regression check)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_dynastore_jobs_excluded_regardless_of_env(monkeypatch):
    """Jobs without APP=dynastore are never included, even if they carry a
    matching ENVIRONMENT tag.  This guard predates the env-filter change and
    must not be affected by it.
    """
    monkeypatch.setenv("ENVIRONMENT", "dev")

    jobs = [
        _make_job("some-other-job", {
            "APP": "something-else", "TASK_TYPE": "ingestion", "ENVIRONMENT": "dev",
        }),
        _make_job("dev-dynastore-ingestion-job", {
            "APP": "dynastore", "TASK_TYPE": "ingestion", "ENVIRONMENT": "dev",
        }),
    ]
    module = _make_module(jobs)

    with patch("dynastore.modules.gcp.gcp_module.run_v2", _run_v2_stub()), \
         patch("dynastore.modules.gcp.tools.jobs.set_job_extras"):
        result = await module.get_job_config()

    assert result == {"ingestion": "dev-dynastore-ingestion-job"}
