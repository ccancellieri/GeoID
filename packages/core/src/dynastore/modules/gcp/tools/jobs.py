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

from typing import TYPE_CHECKING, Dict, Optional
from dynastore.modules import get_protocol
from dynastore.models.protocols import JobExecutionProtocol
from dynastore.tools.cache import cached
import logging

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from dynastore.modules.processes.models import Process

logger = logging.getLogger(__name__)

_JOB_MAP_SYNC: Dict[str, str] = {}

# Per-job extras beyond name (currently: max_retries from MAX_RETRIES env on
# the Cloud Run job). Populated as a side-effect of the GCP get_job_config
# implementation; consulted by GcpJobRunner to honour deploy-time retry intent.
_JOB_EXTRAS_SYNC: Dict[str, Dict[str, object]] = {}


def set_job_extras(task_type: str, extras: Dict[str, object]) -> None:
    """Populate per-job metadata side-channel (called by JobExecutionProtocol impls)."""
    _JOB_EXTRAS_SYNC[task_type] = extras


def get_job_max_retries(task_type: str) -> Optional[int]:
    """Return the MAX_RETRIES env value from the Cloud Run Job, if discovered."""
    extras = _JOB_EXTRAS_SYNC.get(task_type) or {}
    val = extras.get("max_retries")
    if val is None:
        return None
    try:
        return int(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


async def run_cloud_run_job_async(
    job_name: str, args: Optional[list] = None, env_vars: Optional[dict] = None
) -> "Operation":
    """
    Triggers a serverless job asynchronously using the JobExecutionProtocol.

    Args:
        job_name (str): The name of the job to execute.
        args (list, optional): A list of command-line arguments to pass to the job's container.
        env_vars (dict, optional): A dictionary of environment variables to override in the job's container.

    Returns:
        Operation: The long-running operation object for the job execution.

    Raises:
        RuntimeError: If the JobExecutionProtocol implementation is not available.
    """
    job_runner = get_protocol(JobExecutionProtocol)
    if not job_runner:
        raise RuntimeError("JobExecutionProtocol not available. Unable to trigger job.")

    return await job_runner.run_job(job_name, args, env_vars)


async def _fetch_job_config() -> Dict[str, str]:
    job_runner = get_protocol(JobExecutionProtocol)
    if not job_runner:
        logger.warning("JobExecutionProtocol not available. Unable to discover jobs.")
        return {}
    return await job_runner.get_job_config()


@cached(maxsize=1, namespace="job_config", distributed=False, ttl=900, jitter=60)
async def load_job_config() -> Dict[str, str]:
    """
    Discovers deployed jobs and returns a mapping of task_type to job name.
    TTL=900s; also updates the sync snapshot used by can_handle().
    """
    global _JOB_MAP_SYNC
    result = await _fetch_job_config()
    _JOB_MAP_SYNC = result
    return result


def get_job_map_sync() -> Dict[str, str]:
    """Sync read of the last successfully fetched job map."""
    return _JOB_MAP_SYNC


def try_load_process_definition(task_type: str) -> Optional["Process"]:
    """Load a Process definition from a lightweight definition.py module.

    Tries both dotted (``export_features``) and hyphenated (``export-features``)
    task_type forms. Returns the first ``Process`` instance found, or None.
    Definition modules only depend on pydantic — safe to import anywhere.
    """
    import importlib
    from dynastore.modules.processes.models import Process

    candidates = [
        f"dynastore.tasks.{task_type}.definition",
        f"dynastore.tasks.{task_type.replace('-', '_')}.definition",
    ]
    for mod_path in candidates:
        try:
            mod = importlib.import_module(mod_path)
            for attr in vars(mod).values():
                if isinstance(attr, Process):
                    return attr
        except ImportError:
            pass
    return None
