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

"""Utilities for loading component-local .env files and probing the runtime context."""

import inspect
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def load_component_dotenv(cls: "type[Any]") -> None:
    """
    Loads a ``.env`` file co-located with a component's source file (module,
    extension, or task) **without overriding** environment variables that are
    already set.

    Convention: place a ``.env`` file in the same directory as the file that
    contains the class implementing the plugin protocol (``ModuleProtocol``,
    ``ExtensionProtocol``, or ``TaskProtocol``).  The framework discovers and
    loads it automatically before the component is instantiated, so any secrets
    or local-dev overrides declared there are available from the very first line
    of ``__init__``.

    Existing environment variables always take precedence (``override=False``),
    so production/CI values set via Docker or CI secrets are never clobbered.
    """
    try:
        from dotenv import load_dotenv
        source_file = inspect.getfile(cls)
        env_path = Path(source_file).parent / ".env"
        if env_path.is_file():
            load_dotenv(env_path, override=False)
            logger.info(f"Loaded component .env from {env_path}")
    except (TypeError, OSError, ImportError):
        pass


def is_running_as_job() -> bool:
    """Return True when the process is an ephemeral worker (Cloud Run Job or
    local dev / CI), False when it is a long-running Cloud Run Service.

    Cloud Run Services always set the ``K_SERVICE`` environment variable.
    Cloud Run Jobs do not; neither does local dev or a CI runner.
    In all non-service contexts there is no prior in-memory state to recover —
    startup scans that exist solely to reconstruct that state (e.g. the DENY
    policy restore in the private ES driver) are pure waste and must be skipped.
    """
    return not bool(os.getenv("K_SERVICE"))
