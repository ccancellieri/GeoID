"""Utilities for loading component-local .env files."""

import inspect
import logging
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
