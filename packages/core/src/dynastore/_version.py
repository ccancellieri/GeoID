"""Single source of truth for DynaStore version at runtime."""

import importlib.metadata
import logging
import os
import subprocess
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_PACKAGE_NAME = "DynaStore"
_VERSION_UNKNOWN = "0.0.0-unknown"


def get_version() -> str:
    """Return the package version using a two-tier fallback.

    Tier 1: installed package metadata (pip install / wheel / Docker runtime).
            Populated by setuptools-scm at build time from the latest git tag.
    Tier 2: setuptools-scm-generated module (editable install / source layout).
    """
    try:
        return importlib.metadata.version(_PACKAGE_NAME)
    except importlib.metadata.PackageNotFoundError:
        pass
    try:
        from dynastore._scm_version import __version__  # type: ignore[import-not-found]
        return __version__
    except ImportError:
        pass
    logger.warning("Could not determine DynaStore version; using fallback.")
    return _VERSION_UNKNOWN


def get_git_commit() -> str:
    """Return the short git commit hash, or 'unknown' in non-git environments."""
    build_commit = os.environ.get("BUILD_COMMIT", "").strip()
    if build_commit and build_commit != "unknown":
        return build_commit
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            timeout=5,
        ).decode().strip()
    except Exception:
        return "unknown"


def get_build_info() -> dict:
    """Return version info safe for public exposure."""
    return {
        "version": get_version(),
        "commit": get_git_commit(),
        "build_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


VERSION = get_version()
