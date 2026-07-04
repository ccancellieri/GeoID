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

"""Single source of truth for DynaStore version at runtime."""

import importlib.metadata
import logging
import os
import subprocess

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


def get_build_time() -> str:
    """Return the image build timestamp, or the last commit's timestamp as a fallback.

    Tier 1: BUILD_TIME env var, set by the Docker build (no .git in runtime image).
    Tier 2: local git environments — last commit timestamp, close enough for dev.
    Never falls back to "now": that would silently mislabel the current time as
    the build time.
    """
    build_time = os.environ.get("BUILD_TIME", "").strip()
    if build_time:
        return build_time
    try:
        return subprocess.check_output(
            ["git", "log", "-1", "--format=%cI"],
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
        "build_time": get_build_time(),
    }


VERSION = get_version()
