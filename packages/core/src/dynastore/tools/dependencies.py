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

import importlib.metadata
import logging
import os
from typing import List, Optional, Set
from packaging.requirements import Requirement
from packaging.version import parse as parse_version

logger = logging.getLogger(__name__)


def parse_requirements_list(
    file_path: str, processed_files: Optional[Set[str]] = None
) -> Set[str]:
    """
    Recursively parses a requirements.txt file, expanding environment variables.
    Mirrors the logic in setup.py.
    """
    if processed_files is None:
        processed_files = set()

    # Expand variables in the file path itself
    expanded_file_path = os.path.expandvars(file_path)
    if not os.path.isabs(expanded_file_path):
        # We assume the path is relative to the current working directory or provided as absolute
        expanded_file_path = os.path.abspath(expanded_file_path)

    if expanded_file_path in processed_files:
        return set()
    processed_files.add(expanded_file_path)

    if not os.path.exists(expanded_file_path):
        logger.debug(f"Requirement file not found: {expanded_file_path}. Skipping.")
        return set()

    packages = set()
    current_req_dir = os.path.dirname(expanded_file_path)

    try:
        with open(expanded_file_path, "r", encoding="utf-8") as f:
            for line in f:
                # Expand environment variables in the line (like ${APP_DIR})
                processed_line = os.path.expandvars(line.strip())

                if not processed_line or processed_line.startswith("#"):
                    continue

                if processed_line.startswith("-r"):
                    _, next_file = processed_line.split(maxsplit=1)
                    next_file = os.path.expandvars(next_file)

                    if not os.path.isabs(next_file):
                        next_file_path = os.path.join(current_req_dir, next_file)
                    else:
                        next_file_path = next_file

                    packages.update(
                        parse_requirements_list(next_file_path, processed_files)
                    )
                else:
                    packages.add(processed_line)
    except Exception as e:
        logger.error(f"Error parsing requirements file '{expanded_file_path}': {e}")

    return packages


def check_requirements(requirements_file: str) -> bool:
    """
    Checks if all requirements in a requirements.txt file (and its includes) are satisfied.
    """
    if not os.path.exists(requirements_file):
        return True

    requirements = parse_requirements_list(requirements_file)
    for req_str in requirements:
        if not is_requirement_satisfied(req_str):
            logger.debug(f"Requirement '{req_str}' from '{requirements_file}' NOT satisfied.")
            return False

    return True


def is_requirement_satisfied(requirement_str: str) -> bool:
    """
    Checks if a single requirement string is satisfied.
    Handles standard PEP 508 requirements and git URLs.
    """
    try:
        # 1. Handle git/URL based requirements (e.g., pkg @ git+https://...)
        if " @ " in requirement_str:
            package_name = requirement_str.split(" @ ")[0].split("[")[0].strip()
            try:
                importlib.metadata.version(package_name)
                return True
            except importlib.metadata.PackageNotFoundError:
                logger.debug(f"Package '{package_name}' not found (git/url req).")
                return False

        # 2. Handle standard PEP 508 requirements
        req = Requirement(requirement_str)
        try:
            installed_version = importlib.metadata.version(req.name)
            if req.specifier and not req.specifier.contains(
                installed_version, prereleases=True
            ):
                logger.debug(
                    f"Version mismatch for '{req.name}'. Installed: {installed_version}, Required: {req.specifier}"
                )
                return False
            return True
        except importlib.metadata.PackageNotFoundError:
            # Try case-insensitive lookup if possible, or fallback
            # importlib.metadata is generally case-insensitive but let's see.
            logger.debug(f"Package '{req.name}' not found via importlib.metadata.")
            return False

    except Exception as e:
        logger.debug(f"Exception checking requirement '{requirement_str}': {e}")
        # Fallback for unparseable strings
        return _fallback_name_check(requirement_str)


def _fallback_name_check(req_str: str) -> bool:
    """Very basic fallback check for unparseable requirement strings."""
    name = req_str
    for opt in ["==", ">=", "<=", ">", "<", "~=", "[", "@"]:
        name = name.split(opt)[0]

    name = name.strip()
    try:
        importlib.metadata.version(name)
        return True
    except importlib.metadata.PackageNotFoundError:
        return False
