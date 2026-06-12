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

"""Regression test for the IamModule SCOPE gate (#1003).

Background: ``IamModule`` lives in ``dynastore-core`` (always installed) but
must only register itself when the SCOPE includes the ``iam`` extras.  The
old import-side-effect gate (``from tenacity import …``) broke when tenacity
became transitively available via ``drivers_grp → pyiceberg``, and the
follow-up ``import jwt`` gate has the same hazard (PyJWT is reachable via
``module_gcp → gcloud-aio-storage → gcloud-aio-auth``).

The current gate checks for the ``dynastore-ext-iam`` distribution — a name
that cannot be installed by any transitive path.  This test pins that
mechanism so a future refactor does not regress to a package-import gate.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import sys
from unittest.mock import patch

import pytest


def _iam_dist_installed() -> bool:
    """Return True when the dynastore-ext-iam dist-info is present."""
    try:
        importlib.metadata.distribution("dynastore-ext-iam")
        return True
    except importlib.metadata.PackageNotFoundError:
        return False


def _drop_iam_module_from_cache():
    for name in list(sys.modules):
        if name == "dynastore.modules.iam.module" or name.startswith(
            "dynastore.modules.iam.module."
        ):
            sys.modules.pop(name, None)


@pytest.mark.skipif(
    not _iam_dist_installed(),
    reason="dynastore-ext-iam distribution not installed — positive gate test requires it",
)
def test_iam_module_loads_when_distribution_present():
    """In a fully-installed dev env the distribution exists and import succeeds."""
    _drop_iam_module_from_cache()
    mod = importlib.import_module("dynastore.modules.iam.module")
    assert hasattr(mod, "IamModule")


def test_iam_module_raises_import_error_when_distribution_missing():
    """Gate fires: ``discover_and_load_plugins`` will skip IamModule on
    SCOPEs that exclude the ``iam`` extras (``dynastore-ext-iam`` absent)."""
    _drop_iam_module_from_cache()

    real_distribution = importlib.metadata.distribution

    def fake_distribution(name: str):
        if name == "dynastore-ext-iam":
            raise importlib.metadata.PackageNotFoundError(name)
        return real_distribution(name)

    with patch.object(importlib.metadata, "distribution", fake_distribution):
        import pytest
        with pytest.raises(ImportError, match="dynastore-ext-iam"):
            importlib.import_module("dynastore.modules.iam.module")

    _drop_iam_module_from_cache()
