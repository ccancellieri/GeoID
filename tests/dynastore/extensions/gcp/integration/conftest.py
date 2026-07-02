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

import pytest
import os

from tests._repo_paths import CORE_SRC

_CREDS_FILE = CORE_SRC / "modules" / "gcp" / "application_default_credentials_geospatial-review.json"
_REAL_PROJECT = "fao-aip-geospatial-review"
_REAL_REGION = "europe-west1"


@pytest.fixture(scope="session", autouse=True)
def gcp_integration_env():
    """Set GCP env vars from local credential file if present.

    In CI, credentials come from env vars or metadata server.
    The ``@pytest.mark.gcp`` marker handles skipping when no creds exist.
    """
    if _CREDS_FILE.exists():
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(_CREDS_FILE))
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", _REAL_PROJECT)
        os.environ.setdefault("REGION", _REAL_REGION)


# ``catalog_cleaner`` is inherited from tests/dynastore/conftest.py via
# pytest's directory-based conftest resolution; do not redefine it here.
