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
import pytest_asyncio
import os

from tests._repo_paths import CORE_SRC

_CREDS_FILE = CORE_SRC / "modules" / "gcp" / "application_default_credentials_geospatial-review.json"
_REAL_PROJECT = "fao-aip-geospatial-review"
_REAL_REGION = "europe-west1"


@pytest.fixture(scope="session", autouse=True)
def gcp_integration_env():
    """Set GCP env vars from local credential file if present.

    When running in CI, credentials are expected to come from env vars
    (GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_CLOUD_PROJECT, REGION) or
    from the GCE metadata server.  The ``@pytest.mark.gcp`` marker in
    conftest.py handles skipping when no credentials are available at all.
    """
    if _CREDS_FILE.exists():
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(_CREDS_FILE))
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", _REAL_PROJECT)
        os.environ.setdefault("REGION", _REAL_REGION)


@pytest_asyncio.fixture(autouse=True)
async def enable_bucket_provisioning(app_lifespan):
    """Enable GCS bucket provisioning at the platform tier for this suite.

    ``GcpCatalogBucketConfig.provision_enabled`` defaults to ``True`` in code,
    so a fresh DB picks it up via the Pydantic default. The platform row from
    a previous run may persist in the shared test database with any value, so
    we set it explicitly here rather than depending on what the shared DB
    happens to carry.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import ConfigsProtocol
    from dynastore.modules.gcp.gcp_config import GcpCatalogBucketConfig

    configs = get_protocol(ConfigsProtocol)
    if configs:
        await configs.set_config(
            GcpCatalogBucketConfig,
            GcpCatalogBucketConfig(provision_enabled=True),
        )


@pytest_asyncio.fixture(autouse=True)
async def disable_managed_eventing(app_lifespan):
    """
    Sets a platform-level GcpEventingConfig with managed eventing DISABLED.

    Locally there is no Cloud Run service (K_SERVICE), so Pub/Sub push
    subscriptions cannot be created.  Tests that specifically need eventing
    must set K_SERVICE/SERVICE_URL and override this config explicitly.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import ConfigsProtocol
    from dynastore.modules.gcp.gcp_config import (
        GcpEventingConfig,
        ManagedBucketEventing,
    )

    configs = get_protocol(ConfigsProtocol)
    if configs:
        await configs.set_config(
            GcpEventingConfig,
            GcpEventingConfig(
                managed_eventing=ManagedBucketEventing(enabled=False),
            ),
        )


# ``catalog_cleaner`` is inherited from tests/dynastore/conftest.py via
# pytest's directory-based conftest resolution; do not redefine it here.
