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

import logging
logger = logging.getLogger(__name__)

# --- Optional Submodule Discovery ---
# We use try/except blocks to allow the GCP module structure to be scanned even
# in environments (like CI pipelines) where GCP-specific dependencies
# might not be installed.

try:
    # --- Reporter Registration ---
    # By importing the bucket_reporter here, we ensure that whenever the GCPModule
    # is active, the GcsDetailedReporter is automatically registered with the
    # ingestion task system. This makes it discoverable.
    from dynastore.modules.gcp import bucket_reporter
except ImportError as e:
    logger.debug(f"GCP Module: bucket_reporter skipped due to missing dependencies: {e}")

try:
    # By importing the gcp_config here, we ensure that the PluginConfig models
    # auto-register via PluginConfig.__init_subclass__ when the GCP module loads.
    from dynastore.modules.gcp import gcp_config
except ImportError as e:
    logger.debug(f"GCP Module: gcp_config skipped due to missing dependencies: {e}")

try:
    from dynastore.modules.gcp import gcp_ingestion_operations
except ImportError as e:
    logger.debug(f"GCP Module: gcp_ingestion_operations skipped due to missing dependencies: {e}")