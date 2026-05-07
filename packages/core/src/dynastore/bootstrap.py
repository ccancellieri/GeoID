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
from typing import List, Optional
from dynastore import modules

logger = logging.getLogger(__name__)

def bootstrap_foundation():
    """Discover every installed ``dynastore.modules`` entry-point."""
    logger.info("--- [bootstrap.py] Discovering foundational modules... ---")
    modules.discover_modules()
    logger.info("--- [bootstrap.py] Foundational module discovery complete. ---")

def instantiate_foundation(app_state: object, include_only: Optional[List[str]] = None):
    """Instantiate discovered modules and attach to the shared application state.

    ``include_only`` remains for test isolation at the instantiation layer; it
    does NOT affect discovery, which is driven by installed package metadata.
    """
    modules.instantiate_modules(app_state, include_only=include_only)
