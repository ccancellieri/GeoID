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
from dynastore import tasks
from dynastore.bootstrap import bootstrap_foundation, instantiate_foundation

logger = logging.getLogger(__name__)

def bootstrap_task_env(
    app_state: object,
    include_modules: Optional[List[str]] = None,
):
    """Bootstrap the task-worker environment.

    Module/task discovery is driven by installed package metadata.
    ``include_modules`` narrows module *instantiation* for test isolation only.
    """
    bootstrap_foundation()
    instantiate_foundation(app_state, include_only=include_modules)

    logger.info("--- [tasks/bootstrap.py] Discovering background tasks ---")
    tasks.discover_tasks()
    # Ensure get_task_instance() can construct tasks that need app_state even
    # when called outside the manage_tasks() lifespan (capability checks,
    # ad-hoc runner lookups during dispatcher startup).
    tasks.set_task_app_state(app_state)
