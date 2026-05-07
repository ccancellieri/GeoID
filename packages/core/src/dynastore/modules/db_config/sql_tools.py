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

import os
import logging
from dynastore.modules.db_config.query_executor import DbResource, DDLQuery
from dynastore.modules.db_config.locking_tools import acquire_startup_lock

logger = logging.getLogger(__name__)

async def execute_sql_script(conn: DbResource, script_path: str, lock_key: str = "init_script"):
    """
    Executes a raw SQL script file.
    
    Uses the DbResource abstraction to handle both sync and async connections symmetrically.
    Falls back to DDLQuery for execution, which handles statement splitting and formatting.
    """
    if not os.path.exists(script_path):
        logger.warning(f"SQL Script not found: {script_path}")
        return

    async with acquire_startup_lock(conn, lock_key) as active_conn:
        if active_conn is None:
            logger.warning("Could not acquire startup lock. Skipping SQL script execution.")
            return
        logger.info(f"Acquired startup lock '{lock_key}'. Executing SQL script: {script_path}")
        try:
            with open(script_path, "r") as f:
                script_content = f.read()
            
            if not script_content.strip():
                logger.info("Script was empty.")
                return

            # Use DDLQuery which properly handles DbResource abstraction
            # Escape braces for format string safety
            safe_script = script_content.replace("{", "{{").replace("}", "}}")
            await DDLQuery(safe_script).execute(active_conn)

            logger.info(f"Successfully executed script: {script_path}")

        except Exception as e:
            logger.error(f"Failed to execute SQL script '{script_path}': {e}")
            raise
