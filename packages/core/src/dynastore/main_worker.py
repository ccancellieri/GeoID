# src/dynastore/main_worker.py
import asyncio
import logging
import os
import sys
from contextlib import AsyncExitStack
from dynastore import modules
from dynastore.modules.procrastinate.module import get_procrastinate_app  # type: ignore[import]

# --- Basic Configuration ---
log_level_name = os.getenv('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

class AppState:
    """A simple state object to pass to module lifespans."""
    pass

async def main():
    """
    Initializes the application's modules via their lifespans and then starts
    the Procrastinate worker as a long-running process. This is the primary
    entrypoint for the event-worker service.
    """
    logger.info("--- [main_worker.py] Initializing application context for worker... ---")
    
    app_state = AppState()
    # 1. Discover all modules based on the SCOPE environment variable.
    #    This populates the internal module registry.
    modules.discover_modules() # Populates the registry with module classes.

    # 1.5. Instantiate all discovered modules using the shared state object.
    modules.instantiate_modules(app_state)

    # 2. Use the modules.lifespan context manager. This is the key.
    #    It will instantiate all discovered modules and enter their individual
    #    lifespan contexts, setting up database connections, event listeners, etc.
    async with modules.lifespan(app_state):
        logger.info("--- [main_worker.py] All module lifespans entered. Starting Procrastinate worker... ---")
        # Get the app instance *after* the lifespan has started and configured it.
        procrastinate_app = get_procrastinate_app()
        if not procrastinate_app:
            raise RuntimeError("Procrastinate app was not initialized correctly by its module's lifespan.")

        # Now, within the fully initialized context, run the worker indefinitely.
        # This will listen on the database and execute deferred tasks.
        await procrastinate_app.run_worker_async(queues=None, listen_notify=True)

    logger.info("--- [main_worker.py] Procrastinate worker has stopped. All lifecycles shut down. ---")

if __name__ == "__main__":
    asyncio.run(main())