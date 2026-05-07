from .notebooks_module import NotebooksModule
from .models import NotebookCreate, Notebook, NotebookBase, PlatformNotebookCreate, PlatformNotebook, OwnerType
from .notebooks_db import init_notebooks_storage
from .example_registry import register_platform_notebook, get_registered_notebooks

# Built-in notebook registrations live in module-specific submodules
# (catalog/storage/elasticsearch/ingestion). They are loaded by
# NotebooksModule.lifespan() so SCOPE-trimmed Cloud Run Job images that lack
# a module's heavy deps (e.g. shapely via storage) can still import this
# package safely.

__all__ = [
    "NotebooksModule",
    "init_notebooks_storage",
    "NotebookCreate",
    "Notebook",
    "NotebookBase",
    "PlatformNotebookCreate",
    "PlatformNotebook",
    "OwnerType",
    "register_platform_notebook",
    "get_registered_notebooks",
]
