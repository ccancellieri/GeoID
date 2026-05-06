from .notebooks_extension import NotebooksExtension
from . import tenant_initialization
from . import config  # noqa: F401  -- service-exposure plugin registration

__all__ = ["NotebooksExtension"]
