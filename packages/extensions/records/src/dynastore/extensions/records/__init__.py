from .records_service import RecordsService
from . import config  # noqa: F401  -- service-exposure plugin registration

__all__ = ["RecordsService"]
