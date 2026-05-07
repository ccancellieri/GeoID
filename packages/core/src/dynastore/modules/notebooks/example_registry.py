"""In-memory registry for platform-level notebook examples.

Modules call register_platform_notebook() at import time.
NotebooksModule.lifespan() reads the registry and seeds into
notebooks.platform_notebooks with ON CONFLICT DO NOTHING.
"""
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

from dynastore.models.localization import LocalizedText

from .models import PlatformNotebookCreate, OwnerType

logger = logging.getLogger(__name__)

_platform_registry: list[PlatformNotebookCreate] = []
_registered_ids: set[str] = set()


def _coerce_localized(value) -> Optional[LocalizedText]:
    """Convert str, dict, or LocalizedText to LocalizedText (or None)."""
    if value is None:
        return None
    if isinstance(value, LocalizedText):
        return value
    if isinstance(value, str):
        return LocalizedText(en=value)
    if isinstance(value, dict):
        return LocalizedText(**{k: v for k, v in value.items() if v is not None})
    return LocalizedText(en=str(value))


def register_platform_notebook(
    notebook_id: str,
    registered_by: str,
    notebook_path: Optional[Path] = None,
    notebook_content: Optional[dict] = None,
    title: Optional[Union[str, Dict, LocalizedText]] = None,
    description: Optional[Union[str, Dict, LocalizedText]] = None,
    tags: Optional[List[str]] = None,
    owner_type: OwnerType = OwnerType.MODULE,
) -> None:
    """Register an example notebook for platform-level seeding.

    Call at module import time. Seeding happens during NotebooksModule.lifespan().
    Provide either notebook_path (to a .ipynb file) or notebook_content (raw dict).

    title and description override values found in notebook metadata.
    tags are auto-generated from the English title if not provided.
    """
    if notebook_path is None and notebook_content is None:
        raise ValueError("Provide either notebook_path or notebook_content")

    if notebook_id in _registered_ids:
        logger.debug("Platform notebook '%s' already registered, skipping.", notebook_id)
        return

    if notebook_path is not None:
        with open(notebook_path, "r", encoding="utf-8") as f:
            notebook_content = json.load(f)

    assert notebook_content is not None  # guaranteed by the check above
    nb_meta = notebook_content.get("metadata", {})

    resolved_title = _coerce_localized(title or nb_meta.get("title") or notebook_id)
    assert resolved_title is not None  # fallback to notebook_id guarantees non-None
    resolved_description = _coerce_localized(description or nb_meta.get("description"))

    entry = PlatformNotebookCreate(
        notebook_id=notebook_id,
        title=resolved_title,
        description=resolved_description,
        tags=tags or [],
        content=notebook_content,
        metadata=nb_meta,
        registered_by=registered_by,
        owner_type=owner_type,
    )
    _platform_registry.append(entry)
    _registered_ids.add(notebook_id)
    logger.info("Registered platform notebook '%s' from '%s'.", notebook_id, registered_by)


def get_registered_notebooks() -> list[PlatformNotebookCreate]:
    """Return all registered platform notebook entries."""
    return list(_platform_registry)
