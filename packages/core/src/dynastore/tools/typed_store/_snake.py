"""PascalCase → snake_case helper. Kept in a leaf module with no project
imports so anything that needs the conversion (``PersistentModel.class_key``,
``ingestion_reporter`` registry) can share it without dragging
``typed_store.base`` onto the import path — the latter pulls in
``PluginConfig`` registration and creates circular imports on early-load
modules like ``dynastore.tasks.ingestion.reporters``.
"""

from __future__ import annotations

import re

_PASCAL_BOUNDARY_1 = re.compile(r"(.)([A-Z][a-z]+)")
_PASCAL_BOUNDARY_2 = re.compile(r"([a-z0-9])([A-Z])")


def to_snake(name: str) -> str:
    """PascalCase → snake_case. Handles consecutive caps.

    ``GeometryStorage`` → ``geometry_storage``
    ``DGGSConfig``     → ``dggs_config``
    ``WFSPluginConfig`` → ``wfs_plugin_config``
    ``GcsDetailedReporter`` → ``gcs_detailed_reporter``
    Leading underscores survive: ``_DemoDriverA`` → ``_demo_driver_a``.
    """
    leading = ""
    body = name
    while body.startswith("_"):
        leading += "_"
        body = body[1:]
    s1 = _PASCAL_BOUNDARY_1.sub(r"\1_\2", body)
    return leading + _PASCAL_BOUNDARY_2.sub(r"\1_\2", s1).lower()
