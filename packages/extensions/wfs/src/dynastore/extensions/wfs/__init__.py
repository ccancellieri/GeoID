"""OGC WFS 2.0.2 service extension — RETAINED production plugin.

This is the XML-based OGC Web Feature Service 2.0.2 implementation, kept
deliberately alongside the newer OGC API - Features. It is a live extension:
``WFSService`` is registered via the ``dynastore.extensions`` entry point
(see ``pyproject.toml``) and is exercised by the maps installation.

Do NOT auto-flag this package for removal in cleanup sweeps — the audit in
geoid#1511 concluded KEEP. The shared ``FeatureProperties`` / ``FeatureStreamConfig``
helpers (geoid#1504 D-9) also have core consumers (the DWH-join export task and
the features exporter) and must not be dropped as "WFS-only".

Importing ``wfs_config`` here triggers its service-exposure plugin registration.
"""

from . import wfs_config  # noqa: F401  -- service-exposure plugin registration
