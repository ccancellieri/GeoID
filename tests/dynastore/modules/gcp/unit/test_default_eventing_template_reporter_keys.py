"""Regression guard for issue #654 followup.

The default ingestion template baked into ``gcp_catalog_ops`` once shipped
``reporting={"gcs_detailed": ...}`` — a key that nothing in the registry
resolved. Under the old silent-skip behavior the eventing-triggered task
completed without writing any report (the original user-facing #654
symptom). Under the post-#657 fail-loud behavior the task raises and the
managed-eventing path is broken outright.

This test pins the template's reporter keys against the live ingestion
registry so any future drift fails CI instead of production.
"""

from __future__ import annotations

import re


# Pre-warm registration chain: importing the gcp bucket reporter triggers
# ``@ingestion_reporter`` decoration which populates the registry.
import dynastore.modules.gcp.bucket_reporter  # noqa: F401
from dynastore.tasks.ingestion.reporters import _ingestion_reporter_registry


def _default_template_reporter_keys() -> list[str]:
    """Extract literal reporter-name keys from the ``default_ingestion_template``
    defined inline inside ``gcp_catalog_ops.ensure_storage_for_catalog``.

    Parsing the source avoids constructing the template (which would require
    a live DB engine + protocols). The template is small and stable enough
    that a regex pinned to the surrounding context is acceptable.
    """
    import inspect

    from dynastore.modules.gcp import gcp_catalog_ops

    source = inspect.getsource(gcp_catalog_ops)
    # Match the reporting block of the default template only.
    match = re.search(
        r'"reporting":\s*\{(?P<body>.*?)\}\s*,\s*\n\s*"column_mapping"',
        source,
        re.DOTALL,
    )
    assert match, "default_ingestion_template reporting block not found"
    return re.findall(r'"(\w+)"\s*:\s*\{', match.group("body"))


def test_default_eventing_template_uses_registered_reporters() -> None:
    keys = _default_template_reporter_keys()
    assert keys, "expected at least one reporter key in the default template"

    unknown = [k for k in keys if k not in _ingestion_reporter_registry]
    assert not unknown, (
        f"default GCP eventing template references reporters not in the "
        f"ingestion registry: {unknown!r}; available: "
        f"{sorted(_ingestion_reporter_registry)!r}"
    )


def test_gcs_detailed_reporter_registered_under_snake_case() -> None:
    # Belt-and-braces — the registry key is the canonical snake_case form.
    assert "gcs_detailed_reporter" in _ingestion_reporter_registry
    assert "GcsDetailedReporter" not in _ingestion_reporter_registry
    assert "gcs_detailed" not in _ingestion_reporter_registry
