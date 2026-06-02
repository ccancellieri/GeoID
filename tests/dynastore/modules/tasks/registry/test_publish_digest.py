"""Publish digest is deterministic, build-keyed, and order-insensitive."""
from __future__ import annotations

from dynastore.modules.tasks.registry.model import CapabilityRow, compute_publish_digest


def _row(task_key: str) -> CapabilityRow:
    return CapabilityRow(
        service="worker", task_key=task_key, kind="process",
        required_capability=None, mandatory=False, affinity_tier=None,
        service_version="1.2.3", service_commit="abc123", version="abc123",
    )


def test_digest_is_order_insensitive():
    a = compute_publish_digest("abc123", "1.2.3", [_row("gdal"), _row("ingestion")])
    b = compute_publish_digest("abc123", "1.2.3", [_row("ingestion"), _row("gdal")])
    assert a == b


def test_digest_changes_with_commit():
    rows = [_row("gdal")]
    assert compute_publish_digest("abc123", "1.2.3", rows) != compute_publish_digest("def456", "1.2.3", rows)


def test_digest_changes_with_task_set():
    assert compute_publish_digest("abc123", "1.2.3", [_row("gdal")]) != compute_publish_digest("abc123", "1.2.3", [_row("gdal"), _row("ingestion")])
