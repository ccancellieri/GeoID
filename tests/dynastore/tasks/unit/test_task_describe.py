"""Unit tests for TaskProtocol.describe() / payload_schema() self-description."""
from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import BaseModel

from dynastore.tasks.descriptor import TaskDescriptor
from dynastore.tasks.protocols import TaskProtocol


class _Payload(BaseModel):
    indexer_id: str
    count: int = 1


class _RichTask(TaskProtocol):
    """Reindex a collection's items.

    Long form ignored — only the first line is the description.
    """
    mandatory: ClassVar[bool] = True
    affinity_tier: ClassVar[Optional[str]] = "worker"
    payload_model: ClassVar[Optional[type]] = _Payload

    async def run(self, payload):  # pragma: no cover - not exercised
        return None


class _BareTask(TaskProtocol):
    async def run(self, payload):  # pragma: no cover
        return None


def test_describe_populates_static_facets_from_class():
    d = _RichTask.describe()
    assert isinstance(d, TaskDescriptor)
    assert d.name == _RichTask.get_name()
    assert d.mandatory is True
    assert d.affinity_tier == "worker"
    assert d.description == "Reindex a collection's items."
    assert d.payload_schema is not None
    assert d.payload_schema["properties"]["indexer_id"]["type"] == "string"


def test_describe_defaults_are_zero_edit_for_bare_task():
    d = _BareTask.describe()
    assert d.mandatory is False
    assert d.affinity_tier is None
    assert d.description == ""          # no docstring -> empty
    assert d.payload_schema is None     # no payload_model -> None


def test_payload_schema_none_without_model():
    assert _BareTask.payload_schema() is None


def test_payload_schema_returns_json_schema_when_model_declared():
    schema = _RichTask.payload_schema()
    assert schema is not None
    assert "indexer_id" in schema["properties"]
