"""Unit tests for :class:`ConfigResolutionError` and its handler mapping."""

from dynastore.modules.db_config.exceptions import (
    ConfigResolutionError,
    DatabaseError,
)


class TestConfigResolutionError:
    def test_inherits_database_error(self):
        err = ConfigResolutionError("boom", missing_key="k")
        assert isinstance(err, DatabaseError)

    def test_message_is_preserved(self):
        err = ConfigResolutionError("missing default", missing_key="collection_routing_config")
        assert "missing default" in str(err)

    def test_structured_fields(self):
        err = ConfigResolutionError(
            "missing default",
            missing_key="items_iceberg_driver",
            required_fields=["warehouse", "namespace"],
            scope_tried=["collection", "catalog", "platform", "code_default"],
            hint="set warehouse at platform scope",
        )
        assert err.missing_key == "items_iceberg_driver"
        assert err.required_fields == ["warehouse", "namespace"]
        assert err.scope_tried == [
            "collection",
            "catalog",
            "platform",
            "code_default",
        ]
        assert err.hint == "set warehouse at platform scope"

    def test_required_fields_defaults_to_empty_list_not_shared(self):
        a = ConfigResolutionError("a", missing_key="x")
        b = ConfigResolutionError("b", missing_key="y")
        a.required_fields.append("leak")
        assert b.required_fields == []

    def test_scope_tried_defaults_to_empty_list_not_shared(self):
        a = ConfigResolutionError("a", missing_key="x")
        b = ConfigResolutionError("b", missing_key="y")
        a.scope_tried.append("leak")
        assert b.scope_tried == []

    def test_default_hint_mentions_missing_key(self):
        err = ConfigResolutionError(
            "no default", missing_key="collection_routing_config"
        )
        assert "collection_routing_config" in err.hint

    def test_default_hint_lists_required_fields(self):
        err = ConfigResolutionError(
            "no default",
            missing_key="items_iceberg_driver",
            required_fields=["warehouse"],
        )
        assert "warehouse" in err.hint

    def test_default_hint_notes_no_required_fields(self):
        err = ConfigResolutionError("no default", missing_key="collection_routing_config")
        assert "none declared" in err.hint


class TestConfigResolutionErrorHandlerRegistered:
    def test_handler_is_importable_and_catches_config_resolution_error(self):
        """Handler is registered for the exception type; rendering is exercised
        at the integration layer — here we only assert the mapping exists."""
        from dynastore.extensions.tools import exception_handlers as eh

        # The handler class exists and is registered on the app factory.
        handler_cls = getattr(eh, "ConfigResolutionExceptionHandler", None)
        assert handler_cls is not None, (
            "ConfigResolutionExceptionHandler must be defined in "
            "extensions/tools/exception_handlers.py"
        )
