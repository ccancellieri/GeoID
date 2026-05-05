import pytest

from dynastore.models.protocols.storage_driver import Capability


class TestCapabilityEnum:
    def test_standard_values(self):
        assert Capability.READ_ONLY == "read_only"
        assert Capability.STREAMING == "streaming"
        assert Capability.EXPORT == "export"
        assert Capability.TIME_TRAVEL == "time_travel"
        assert Capability.SOFT_DELETE == "soft_delete"
        assert Capability.VERSIONING == "versioning"
        assert Capability.SCHEMA_EVOLUTION == "schema_evolution"
        assert Capability.SNAPSHOTS == "snapshots"

    def test_read_flavours_moved_to_hint(self):
        """Read-flavour capabilities (FULLTEXT, SPATIAL_FILTER, AGGREGATION,
        COUNT, STATISTICS, SORT, GROUP_BY, ATTRIBUTE_FILTER) were retired
        from ``Capability`` in PR #3b and moved to the ``Hint``
        catalogue.  They're per-request preferences, not structural facts.
        """
        for moved in (
            "FULLTEXT", "SPATIAL_FILTER", "ATTRIBUTE_FILTER",
            "AGGREGATION", "COUNT", "STATISTICS", "SORT", "GROUP_BY",
        ):
            assert not hasattr(Capability, moved), (
                f"Capability.{moved} should have moved to Hint catalogue"
            )

    def test_is_str(self):
        assert isinstance(Capability.STREAMING, str)

    def test_frozenset_membership(self):
        caps = frozenset({Capability.STREAMING, Capability.EXPORT})
        assert Capability.STREAMING in caps
        assert Capability.READ_ONLY not in caps

    def test_mixed_with_custom_strings(self):
        caps = frozenset({Capability.STREAMING, "custom_capability"})
        assert Capability.STREAMING in caps
        assert "custom_capability" in caps
        assert "streaming" in caps

    def test_all_members(self):
        # Capability is a plain class (not Enum) — collect string constants via inspection
        import inspect
        members = [
            v for k, v in inspect.getmembers(Capability)
            if not k.startswith("_") and isinstance(v, str)
        ]
        assert len(members) >= 10
