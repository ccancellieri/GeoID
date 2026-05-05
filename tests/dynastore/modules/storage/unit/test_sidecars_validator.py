"""Fix 2: Sidecar validation must fail loud on malformed entries.

Post-rebase note (M1b.1): the branch replaced the hand-rolled
``validate_sidecars_polymorphic`` field-validator with Pydantic's
native ``Annotated[Union[...], Discriminator("sidecar_type")]`` on the
``sidecars`` field of ``ItemsPostgresqlDriverConfig``
(``ItemsPostgresqlDriverConfig`` is the legacy alias).  Pydantic
emits its own error shape for discriminator failures / wrong types,
which this test file pins against the native Pydantic messages
rather than the pre-rebase hand-rolled strings.

The fail-loud guarantee is strictly tighter than before:
``aa6b8e7``'s hand-rolled loop silently fell back to the base
``SidecarConfig`` for unknown ``sidecar_type`` values; Pydantic's
discriminated union raises ``union_tag_invalid`` on those, closing
the silent-failure window the original commit was worried about.
"""

import pytest

from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarConfig
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
)


class TestSidecarsValidatorHappyPaths:
    def test_typed_instances_pass_through(self):
        cfg = ItemsPostgresqlDriverConfig(
            sidecars=[GeometriesSidecarConfig()],
        )
        assert len(cfg.sidecars) == 1
        assert isinstance(cfg.sidecars[0], GeometriesSidecarConfig)

    def test_dict_with_discriminator_coerces_to_subclass(self):
        cfg = ItemsPostgresqlDriverConfig(
            sidecars=[{"sidecar_type": "geometries"}],
        )
        assert len(cfg.sidecars) == 1
        assert isinstance(cfg.sidecars[0], GeometriesSidecarConfig)
        assert cfg.sidecars[0].sidecar_type == "geometries"

    def test_mixed_list_resolves_correctly(self):
        cfg = ItemsPostgresqlDriverConfig(
            sidecars=[
                GeometriesSidecarConfig(),
                {"sidecar_type": "attributes"},
            ],
        )
        assert len(cfg.sidecars) == 2
        assert cfg.sidecars[0].sidecar_type == "geometries"
        assert cfg.sidecars[1].sidecar_type == "attributes"


class TestSidecarsValidatorFailsLoud:
    def test_dict_without_sidecar_type_raises(self):
        # Pydantic discriminated-union error: "Unable to extract tag using
        # discriminator 'sidecar_type'"
        with pytest.raises(ValueError, match="sidecar_type"):
            ItemsPostgresqlDriverConfig(sidecars=[{"enabled": True}])

    def test_dict_with_empty_sidecar_type_raises(self):
        # Empty-string tag: Pydantic fails to match any union member →
        # ``union_tag_invalid``; the ``'sidecar_type'`` string still
        # appears in the error message.
        with pytest.raises(ValueError, match="sidecar_type"):
            ItemsPostgresqlDriverConfig(
                sidecars=[{"sidecar_type": "", "enabled": True}]
            )

    def test_non_dict_non_sidecarconfig_raises(self):
        # Pydantic's native ``model_type`` error for a non-dict,
        # non-SidecarConfig input.  Wording switched to "instance of
        # SidecarConfig" with the registry-based discriminator (PR-G3)
        # since the field type is now ``SidecarConfig`` plus a
        # ``BeforeValidator``, not a discriminated ``Union[...]``.
        with pytest.raises(
            ValueError, match="valid dictionary or instance of SidecarConfig"
        ):
            ItemsPostgresqlDriverConfig(sidecars=["not-a-config"])

    def test_error_message_includes_index_for_multi_item_list(self):
        # Pydantic surfaces list indices as ``sidecars.1`` (dot-separated
        # loc path), not ``sidecars[1]``.  Pin the dot form so regressions
        # in Pydantic's loc formatting are visible.
        with pytest.raises(ValueError, match=r"sidecars\.1"):
            ItemsPostgresqlDriverConfig(
                sidecars=[
                    GeometriesSidecarConfig(),
                    {"enabled": True},  # bad item at idx 1
                ]
            )


class TestSidecarsValidatorRoundTrip:
    def test_model_dump_then_validate_preserves_subclass(self):
        """Round-trip via model_dump (mode=python) + model_validate is the path
        exercised by ConfigService.get_config tier-merge at config_service.py:299.
        """
        original = ItemsPostgresqlDriverConfig(
            sidecars=[GeometriesSidecarConfig()],
        )
        dumped = original.model_dump(mode="python")
        restored = ItemsPostgresqlDriverConfig.model_validate(dumped)
        assert isinstance(restored.sidecars[0], GeometriesSidecarConfig)
        assert restored.sidecars[0].sidecar_type == "geometries"

    def test_model_dump_json_then_validate_preserves_subclass(self):
        original = ItemsPostgresqlDriverConfig(
            sidecars=[GeometriesSidecarConfig()],
        )
        payload = original.model_dump_json()
        restored = ItemsPostgresqlDriverConfig.model_validate_json(payload)
        assert isinstance(restored.sidecars[0], SidecarConfig)
        assert restored.sidecars[0].sidecar_type == "geometries"

    def test_exclude_unset_preserves_discriminator(self):
        """Regression: the Cloud Run ingestion crash was caused by this exact
        round-trip. Default-constructed sidecars were dumped with
        ``exclude_unset=True`` at config_service.py:515-519 and landed in the
        DB as ``[{}, {}, {}, {}]`` — the fail-loud validator then rejected
        them on read. The discriminator must survive exclude_unset=True even
        when the caller never explicitly passed it.
        """
        original = ItemsPostgresqlDriverConfig(
            sidecars=[GeometriesSidecarConfig()],
        )
        dumped = original.sidecars[0].model_dump(mode="python", exclude_unset=True)
        assert dumped.get("sidecar_type") == "geometries", (
            f"exclude_unset stripped the discriminator — dumped={dumped}"
        )

    def test_exclude_unset_round_trip_through_driver_config(self):
        """End-to-end version of the previous test: dump the whole driver
        config with exclude_unset=True (the shape written to the DB) and
        verify each sidecar still carries its discriminator.
        """
        original = ItemsPostgresqlDriverConfig(
            sidecars=[GeometriesSidecarConfig()],
        )
        dumped = original.model_dump(mode="json", exclude_unset=True)
        for idx, sc in enumerate(dumped.get("sidecars", [])):
            assert sc.get("sidecar_type"), (
                f"sidecars[{idx}] lost discriminator under exclude_unset: {sc}"
            )
        # And the round-trip back through the validator must succeed.
        restored = ItemsPostgresqlDriverConfig.model_validate(dumped)
        assert restored.sidecars[0].sidecar_type == "geometries"
