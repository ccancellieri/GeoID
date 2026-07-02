#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Cascade cleanup owner for catalog-scoped IAM policies.

:class:`IamCatalogScopedOwner` deletes all ``iam.policies`` rows whose
``partition_key`` equals the deleted catalog_id, then drops the per-catalog
LIST partition table if one was created via
:meth:`~.postgres_policy_storage.PostgresPolicyStorage.ensure_policy_partition`.

The ``iam.policies`` table is PARTITION BY LIST (partition_key).  Per-catalog
partitions are lazily created by :meth:`ensure_policy_partition` the first
time a policy with that partition_key is seeded.  A global ``DEFAULT`` partition
catches all un-partitioned rows.  The DELETE alone is safe and idempotent when
no per-catalog partition exists (rows land in DEFAULT); the DROP TABLE is
conditional on the partition's existence.

Register via :func:`register_owners` from the IAM module lifespan BEFORE the
CascadeCleanupRegistry is frozen.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Iterable

from dynastore.modules.catalog.resource_owner import (
    BaseResourceOwner,
    CleanupMode,
    CleanupOutcome,
    CleanupRef,
    ResourceScope,
    ScopeRef,
)

if TYPE_CHECKING:
    from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry

logger = logging.getLogger(__name__)

_IAM_SCHEMA = "iam"


class IamCatalogScopedOwner(BaseResourceOwner):
    """Cleans up catalog-scoped ``iam.policies`` rows on catalog hard-delete."""

    owner_id: ClassVar[str] = "iam.catalog_scoped"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG,)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        if scope_ref.scope != ResourceScope.CATALOG:
            return []
        return [
            CleanupRef(
                kind="iam_policies",
                locator=scope_ref.catalog_id,
                owner_id=self.owner_id,
                metadata={"partition_key": scope_ref.catalog_id},
            )
        ]

    async def cleanup_one(
        self,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome:
        if mode == CleanupMode.SOFT:
            return CleanupOutcome.DONE

        catalog_id = ref.metadata.get("partition_key") or ref.locator

        if dry_run:
            logger.info(
                "IamCatalogScopedOwner: dry-run — would delete iam.policies "
                "WHERE partition_key=%r and drop partition table.",
                catalog_id,
            )
            return CleanupOutcome.DONE

        try:
            from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler, managed_transaction
            from dynastore.tools.protocol_helpers import get_engine

            engine = get_engine()
            if engine is None:
                logger.error("IamCatalogScopedOwner: no DB engine — cannot clean up.")
                return CleanupOutcome.RETRY

            async with managed_transaction(engine) as conn:
                await DQLQuery(
                    "DELETE FROM iam.policies WHERE partition_key = :partition_key;",
                    result_handler=ResultHandler.NONE,
                ).execute(conn, partition_key=catalog_id)

                from dynastore.tools.db import validate_sql_identifier
                try:
                    validate_sql_identifier(catalog_id)
                    partition_table = f"policies_{catalog_id}"
                    exists = await DQLQuery(
                        "SELECT 1 FROM pg_tables"
                        " WHERE schemaname = 'iam' AND tablename = :tname;",
                        result_handler=ResultHandler.ONE_OR_NONE,
                    ).execute(conn, tname=partition_table)
                    if exists:
                        from dynastore.modules.db_config.query_executor import DDLQuery
                        await DDLQuery(
                            f'DROP TABLE IF EXISTS iam."{partition_table}";'
                        ).execute(conn)
                        logger.info(
                            "IamCatalogScopedOwner: dropped partition table iam.%r.",
                            partition_table,
                        )
                except Exception as drop_exc:
                    logger.warning(
                        "IamCatalogScopedOwner: could not drop partition table "
                        "for %r (non-fatal — rows already deleted): %s",
                        catalog_id, drop_exc,
                    )

            # Every catalog-scoped grant that fed CatalogMembershipHandler
            # (modules/iam/conditions.py) is now gone. get_membership_cached
            # (extensions/iam/membership_cache.py) keys its 60s-TTL cache on
            # iam_rule_version() — the platform "iam" binding-version counter
            # — so without this bump a principal whose membership was cached
            # before the delete keeps passing catalog_membership_required for
            # up to the rest of that TTL window. Bump both counters, mirroring
            # PostgresIamStorage._bump_binding_version /
            # PostgresPolicyStorage._bump_binding_version — every other write
            # to iam.policies / iam grants already does this; a catalog
            # hard-delete removing those rows out from under the cache must
            # too (#2674).
            from dynastore.modules.iam.phantom_token import bump_binding_version
            await bump_binding_version(catalog_id)
            await bump_binding_version("iam")

            logger.info(
                "IamCatalogScopedOwner: deleted iam.policies rows for partition_key=%r.",
                catalog_id,
            )
            return CleanupOutcome.DONE

        except Exception as exc:  # noqa: BLE001
            logger.error(
                "IamCatalogScopedOwner: failed to clean up iam.policies "
                "for partition_key=%r: %s",
                catalog_id, exc, exc_info=True,
            )
            return CleanupOutcome.RETRY


def register_owners(registry: "CascadeCleanupRegistry") -> None:
    """Register IAM cascade owners into *registry*.

    Call from IAM module lifespan startup, BEFORE
    :meth:`~dynastore.modules.catalog.cascade_registry.CascadeCleanupRegistry.freeze`.
    """
    registry.register(IamCatalogScopedOwner())
    logger.info("IamModule: registered cascade owner %r.", IamCatalogScopedOwner.owner_id)
