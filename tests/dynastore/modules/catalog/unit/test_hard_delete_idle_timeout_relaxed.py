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

"""Regression: the hard-delete transaction must relax
``idle_in_transaction_session_timeout`` for its own duration.

Background
----------
``delete_collection(force=True)`` snapshots CleanupRefs inside the open delete
transaction. ``RoutingDrivenCascadeOwner.describe_scope`` — pinned to run inside
that transaction by ``test_hard_delete_config_snapshot_outside_txn`` — resolves
routing config through ``ConfigsProtocol`` on its *own* pooled connection (so it
observes live, pre-drop config), and ``_route_delete_metadata`` does the same
during the purge. While those second-connection reads run, the delete
transaction's connection sits idle.

The #2250 fix moved the *top-level* config snapshot out of the transaction, but
these in-txn second-connection reads remain by design. Under a cold config cache
or pool contention the idle gap exceeds the 30s default
``idle_in_transaction_session_timeout``; PostgreSQL terminates the backend, and
the next statement on the connection fails with::

    InterfaceError: cannot call PreparedStatement.fetch():
    the underlying connection is closed

(or the same on ``SAVEPOINT``), leaving the collection stuck in ``DELETING``
after the GCS folder was already removed by the async destroyer.

The fix issues ``SET LOCAL idle_in_transaction_session_timeout = '0'`` at the
top of the delete transaction (auto-reverts on commit), gated to the path the
service owns (``db_resource is None``) so a caller-supplied transaction is never
mutated. These source-shape guards pin the fix so a future refactor that drops
it fails loudly.
"""

from __future__ import annotations

import inspect

from dynastore.modules.catalog.catalog_service import CatalogService
from dynastore.modules.catalog.collection_service import CollectionService


def test_delete_txn_relaxes_idle_in_transaction_timeout() -> None:
    """The delete transaction must SET LOCAL idle_in_transaction_session_timeout."""
    src = inspect.getsource(CollectionService.delete_collection)
    assert "idle_in_transaction_session_timeout" in src, (
        "delete_collection must relax idle_in_transaction_session_timeout for "
        "the delete transaction. The cascade describe_scope and "
        "_route_delete_metadata read config on a second pooled connection while "
        "this transaction's connection sits idle; under a cold cache that idle "
        "gap trips the 30s default and PostgreSQL kills the connection mid-purge."
    )


def test_idle_timeout_relax_uses_set_local() -> None:
    """It must be SET LOCAL (transaction-scoped, auto-reverting), not a global."""
    src = inspect.getsource(CollectionService.delete_collection)
    assert "SET LOCAL idle_in_transaction_session_timeout" in src, (
        "the relaxation must use SET LOCAL so it reverts on commit/rollback and "
        "never leaks to other transactions on the pooled connection."
    )


def test_idle_timeout_relax_precedes_cascade_snapshot() -> None:
    """The relaxation must run before the cascade snapshot / purge, i.e. before
    any second-connection read can put the transaction connection idle.
    """
    src = inspect.getsource(CollectionService.delete_collection)
    relax_at = src.find("SET LOCAL idle_in_transaction_session_timeout")
    snapshot_at = src.find("snapshot_and_enqueue")
    assert relax_at != -1, "SET LOCAL idle_in_transaction relaxation missing"
    assert snapshot_at != -1, "snapshot_and_enqueue call missing from delete_collection"
    assert relax_at < snapshot_at, (
        "idle_in_transaction_session_timeout must be relaxed BEFORE the cascade "
        "snapshot runs — describe_scope's second-connection config read is the "
        "first thing that holds the transaction connection idle."
    )


def test_idle_timeout_relax_gated_to_owned_transaction() -> None:
    """The relaxation must be gated on ``db_resource is None`` so it only touches
    a transaction this service owns, never a caller-supplied one.
    """
    src = inspect.getsource(CollectionService.delete_collection)
    relax_at = src.find("SET LOCAL idle_in_transaction_session_timeout")
    guard_at = src.rfind("db_resource is None", 0, relax_at)
    assert guard_at != -1, (
        "the SET LOCAL relaxation must sit under an `if db_resource is None:` "
        "guard so it never mutates a caller-supplied (joined) transaction."
    )


# ---------------------------------------------------------------------------
# Catalog-level hard delete shares the same mechanism: _purge_catalog_storage
# calls snapshot_and_enqueue (describe_scope -> second-connection routing-config
# read) inside delete_catalog's transaction. The same relaxation applies.
# ---------------------------------------------------------------------------


def test_delete_catalog_relaxes_idle_in_transaction_timeout() -> None:
    """delete_catalog must relax idle_in_transaction_session_timeout via SET LOCAL."""
    src = inspect.getsource(CatalogService.delete_catalog)
    assert "SET LOCAL idle_in_transaction_session_timeout" in src, (
        "delete_catalog must relax idle_in_transaction_session_timeout for the "
        "delete transaction — _purge_catalog_storage's cascade describe_scope "
        "reads routing config on a second pooled connection while this "
        "transaction's connection sits idle, tripping the 30s default under a "
        "cold cache and killing the connection mid-delete."
    )


def test_delete_catalog_idle_relax_precedes_purge() -> None:
    """The relaxation must run before the purge call that triggers the cascade
    snapshot's second-connection read.
    """
    src = inspect.getsource(CatalogService.delete_catalog)
    relax_at = src.find("SET LOCAL idle_in_transaction_session_timeout")
    # match the call (`self._purge_catalog_storage(...)`), not the comment mention
    purge_at = src.find("self._purge_catalog_storage")
    assert relax_at != -1, "SET LOCAL idle_in_transaction relaxation missing"
    assert purge_at != -1, "_purge_catalog_storage call missing from delete_catalog"
    assert relax_at < purge_at, (
        "idle_in_transaction_session_timeout must be relaxed BEFORE "
        "_purge_catalog_storage runs the cascade snapshot."
    )


def test_delete_catalog_idle_relax_gated_to_owned_transaction() -> None:
    """The catalog relaxation must also be gated on ``db_resource is None``."""
    src = inspect.getsource(CatalogService.delete_catalog)
    relax_at = src.find("SET LOCAL idle_in_transaction_session_timeout")
    guard_at = src.rfind("db_resource is None", 0, relax_at)
    assert guard_at != -1, (
        "the catalog SET LOCAL relaxation must sit under an "
        "`if db_resource is None:` guard so it never mutates a caller-supplied "
        "transaction."
    )
