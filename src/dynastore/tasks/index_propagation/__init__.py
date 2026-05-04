#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License");

"""Index-propagation task — surgical per-item retry path for the OUTBOX
failure policy.

Decoupled from the heavy ``elasticsearch_indexer`` Cloud Run Job:

* ``elasticsearch_indexer`` (existing) — operator-triggered full
  collection / catalog rebuild.  Runs as a Cloud Run Job.

* ``index_propagation`` (this task) — single-item retry, enqueued by the
  :class:`~dynastore.modules.storage.index_dispatcher.IndexDispatcher`
  in the same PG transaction as the upstream data write when an
  in-process indexer call fails with ``on_failure=OUTBOX``.  Drained by
  the regular tasks worker pool — no dedicated infrastructure.

Both task types operate on the same generic
:class:`~dynastore.models.protocols.indexer.Indexer` Protocol; the
distinction is granularity, not backend.
"""

from .task import IndexPropagationInputs, IndexPropagationTask

__all__ = ["IndexPropagationInputs", "IndexPropagationTask"]
