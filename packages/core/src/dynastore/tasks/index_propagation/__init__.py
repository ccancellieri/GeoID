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

"""Index-propagation task — surgical per-item retry path for the OUTBOX
failure policy.

DEPRECATED (un-fao/GeoID#2732 step 1): the ``IndexDispatcher`` no longer
enqueues rows of this task type — ``on_failure=OUTBOX`` now enqueues into
the storage-plane outbox (``tasks.storage``, drained by ``storage_drain``)
instead. This package remains only so rows enqueued before the cutover
still drain during the migration window; see
:class:`~dynastore.modules.storage.index_dispatcher.StoragePlaneOutboxWriter`
for the current producer.

Decoupled from the heavy ``elasticsearch_indexer`` Cloud Run Job:

* ``elasticsearch_indexer`` (existing) — operator-triggered full
  collection / catalog rebuild.  Runs as a Cloud Run Job.

* ``index_propagation`` (this task, deprecated) — single-item retry,
  formerly enqueued by the
  :class:`~dynastore.modules.storage.index_dispatcher.IndexDispatcher`
  in the same PG transaction as the upstream data write when an
  in-process indexer call failed with ``on_failure=OUTBOX``.  Drained by
  the regular tasks worker pool — no dedicated infrastructure.

Both task types operate on the same generic
:class:`~dynastore.models.protocols.indexer.Indexer` Protocol; the
distinction is granularity, not backend.
"""

from .task import IndexPropagationInputs, IndexPropagationTask

__all__ = ["IndexPropagationInputs", "IndexPropagationTask"]
