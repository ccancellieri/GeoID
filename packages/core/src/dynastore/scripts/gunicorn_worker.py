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

"""Gunicorn worker class binding a bounded connection-drain budget.

Upstream ``uvicorn.workers.UvicornWorker`` never sets uvicorn's own
``timeout_graceful_shutdown``, so ``uvicorn.server.Server.shutdown()``
waits *indefinitely* for every open connection to close before it ever
runs the ASGI lifespan shutdown handlers (DB engine dispose, background
service stop, LISTEN/NOTIFY connection teardown, ...). A single stalled
connection -- an idle keep-alive, a slow client, a wedged long poll --
means that wait never completes, and the worker process sits inert
until something *external* hard-kills it: gunicorn's own
``--graceful-timeout``, or the platform's SIGKILL deadline (Cloud Run
gives roughly 10s after SIGTERM before killing the container). Either
kill lands before our lifespan teardown ever runs, so an otherwise
healthy in-flight request gets its connection severed mid-response and
DB-side resources (sessions, LISTEN connections, advisory locks) are
never released.

Binding ``timeout_graceful_shutdown`` here bounds uvicorn's own wait so
it always reaches lifespan shutdown -- and therefore always releases DB
resources -- comfortably before gunicorn's or the platform's outer kill
fires. See ``start.sh`` for the paired ``--graceful-timeout`` budget.
"""

import os

from uvicorn.workers import UvicornWorker

# Deploy-time knob, not application behavior — read once at worker import
# time, same as the other GUNICORN_* server-process flags start.sh reads.
_DRAIN_SECONDS = int(os.environ.get("GUNICORN_DRAIN_TIMEOUT", "8"))


class DrainAwareUvicornWorker(UvicornWorker):
    """UvicornWorker with a bounded, rather than indefinite, drain budget."""

    CONFIG_KWARGS = {
        **UvicornWorker.CONFIG_KWARGS,
        "timeout_graceful_shutdown": _DRAIN_SECONDS,
    }
