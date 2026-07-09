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

"""Engine doubles for tests that stub the DB layer entirely.

Shared home for the fake-async-engine pattern so each test file stops
growing its own subtly-different copy (the drift is what kept
re-introducing the ``MagicMock``-as-event-target bug).
"""

from typing import Any


def make_fake_async_engine() -> Any:
    """A stand-in for the object ``create_async_engine`` returns, for tests
    that stub the drain/claim loop and only exercise ``run()``-level control
    flow.

    ``create_task_engine`` (and the serving-engine factory) unconditionally
    register SQLAlchemy event listeners on ``engine.sync_engine``
    (``_arm_client_socket_keepalive``, the pooler timeout guard). A bare
    ``MagicMock`` does not satisfy SQLAlchemy's event-target validation and
    raises ``InvalidRequestError: No such event 'connect' for target ...``,
    so ``sync_engine`` here is a real, never-started sync ``Engine`` — a
    valid event target. Nothing in these tests ever opens a connection
    through it, so the listeners themselves are never invoked.
    """
    from unittest.mock import AsyncMock, MagicMock

    from sqlalchemy import create_engine

    engine = MagicMock()
    engine.sync_engine = create_engine("sqlite://")
    engine.dispose = AsyncMock()
    return engine
