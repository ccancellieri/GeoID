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

"""Protocol-driven autoscaling control loop.

- ``config.py``        — ``ScalingPolicyConfig`` (hot-reloadable thresholds).
- ``aggregator.py``     — Valkey signal document read/write + the pure
                           ``compute_desired_min`` decision function.
- ``publisher.py``      — ``ScalingSignalPublisher``, a ``RUN_EVERYWHERE``
                           periodic service that collects this pod's signals
                           and publishes them.
- ``noop_actuator.py``  — fallback ``PlatformScalingProtocol`` for
                           deployments with no platform-specific actuator
                           registered (e.g. non-GCP).

The platform-specific actuator + leader-elected reconciler live beside the
cloud provider they actuate — see ``dynastore.modules.gcp.scaling_reconciler``
for the Cloud Run implementation.
"""
