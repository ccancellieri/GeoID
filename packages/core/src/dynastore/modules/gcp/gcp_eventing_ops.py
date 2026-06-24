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

import asyncio
import logging
from typing import TYPE_CHECKING, Optional, Dict, Tuple, Any

if TYPE_CHECKING:
    from google.api_core import exceptions as google_exceptions
    from google.api_core.exceptions import Aborted
    from google.cloud import pubsub_v1
    from google.cloud import storage
    from dynastore.modules.db_config.query_executor import DbResource
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.gcp.bucket_service import BucketService
else:
    try:
        from google.api_core import exceptions as google_exceptions
        from google.api_core.exceptions import Aborted
    except ImportError:
        google_exceptions = None  # type: ignore[assignment]
        Aborted = None  # type: ignore[assignment]

    try:
        from google.cloud import pubsub_v1
    except ImportError:
        pubsub_v1 = None  # type: ignore[assignment]

from dynastore.modules.concurrency import run_in_thread
from dynastore.models.driver_context import DriverContext
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.catalog.log_manager import log_info
from dynastore.modules.gcp.gcp_config import (
    GcpCatalogBucketConfig,
    GcpEventingConfig,
    ManagedBucketEventing,
    GcsNotificationEventType,
)
from dynastore.modules.gcp.models import (
    PushSubscriptionConfig,
    PUBSUB_JWT_AUDIENCE,
)
from dynastore.modules.gcp.tools import bucket as bucket_tool
from dynastore.modules.catalog.lifecycle_manager import LifecycleContext

logger = logging.getLogger(__name__)

# Bounded retry count for create_topic. Concurrent callers hitting the same
# per-catalog topic (e.g. background lifecycle setup + an explicit
# setup_catalog_gcp_resources call) can race and receive a 409/Aborted.
# With base-1s exponential backoff this spans ~31s across 6 attempts
# (1+2+4+8+16) before giving up.
_TOPIC_CREATE_MAX_ATTEMPTS = 6

# Bounded retry count for create_subscription. Binding a subscription needs
# ``pubsub.topics.attachSubscription`` on the topic, which can transiently 403
# right after the topic + its IAM policy are created (IAM eventual
# consistency). With base-1s exponential backoff this spans ~31s across 6
# attempts (1+2+4+8+16) before giving up, comfortably covering typical IAM
# propagation while still failing fast on a genuine permission gap.
_SUBSCRIPTION_CREATE_MAX_ATTEMPTS = 6


class OrphanSubscriptionClash(RuntimeError):
    """A push subscription already exists with a topic binding we cannot satisfy.

    Subscription→topic binding is immutable in Pub/Sub: ``modify_push_config``
    refreshes the endpoint/OIDC token but cannot rebind the subscription.
    The most common cause is a prior teardown that deleted the topic but left
    the subscription orphaned (pre-fix ``teardown_managed_eventing_channel``
    relied on the false belief that topic-delete cascades to subscriptions).

    Raised at provisioning time so the caller can roll back the just-created
    topic + notifications instead of leaving a half-wired bucket where
    OBJECT_FINALIZE messages silently vanish.
    """

    def __init__(self, *, sub_path: str, bound_to: str, expected: str, project_id: str):
        self.sub_path = sub_path
        self.bound_to = bound_to
        self.expected = expected
        sub_id = sub_path.rsplit("/", 1)[-1]
        message = (
            f"Pub/Sub subscription clash:\n"
            f"  subscription: {sub_path}\n"
            f"  bound to:     {bound_to}\n"
            f"  expected:     {expected}\n"
            f"\n"
            f"Subscription→topic binding is immutable in Pub/Sub, so re-provisioning\n"
            f"cannot rebind this subscription. The most common cause is a prior\n"
            f"teardown that deleted the topic but left the subscription orphaned.\n"
            f"\n"
            f"To recover:\n"
            f"  gcloud pubsub subscriptions delete {sub_id} --project={project_id}\n"
            f"\n"
            f"Then retry the catalog provisioning (POST/PUT the eventing config or\n"
            f"re-trigger the gcp_provision_catalog task)."
        )
        super().__init__(message)


class GcpEventingOpsMixin:
    """Mixin providing Pub/Sub and eventing operations for GCPModule.

    Depends on the following methods being provided by the concrete host class (GCPModule):
    """

    # --- Host interface stubs (provided by GCPModule) ---

    def get_project_id(self) -> Optional[str]: ...

    def get_region(self) -> Optional[str]: ...

    def get_account_email(self) -> Optional[str]: ...

    async def get_self_url(self) -> str: ...

    def get_publisher_client(self) -> "pubsub_v1.PublisherClient": ...

    def get_storage_client(self) -> "storage.Client": ...

    def get_bucket_service(self) -> "BucketService": ...

    def get_subscriber_client(self) -> "pubsub_v1.SubscriberClient": ...

    def get_config_service(self) -> "ConfigsProtocol": ...

    @property
    def engine(self) -> "DbResource": ...

    async def setup_catalog_gcp_resources(
        self, catalog_id: str, context: Optional[LifecycleContext] = None
    ) -> Tuple[str, GcpEventingConfig]: ...

    def generate_default_topic_id(self, catalog_id: str) -> str:
        """Generates the deterministic default Pub/Sub topic ID for a catalog.

        ``catalog_id`` is the immutable internal catalog identifier (shape
        ``c_<suffix>``).  Topics are therefore named ``ds-c_<suffix>-events``.

        Upgrade note: before the catalog id-model redesign, callers passed the
        PostgreSQL schema name (shape ``s_<suffix>``) instead, producing topics
        named ``ds-s_<suffix>-events``.  A non-clean-break upgrade must locate
        and delete those legacy ``ds-s_*`` topics during catalog hard-delete, as
        teardown will compute the new ``ds-c_*`` name and will not find them.
        """
        return f"ds-{catalog_id}-events"

    def generate_default_subscription_id(self, catalog_id: str) -> str:
        """Generates the deterministic default Pub/Sub subscription ID for a catalog.

        ``catalog_id`` is the immutable internal catalog identifier (shape
        ``c_<suffix>``).  See ``generate_default_topic_id`` for the upgrade note
        regarding legacy ``s_<suffix>``-derived names.
        """
        return f"ds-{catalog_id}-default-sub"

    async def apply_eventing_config(
        self, catalog_id: str, config: GcpEventingConfig, conn=None
    ):
        """Applies eventing configuration changes to the live GCP resources."""
        logger.info(
            f"Applying eventing configuration changes for catalog '{catalog_id}'."
        )

        # Handle the managed eventing system
        if config.managed_eventing:
            if config.managed_eventing.enabled:
                logger.info(
                    f"Setting up or updating managed eventing for catalog '{catalog_id}'."
                )
                # This call is already idempotent and manages Topic, Notification, and Subscription
                await self.setup_managed_eventing_channel(
                    catalog_id, config.managed_eventing, conn=conn
                )
                # We don't save back here to avoid infinite loops, the caller (ConfigManager) just saved the config.
                # However, setup_managed_eventing_channel might update output fields (topic_path, etc.)
                # If they changed, we might need a way to update the DB without triggering the hook again.
                # For now, we assume these output fields are stable once established.
            else:
                logger.info(
                    f"Tearing down managed eventing for catalog '{catalog_id}'."
                )
                await self.teardown_managed_eventing_channel(
                    catalog_id, config.managed_eventing
                )

        # Handle custom subscriptions to external topics
        for sub in config.custom_subscriptions:
            if sub.enabled:
                logger.info(
                    f"Setting up external subscription '{sub.id}' for catalog '{catalog_id}'."
                )
                push_attributes = {
                    "subscription_id": sub.id,
                    "catalog_id": catalog_id,
                    "subscription_type": "custom",
                    "custom_subscription_id": sub.id,
                }
                await self.setup_push_subscription(
                    sub.topic_path, sub.subscription, custom_attributes=push_attributes
                )
            else:
                logger.info(
                    f"Tearing down external subscription '{sub.id}' for catalog '{catalog_id}'."
                )
                await self.teardown_external_subscription(sub.subscription)

    async def setup_managed_eventing_channel(
        self,
        catalog_id: str,
        managed_config: ManagedBucketEventing,
        bucket_name: Optional[str] = None,
        conn=None,
        context: Optional[LifecycleContext] = None,
    ) -> ManagedBucketEventing:
        """Creates/updates the full managed eventing pipeline: Topic -> GCS Notification -> Subscription.

        conn is optional. If provided it is used only for the bucket name lookup (a short read).
        All GCP API calls (gRPC) happen after the connection is released to avoid
        ConnectionDoesNotExistError from asyncpg closing idle connections.
        """
        project_id = self.get_project_id()
        if not project_id:
            raise RuntimeError(
                "Cannot setup managed eventing: GCP Project ID is not available."
            )
        # If managed_config.subscription is None, initialize it with a default ID.
        if managed_config.subscription is None:
            managed_config.subscription = PushSubscriptionConfig(
                subscription_id=self.generate_default_subscription_id(catalog_id)
            )

        # 1. Create the managed topic idempotently.
        publisher_client = self.get_publisher_client()
        topic_id = managed_config.topic_id or self.generate_default_topic_id(catalog_id)
        topic_path = publisher_client.topic_path(project_id, topic_id)

        # Retry on transient concurrency conflicts (Pub/Sub 409 "raced with
        # another user request" surfaces as Aborted when two concurrent callers
        # attempt to create or configure the same topic simultaneously — e.g.
        # the background lifecycle setup and an explicit setup_catalog_gcp_resources
        # call overlapping on a shared project).  AlreadyExists is terminal-ok
        # (idempotent adopt); everything else is re-raised after max attempts.
        for _attempt in range(1, _TOPIC_CREATE_MAX_ATTEMPTS + 1):
            try:
                logger.info(f"Attempting to create topic with path: '{topic_path}'")
                await run_in_thread(publisher_client.create_topic, name=topic_path)
                logger.info(f"Created managed Pub/Sub topic: {topic_path}")
                await log_info(catalog_id, "gcp_topic_created", f"Created managed Pub/Sub topic: {topic_path}")
                break
            except google_exceptions.AlreadyExists:
                logger.debug(f"Managed Pub/Sub topic '{topic_path}' already exists.")
                await log_info(catalog_id, "gcp_topic_adopted", f"Managed Pub/Sub topic already exists, adopting: {topic_path}")
                break
            except (
                Aborted,
                google_exceptions.Conflict,
                google_exceptions.ServiceUnavailable,
                google_exceptions.InternalServerError,
                google_exceptions.Unknown,
            ) as transient_err:
                if _attempt == _TOPIC_CREATE_MAX_ATTEMPTS:
                    logger.error(
                        f"create_topic for '{topic_path}' failed after {_attempt} "
                        f"attempts (last error: {transient_err})."
                    )
                    raise
                delay = 1.0 * (2 ** (_attempt - 1))
                logger.warning(
                    f"Transient error creating topic '{topic_path}' "
                    f"(concurrent create race): {transient_err}. "
                    f"Attempt {_attempt}/{_TOPIC_CREATE_MAX_ATTEMPTS}, "
                    f"retrying in {delay}s..."
                )
                await asyncio.sleep(delay)
        managed_config.topic_path = topic_path

        # Grant the GCS service account permission to publish to the newly created topic.
        storage_client = self.get_storage_client()
        gcs_service_account_email = await run_in_thread(
            storage_client.get_service_account_email, project=project_id
        )
        logger.info(
            f"Granting Pub/Sub Publisher role to GCS service account '{gcs_service_account_email}' on topic '{topic_path}'."
        )

        # Retry logic for IAM policy operations to handle transient "Socket closed" errors
        for attempt in range(1, 6):
            try:
                policy = await run_in_thread(
                    publisher_client.get_iam_policy, request={"resource": topic_path}
                )
                policy.bindings.add(
                    role="roles/pubsub.publisher",
                    members=[f"serviceAccount:{gcs_service_account_email}"],
                )
                await run_in_thread(
                    publisher_client.set_iam_policy,
                    request={"resource": topic_path, "policy": policy},
                )
                break
            except (
                google_exceptions.ServiceUnavailable,
                google_exceptions.InternalServerError,
                google_exceptions.Unknown,
                Aborted,
            ) as e:
                # "Socket closed" can appear as ServiceUnavailable or Unknown depending on the gRPC layer
                if attempt == 5:
                    logger.error(
                        f"Failed to update IAM policy for topic '{topic_path}' after {attempt} attempts: {e}"
                    )
                    raise

                delay = 1.0 * (2 ** (attempt - 1))
                logger.warning(
                    f"Retrying IAM policy update for topic '{topic_path}' due to error: {e}. Attempt {attempt}/5. Retrying in {delay}s..."
                )
                await asyncio.sleep(delay)

        # 2. Create GCS notifications for each configured prefix.
        if not bucket_name:
            config_service = self.get_config_service()
            if conn:
                cfg = await config_service.get_config(
                    GcpCatalogBucketConfig,
                    catalog_id=catalog_id,
                    ctx=DriverContext(db_resource=conn),
                )
                bucket_name = cfg.bucket_name
            else:
                async with managed_transaction(self.engine) as legacy_conn:
                    cfg = await config_service.get_config(
                        GcpCatalogBucketConfig,
                        catalog_id=catalog_id,
                        ctx=DriverContext(db_resource=legacy_conn),
                    )
                    bucket_name = cfg.bucket_name
        if not bucket_name:
            raise RuntimeError(
                f"Cannot setup GCS notification: Bucket for catalog '{catalog_id}' does not exist."
            )

        bucket = storage_client.bucket(bucket_name)

        # Listing notifications can briefly return 404 if the bucket has just been
        # created or was externally deleted. We treat NotFound as recoverable:
        # ensure the bucket exists (via BucketService), wait for readiness, then retry with backoff.
        async def _list_notifications_with_retry(
            bucket_obj,
            bucket_name_str,
            max_retries: int = 8,
            initial_delay: float = 0.5,
        ):
            """
            Retries listing notifications with exponential backoff to handle GCS eventual consistency.
            Even after a bucket exists, it may not be immediately ready for all operations.
            """

            def _list_notifications_sync():
                return list(bucket_obj.list_notifications())

            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return await run_in_thread(_list_notifications_sync)
                except google_exceptions.NotFound as e:
                    last_exception = e
                    if attempt < max_retries:
                        # Exponential backoff: 0.5s, 1s, 2s, 4s, 8s, 16s, 32s, 64s (max ~127s total)
                        delay = initial_delay * (2 ** (attempt - 1))
                        logger.debug(
                            f"Attempt {attempt}/{max_retries}: Bucket '{bucket_name_str}' not ready for listing notifications. "
                            f"Retrying in {delay}s..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.warning(
                            f"Failed to list notifications after {max_retries} attempts. "
                            f"Bucket '{bucket_name_str}' may not be fully ready yet."
                        )
                        raise
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception

        try:
            existing_notifications = await _list_notifications_with_retry(
                bucket, bucket_name
            )
        except google_exceptions.NotFound as e:
            # Attempt a repair: ensure the bucket exists and is ready, then retry with backoff.
            logger.warning(
                f"GCS bucket '{bucket_name}' for catalog '{catalog_id}' not found when listing notifications. "
                f"Attempting to repair by ensuring bucket existence and retrying. Error: {e}"
            )

            bucket_manager = self.get_bucket_service()
            repaired_bucket_name = (
                await bucket_manager.ensure_storage_for_catalog(
                    catalog_id, conn=conn
                )
            )
            if not repaired_bucket_name:
                logger.error(
                    f"Failed to repair missing bucket for catalog '{catalog_id}'. "
                    f"Original bucket name was '{bucket_name}'."
                )
                raise RuntimeError(
                    f"Cannot setup GCS notification: Bucket for catalog '{catalog_id}' "
                    f"does not exist and could not be recreated."
                ) from e

            # If the bucket name changed, rebuild the bucket handle.
            if repaired_bucket_name != bucket_name:
                logger.info(
                    f"Bucket name for catalog '{catalog_id}' changed from '{bucket_name}' "
                    f"to '{repaired_bucket_name}' during repair."
                )
                bucket_name = repaired_bucket_name
                bucket = storage_client.bucket(bucket_name)

            # Wait for the bucket to be fully ready/visible.
            # Note: Even if wait_for_bucket_ready returns True, the bucket might not be ready
            # for all operations (like listing notifications) due to eventual consistency.
            # We'll rely on the retry logic below to handle this.
            ready = await bucket_manager.wait_for_bucket_ready(bucket_name)
            if not ready:
                logger.warning(
                    f"Repaired bucket '{bucket_name}' for catalog '{catalog_id}' did not report as ready "
                    f"within the expected time window, but will attempt to list notifications with retries."
                )
            else:
                # Even if the bucket reports as ready, give it a moment for eventual consistency
                # before attempting to list notifications
                await asyncio.sleep(0.5)

            # Retry listing notifications with backoff after repair.
            # The retry logic will handle eventual consistency even if wait_for_bucket_ready
            # returned False or if the bucket exists but isn't ready for listing notifications yet.
            try:
                existing_notifications = await _list_notifications_with_retry(
                    bucket, bucket_name
                )
            except google_exceptions.NotFound as e2:
                logger.error(
                    f"Bucket '{bucket_name}' for catalog '{catalog_id}' is still reported as missing "
                    f"after repair and retry attempts. Project: '{project_id}'. Error: {e2}"
                )

                raise RuntimeError(
                    f"Cannot setup GCS notification: Bucket '{bucket_name}' for catalog '{catalog_id}' "
                    f"is missing even after repair and retry attempts."
                ) from e2

        event_types = managed_config.event_types or [
            GcsNotificationEventType.OBJECT_FINALIZE
        ]
        managed_config.gcs_notification_ids = []

        # We need to ensure each prefix in managed_config.blob_name_prefixes has a notification.
        prefixes_to_setup = managed_config.blob_name_prefixes or [
            None
        ]  # None means entire bucket

        # Stamp catalog_id (and where applicable, collection_id) on every
        # message GCS publishes for this notification. The Pub/Sub HTTP
        # push handler reads message.attributes via the get_attr() helper
        # in bucket_service, so this gives the consumer a third independent
        # source for catalog_id (after pushConfig HTTP headers + manually
        # injected attributes), robust against subscription-config drift
        # / orphaned notifications inherited from a previous deployment.
        notification_attributes: Dict[str, str] = {
            "catalog_id": catalog_id,
            "subscription_type": "managed",
        }

        for prefix in prefixes_to_setup:
            # Per-prefix attributes: the catalog_id is constant; the
            # collection_id is derivable from the prefix when it points at
            # collections/<collection_id>/. Stamping it pre-empts the
            # path-parse fallback in handle_gcs_notification for the common
            # collection-tier case.
            prefix_attributes = dict(notification_attributes)
            if prefix and prefix.startswith(f"{bucket_tool.COLLECTIONS_FOLDER}/"):
                # prefix shape: "collections/<collection_id>/" (trailing slash optional)
                tail = prefix[len(bucket_tool.COLLECTIONS_FOLDER) + 1 :].rstrip("/")
                if tail and "/" not in tail:
                    prefix_attributes["collection_id"] = tail

            match_found = False
            for notif in (existing_notifications or []):
                # topic_name in GCS notification is the short ID, not full path
                if notif.topic_name == topic_id and notif.topic_project == project_id:
                    existing_attrs = dict(notif.custom_attributes or {})
                    if (
                        notif.blob_name_prefix == prefix
                        and set(notif.event_types or []) == set(event_types)
                        and notif.payload_format == managed_config.payload_format
                        and existing_attrs == prefix_attributes
                    ):
                        managed_config.gcs_notification_ids.append(
                            notif.notification_id
                        )
                        logger.info(
                            f"Existing matching GCS notification '{notif.notification_id}' found for prefix '{prefix}' on bucket '{bucket_name}'."
                        )
                        match_found = True
                        break

            if not match_found:
                notification = bucket.notification(
                    topic_name=topic_id,
                    topic_project=project_id,
                    payload_format=managed_config.payload_format,
                    event_types=event_types,
                    blob_name_prefix=prefix,
                    custom_attributes=prefix_attributes,
                )

                await run_in_thread(notification.create)
                managed_config.gcs_notification_ids.append(notification.notification_id or "")
                logger.info(
                    f"Successfully created GCS notification "
                    f"'{notification.notification_id}' for prefix '{prefix}' on "
                    f"bucket '{bucket_name}' with attributes "
                    f"{list(prefix_attributes.keys())}."
                )
                await log_info(
                    catalog_id,
                    "gcp_gcs_notification_created",
                    f"Created GCS notification '{notification.notification_id}' for prefix '{prefix}' on bucket '{bucket_name}'.",
                )

        managed_config.bucket_id = bucket_name

        # 3. Create the push subscription to the managed topic.
        # If we hit a subscription clash (orphan from a prior teardown that
        # leaked the subscription), roll back the resources we just created
        # in steps 1+2 so the bucket isn't left half-wired. We leave the
        # bucket itself alone — it may pre-exist with user data.
        push_attributes = {
            "subscription_id": managed_config.subscription.subscription_id,
            "catalog_id": catalog_id,
            "subscription_type": "managed",
        }
        try:
            updated_subscription = await self.setup_push_subscription(
                topic_path, managed_config.subscription, custom_attributes=push_attributes
            )
        except OrphanSubscriptionClash as clash:
            logger.error(
                f"Subscription clash provisioning eventing for catalog '{catalog_id}'. "
                f"Rolling back just-created topic+notifications. Details:\n{clash}"
            )
            await self._rollback_eventing_resources(
                catalog_id=catalog_id,
                topic_path=topic_path,
                bucket_name=bucket_name,
                notification_ids=list(managed_config.gcs_notification_ids),
            )
            raise
        managed_config.subscription = updated_subscription

        return managed_config

    async def _rollback_eventing_resources(
        self,
        *,
        catalog_id: str,
        topic_path: str,
        bucket_name: Optional[str],
        notification_ids: list,
    ) -> None:
        """Best-effort teardown of resources created in ``setup_managed_eventing_channel``
        when downstream steps fail (e.g. ``OrphanSubscriptionClash``).

        Each delete is independent — a failure in one does not skip the next.
        The bucket is intentionally NOT touched (may pre-exist with user data).
        """
        # GCS notifications.
        if bucket_name and notification_ids:
            for notif_id in notification_ids:
                try:
                    await self.get_bucket_service().teardown_gcs_notification(
                        bucket_name, notif_id
                    )
                    logger.info(
                        f"Rollback: deleted GCS notification '{notif_id}' on bucket '{bucket_name}'."
                    )
                except Exception as e:  # noqa: BLE001 — never block rollback
                    logger.error(
                        f"Rollback: failed to delete GCS notification '{notif_id}' on bucket '{bucket_name}': {e}",
                        exc_info=True,
                    )

        # Topic. Subscription is deliberately left alone — it is the conflicting
        # orphan and operator action is required to remove it (the recovery
        # command is included in the OrphanSubscriptionClash message).
        try:
            await run_in_thread(
                self.get_publisher_client().delete_topic,
                request={"topic": topic_path},
            )
            logger.info(f"Rollback: deleted topic '{topic_path}' for catalog '{catalog_id}'.")
        except google_exceptions.NotFound:
            pass
        except Exception as e:  # noqa: BLE001 — never block rollback
            logger.error(
                f"Rollback: failed to delete topic '{topic_path}' for catalog '{catalog_id}': {e}",
                exc_info=True,
            )

    async def setup_push_subscription(
        self,
        topic_path: str,
        sub_config: PushSubscriptionConfig,
        custom_attributes: Optional[Dict[str, str]] = None,
    ) -> PushSubscriptionConfig:
        """Creates a push subscription to a given topic. Idempotent."""
        project_id = self.get_project_id()
        region = self.get_region()
        if not project_id or not region:
            raise RuntimeError(
                "Cannot determine self URL for push subscription: Project ID or Region could not be determined."
            )

        base_url = await self.get_self_url()
        push_endpoint = f"{base_url}/gcp/events/pubsub-push"

        # Ensure custom_attributes is not None for logging and usage
        attributes = custom_attributes or {}

        # Pub/Sub requires HTTPS for push endpoints. In local/test environments
        # where HTTPS is not available, we skip the push configuration to avoid 400 errors.
        if not push_endpoint.startswith("https://"):
            logger.warning(
                f"Push endpoint '{push_endpoint}' does not use HTTPS. Skipping push configuration as it is required by GCP Pub/Sub."
            )
            push_config = None
        else:
            # The custom attributes must be attached to the parent PushConfig object,
            # not the OidcToken object, for them to be included in the push message.
            oidc_token_config = pubsub_v1.types.PushConfig.OidcToken(  # type: ignore[attr-defined]
                service_account_email=self.get_account_email(),
                audience=PUBSUB_JWT_AUDIENCE,
            )

            # For compatibility with some library versions, attributes must be placed
            # on the PushConfig. This will send them as HTTP headers.
            push_config = pubsub_v1.types.PushConfig(  # type: ignore[attr-defined]
                push_endpoint=push_endpoint,
                oidc_token=oidc_token_config,
                attributes=attributes,
            )

        subscriber_client = self.get_subscriber_client()
        subscription_path = subscriber_client.subscription_path(
            project_id, sub_config.subscription_id
        )

        subscription_args = {
            "name": subscription_path,
            "topic": topic_path,
            "push_config": push_config,
            "ack_deadline_seconds": sub_config.ack_deadline_seconds,
            # "retain_acked_messages": sub_config.retain_acked_messages,
            # "enable_message_ordering": sub_config.enable_message_ordering,
            # "filter": sub_config.filter,
            # "enable_exactly_once_delivery": sub_config.enable_exactly_once_delivery
        }

        # Message retention duration
        # retention_duration = pubsub_v1.types.Duration()
        # retention_duration.seconds = sub_config.message_retention_duration_days * 24 * 60 * 60
        # subscription_args["message_retention_duration"] = retention_duration

        # Dead letter policy
        # if sub_config.dead_letter_policy:
        #     subscription_args["dead_letter_policy"] = pubsub_v1.types.DeadLetterPolicy(
        #         dead_letter_topic=sub_config.dead_letter_policy.dead_letter_topic,
        #         max_delivery_attempts=sub_config.dead_letter_policy.max_delivery_attempts,
        #     )

        # # Retry policy (exponential backoff)
        # if sub_config.retry_policy:
        #     min_backoff = pubsub_v1.types.Duration(seconds=sub_config.retry_policy.minimum_backoff_seconds)
        #     max_backoff = pubsub_v1.types.Duration(seconds=sub_config.retry_policy.maximum_backoff_seconds)
        #     subscription_args["retry_policy"] = pubsub_v1.types.RetryPolicy(
        #         minimum_backoff=min_backoff,
        #         maximum_backoff=max_backoff,
        #     )

        # # Expiration policy (TTL)
        # if sub_config.expiration_policy:
        #     ttl_duration = pubsub_v1.types.Duration(seconds=sub_config.expiration_policy.ttl_days * 24 * 60 * 60)
        #     subscription_args["expiration_policy"] = pubsub_v1.types.ExpirationPolicy(ttl=ttl_duration)

        try:
            # Run blocking create_subscription via the shared concurrency
            # backend. Binding a subscription requires
            # ``pubsub.topics.attachSubscription`` on the topic; immediately
            # after the topic and its IAM policy are created that permission
            # can transiently return 403 PERMISSION_DENIED until the policy
            # propagates (eventual consistency). Retry with bounded exponential
            # backoff so a propagation race self-heals instead of failing — and
            # dead-lettering — the catalog provisioning task. Mirrors the
            # transient-error retry around the topic IAM-policy update above.
            for attempt in range(1, _SUBSCRIPTION_CREATE_MAX_ATTEMPTS + 1):
                try:
                    await run_in_thread(
                        subscriber_client.create_subscription, **subscription_args
                    )
                    break
                except (
                    google_exceptions.PermissionDenied,
                    google_exceptions.ServiceUnavailable,
                    google_exceptions.InternalServerError,
                    google_exceptions.Unknown,
                    Aborted,
                ) as transient_err:
                    if attempt == _SUBSCRIPTION_CREATE_MAX_ATTEMPTS:
                        logger.error(
                            f"create_subscription for '{subscription_path}' "
                            f"failed after {attempt} attempts "
                            f"(last error: {transient_err})."
                        )
                        raise
                    delay = 1.0 * (2 ** (attempt - 1))
                    logger.warning(
                        f"Transient error creating subscription "
                        f"'{subscription_path}' (commonly attachSubscription IAM "
                        f"not yet propagated): {transient_err}. "
                        f"Attempt {attempt}/{_SUBSCRIPTION_CREATE_MAX_ATTEMPTS}, "
                        f"retrying in {delay}s..."
                    )
                    await asyncio.sleep(delay)
            logger.info(
                f"Created Pub/Sub push subscription '{subscription_path}' to endpoint '{push_endpoint}' with attributes {list(attributes.keys())}."
            )
            _sub_catalog_id = attributes.get("catalog_id")
            if _sub_catalog_id:
                await log_info(
                    _sub_catalog_id,
                    "gcp_subscription_created",
                    f"Created Pub/Sub push subscription '{subscription_path}'.",
                )
        except google_exceptions.AlreadyExists as already_exists_err:
            # Pub/Sub binds subscription→topic immutably. Before refreshing
            # push_config, verify the existing subscription is bound to the
            # topic we expect; otherwise modify_push_config would silently
            # succeed while messages on the new topic still have no subscriber.
            # Pub/Sub returns ``existing.topic == "_deleted-topic_"`` when the
            # bound topic has been tombstoned.
            try:
                existing = await run_in_thread(
                    subscriber_client.get_subscription,
                    request={"subscription": subscription_path},
                )
            except Exception as e:
                logger.error(
                    f"Failed to inspect existing subscription '{subscription_path}' "
                    f"after AlreadyExists: {e}"
                )
                raise

            if existing.topic != topic_path:
                raise OrphanSubscriptionClash(
                    sub_path=subscription_path,
                    bound_to=existing.topic or "<unknown>",
                    expected=topic_path,
                    project_id=project_id,
                ) from already_exists_err

            logger.debug(
                f"Pub/Sub subscription '{subscription_path}' already exists and is "
                f"bound to the expected topic. Updating PushConfig to ensure "
                f"attributes are current."
            )
            try:
                await run_in_thread(
                    subscriber_client.modify_push_config,
                    request={
                        "subscription": subscription_path,
                        "push_config": push_config,
                    },
                )
                logger.info(
                    f"Successfully updated PushConfig (attributes: {list(attributes.keys())}) for existing subscription '{subscription_path}'."
                )
                _sub_catalog_id = attributes.get("catalog_id")
                if _sub_catalog_id:
                    await log_info(
                        _sub_catalog_id,
                        "gcp_subscription_adopted",
                        f"Pub/Sub subscription already exists, push config refreshed: '{subscription_path}'.",
                    )
            except Exception as e:
                logger.error(
                    f"Failed to update PushConfig for existing subscription '{subscription_path}': {e}"
                )

        sub_config.subscription_path = subscription_path
        return sub_config

    async def teardown_managed_eventing_channel(
        self, catalog_id: str, managed_config: ManagedBucketEventing
    ):
        """Tears down the full managed eventing pipeline."""
        # 1. Delete all GCS notification resources from the bucket.
        if managed_config.gcs_notification_ids:
            bucket_name = await self.get_bucket_service().get_storage_identifier(
                catalog_id
            )
            if bucket_name:
                for notif_id in managed_config.gcs_notification_ids:
                    await self.get_bucket_service().teardown_gcs_notification(
                        bucket_name, notif_id
                    )
            else:
                logger.warning(
                    f"Could not determine bucket name for catalog '{catalog_id}'. Skipping GCS notification teardown."
                )

        # 2. Delete the managed subscription FIRST. Deleting a Pub/Sub topic
        # does NOT cascade to its subscriptions — they survive as orphans bound
        # to a tombstoned topic, and since subscription→topic binding is
        # immutable in Pub/Sub they cannot be re-bound by a future re-provision.
        # Without explicit teardown here, a later catalog re-create would hit
        # AlreadyExists in setup_push_subscription and either silently keep the
        # orphan or now raise OrphanSubscriptionClash.
        if managed_config.subscription and managed_config.subscription.subscription_path:
            subscriber_client = self.get_subscriber_client()
            try:
                await run_in_thread(
                    subscriber_client.delete_subscription,
                    request={"subscription": managed_config.subscription.subscription_path},
                )
                logger.info(
                    f"Deleted managed Pub/Sub subscription: {managed_config.subscription.subscription_path}"
                )
                await log_info(
                    catalog_id,
                    "gcp_subscription_deleted",
                    f"Deleted managed Pub/Sub subscription: {managed_config.subscription.subscription_path}",
                )
            except google_exceptions.NotFound:
                logger.debug(
                    f"Managed Pub/Sub subscription '{managed_config.subscription.subscription_path}' not found. Nothing to delete."
                )

        # 3. Delete the managed topic.
        if managed_config.topic_path:
            publisher_client = self.get_publisher_client()
            try:
                await run_in_thread(
                    publisher_client.delete_topic,
                    request={"topic": managed_config.topic_path},
                )
                logger.info(
                    f"Deleted managed Pub/Sub topic: {managed_config.topic_path}"
                )
                await log_info(
                    catalog_id,
                    "gcp_topic_deleted",
                    f"Deleted managed Pub/Sub topic: {managed_config.topic_path}",
                )
            except google_exceptions.NotFound:
                logger.debug(
                    f"Managed Pub/Sub topic '{managed_config.topic_path}' not found. Nothing to delete."
                )

            # # Dispatch event: Managed Eventing Teardown
            # await events_module.create_event(
            #     event_type=GcpEventType.MANAGED_EVENTING_TEARDOWN.value,
            #     payload={"catalog_id": catalog_id, "topic_path": managed_config.topic_path})

    async def teardown_external_subscription(self, sub_config: PushSubscriptionConfig):
        """
        Deletes a single external Pub/Sub subscription.
        """
        if sub_config.subscription_path:
            subscriber_client = self.get_subscriber_client()
            try:
                # Run blocking delete_subscription via the shared concurrency backend
                await run_in_thread(
                    subscriber_client.delete_subscription,
                    request={"subscription": sub_config.subscription_path},
                )
                logger.info(
                    f"Deleted Pub/Sub subscription: {sub_config.subscription_path}"
                )
            except google_exceptions.NotFound:
                logger.debug(
                    f"Pub/Sub subscription '{sub_config.subscription_path}' not found. Nothing to delete."
                )
            except Exception as e:
                logger.error(
                    f"Failed to delete Pub/Sub subscription '{sub_config.subscription_path}': {e}",
                    exc_info=True,
                )

    async def set_eventing_config(
        self, catalog_id: str, config: GcpEventingConfig, conn=None
    ) -> GcpEventingConfig:
        """Persists the eventing configuration for a catalog."""
        config_service = self.get_config_service()
        # Pass the connection to reuse the transaction
        await config_service.set_config(
            GcpEventingConfig, config, catalog_id=catalog_id, ctx=DriverContext(db_resource=conn
        ))
        # Re-fetch to confirm and return the validated model.
        # Passing the class (not a string id) narrows the return type to
        # GcpEventingConfig without a cast().
        return await config_service.get_config(
            GcpEventingConfig, catalog_id, ctx=DriverContext(db_resource=conn
        ))

    async def setup_catalog_eventing(self, catalog_id: str) -> Tuple[str, Any]:
        """EventingProtocol: Sets up GCP eventing for a catalog."""
        # For GCP, this ensures both bucket and eventing are ready
        return await self.setup_catalog_gcp_resources(catalog_id)

    async def teardown_catalog_eventing(
        self, catalog_id: str, config: Optional[Any] = None
    ) -> None:
        """EventingProtocol: Tears down GCP eventing for a catalog."""
        if config and hasattr(config, "managed_eventing") and config.managed_eventing:
            await self.teardown_managed_eventing_channel(
                catalog_id, config.managed_eventing
            )

        if config is None:
            # Force cleanup of deterministic default resources (topics/subscriptions)
            # This is useful during hard deletion where config might be already missing.
            project_id = self.get_project_id()
            if not project_id:
                return

            # Default Topic (deleting topic deletes its subscriptions).
            # topic_id is derived from the internal catalog_id (ds-c_<suffix>-events).
            # Catalogs provisioned before the id-model redesign used a schema-derived
            # name (ds-s_<suffix>-events).  A non-clean-break upgrade must also attempt
            # deletion of that legacy name here; this deploy is clean-break so it is
            # not needed.
            topic_id = self.generate_default_topic_id(catalog_id)
            topic_path = self.get_publisher_client().topic_path(project_id, topic_id)
            try:
                await run_in_thread(
                    self.get_publisher_client().delete_topic,
                    request={"topic": topic_path},
                )
                logger.info(f"Forcefully deleted default topic: {topic_path}")
                await log_info(
                    catalog_id,
                    "gcp_topic_deleted",
                    f"Forcefully deleted default Pub/Sub topic: {topic_path}",
                )
            except google_exceptions.NotFound:
                pass
            except Exception as e:
                logger.error(
                    f"Failed to force delete default topic '{topic_path}': {e}"
                )

            # Default Subscription (if it exists separately, though deleting topic should handle it)
            sub_id = self.generate_default_subscription_id(catalog_id)
            sub_path = self.get_subscriber_client().subscription_path(
                project_id, sub_id
            )
            try:
                await run_in_thread(
                    self.get_subscriber_client().delete_subscription,
                    request={"subscription": sub_path},
                )
                logger.info(f"Forcefully deleted default subscription: {sub_path}")
                await log_info(
                    catalog_id,
                    "gcp_subscription_deleted",
                    f"Forcefully deleted default Pub/Sub subscription: {sub_path}",
                )
            except google_exceptions.NotFound:
                pass
            except Exception as e:
                logger.error(
                    f"Failed to force delete default subscription '{sub_path}': {e}"
                )

    async def get_eventing_config(
        self,
        catalog_id: str,
        conn=None,
        context: Optional[LifecycleContext] = None,
    ) -> Optional[GcpEventingConfig]:
        """Internal helper to fetch and parse a catalog's eventing config."""
        config_service = self.get_config_service()
        # Pass the connection to reuse the transaction if provided
        config = await config_service.get_config(
            GcpEventingConfig,
            catalog_id,
            ctx=DriverContext(db_resource=conn),
            config_snapshot=context.config if context else None,
        )

        if isinstance(config, GcpEventingConfig):
            return config
        if isinstance(config, dict):
            return GcpEventingConfig.model_validate(config)
        return None
