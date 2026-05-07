#    Copyright 2025 FAO
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

#    dynastore/modules/gcp/models.py

from typing import Optional, Literal, List, Any, Dict, Annotated
from pydantic import BaseModel, Field, model_validator, AliasChoices, ConfigDict
from datetime import date


from enum import Enum, StrEnum

# --- GCP Specific Event Types ---
class GcpEventType(StrEnum):
    BUCKET_CREATED = "gcp.bucket.created"
    BUCKET_DELETED = "gcp.bucket.deleted" # For future use
    OBJECT_UPLOADED = "gcp.object.uploaded" # Triggered by Pub/Sub push
    MANAGED_EVENTING_SETUP = "gcp.managed_eventing.setup"
    MANAGED_EVENTING_TEARDOWN = "gcp.managed_eventing.teardown"


# This should be configured via environment variables for security.
# It must match the 'audience' you set when creating the Pub/Sub push subscription.
PUBSUB_JWT_AUDIENCE = "dynastore-gcp-webhook"

class DeadLetterPolicy(BaseModel):
    """Configuration for a subscription's dead-letter policy."""
    dead_letter_topic: str = Field(..., description="The full resource path of the Pub/Sub topic to use as a dead-letter topic.")
    max_delivery_attempts: int = Field(5, ge=5, le=100, description="The maximum number of delivery attempts for a message before it is sent to the dead-letter topic.")


class RetryPolicy(BaseModel):
    """
    Configuration for a subscription's retry policy. If not provided, the subscription
    uses the default "retry immediately" policy.
    """
    minimum_backoff_seconds: int = Field(..., ge=1, description="The minimum delay in seconds between consecutive redelivery attempts.")
    maximum_backoff_seconds: int = Field(..., le=600, description="The maximum delay in seconds between consecutive redelivery attempts.")

    @model_validator(mode='after')
    def check_backoff_values(self) -> 'RetryPolicy':
        if self.minimum_backoff_seconds >= self.maximum_backoff_seconds:
            raise ValueError("maximum_backoff_seconds must be greater than minimum_backoff_seconds")
        return self

class ExpirationPolicy(BaseModel):
    """Configuration for a subscription's expiration policy (TTL)."""
    ttl_days: int = Field(365, ge=1, description="The subscription will be deleted after this many days of inactivity.")

class PushSubscriptionConfig(BaseModel):
    """Defines the configuration for a Pub/Sub push subscription."""
    subscription_id: str = Field(..., description="The short, user-defined ID for the Pub/Sub subscription.")
    # --- Output fields managed by the system ---
    subscription_path: Optional[str] = Field(default=None, description="The full resource path of the created Pub/Sub subscription. This is an output field managed by the system.")
    # --- User-configurable subscription settings ---
    ack_deadline_seconds: int = Field(default=10, ge=10, le=600, description="The acknowledgment deadline for messages in seconds.")
    message_retention_duration_days: int = Field(default=7, ge=1, le=7, description="How long to retain unacknowledged messages in days.")
    retain_acked_messages: bool = Field(default=False, description="Whether to retain acknowledged messages.")
    enable_message_ordering: bool = Field(default=False, description="If true, messages with the same ordering key are delivered in order.")
    filter: Optional[str] = Field(default=None, description="A filter to apply to messages from the topic.")
    enable_exactly_once_delivery: bool = Field(default=False, description="If true, enables exactly-once delivery.")
    dead_letter_policy: Optional[DeadLetterPolicy] = Field(default=None, description="The dead-lettering configuration for this subscription.")
    retry_policy: Optional[RetryPolicy] = Field(default=None, description="The exponential backoff retry policy for this subscription.")
    expiration_policy: Optional[ExpirationPolicy] = Field(default=None, description="The time-to-live (TTL) or expiration policy for this subscription.")


class ExternalTopicSubscription(BaseModel):
    """
    Defines a subscription to an existing, external Pub/Sub topic.
    The system will only manage the Pub/Sub subscription, not the topic or the event source.
    """
    id: str = Field(..., description="A unique identifier for this notification configuration.")
    enabled: bool = Field(False, description="If true, create and enable the subscription.")
    topic_path: str = Field(..., description="The full resource path of the existing external Pub/Sub topic to subscribe to (e.g., 'projects/another-project/topics/external-events').")
    subscription: PushSubscriptionConfig = Field(..., description="Configuration for the push subscription to the external topic.")
    target_collection_id: Annotated[Optional[str], Field(None, description="The ID of the collection whose actions should be triggered by events from this subscription.")]

class GcpBucketDetails(BaseModel):
    """Response model for bucket details."""
    name: str
    id: str
    location: str
    project_number: int
    storage_class: str
    time_created: str
    updated: str

class LifecycleAction(BaseModel):
    """Defines an action to take on an object when a condition is met."""
    type: Literal["Delete", "SetStorageClass"] = Field(..., description="The type of action to take.")
    storage_class: Optional[Literal["NEARLINE", "COLDLINE", "ARCHIVE"]] = Field(None, description="The storage class to set. Required if type is 'SetStorageClass'.")

    @model_validator(mode='after')
    def check_storage_class_required(self) -> 'LifecycleAction':
        if self.type == "SetStorageClass" and not self.storage_class:
            raise ValueError("storage_class must be provided when action type is 'SetStorageClass'")
        return self

class LifecycleCondition(BaseModel):
    """Defines the condition under which a lifecycle rule's action is taken."""
    age: Optional[int] = Field(None, description="Age of an object in days. This condition is met when an object reaches the specified age.")
    created_before: Optional[date] = Field(None, description="This condition is met for objects created before this date.")
    num_newer_versions: Optional[int] = Field(None, description="For versioned buckets, this condition is met when an object has this many newer versions.")
    is_live: Optional[bool] = Field(None, description="For versioned buckets, this condition is met when an object is live (not archived).")


class LifecycleRule(BaseModel):
    """A single lifecycle management rule, consisting of an action and a condition."""
    action: LifecycleAction
    condition: LifecycleCondition