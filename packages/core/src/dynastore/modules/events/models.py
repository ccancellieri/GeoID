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

# dynastore/modules/events/models.py

from pydantic import BaseModel, Field, HttpUrl, SecretStr, ConfigDict
from typing import Optional, Dict, Any, List
from uuid import UUID, uuid4
from datetime import datetime, timezone
from enum import Enum

# --- Constants ---
API_KEY_NAME = "X-DynaStore-Event-Key"

# --- Event Models ---

class EventStatusEnum(str, Enum):
    """Defines the lifecycle status of an event."""
    PENDING = "PENDING"
    LOCKED = "LOCKED"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"

class EventChannel(str, Enum):
    """Defines the notification channels."""
    EVENTS = "dynastore_events_channel"

class EventBase(BaseModel):
    """Base model for an event, containing the core data."""
    event_type: str = Field(..., description="A unique name for the event, e.g., 'collection.updated'.")
    payload: Dict[str, Any] = Field(default_factory=dict, description="The JSON data payload of the event.")
    
    model_config = ConfigDict(from_attributes=True)

class EventCreate(EventBase):
    """Model used to create a new event."""
    pass

class Event(EventBase):
    """The full event model, representing a row in the database."""
    event_id: int = Field(..., description="Primary key, a unique, ordered event identifier.")
    status: EventStatusEnum = Field(..., description="The current processing status of the event.")
    created_at: datetime = Field(..., description="Timestamp when the event was created.")
    locked_until: datetime = Field(..., description="For consumers: the time until which this event is locked for processing.")
    retry_count: int = Field(..., description="The number of times this event has failed processing.")
    error_message: Optional[str] = Field(None, description="The last error message if processing failed.")

class EventBatch(BaseModel):
    """A model representing a batch of consumed events for processing."""
    events: List[Event]
    lease_id: UUID = Field(default_factory=uuid4, description="A unique identifier for this specific batch, used for ACK/NACK operations.")

# --- Subscription Models ---

class AuthMethod(str, Enum):
    """Defines the authentication method for the webhook."""
    NONE = "NONE"
    API_KEY = "API_KEY" # Simple shared secret
    OIDC = "OIDC"
    OAUTH2_CLIENT_CREDENTIALS = "OAUTH2_CLIENT_CREDENTIALS"

class AuthConfigBase(BaseModel):
    auth_method: AuthMethod = Field(..., description="The authentication method to use.")

class AuthConfigNone(AuthConfigBase):
    auth_method: AuthMethod = AuthMethod.NONE

class AuthConfigAPIKey(AuthConfigBase):
    """Configuration for simple shared secret API Key auth."""
    auth_method: AuthMethod = AuthMethod.API_KEY
    header_name: str = Field("X-API-Key", description="The HTTP header to send the key in.")

class AuthConfigOIDC(AuthConfigBase):
    auth_method: AuthMethod = AuthMethod.OIDC
    audience: Optional[str] = Field(None, description="The audience to request for the OIDC token.")

class AuthConfigOAuth2(AuthConfigBase):
    auth_method: AuthMethod = AuthMethod.OAUTH2_CLIENT_CREDENTIALS
    token_url: HttpUrl = Field(..., description="The URL of the OAuth2 token endpoint.")
    client_id: str = Field(..., description="The Client ID.")
    client_secret: SecretStr = Field(..., description="The Client Secret.")
    scopes: Optional[List[str]] = Field(None, description="A list of scopes to request.")

# A union of all possible auth configs
AuthConfiguration = AuthConfigNone | AuthConfigAPIKey | AuthConfigOIDC | AuthConfigOAuth2

class EventSubscriptionBase(BaseModel):
    """Base model for an event subscription."""
    subscriber_name: str = Field(..., description="A unique name for the consuming service, e.g., 'gcp-module-worker'.")
    event_type: str = Field(..., description="The event type to subscribe to, e.g., 'catalog.hard_deletion'.")
    webhook_url: HttpUrl = Field(..., description="The HTTPS endpoint to which the event payload will be POSTed.")
    auth_config: AuthConfiguration = Field(
        ..., 
        description="The authentication configuration for the webhook."
    )
    
    model_config = ConfigDict(from_attributes=True)

class EventSubscriptionCreate(EventSubscriptionBase):
    """Model used to create a new subscription."""
    pass

class EventSubscription(EventSubscriptionBase):
    """The full subscription model, representing a row in the database."""
    subscription_id: UUID = Field(..., description="The unique identifier for this subscription.")

