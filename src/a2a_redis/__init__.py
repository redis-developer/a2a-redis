"""Redis components for the Agent-to-Agent (A2A) Python SDK."""

from .push_notification_config_store import RedisPushNotificationConfigStore
from .streams_queue_manager import RedisStreamsQueueManager
from .streams_queue import RedisStreamsEventQueue
from .pubsub_queue_manager import RedisPubSubQueueManager
from .pubsub_queue import RedisPubSubEventQueue
from .task_store import RedisJSONTaskStore, RedisTaskStore
from .streams_consumer_strategy import ConsumerGroupStrategy, ConsumerGroupConfig
from .queue_types import QueueType
from .event_queue_protocol import EventQueueProtocol

__version__ = "0.1.1"

__all__ = [
    # Task storage
    "RedisTaskStore",
    "RedisJSONTaskStore",
    # Queue managers
    "RedisStreamsQueueManager",
    "RedisPubSubQueueManager",
    # Event queues
    "RedisStreamsEventQueue",
    "RedisPubSubEventQueue",
    # Configuration and utilities
    "RedisPushNotificationConfigStore",
    "ConsumerGroupStrategy",
    "ConsumerGroupConfig",
    "QueueType",
    # Protocols
    "EventQueueProtocol",
]
