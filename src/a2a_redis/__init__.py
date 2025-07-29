"""Redis components for the Agent-to-Agent (A2A) Python SDK.

This package provides Redis-backed implementations of core A2A components:
- RedisTaskStore: Redis-backed task storage
- RedisQueueManager: Redis Streams-based event queue
- RedisPushNotificationConfigStore: Redis-backed push notification configuration storage
"""

from .push_notification_config_store import RedisPushNotificationConfigStore
from .queue_manager import RedisQueueManager
from .task_store import RedisJSONTaskStore, RedisTaskStore

__version__ = "0.1.0"

__all__ = [
    "RedisTaskStore",
    "RedisJSONTaskStore", 
    "RedisQueueManager",
    "RedisPushNotificationConfigStore",
]