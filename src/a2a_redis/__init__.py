"""Redis components for the Agent-to-Agent (A2A) Python SDK.

This package provides Redis-backed implementations of core A2A components:

**Task Storage**:
- RedisTaskStore: Redis hash-backed task storage
- RedisJSONTaskStore: Redis JSON-backed task storage with native JSON support

**Queue Management**:
- RedisStreamsQueueManager: Persistent, reliable queues with consumer groups
- RedisPubSubQueueManager: Real-time, fire-and-forget broadcasting queues
- RedisPubSubEventQueue: Direct pub/sub event queue implementation
- RedisStreamsEventQueue: Direct streams event queue implementation

**Configuration**:
- RedisPushNotificationConfigStore: Push notification configuration storage
- ConsumerGroupConfig: Consumer group strategy configuration
- ConsumerGroupStrategy: Enumeration of consumer group strategies

Choose the appropriate queue manager based on your requirements:

**RedisStreamsQueueManager**:
- ✅ Persistent event storage with replay capability
- ✅ Guaranteed delivery with acknowledgments
- ✅ Consumer groups for load balancing
- ✅ Automatic failure recovery
- ❌ Higher memory usage
- Best for: Task queues, audit trails, reliable processing

**RedisPubSubQueueManager**:
- ✅ Real-time, low-latency delivery
- ✅ Minimal memory usage
- ✅ Natural broadcasting pattern
- ❌ No persistence (offline consumers miss events)
- ❌ No delivery guarantees
- Best for: Live notifications, real-time updates, system events
"""

from .push_notification_config_store import RedisPushNotificationConfigStore
from .streams_queue_manager import RedisStreamsQueueManager
from .streams_queue import RedisStreamsEventQueue
from .pubsub_queue_manager import RedisPubSubQueueManager
from .pubsub_queue import RedisPubSubEventQueue
from .task_store import RedisJSONTaskStore, RedisTaskStore
from .streams_consumer_strategy import ConsumerGroupStrategy, ConsumerGroupConfig
from .queue_types import QueueType
from .event_queue_protocol import EventQueueProtocol

__version__ = "0.1.0"

__all__ = [
    # Task storage
    "RedisTaskStore",
    "RedisJSONTaskStore",
    # Queue managers
    "RedisStreamsQueueManager",
    "RedisPubSubQueueManager",
    # Event queues (direct implementations)
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
