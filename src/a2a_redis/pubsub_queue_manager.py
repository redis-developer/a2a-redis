"""Redis Pub/Sub queue manager implementation for the A2A Python SDK.

This module provides a Redis Pub/Sub-based QueueManager implementation for real-time,
fire-and-forget event delivery with natural broadcasting patterns.

For reliable, persistent event processing, consider RedisStreamsQueueManager instead.
"""

from typing import Dict, Optional

import redis
from a2a.server.events.queue_manager import QueueManager
from a2a.server.events.event_queue import EventQueue

from .pubsub_queue import RedisPubSubEventQueue


class RedisPubSubQueueManager(QueueManager):
    """Redis Pub/Sub-backed implementation of the A2A QueueManager interface.

    This queue manager uses Redis Pub/Sub for real-time, fire-and-forget event delivery
    with natural broadcasting patterns and minimal latency.

    **Key Features**:
    - **Real-time delivery**: Events delivered immediately to active subscribers
    - **No persistence**: Events not stored, only delivered to active consumers
    - **Fire-and-forget**: No acknowledgments or delivery guarantees
    - **Broadcasting**: All subscribers receive all events
    - **Low latency**: Minimal overhead for immediate delivery
    - **Minimal memory usage**: No storage of events

    **Use Cases**:
    - Live status updates and notifications
    - Real-time dashboard updates
    - System event broadcasting
    - Non-critical event distribution
    - Low-latency requirements
    - Simple fan-out scenarios

    **Not suitable for**:
    - Critical event processing requiring guarantees
    - Systems requiring event replay or audit trails
    - Offline-capable applications
    - Work queues requiring load balancing

    **Behavior Notes**:
    - Events published before subscribers are active will be lost
    - All subscribers receive all events (broadcast pattern)
    - No consumer groups or load balancing
    - Network partitions can cause message loss

    Example:
        # Basic pub/sub queue manager
        manager = RedisPubSubQueueManager(redis_client)

        # With custom channel prefix
        manager = RedisPubSubQueueManager(redis_client, prefix="notifications:")

        # Get a queue and publish events
        queue = await manager.create_or_tap("task_123")
        queue.enqueue_event(event_data)
    """

    def __init__(self, redis_client: redis.Redis, prefix: str = "pubsub:"):
        """Initialize the Redis Pub/Sub queue manager.

        Args:
            redis_client: Redis client instance
            prefix: Key prefix for pub/sub channels
        """
        self.redis = redis_client
        self.prefix = prefix
        self._queues: Dict[str, EventQueue] = {}

    def _create_queue(self, task_id: str) -> EventQueue:
        """Create a Redis Pub/Sub queue instance for a task."""
        return RedisPubSubEventQueue(self.redis, task_id, self.prefix)

    async def add(self, task_id: str, queue: EventQueue) -> None:
        """Add a queue for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier
            queue: EventQueue instance to add (ignored, we create our own)
        """
        # For Redis implementation, we create our own queue but this maintains interface
        self._queues[task_id] = self._create_queue(task_id)

    async def close(self, task_id: str) -> None:
        """Close a queue for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier
        """
        if task_id in self._queues:
            await self._queues[task_id].close()
            del self._queues[task_id]

    async def create_or_tap(self, task_id: str) -> EventQueue:
        """Create or get existing queue for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier

        Returns:
            EventQueue instance for the task
        """
        if task_id not in self._queues:
            self._queues[task_id] = self._create_queue(task_id)
        return self._queues[task_id]

    async def get(self, task_id: str) -> Optional[EventQueue]:
        """Get existing queue for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier

        Returns:
            EventQueue instance or None if not found
        """
        return self._queues.get(task_id)

    async def tap(self, task_id: str) -> Optional[EventQueue]:
        """Create a tap of existing queue for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier

        Returns:
            EventQueue tap or None if queue doesn't exist
        """
        if task_id in self._queues:
            return self._queues[task_id].tap()
        return None
