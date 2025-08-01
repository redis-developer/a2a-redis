"""Redis Pub/Sub queue manager implementation for the A2A Python SDK.

This module provides a Redis Pub/Sub-based QueueManager implementation for real-time,
fire-and-forget event delivery with natural broadcasting patterns.

For reliable, persistent event processing, consider RedisStreamsQueueManager instead.
"""

from typing import Dict, Optional

import redis.asyncio as redis
from a2a.server.events.queue_manager import QueueManager

from .event_queue_protocol import EventQueueProtocol
from .pubsub_queue import RedisPubSubEventQueue


class RedisPubSubQueueManager(QueueManager):
    """Redis Pub/Sub-backed QueueManager for real-time, fire-and-forget event delivery.

    Provides immediate event broadcasting with minimal latency but no persistence
    or delivery guarantees. See README.md for detailed use cases and trade-offs.
    """

    def __init__(self, redis_client: redis.Redis, prefix: str = "pubsub:"):
        """Initialize the Redis Pub/Sub queue manager.

        Args:
            redis_client: Redis client instance
            prefix: Key prefix for pub/sub channels
        """
        self.redis = redis_client
        self.prefix = prefix
        self._queues: Dict[str, EventQueueProtocol] = {}

    def _create_queue(self, task_id: str) -> EventQueueProtocol:
        """Create a Redis Pub/Sub queue instance for a task."""
        return RedisPubSubEventQueue(self.redis, task_id, self.prefix)

    async def add(self, task_id: str, queue: EventQueueProtocol) -> None:  # type: ignore[override]
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

    async def create_or_tap(self, task_id: str) -> EventQueueProtocol:  # type: ignore[override]
        """Create or get existing queue for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier

        Returns:
            EventQueue instance for the task
        """
        if task_id not in self._queues:
            self._queues[task_id] = self._create_queue(task_id)
        return self._queues[task_id]

    async def get(self, task_id: str) -> Optional[EventQueueProtocol]:  # type: ignore[override]
        """Get existing queue for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier

        Returns:
            EventQueue instance or None if not found
        """
        return self._queues.get(task_id)

    async def tap(self, task_id: str) -> Optional[EventQueueProtocol]:  # type: ignore[override]
        """Create a tap of existing queue for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier

        Returns:
            EventQueue tap or None if queue doesn't exist
        """
        if task_id in self._queues:
            return self._queues[task_id].tap()
        return None
