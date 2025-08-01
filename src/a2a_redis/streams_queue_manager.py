"""Redis Streams queue manager implementation for the A2A Python SDK.

This module provides a Redis Streams-based QueueManager implementation for persistent,
reliable event delivery with consumer groups, acknowledgments, and replay capability.

For real-time, fire-and-forget scenarios, consider RedisPubSubQueueManager instead.
"""

from typing import Dict, Optional

import redis.asyncio as redis
from a2a.server.events.queue_manager import QueueManager

from .event_queue_protocol import EventQueueProtocol
from .streams_queue import RedisStreamsEventQueue
from .streams_consumer_strategy import ConsumerGroupConfig


class RedisStreamsQueueManager(QueueManager):
    """Redis Streams-backed QueueManager for persistent, reliable event delivery.

    Provides guaranteed delivery with consumer groups, acknowledgments, and replay
    capability. See README.md for detailed use cases and trade-offs.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        prefix: str = "stream:",
        consumer_config: Optional[ConsumerGroupConfig] = None,
    ):
        """Initialize the Redis Streams queue manager.

        Args:
            redis_client: Redis client instance
            prefix: Key prefix for stream storage
            consumer_config: Consumer group configuration
        """
        self.redis = redis_client
        self.prefix = prefix
        self.consumer_config = consumer_config or ConsumerGroupConfig()
        self._queues: Dict[str, EventQueueProtocol] = {}

    def _create_queue(self, task_id: str) -> EventQueueProtocol:
        """Create a Redis Streams queue instance for a task."""
        return RedisStreamsEventQueue(
            self.redis, task_id, self.prefix, self.consumer_config
        )

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
