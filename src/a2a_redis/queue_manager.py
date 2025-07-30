"""Redis-backed queue manager implementation for the A2A Python SDK."""

import json
from typing import Dict, Optional, Any, Union, cast

import redis
from a2a.server.events.queue_manager import QueueManager
from a2a.server.events.event_queue import EventQueue
from a2a.types import Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent


class RedisEventQueue(EventQueue):
    """Redis-backed implementation of EventQueue using Redis Lists."""

    def __init__(self, redis_client: redis.Redis, task_id: str, prefix: str = "queue:"):
        """Initialize Redis event queue.

        Args:
            redis_client: Redis client instance
            task_id: Task identifier this queue is for
            prefix: Key prefix for queue storage
        """
        self.redis = redis_client
        self.task_id = task_id
        self.prefix = prefix
        self._closed = False
        self._queue_key = f"{prefix}{task_id}"

    async def enqueue_event(
        self,
        event: Union[Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent],
    ) -> None:
        """Add an event to the queue."""
        if self._closed:
            raise RuntimeError("Cannot enqueue to closed queue")

        # Serialize event - convert to dict if it has model_dump, otherwise assume it's serializable
        if hasattr(event, "model_dump"):
            event_data = event.model_dump()
        else:
            event_data = event

        serialized_event = json.dumps(
            {"type": type(event).__name__, "data": event_data}
        )

        # Push to Redis list (LPUSH for FIFO with BRPOP)
        self.redis.lpush(self._queue_key, serialized_event)

    async def dequeue_event(
        self, no_wait: bool = False
    ) -> Union[Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent]:
        """Remove and return an event from the queue."""
        if self._closed:
            raise RuntimeError("Cannot dequeue from closed queue")

        result: Optional[bytes] = None

        if no_wait:
            # Non-blocking pop
            result = cast(Optional[bytes], self.redis.rpop(self._queue_key))  # type: ignore[misc]
            if result is None:
                raise RuntimeError("No events available")
        else:
            # Blocking pop with 1 second timeout
            brpop_result = cast(Any, self.redis.brpop([self._queue_key], timeout=1))  # type: ignore[misc]
            if brpop_result is None:
                raise RuntimeError("No events available")
            result = brpop_result[1]  # brpop returns (key, value)

        # Deserialize event
        if result is None:
            raise RuntimeError("No events available")

        result_str = result.decode()
        event_data = json.loads(result_str)

        # For now, return the data dict - in a real implementation you'd reconstruct the proper type
        return event_data["data"]

    async def close(self) -> None:
        """Close the queue."""
        self._closed = True
        # Optionally clean up the Redis key
        # self.redis.delete(self._queue_key)

    def is_closed(self) -> bool:
        """Check if the queue is closed."""
        return self._closed

    def tap(self) -> "EventQueue":
        """Create a tap (copy) of this queue."""
        # Create a new queue that shares the same Redis key
        return RedisEventQueue(self.redis, self.task_id, self.prefix)

    def task_done(self) -> None:
        """Mark a task as done (for compatibility)."""
        pass  # Redis doesn't need explicit task_done like Python's queue.Queue


class RedisQueueManager(QueueManager):
    """Redis-backed implementation of the A2A QueueManager interface."""

    def __init__(self, redis_client: redis.Redis, prefix: str = "queue:"):
        """Initialize the Redis queue manager.

        Args:
            redis_client: Redis client instance
            prefix: Key prefix for queue storage
        """
        self.redis = redis_client
        self.prefix = prefix
        self._queues: Dict[str, RedisEventQueue] = {}

    async def add(self, task_id: str, queue: EventQueue) -> None:
        """Add a queue for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier
            queue: EventQueue instance to add
        """
        # For Redis implementation, we create our own queue but this maintains interface
        self._queues[task_id] = RedisEventQueue(self.redis, task_id, self.prefix)

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
            self._queues[task_id] = RedisEventQueue(self.redis, task_id, self.prefix)
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
