"""Redis Streams-backed event queue implementation for the A2A Python SDK.

This module provides a Redis Streams-based implementation of EventQueue,
offering persistent, reliable event delivery with consumer groups, acknowledgments, and replay capability.

**Key Features**:
- **Persistent storage**: Events remain in streams until explicitly trimmed
- **Guaranteed delivery**: Consumer groups with acknowledgments prevent message loss
- **Load balancing**: Multiple consumers can share work via consumer groups
- **Failure recovery**: Unacknowledged messages can be reclaimed by other consumers
- **Event replay**: Historical events can be re-read from any point in time
- **Ordering**: Maintains strict insertion order with unique message IDs

**Use Cases**:
- Task event queues requiring reliability
- Audit trails and event history
- Work distribution systems
- Systems requiring failure recovery
- Multi-consumer load balancing

**Trade-offs**:
- Higher memory usage (events persist)
- More complex setup (consumer groups)
- Slightly higher latency than pub/sub

For real-time, fire-and-forget scenarios, consider RedisPubSubEventQueue instead.
"""

import json
from typing import Optional, Union

import redis.asyncio as redis
from a2a.types import Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent

from .event_queue_protocol import EventQueueProtocol
from .streams_consumer_strategy import ConsumerGroupConfig


class RedisStreamsEventQueue:
    """Redis Streams-backed EventQueue for persistent, reliable event delivery.

    Provides guaranteed delivery with consumer groups, acknowledgments, and replay
    capability. See README.md for detailed use cases and trade-offs.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        task_id: str,
        prefix: str = "stream:",
        consumer_config: Optional[ConsumerGroupConfig] = None,
    ):
        """Initialize Redis Streams event queue.

        Args:
            redis_client: Redis client instance
            task_id: Task identifier this queue is for
            prefix: Key prefix for stream storage
            consumer_config: Consumer group configuration
        """
        self.redis = redis_client
        self.task_id = task_id
        self.prefix = prefix
        self._closed = False
        self._stream_key = f"{prefix}{task_id}"

        # Consumer group configuration
        self.consumer_config = consumer_config or ConsumerGroupConfig()
        self.consumer_group = self.consumer_config.get_consumer_group_name(task_id)
        self.consumer_id = self.consumer_config.get_consumer_id()

        # Consumer group will be ensured on first use
        self._consumer_group_ensured = False

    async def _ensure_consumer_group(self) -> None:
        """Create consumer group if it doesn't exist."""
        try:
            # XGROUP CREATE stream_key group_name 0 MKSTREAM
            await self.redis.xgroup_create(
                self._stream_key, self.consumer_group, id="0", mkstream=True
            )  # type: ignore[misc]
        except Exception as e:  # type: ignore[misc]
            if "BUSYGROUP" not in str(e):  # Group already exists
                raise

    async def enqueue_event(
        self,
        event: Union[Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent],
    ) -> None:
        """Add an event to the stream.

        Args:
            event: Event to add to the stream

        Raises:
            RuntimeError: If queue is closed
        """
        if self._closed:
            raise RuntimeError("Cannot enqueue to closed queue")

        # Ensure consumer group exists on first use
        if not self._consumer_group_ensured:
            await self._ensure_consumer_group()
            self._consumer_group_ensured = True

        # Serialize event - convert to dict if it has model_dump, otherwise assume it's serializable
        if hasattr(event, "model_dump"):
            event_data = event.model_dump()
        else:
            event_data = event

        # Create stream entry with event data
        fields = {
            "event_type": type(event).__name__,
            "event_data": json.dumps(event_data, default=str),
        }

        # Add to Redis stream (XADD)
        await self.redis.xadd(self._stream_key, fields)  # type: ignore[misc]

    async def dequeue_event(
        self, no_wait: bool = False
    ) -> Union[Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent]:
        """Remove and return an event from the stream.

        Args:
            no_wait: If True, return immediately if no events available

        Returns:
            Event data dictionary

        Raises:
            RuntimeError: If queue is closed or no events available
        """
        if self._closed:
            raise RuntimeError("Cannot dequeue from closed queue")

        # Ensure consumer group exists on first use
        if not self._consumer_group_ensured:
            await self._ensure_consumer_group()
            self._consumer_group_ensured = True

        # Read from consumer group
        timeout = 0 if no_wait else 1000  # 0 = non-blocking, 1000ms = 1 second timeout

        try:
            # XREADGROUP GROUP group_name consumer_id COUNT 1 BLOCK timeout STREAMS stream_key >
            result = await self.redis.xreadgroup(
                self.consumer_group,
                self.consumer_id,
                {self._stream_key: ">"},
                count=1,
                block=timeout,
            )  # type: ignore[misc]

            if not result or not result[0][1]:  # No messages available
                raise RuntimeError("No events available")

            # Extract message data
            _, messages = result[0]
            message_id, fields = messages[0]

            # Deserialize event data
            event_data = json.loads(fields[b"event_data"].decode())

            # Acknowledge the message
            await self.redis.xack(self._stream_key, self.consumer_group, message_id)  # type: ignore[misc]

            return event_data

        except Exception as e:  # type: ignore[misc]
            if "NOGROUP" in str(e):
                # Consumer group was deleted, recreate it
                await self._ensure_consumer_group()
                raise RuntimeError("Consumer group recreated, try again")
            raise RuntimeError(f"Error reading from stream: {e}")

    async def close(self) -> None:
        """Close the queue and clean up pending messages."""
        self._closed = True
        # Optionally clean up pending messages for this consumer
        try:
            # Get pending messages for this consumer
            pending = await self.redis.xpending_range(  # type: ignore[misc]
                self._stream_key,
                self.consumer_group,
                min="-",
                max="+",
                count=100,
                consumername=self.consumer_id,
            )

            # Acknowledge any pending messages to prevent them from being stuck
            if pending:
                message_ids = [msg["message_id"] for msg in pending]
                await self.redis.xack(
                    self._stream_key, self.consumer_group, *message_ids
                )  # type: ignore[misc]

        except Exception:  # type: ignore[misc]
            # Consumer group might not exist, ignore
            pass

    def is_closed(self) -> bool:
        """Check if the queue is closed."""
        return self._closed

    def tap(self) -> "EventQueueProtocol":
        """Create a tap (copy) of this queue.

        Creates a new queue with the same stream but different consumer ID
        for independent message processing.
        """
        return RedisStreamsEventQueue(
            self.redis, self.task_id, self.prefix, self.consumer_config
        )

    def task_done(self) -> None:
        """Mark a task as done (for compatibility)."""
        pass  # Stream acknowledgment is handled in dequeue_event
