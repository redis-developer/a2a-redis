"""Redis Pub/Sub-backed event queue implementation for the A2A Python SDK.

This module provides a Redis Pub/Sub-based implementation of EventQueue as an alternative
to the default Redis Streams implementation. Choose based on your use case:

**Redis Streams (default - RedisEventQueue)**:
- ✅ Persistent event storage (events survive consumer restarts)
- ✅ Guaranteed delivery with acknowledgments
- ✅ Consumer groups for load balancing
- ✅ Event replay and audit trail
- ✅ Automatic failure recovery
- ❌ Higher memory usage (events persist until trimmed)
- ❌ More complex setup (consumer groups)

**Redis Pub/Sub (this module - RedisPubSubEventQueue)**:
- ✅ Real-time, low-latency delivery
- ✅ Minimal memory usage (fire-and-forget)
- ✅ Simple broadcast pattern
- ✅ Natural fan-out to multiple consumers
- ❌ No persistence (offline consumers miss events)
- ❌ No delivery guarantees
- ❌ No replay capability
- ❌ Limited error recovery

**When to use Pub/Sub**:
- Real-time notifications (UI updates, live dashboards)
- Broadcasting system events
- Non-critical event distribution
- Low-latency requirements
- Simple fan-out scenarios

**When to use Streams**:
- Task event queues requiring reliability
- Audit trails and event history
- Work distribution requiring guarantees
- Systems requiring replay capability
- Critical event processing
"""

import json
import asyncio
from typing import Union, Optional, Dict, Any

import redis.asyncio as redis
from redis.asyncio.client import PubSub
from a2a.types import Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent

from .event_queue_protocol import EventQueueProtocol


class RedisPubSubEventQueue:
    """Redis Pub/Sub-backed EventQueue for real-time, fire-and-forget event delivery.

    Provides immediate event broadcasting with minimal latency but no persistence
    or delivery guarantees. See README.md for detailed use cases and trade-offs.
    """

    def __init__(
        self, redis_client: redis.Redis, task_id: str, prefix: str = "pubsub:"
    ):
        """Initialize Redis Pub/Sub event queue.

        Args:
            redis_client: Redis client instance
            task_id: Task identifier this queue is for
            prefix: Key prefix for pub/sub channels
        """
        self.redis = redis_client
        self.task_id = task_id
        self.prefix = prefix
        self._closed = False
        self._channel = f"{prefix}{task_id}"

        # Pub/Sub subscription management
        self._pubsub: Optional[PubSub] = None
        self._setup_complete = False

    async def _ensure_setup(self) -> None:
        """Ensure pub/sub subscription is set up."""
        if self._setup_complete or self._closed:
            return

        self._pubsub = self.redis.pubsub()  # type: ignore[misc]
        await self._pubsub.subscribe(self._channel)  # type: ignore[misc]
        self._setup_complete = True

    async def enqueue_event(
        self,
        event: Union[Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent],
    ) -> None:
        """Publish an event to the pub/sub channel.

        Events are immediately published to all active subscribers. If no subscribers
        are listening, the event is lost.

        Args:
            event: Event to publish

        Raises:
            RuntimeError: If queue is closed
        """
        if self._closed:
            raise RuntimeError("Cannot enqueue to closed queue")

        # Ensure subscription setup
        await self._ensure_setup()

        # Serialize event - convert to dict if it has model_dump, otherwise assume it's serializable
        if hasattr(event, "model_dump"):
            event_data = event.model_dump()
        else:
            event_data = event

        # Create message with event data
        message = json.dumps(
            {"event_type": type(event).__name__, "event_data": event_data}, default=str
        )

        # Publish to Redis pub/sub channel
        await self.redis.publish(self._channel, message)  # type: ignore[misc]

    async def dequeue_event(
        self, no_wait: bool = False
    ) -> Union[Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent]:
        """Remove and return an event from the queue.

        This method retrieves events that were published to the channel and received
        by this subscriber. Events published before subscription started are not available.

        Args:
            no_wait: If True, return immediately if no events available

        Returns:
            Event data dictionary

        Raises:
            RuntimeError: If queue is closed or no events available
        """
        if self._closed:
            raise RuntimeError("Cannot dequeue from closed queue")

        # Ensure subscription setup
        await self._ensure_setup()

        if not self._pubsub:
            raise RuntimeError("Pub/sub not initialized")

        timeout = 0.1 if no_wait else 1.0  # Shorter timeout for no_wait

        try:
            # Get message with timeout
            message: Optional[Dict[str, Any]] = await asyncio.wait_for(  # type: ignore[assignment]
                self._pubsub.get_message(ignore_subscribe_messages=True),  # type: ignore[misc]
                timeout=timeout,
            )

            if message is None:
                raise RuntimeError("No events available")

            # Deserialize event data - message["data"] should be bytes
            data_bytes = message["data"]  # type: ignore[misc]
            if isinstance(data_bytes, bytes):
                message_data = json.loads(data_bytes.decode())
            else:
                message_data = json.loads(str(data_bytes))  # type: ignore[misc]
            return message_data["event_data"]

        except asyncio.TimeoutError:
            raise RuntimeError("No events available")

    async def close(self) -> None:
        """Close the queue and clean up pub/sub subscription."""
        self._closed = True

        # Clean up pub/sub subscription
        if self._pubsub:
            try:
                await self._pubsub.unsubscribe(self._channel)  # type: ignore[misc]
                await self._pubsub.close()  # type: ignore[misc]
            except Exception:
                pass
            finally:
                self._pubsub = None

                self._setup_complete = False

    def is_closed(self) -> bool:
        """Check if the queue is closed."""
        return self._closed

    def tap(self) -> "EventQueueProtocol":
        """Create a tap (copy) of this queue.

        For pub/sub, this creates a new subscriber to the same channel.
        All taps will receive the same events (broadcast behavior).
        """
        return RedisPubSubEventQueue(self.redis, self.task_id, self.prefix)

    def task_done(self) -> None:
        """Mark a task as done (no-op for pub/sub)."""
        pass  # Pub/sub doesn't need explicit task completion
