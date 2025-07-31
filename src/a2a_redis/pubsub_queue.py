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
import threading
from typing import Union, Optional, Any
from queue import Queue, Empty

import redis
from redis.client import PubSub
from a2a.server.events.event_queue import EventQueue
from a2a.types import Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent


class RedisPubSubEventQueue(EventQueue):
    """Redis Pub/Sub-backed implementation of EventQueue.

    This implementation uses Redis Pub/Sub for real-time, fire-and-forget event delivery.
    Events are delivered immediately to active subscribers but are lost if no subscribers
    are listening or if subscribers are offline.

    **Key Characteristics**:
    - **Real-time delivery**: Events delivered immediately to active subscribers
    - **No persistence**: Events not stored, only delivered to active consumers
    - **Fire-and-forget**: No acknowledgments or delivery guarantees
    - **Broadcasting**: All subscribers receive all events
    - **Low latency**: Minimal overhead for immediate delivery

    **Use Cases**:
    - Live status updates and notifications
    - Real-time dashboard updates
    - System event broadcasting
    - Non-critical event distribution

    **Not suitable for**:
    - Critical event processing requiring guarantees
    - Systems requiring event replay or audit trails
    - Offline-capable applications
    - Work queues requiring load balancing
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

        # Internal queue for buffering received messages
        self._message_queue: Queue[Any] = Queue()

        # Pub/Sub subscription management
        self._pubsub: Optional[PubSub] = None
        self._subscriber_thread: Optional[threading.Thread] = None
        self._setup_subscription()

    def _setup_subscription(self) -> None:
        """Set up Redis pub/sub subscription and background thread."""
        if self._closed:
            return

        self._pubsub = self.redis.pubsub()
        self._pubsub.subscribe(self._channel)

        # Start background thread to listen for messages
        self._subscriber_thread = threading.Thread(
            target=self._message_listener, daemon=True
        )
        self._subscriber_thread.start()

    def _message_listener(self) -> None:
        """Background thread that listens for pub/sub messages."""
        if not self._pubsub:
            return

        try:
            for message in self._pubsub.listen():
                if self._closed:
                    break

                # Skip subscription confirmation messages
                if message["type"] != "message":
                    continue

                # Add message to internal queue
                self._message_queue.put(message["data"])

        except Exception:
            # Silently handle connection errors in background thread
            pass

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
        await self.redis.publish(self._channel, message)

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

        timeout = 0 if no_wait else 1  # 1 second timeout for blocking

        try:
            message_data = self._message_queue.get(timeout=timeout)

            # Deserialize event data
            message = json.loads(message_data.decode())
            return message["event_data"]

        except Empty:
            raise RuntimeError("No events available")

    async def close(self) -> None:
        """Close the queue and clean up pub/sub subscription."""
        self._closed = True

        # Clean up pub/sub subscription
        if self._pubsub:
            try:
                self._pubsub.unsubscribe(self._channel)
                self._pubsub.close()
            except Exception:
                pass
            finally:
                self._pubsub = None

        # Wait for subscriber thread to finish
        if self._subscriber_thread and self._subscriber_thread.is_alive():
            self._subscriber_thread.join(timeout=1.0)
            self._subscriber_thread = None

    def is_closed(self) -> bool:
        """Check if the queue is closed."""
        return self._closed

    def tap(self) -> "EventQueue":
        """Create a tap (copy) of this queue.

        For pub/sub, this creates a new subscriber to the same channel.
        All taps will receive the same events (broadcast behavior).
        """
        return RedisPubSubEventQueue(self.redis, self.task_id, self.prefix)

    def task_done(self) -> None:
        """Mark a task as done (no-op for pub/sub)."""
        pass  # Pub/sub doesn't need explicit task completion
