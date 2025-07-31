"""Queue type definitions for Redis-backed event queues."""

from enum import Enum


class QueueType(Enum):
    """Types of Redis-backed event queues available.

    Each queue type has different characteristics and use cases:

    **STREAMS** (default, recommended for most use cases):
    - Persistent event storage with replay capability
    - Guaranteed delivery with acknowledgments
    - Consumer groups for load balancing
    - Automatic failure recovery
    - Higher memory usage
    - Best for: Task queues, audit trails, reliable processing

    **PUBSUB** (for real-time, low-latency scenarios):
    - Fire-and-forget, real-time delivery
    - No persistence (events lost if no active subscribers)
    - Natural broadcasting to multiple consumers
    - Minimal memory usage
    - Best for: Live notifications, real-time updates, system events
    """

    STREAMS = "streams"
    """Redis Streams-based queue with persistence and reliability."""

    PUBSUB = "pubsub"
    """Redis Pub/Sub-based queue for real-time, fire-and-forget delivery."""
