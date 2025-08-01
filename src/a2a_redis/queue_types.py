"""Queue type definitions for Redis-backed event queues."""

from enum import Enum


class QueueType(Enum):
    """Types of Redis-backed event queues available.

    See README.md for detailed characteristics and use cases.
    """

    STREAMS = "streams"
    """Redis Streams-based queue with persistence and reliability."""

    PUBSUB = "pubsub"
    """Redis Pub/Sub-based queue for real-time, fire-and-forget delivery."""
