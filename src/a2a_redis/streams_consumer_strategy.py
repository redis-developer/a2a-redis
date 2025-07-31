"""Consumer group strategies for Redis Streams."""

from enum import Enum
from typing import Optional
import uuid


class ConsumerGroupStrategy(Enum):
    """Strategies for Redis Stream consumer group behavior."""

    SHARED_LOAD_BALANCING = "shared_load_balancing"
    """Multiple A2A instances share work via same consumer group (load balancing)."""

    INSTANCE_ISOLATED = "instance_isolated"
    """Each A2A instance gets its own consumer group (parallel processing)."""

    CUSTOM = "custom"
    """User provides custom consumer group name."""


class ConsumerGroupConfig:
    """Configuration for consumer group behavior."""

    def __init__(
        self,
        strategy: ConsumerGroupStrategy = ConsumerGroupStrategy.SHARED_LOAD_BALANCING,
        custom_group_name: Optional[str] = None,
        consumer_prefix: str = "a2a",
        instance_id: Optional[str] = None,
    ):
        """Initialize consumer group configuration.

        Args:
            strategy: Consumer group strategy to use
            custom_group_name: Custom group name (required if strategy is CUSTOM)
            consumer_prefix: Prefix for consumer IDs
            instance_id: Unique instance identifier (auto-generated if None)
        """
        self.strategy = strategy
        self.custom_group_name = custom_group_name
        self.consumer_prefix = consumer_prefix
        self.instance_id = instance_id or uuid.uuid4().hex[:8]

        if strategy == ConsumerGroupStrategy.CUSTOM and not custom_group_name:
            raise ValueError("custom_group_name required when using CUSTOM strategy")

    def get_consumer_group_name(self, task_id: str) -> str:
        """Get the consumer group name based on strategy."""
        if self.strategy == ConsumerGroupStrategy.SHARED_LOAD_BALANCING:
            return f"processors-{task_id}"
        elif self.strategy == ConsumerGroupStrategy.INSTANCE_ISOLATED:
            return f"processors-{task_id}-{self.instance_id}"
        elif self.strategy == ConsumerGroupStrategy.CUSTOM:
            if self.custom_group_name is None:
                raise ValueError("custom_group_name cannot be None for CUSTOM strategy")
            return self.custom_group_name
        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")

    def get_consumer_id(self) -> str:
        """Get unique consumer ID for this instance."""
        return f"{self.consumer_prefix}-{self.instance_id}"
