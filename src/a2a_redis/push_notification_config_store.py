"""Redis-backed push notification config store implementation for the A2A Python SDK."""

import json
from typing import List, Optional

import redis.asyncio as redis
from a2a.server.tasks.push_notification_config_store import PushNotificationConfigStore
from a2a.types import PushNotificationConfig


class RedisPushNotificationConfigStore(PushNotificationConfigStore):
    """Redis-backed implementation of the A2A PushNotificationConfigStore interface."""

    def __init__(self, redis_client: redis.Redis, prefix: str = "push_config:"):
        """Initialize the Redis push notification config store.

        Args:
            redis_client: Redis client instance
            prefix: Key prefix for config storage
        """
        self.redis = redis_client
        self.prefix = prefix

    def _task_key(self, task_id: str) -> str:
        """Generate the Redis key for task push notification configs."""
        return f"{self.prefix}{task_id}"

    async def get_info(self, task_id: str) -> List[PushNotificationConfig]:
        """Get push notification configs for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier

        Returns:
            List of PushNotificationConfig objects
        """
        configs_data = await self.redis.hgetall(self._task_key(task_id))  # type: ignore[misc]
        if not configs_data:
            return []

        configs: List[PushNotificationConfig] = []
        for config_id_bytes, config_json_bytes in configs_data.items():  # type: ignore[misc]
            config_id = (
                config_id_bytes.decode()
                if isinstance(config_id_bytes, bytes)
                else str(config_id_bytes)  # type: ignore[misc]
            )
            config_json = (
                config_json_bytes.decode()
                if isinstance(config_json_bytes, bytes)
                else str(config_json_bytes)  # type: ignore[misc]
            )

            try:
                config_data = json.loads(config_json)
                # Add the config_id to the data
                config_data["id"] = config_id
                config = PushNotificationConfig(**config_data)
                configs.append(config)
            except (json.JSONDecodeError, TypeError, ValueError):
                # Skip invalid configs
                continue

        return configs

    async def set_info(
        self, task_id: str, notification_config: PushNotificationConfig
    ) -> None:
        """Set push notification config for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier
            notification_config: Push notification configuration
        """
        # Use config id as the field name, or generate one if not provided
        current_configs = await self.get_info(task_id)
        config_id = notification_config.id or f"config_{len(current_configs)}"

        # Serialize the config (exclude id from the stored data since it's the key)
        config_data = notification_config.model_dump()
        if "id" in config_data:
            del config_data["id"]

        config_json = json.dumps(config_data)
        await self.redis.hset(self._task_key(task_id), config_id, config_json)  # type: ignore[misc]

    async def delete_info(self, task_id: str, config_id: Optional[str] = None) -> None:
        """Delete push notification config(s) for a task (a2a-sdk interface).

        Args:
            task_id: Task identifier
            config_id: Specific config ID to delete, or None to delete all configs
        """
        if config_id:
            # Delete specific config
            await self.redis.hdel(self._task_key(task_id), config_id)  # type: ignore[misc]
        else:
            # Delete all configs for the task
            await self.redis.delete(self._task_key(task_id))  # type: ignore[misc]
