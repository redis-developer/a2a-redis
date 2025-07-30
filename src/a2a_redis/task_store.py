"""Redis-backed task store implementation for the A2A Python SDK."""

import json
from typing import Any, Dict, List, Optional, cast, Union

import redis
from a2a.server.tasks.task_store import TaskStore
from a2a.types import Task


class RedisTaskStore(TaskStore):
    """Redis-backed implementation of the A2A TaskStore interface."""

    def __init__(self, redis_client: redis.Redis, prefix: str = "task:"):
        """Initialize the Redis task store.

        Args:
            redis_client: Redis client instance
            prefix: Key prefix for task storage
        """
        self.redis = redis_client
        self.prefix = prefix

    def _task_key(self, task_id: str) -> str:
        """Generate the Redis key for a task."""
        return f"{self.prefix}{task_id}"

    def _serialize_data(self, data: Union[Dict[str, Any], Task]) -> Dict[str, str]:
        """Serialize task data for Redis storage."""
        # Convert Task to dict if necessary
        if hasattr(data, "model_dump") and callable(getattr(data, "model_dump")):
            data_dict = cast(Dict[str, Any], data.model_dump())  # type: ignore[misc]
        else:
            data_dict = cast(Dict[str, Any], data)

        serialized: Dict[str, str] = {}
        for key, value in data_dict.items():
            key_str = str(key)
            if isinstance(value, (dict, list)):
                serialized[key_str] = json.dumps(value)
            else:
                serialized[key_str] = str(value)
        return serialized

    def _deserialize_data(self, data: Dict[bytes, bytes]) -> Dict[str, Any]:
        """Deserialize task data from Redis."""
        if not data:
            return {}

        result: Dict[str, Any] = {}
        for key, value in data.items():
            key_str = key.decode()
            value_str = value.decode()

            # Try to deserialize JSON data
            try:
                result[key_str] = json.loads(value_str)
            except json.JSONDecodeError:
                result[key_str] = value_str

        return result

    # A2A TaskStore interface methods
    async def save(self, task: Task) -> None:
        """Save a task to Redis (A2A interface method).

        Args:
            task: Task object to save
        """
        task_data = (
            task.model_dump()
            if hasattr(task, "model_dump")
            else cast(Dict[str, Any], task)
        )
        task_id = task_data.get("id", str(task_data))
        serialized_data = self._serialize_data(task)
        self.redis.hset(self._task_key(task_id), mapping=serialized_data)  # type: ignore[misc]

    async def get(self, task_id: str) -> Optional[Task]:
        """Retrieve a task from Redis (A2A interface method).

        Args:
            task_id: Task identifier

        Returns:
            Task object or None if not found
        """
        data: Dict[bytes, bytes] = self.redis.hgetall(self._task_key(task_id))  # type: ignore[assignment]
        if not data:
            return None

        deserialized_data = self._deserialize_data(data)
        try:
            return Task(**deserialized_data) if deserialized_data else None
        except (TypeError, ValueError):
            # If Task construction fails, return None
            return None

    async def delete(self, task_id: str) -> None:
        """Delete a task from Redis (A2A interface method).

        Args:
            task_id: Task identifier
        """
        self.redis.delete(self._task_key(task_id))

    # Additional convenience methods (backward compatibility)
    async def create_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """Create a new task in Redis (convenience method)."""
        task = Task(id=task_id, **task_data)
        await self.save(task)

    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a task from Redis (convenience method)."""
        task = await self.get(task_id)
        return task.model_dump() if task else None

    async def update_task(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing task in Redis (convenience method)."""
        if not self.redis.exists(self._task_key(task_id)):
            return False

        serialized_updates = self._serialize_data(updates)
        self.redis.hset(self._task_key(task_id), mapping=serialized_updates)  # type: ignore[misc]
        return True

    async def delete_task(self, task_id: str) -> None:
        """Delete a task from Redis (convenience method)."""
        await self.delete(task_id)

    def list_task_ids(self, pattern: str = "*") -> List[str]:
        """List all task IDs matching a pattern."""
        keys: List[bytes] = self.redis.keys(f"{self.prefix}{pattern}")  # type: ignore[assignment]
        return [key.decode().replace(self.prefix, "") for key in keys]

    def task_exists(self, task_id: str) -> bool:
        """Check if a task exists in Redis."""
        return bool(self.redis.exists(self._task_key(task_id)))


class RedisJSONTaskStore(TaskStore):
    """Redis JSON-backed implementation of the A2A TaskStore interface.

    Requires RedisJSON module to be available in Redis.
    """

    def __init__(self, redis_client: redis.Redis, prefix: str = "task:"):
        """Initialize the Redis JSON task store.

        Args:
            redis_client: Redis client instance with JSON support
            prefix: Key prefix for task storage
        """
        self.redis = redis_client
        self.prefix = prefix

    def _task_key(self, task_id: str) -> str:
        """Generate the Redis key for a task."""
        return f"{self.prefix}{task_id}"

    # A2A TaskStore interface methods
    async def save(self, task: Task) -> None:
        """Save a task to Redis using JSON (A2A interface method).

        Args:
            task: Task object to save
        """
        task_data = (
            task.model_dump()
            if hasattr(task, "model_dump")
            else cast(Dict[str, Any], task)
        )
        task_id = task_data.get("id", str(task_data))
        self.redis.json().set(self._task_key(task_id), "$", task_data)

    async def get(self, task_id: str) -> Optional[Task]:
        """Retrieve a task from Redis using JSON (A2A interface method).

        Args:
            task_id: Task identifier

        Returns:
            Task object or None if not found
        """
        try:
            result: Any = self.redis.json().get(self._task_key(task_id))  # type: ignore[misc]
            if result:
                # RedisJSON get with JSONPath can return list or dict
                task_data: Dict[str, Any] = {}
                if isinstance(result, list) and result:  # Check if list is not empty
                    task_data = cast(Dict[str, Any], result[0])
                elif isinstance(result, dict):
                    task_data = cast(Dict[str, Any], result)
                else:
                    return None

                try:
                    return Task(**task_data)
                except (TypeError, ValueError):
                    return None
            return None
        except (redis.ResponseError, Exception):
            return None

    async def delete(self, task_id: str) -> None:
        """Delete a task from Redis (A2A interface method).

        Args:
            task_id: Task identifier
        """
        self.redis.delete(self._task_key(task_id))

    # Additional convenience methods (backward compatibility)
    async def create_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """Create a new task in Redis using JSON (convenience method)."""
        task = Task(id=task_id, **task_data)
        await self.save(task)

    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a task from Redis using JSON (convenience method)."""
        task = await self.get(task_id)
        return task.model_dump() if task else None

    async def update_task(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing task in Redis using JSON (convenience method).

        Args:
            task_id: Task identifier
            updates: Dictionary of fields to update

        Returns:
            True if task was updated, False if task doesn't exist
        """
        try:
            task_data = await self.get_task(task_id)
            if task_data is None:
                return False

            task_data.update(updates)
            await self.create_task(task_id, task_data)
            return True
        except redis.ResponseError:
            return False

    async def delete_task(self, task_id: str) -> None:
        """Delete a task from Redis (convenience method)."""
        await self.delete(task_id)

    def list_task_ids(self, pattern: str = "*") -> List[str]:
        """List all task IDs matching a pattern."""
        keys: List[bytes] = self.redis.keys(f"{self.prefix}{pattern}")  # type: ignore[assignment]
        return [key.decode().replace(self.prefix, "") for key in keys]

    def task_exists(self, task_id: str) -> bool:
        """Check if a task exists in Redis."""
        return bool(self.redis.exists(self._task_key(task_id)))
