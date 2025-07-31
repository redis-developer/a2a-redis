"""Redis-backed task store implementations for the A2A Python SDK."""

import json
from typing import Any, Dict, List, Optional, Union

import redis.asyncio as redis
from a2a.server.tasks.task_store import TaskStore
from a2a.types import Task


class RedisTaskStore(TaskStore):
    """Redis hash-backed TaskStore with JSON serialization for complex objects.

    General-purpose task storage using Redis hashes. For JSON-native features,
    consider RedisJSONTaskStore instead.
    """

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
            data_dict = data.model_dump()  # type: ignore[misc]
        else:
            data_dict = data  # type: ignore[assignment]

        serialized: Dict[str, str] = {}
        for key, value in data_dict.items():  # type: ignore[misc]
            key_str = str(key)  # type: ignore[misc]
            if isinstance(value, (dict, list)):
                serialized[key_str] = json.dumps(value, default=str)
            elif hasattr(value, "model_dump") and callable(  # type: ignore[misc]
                getattr(value, "model_dump")  # type: ignore[misc]
            ):
                # Handle Pydantic models by marking them and serializing their dict representation
                serialized[key_str] = json.dumps(
                    {
                        "_type": value.__class__.__module__  # type: ignore[misc]
                        + "."
                        + value.__class__.__name__,  # type: ignore[misc]
                        "_data": value.model_dump(),  # type: ignore[misc]
                    },
                    default=str,
                )
            else:
                if value is None:
                    serialized[key_str] = "null"  # JSON null representation
                else:
                    serialized[key_str] = str(value)  # type: ignore[misc]
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
                json_data = json.loads(value_str)
                # Check if this is a serialized Pydantic model
                if (
                    isinstance(json_data, dict)
                    and "_type" in json_data
                    and "_data" in json_data
                ):
                    # Reconstruct the original object
                    type_name = json_data["_type"]  # type: ignore[misc]
                    if type_name == "a2a.types.TaskStatus":
                        from a2a.types import TaskStatus, TaskState

                        data_dict = json_data["_data"]  # type: ignore[misc]
                        # Convert state string back to TaskState enum
                        if "state" in data_dict and isinstance(data_dict["state"], str):
                            data_dict["state"] = TaskState(data_dict["state"])
                        result[key_str] = TaskStatus(**data_dict)  # type: ignore[misc]
                    else:
                        # For unknown types, just return the data dict
                        result[key_str] = json_data["_data"]
                else:
                    result[key_str] = json_data
            except json.JSONDecodeError:
                result[key_str] = value_str

        return result

    async def save(self, task: Task) -> None:
        """Save a task to Redis.

        Args:
            task: Task instance to save
        """
        serialized_data = self._serialize_data(task)
        await self.redis.hset(self._task_key(task.id), mapping=serialized_data)  # type: ignore[misc]

    async def get(self, task_id: str) -> Optional[Task]:
        """Retrieve a task from Redis.

        Args:
            task_id: Task identifier

        Returns:
            Task instance or None if not found
        """
        data = await self.redis.hgetall(self._task_key(task_id))  # type: ignore[misc]
        if not data:
            return None

        task_data = self._deserialize_data(data)  # type: ignore[arg-type]
        return Task(**task_data)

    async def delete(self, task_id: str) -> None:
        """Delete a task from Redis.

        Args:
            task_id: Task identifier
        """
        await self.redis.delete(self._task_key(task_id))  # type: ignore[misc]

    async def update_task(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing task in Redis.

        Args:
            task_id: Task identifier
            updates: Dictionary of fields to update

        Returns:
            True if task was updated, False if task doesn't exist
        """
        if not await self.redis.exists(self._task_key(task_id)):  # type: ignore[misc]
            return False

        serialized_updates = self._serialize_data(updates)
        await self.redis.hset(self._task_key(task_id), mapping=serialized_updates)  # type: ignore[misc]
        return True

    async def list_task_ids(self, pattern: str = "*") -> List[str]:
        """List all task IDs matching a pattern.

        Args:
            pattern: Pattern to match task IDs against

        Returns:
            List of task IDs
        """
        keys = await self.redis.keys(f"{self.prefix}{pattern}")  # type: ignore[misc]
        return [key.decode().replace(self.prefix, "") for key in keys]  # type: ignore[misc]

    async def task_exists(self, task_id: str) -> bool:
        """Check if a task exists in Redis.

        Args:
            task_id: Task identifier

        Returns:
            True if task exists, False otherwise
        """
        return bool(await self.redis.exists(self._task_key(task_id)))  # type: ignore[misc]


class RedisJSONTaskStore(TaskStore):
    """Redis JSON-backed TaskStore for native JSON operations.

    Requires Redis server with RedisJSON module. Provides better performance
    for complex nested data structures and JSONPath queries.
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

    async def save(self, task: Task) -> None:
        """Save a task to Redis using JSON.

        Args:
            task: Task instance to save
        """
        task_data = task.model_dump() if hasattr(task, "model_dump") else task
        await self.redis.json().set(self._task_key(task.id), "$", task_data)  # type: ignore[misc]

    async def get(self, task_id: str) -> Optional[Task]:
        """Retrieve a task from Redis using JSON.

        Args:
            task_id: Task identifier

        Returns:
            Task instance or None if not found
        """
        try:
            result = await self.redis.json().get(self._task_key(task_id))  # type: ignore[misc]
            if result:
                # RedisJSON get with JSONPath can return list or dict
                if isinstance(result, list) and result:
                    task_data = result[0]  # type: ignore[misc]
                elif isinstance(result, dict):
                    task_data = result  # type: ignore[assignment]
                else:
                    return None
                return Task(**task_data)  # type: ignore[misc]
            return None
        except (Exception,):  # type: ignore[misc]
            return None

    async def delete(self, task_id: str) -> None:
        """Delete a task from Redis.

        Args:
            task_id: Task identifier
        """
        await self.redis.delete(self._task_key(task_id))  # type: ignore[misc]

    async def update_task(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing task in Redis using JSON.

        Args:
            task_id: Task identifier
            updates: Dictionary of fields to update

        Returns:
            True if task was updated, False if task doesn't exist
        """
        try:
            task = await self.get(task_id)
            if task is None:
                return False

            task_data = task.model_dump() if hasattr(task, "model_dump") else task  # type: ignore[misc]
            task_data.update(updates)  # type: ignore[misc]
            updated_task = Task(**task_data)  # type: ignore[misc]
            await self.save(updated_task)
            return True
        except Exception:  # type: ignore[misc]
            return False

    async def list_task_ids(self, pattern: str = "*") -> List[str]:
        """List all task IDs matching a pattern.

        Args:
            pattern: Pattern to match task IDs against

        Returns:
            List of task IDs
        """
        keys = await self.redis.keys(f"{self.prefix}{pattern}")  # type: ignore[misc]
        return [key.decode().replace(self.prefix, "") for key in keys]  # type: ignore[misc]

    async def task_exists(self, task_id: str) -> bool:
        """Check if a task exists in Redis.

        Args:
            task_id: Task identifier

        Returns:
            True if task exists, False otherwise
        """
        return bool(await self.redis.exists(self._task_key(task_id)))  # type: ignore[misc]
