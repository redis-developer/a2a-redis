"""Redis-backed task store implementations for the A2A Python SDK."""

import json
from typing import Any, Dict, List, Optional, Union

import redis
from a2a.server.tasks.task_store import TaskStore
from a2a.types import Task


class RedisTaskStore(TaskStore):
    """Redis hash-backed implementation of the A2A TaskStore interface.

    This implementation stores tasks as Redis hashes, with complex objects
    serialized as JSON. It provides a balance between performance and
    functionality for most use cases.

    **Features**:
    - Stores tasks as Redis hashes for efficient field access
    - Automatic serialization/deserialization of complex objects
    - Support for task metadata and custom fields
    - Efficient key-based operations

    **Use Cases**:
    - General-purpose task storage
    - Applications requiring field-level access to task data
    - Systems with moderate data complexity

    For JSON-native features, consider RedisJSONTaskStore instead.
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
            data_dict = data.model_dump()
        else:
            data_dict = data

        serialized: Dict[str, str] = {}
        for key, value in data_dict.items():
            key_str = str(key)
            if isinstance(value, (dict, list)):
                serialized[key_str] = json.dumps(value, default=str)
            elif hasattr(value, "model_dump") and callable(
                getattr(value, "model_dump")
            ):
                # Handle Pydantic models by marking them and serializing their dict representation
                serialized[key_str] = json.dumps(
                    {
                        "_type": value.__class__.__module__
                        + "."
                        + value.__class__.__name__,
                        "_data": value.model_dump(),
                    },
                    default=str,
                )
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
                json_data = json.loads(value_str)
                # Check if this is a serialized Pydantic model
                if (
                    isinstance(json_data, dict)
                    and "_type" in json_data
                    and "_data" in json_data
                ):
                    # Reconstruct the original object
                    type_name = json_data["_type"]
                    if type_name == "a2a.types.TaskStatus":
                        from a2a.types import TaskStatus, TaskState

                        data_dict = json_data["_data"]
                        # Convert state string back to TaskState enum
                        if "state" in data_dict and isinstance(data_dict["state"], str):
                            data_dict["state"] = TaskState(data_dict["state"])
                        result[key_str] = TaskStatus(**data_dict)
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
        await self.redis.hset(self._task_key(task.id), mapping=serialized_data)

    async def get(self, task_id: str) -> Optional[Task]:
        """Retrieve a task from Redis.

        Args:
            task_id: Task identifier

        Returns:
            Task instance or None if not found
        """
        data: Dict[bytes, bytes] = await self.redis.hgetall(self._task_key(task_id))
        if not data:
            return None

        task_data = self._deserialize_data(data)
        return Task(**task_data)

    async def delete(self, task_id: str) -> None:
        """Delete a task from Redis.

        Args:
            task_id: Task identifier
        """
        await self.redis.delete(self._task_key(task_id))

    async def update_task(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing task in Redis.

        Args:
            task_id: Task identifier
            updates: Dictionary of fields to update

        Returns:
            True if task was updated, False if task doesn't exist
        """
        if not await self.redis.exists(self._task_key(task_id)):
            return False

        serialized_updates = self._serialize_data(updates)
        await self.redis.hset(self._task_key(task_id), mapping=serialized_updates)
        return True

    async def list_task_ids(self, pattern: str = "*") -> List[str]:
        """List all task IDs matching a pattern.

        Args:
            pattern: Pattern to match task IDs against

        Returns:
            List of task IDs
        """
        keys = await self.redis.keys(f"{self.prefix}{pattern}")
        return [key.decode().replace(self.prefix, "") for key in keys]

    async def task_exists(self, task_id: str) -> bool:
        """Check if a task exists in Redis.

        Args:
            task_id: Task identifier

        Returns:
            True if task exists, False otherwise
        """
        return bool(await self.redis.exists(self._task_key(task_id)))


class RedisJSONTaskStore(TaskStore):
    """Redis JSON-backed implementation of the A2A TaskStore interface.

    This implementation uses Redis JSON for native JSON storage and manipulation,
    providing better performance for complex nested data structures.

    **Features**:
    - Native JSON storage with RedisJSON
    - Efficient nested data operations
    - JSONPath query support
    - Atomic JSON operations

    **Requirements**:
    - Redis server with RedisJSON module installed

    **Use Cases**:
    - Applications with complex nested task data
    - Systems requiring JSONPath queries
    - High-performance JSON operations
    - Applications leveraging Redis JSON features

    **Considerations**:
    - Requires RedisJSON module
    - Slightly different API behavior for complex objects
    - Better performance for large JSON documents
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
        self.redis.json().set(self._task_key(task.id), "$", task_data)

    async def get(self, task_id: str) -> Optional[Task]:
        """Retrieve a task from Redis using JSON.

        Args:
            task_id: Task identifier

        Returns:
            Task instance or None if not found
        """
        try:
            result = self.redis.json().get(self._task_key(task_id))
            if result:
                # RedisJSON get with JSONPath can return list or dict
                if isinstance(result, list) and result:
                    task_data = result[0]
                elif isinstance(result, dict):
                    task_data = result
                else:
                    return None
                return Task(**task_data)
            return None
        except (redis.ResponseError, Exception):
            return None

    async def delete(self, task_id: str) -> None:
        """Delete a task from Redis.

        Args:
            task_id: Task identifier
        """
        self.redis.delete(self._task_key(task_id))

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

            task_data = task.model_dump() if hasattr(task, "model_dump") else task
            task_data.update(updates)
            updated_task = Task(**task_data)
            await self.save(updated_task)
            return True
        except redis.ResponseError:
            return False

    async def list_task_ids(self, pattern: str = "*") -> List[str]:
        """List all task IDs matching a pattern.

        Args:
            pattern: Pattern to match task IDs against

        Returns:
            List of task IDs
        """
        keys = await self.redis.keys(f"{self.prefix}{pattern}")
        return [key.decode().replace(self.prefix, "") for key in keys]

    async def task_exists(self, task_id: str) -> bool:
        """Check if a task exists in Redis.

        Args:
            task_id: Task identifier

        Returns:
            True if task exists, False otherwise
        """
        return bool(await self.redis.exists(self._task_key(task_id)))
