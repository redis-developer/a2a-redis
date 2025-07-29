"""Redis-backed task store implementation for the A2A Python SDK."""

import json
from typing import Any, Dict, List, Optional

import redis
from a2a.server.tasks.task_store import TaskStore


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
    
    def create_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """Create a new task in Redis.
        
        Args:
            task_id: Unique identifier for the task
            task_data: Task data dictionary
        """
        # Serialize complex data to JSON strings
        serialized_data = {}
        for key, value in task_data.items():
            if isinstance(value, (dict, list)):
                serialized_data[key] = json.dumps(value)
            else:
                serialized_data[key] = str(value)
        
        self.redis.hset(self._task_key(task_id), mapping=serialized_data)
    
    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a task from Redis.
        
        Args:
            task_id: Task identifier
            
        Returns:
            Task data dictionary or None if not found
        """
        data = self.redis.hgetall(self._task_key(task_id))
        if not data:
            return None
        
        # Deserialize data and convert bytes to strings
        result = {}
        for key, value in data.items():
            key_str = key.decode() if isinstance(key, bytes) else key
            value_str = value.decode() if isinstance(value, bytes) else value
            
            # Try to deserialize JSON data
            try:
                result[key_str] = json.loads(value_str)
            except json.JSONDecodeError:
                result[key_str] = value_str
        
        return result
    
    def update_task(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing task in Redis.
        
        Args:
            task_id: Task identifier
            updates: Dictionary of fields to update
            
        Returns:
            True if task was updated, False if task doesn't exist
        """
        if not self.redis.exists(self._task_key(task_id)):
            return False
        
        # Serialize complex data to JSON strings
        serialized_updates = {}
        for key, value in updates.items():
            if isinstance(value, (dict, list)):
                serialized_updates[key] = json.dumps(value)
            else:
                serialized_updates[key] = str(value)
        
        self.redis.hset(self._task_key(task_id), mapping=serialized_updates)
        return True
    
    def delete_task(self, task_id: str) -> bool:
        """Delete a task from Redis.
        
        Args:
            task_id: Task identifier
            
        Returns:
            True if task was deleted, False if task didn't exist
        """
        return bool(self.redis.delete(self._task_key(task_id)))
    
    def list_tasks(self, pattern: str = "*") -> List[str]:
        """List all task IDs matching a pattern.
        
        Args:
            pattern: Pattern to match task IDs
            
        Returns:
            List of task IDs
        """
        keys = self.redis.keys(f"{self.prefix}{pattern}")
        return [key.decode().replace(self.prefix, "") for key in keys]
    
    def task_exists(self, task_id: str) -> bool:
        """Check if a task exists in Redis.
        
        Args:
            task_id: Task identifier
            
        Returns:
            True if task exists, False otherwise
        """
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
    
    def create_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """Create a new task in Redis using JSON.
        
        Args:
            task_id: Unique identifier for the task
            task_data: Task data dictionary
        """
        self.redis.json().set(self._task_key(task_id), "$", task_data)
    
    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a task from Redis using JSON.
        
        Args:
            task_id: Task identifier
            
        Returns:
            Task data dictionary or None if not found
        """
        try:
            return self.redis.json().get(self._task_key(task_id))
        except redis.ResponseError:
            return None
    
    def update_task(self, task_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing task in Redis using JSON.
        
        Args:
            task_id: Task identifier
            updates: Dictionary of fields to update
            
        Returns:
            True if task was updated, False if task doesn't exist
        """
        try:
            task = self.get_task(task_id)
            if task is None:
                return False
            
            task.update(updates)
            self.create_task(task_id, task)
            return True
        except redis.ResponseError:
            return False
    
    def delete_task(self, task_id: str) -> bool:
        """Delete a task from Redis.
        
        Args:
            task_id: Task identifier
            
        Returns:
            True if task was deleted, False if task didn't exist
        """
        return bool(self.redis.delete(self._task_key(task_id)))
    
    def list_tasks(self, pattern: str = "*") -> List[str]:
        """List all task IDs matching a pattern.
        
        Args:
            pattern: Pattern to match task IDs
            
        Returns:
            List of task IDs
        """
        keys = self.redis.keys(f"{self.prefix}{pattern}")
        return [key.decode().replace(self.prefix, "") for key in keys]
    
    def task_exists(self, task_id: str) -> bool:
        """Check if a task exists in Redis.
        
        Args:
            task_id: Task identifier
            
        Returns:
            True if task exists, False otherwise
        """
        return bool(self.redis.exists(self._task_key(task_id)))