"""Tests for RedisTaskStore and RedisJSONTaskStore."""

import json
import pytest
from unittest.mock import MagicMock, patch

from a2a_redis.task_store import RedisTaskStore, RedisJSONTaskStore


class TestRedisTaskStore:
    """Tests for RedisTaskStore."""
    
    def test_init(self, mock_redis):
        """Test RedisTaskStore initialization."""
        store = RedisTaskStore(mock_redis, prefix="test:")
        assert store.redis == mock_redis
        assert store.prefix == "test:"
    
    def test_task_key_generation(self, mock_redis):
        """Test task key generation."""
        store = RedisTaskStore(mock_redis, prefix="task:")
        assert store._task_key("123") == "task:123"
    
    def test_create_task(self, mock_redis, sample_task_data):
        """Test task creation."""
        store = RedisTaskStore(mock_redis)
        store.create_task("task_123", sample_task_data)
        
        # Verify hset was called with serialized data
        mock_redis.hset.assert_called_once()
        call_args = mock_redis.hset.call_args
        assert call_args[0][0] == "task:task_123"  # key
        
        # Check that complex data was JSON serialized
        mapping = call_args[1]["mapping"]
        assert "metadata" in mapping
        assert isinstance(mapping["metadata"], str)  # Should be JSON string
        assert json.loads(mapping["metadata"]) == sample_task_data["metadata"]
    
    def test_get_task_exists(self, mock_redis, sample_task_data):
        """Test retrieving an existing task."""
        # Mock Redis response
        mock_redis.hgetall.return_value = {
            b"id": b"task_123",
            b"status": b"pending", 
            b"description": b"Test task",
            b"metadata": json.dumps(sample_task_data["metadata"]).encode()
        }
        
        store = RedisTaskStore(mock_redis)
        result = store.get_task("task_123")
        
        assert result is not None
        assert result["id"] == "task_123"
        assert result["status"] == "pending"
        assert result["metadata"] == sample_task_data["metadata"]
        mock_redis.hgetall.assert_called_once_with("task:task_123")
    
    def test_get_task_not_exists(self, mock_redis):
        """Test retrieving a non-existent task."""
        mock_redis.hgetall.return_value = {}
        
        store = RedisTaskStore(mock_redis)
        result = store.get_task("nonexistent")
        
        assert result is None
        mock_redis.hgetall.assert_called_once_with("task:nonexistent")
    
    def test_update_task_exists(self, mock_redis):
        """Test updating an existing task."""
        mock_redis.exists.return_value = True
        
        store = RedisTaskStore(mock_redis)
        updates = {"status": "completed", "metadata": {"updated": True}}
        result = store.update_task("task_123", updates)
        
        assert result is True
        mock_redis.exists.assert_called_once_with("task:task_123")
        mock_redis.hset.assert_called_once()
    
    def test_update_task_not_exists(self, mock_redis):
        """Test updating a non-existent task."""
        mock_redis.exists.return_value = False
        
        store = RedisTaskStore(mock_redis)
        result = store.update_task("nonexistent", {"status": "completed"})
        
        assert result is False
        mock_redis.exists.assert_called_once_with("task:nonexistent")
        mock_redis.hset.assert_not_called()
    
    def test_delete_task(self, mock_redis):
        """Test task deletion."""
        mock_redis.delete.return_value = 1
        
        store = RedisTaskStore(mock_redis)
        result = store.delete_task("task_123")
        
        assert result is True
        mock_redis.delete.assert_called_once_with("task:task_123")
    
    def test_delete_task_not_exists(self, mock_redis):
        """Test deleting a non-existent task."""
        mock_redis.delete.return_value = 0
        
        store = RedisTaskStore(mock_redis)
        result = store.delete_task("nonexistent")
        
        assert result is False
        mock_redis.delete.assert_called_once_with("task:nonexistent")
    
    def test_list_tasks(self, mock_redis):
        """Test listing tasks."""
        mock_redis.keys.return_value = [b"task:123", b"task:456", b"task:789"]
        
        store = RedisTaskStore(mock_redis)
        result = store.list_tasks()
        
        assert result == ["123", "456", "789"]
        mock_redis.keys.assert_called_once_with("task:*")
    
    def test_list_tasks_with_pattern(self, mock_redis):
        """Test listing tasks with pattern."""
        mock_redis.keys.return_value = [b"task:user_123", b"task:user_456"]
        
        store = RedisTaskStore(mock_redis)
        result = store.list_tasks("user_*")
        
        assert result == ["user_123", "user_456"]
        mock_redis.keys.assert_called_once_with("task:user_*")
    
    def test_task_exists(self, mock_redis):
        """Test checking if task exists."""
        mock_redis.exists.return_value = True
        
        store = RedisTaskStore(mock_redis)
        result = store.task_exists("task_123")
        
        assert result is True
        mock_redis.exists.assert_called_once_with("task:task_123")


class TestRedisTaskStoreIntegration:
    """Integration tests for RedisTaskStore with real Redis."""
    
    def test_full_task_lifecycle(self, task_store, sample_task_data):
        """Test complete task lifecycle with real Redis."""
        task_id = "integration_test_task"
        
        # Task should not exist initially
        assert not task_store.task_exists(task_id)
        assert task_store.get_task(task_id) is None
        
        # Create task
        task_store.create_task(task_id, sample_task_data)
        assert task_store.task_exists(task_id)
        
        # Retrieve task
        retrieved_task = task_store.get_task(task_id)
        assert retrieved_task is not None
        assert retrieved_task["id"] == sample_task_data["id"]
        assert retrieved_task["status"] == sample_task_data["status"]
        assert retrieved_task["metadata"] == sample_task_data["metadata"]
        
        # Update task
        updates = {"status": "in_progress", "progress": 50}
        assert task_store.update_task(task_id, updates)
        
        updated_task = task_store.get_task(task_id)
        assert updated_task["status"] == "in_progress"
        assert updated_task["progress"] == "50"  # String due to Redis storage
        
        # List tasks
        task_list = task_store.list_tasks()
        assert task_id in task_list
        
        # Delete task
        assert task_store.delete_task(task_id)
        assert not task_store.task_exists(task_id)
        assert task_store.get_task(task_id) is None


class TestRedisJSONTaskStore:
    """Tests for RedisJSONTaskStore."""
    
    def test_init(self, mock_redis):
        """Test RedisJSONTaskStore initialization."""
        store = RedisJSONTaskStore(mock_redis, prefix="json:")
        assert store.redis == mock_redis
        assert store.prefix == "json:"
    
    def test_create_task(self, mock_redis, sample_task_data):
        """Test task creation with JSON."""
        mock_json = MagicMock()
        mock_redis.json.return_value = mock_json
        
        store = RedisJSONTaskStore(mock_redis)
        store.create_task("task_123", sample_task_data)
        
        mock_redis.json.assert_called_once()
        mock_json.set.assert_called_once_with("task:task_123", "$", sample_task_data)
    
    def test_get_task_exists(self, mock_redis, sample_task_data):
        """Test retrieving an existing task with JSON."""
        mock_json = MagicMock()
        mock_json.get.return_value = sample_task_data
        mock_redis.json.return_value = mock_json
        
        store = RedisJSONTaskStore(mock_redis)
        result = store.get_task("task_123")
        
        assert result == sample_task_data
        mock_json.get.assert_called_once_with("task:task_123")
    
    def test_get_task_redis_error(self, mock_redis):
        """Test retrieving task when Redis JSON operation fails."""
        mock_json = MagicMock()
        mock_json.get.side_effect = Exception("Redis error")
        mock_redis.json.return_value = mock_json
        
        store = RedisJSONTaskStore(mock_redis)
        result = store.get_task("task_123")
        
        assert result is None
    
    def test_update_task_exists(self, mock_redis, sample_task_data):
        """Test updating an existing task with JSON."""
        mock_json = MagicMock()
        mock_json.get.return_value = sample_task_data
        mock_redis.json.return_value = mock_json
        
        store = RedisJSONTaskStore(mock_redis)
        updates = {"status": "completed"}
        result = store.update_task("task_123", updates)
        
        assert result is True
        # Should fetch, update, and save
        mock_json.get.assert_called_once_with("task:task_123")
        mock_json.set.assert_called_once()
    
    def test_update_task_not_exists(self, mock_redis):
        """Test updating a non-existent task with JSON."""
        mock_json = MagicMock()
        mock_json.get.return_value = None
        mock_redis.json.return_value = mock_json
        
        store = RedisJSONTaskStore(mock_redis)
        result = store.update_task("nonexistent", {"status": "completed"})
        
        assert result is False