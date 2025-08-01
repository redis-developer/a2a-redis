"""Tests for RedisTaskStore and RedisJSONTaskStore."""

import json
import pytest
from unittest.mock import MagicMock

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

    @pytest.mark.asyncio
    async def test_save_task(self, mock_redis, sample_task_data):
        """Test task saving."""
        from a2a.types import Task

        # Create Task object from sample data
        task = Task(**sample_task_data)

        store = RedisTaskStore(mock_redis)
        await store.save(task)

        # Verify hset was called with serialized data
        mock_redis.hset.assert_called_once()
        call_args = mock_redis.hset.call_args
        assert call_args[0][0] == "task:task_123"  # key

        # Check that complex data was JSON serialized
        mapping = call_args[1]["mapping"]
        assert "metadata" in mapping
        assert isinstance(mapping["metadata"], str)  # Should be JSON string
        assert json.loads(mapping["metadata"]) == sample_task_data["metadata"]

    @pytest.mark.asyncio
    async def test_get_task_exists(self, mock_redis, sample_task_data):
        """Test retrieving an existing task."""

        # Mock Redis response in the format RedisTaskStore would actually store it
        mock_redis.hgetall.return_value = {
            b"id": b"task_123",
            b"context_id": b"context_456",
            b"status": json.dumps(
                {"_type": "a2a.types.TaskStatus", "_data": {"state": "submitted"}}
            ).encode(),
            b"metadata": json.dumps(sample_task_data["metadata"]).encode(),
        }

        store = RedisTaskStore(mock_redis)
        result = await store.get("task_123")

        assert result is not None
        assert result.id == "task_123"
        assert result.context_id == "context_456"
        assert result.metadata == sample_task_data["metadata"]
        mock_redis.hgetall.assert_called_once_with("task:task_123")

    @pytest.mark.asyncio
    async def test_get_task_not_exists(self, mock_redis):
        """Test retrieving a non-existent task."""
        mock_redis.hgetall.return_value = {}

        store = RedisTaskStore(mock_redis)
        result = await store.get("nonexistent")

        assert result is None
        mock_redis.hgetall.assert_called_once_with("task:nonexistent")

    def test_serialize_data_edge_cases(self, mock_redis):
        """Test serialization edge cases."""
        store = RedisTaskStore(mock_redis)

        # Test with various data types
        data = {
            "string": "test",
            "number": 42,
            "boolean": True,
            "none": None,
            "list": [1, 2, 3],
            "dict": {"nested": "value"},
        }

        serialized = store._serialize_data(data)

        assert serialized["string"] == "test"
        assert serialized["number"] == "42"
        assert serialized["boolean"] == "True"
        assert serialized["none"] == "null"
        assert json.loads(serialized["list"]) == [1, 2, 3]
        assert json.loads(serialized["dict"]) == {"nested": "value"}

    def test_deserialize_data_edge_cases(self, mock_redis):
        """Test deserialization edge cases."""
        store = RedisTaskStore(mock_redis)

        # Test with empty data
        result = store._deserialize_data({})
        assert result == {}

        # Test with mixed data types
        redis_data = {
            b"string": b"test",
            b"json_list": b"[1, 2, 3]",
            b"json_dict": b'{"key": "value"}',
            b"invalid_json": b'{"incomplete"',
        }

        result = store._deserialize_data(redis_data)

        assert result["string"] == "test"
        assert result["json_list"] == [1, 2, 3]
        assert result["json_dict"] == {"key": "value"}
        assert result["invalid_json"] == '{"incomplete"'  # Falls back to string

    @pytest.mark.asyncio
    async def test_update_task_exists(self, mock_redis):
        """Test updating an existing task."""
        mock_redis.exists.return_value = True

        store = RedisTaskStore(mock_redis)
        updates = {"status": "completed", "metadata": {"updated": True}}
        result = await store.update_task("task_123", updates)

        assert result is True
        mock_redis.exists.assert_called_once_with("task:task_123")
        mock_redis.hset.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_task_not_exists(self, mock_redis):
        """Test updating a non-existent task."""
        mock_redis.exists.return_value = False

        store = RedisTaskStore(mock_redis)
        result = await store.update_task("nonexistent", {"status": "completed"})

        assert result is False
        mock_redis.exists.assert_called_once_with("task:nonexistent")
        mock_redis.hset.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_task(self, mock_redis):
        """Test task deletion."""
        mock_redis.delete.return_value = 1

        store = RedisTaskStore(mock_redis)
        await store.delete("task_123")
        mock_redis.delete.assert_called_once_with("task:task_123")

    @pytest.mark.asyncio
    async def test_delete_task_not_exists(self, mock_redis):
        """Test deleting a non-existent task."""
        mock_redis.delete.return_value = 0

        store = RedisTaskStore(mock_redis)
        await store.delete("nonexistent")
        mock_redis.delete.assert_called_once_with("task:nonexistent")

    @pytest.mark.asyncio
    async def test_list_task_ids(self, mock_redis):
        """Test listing task IDs."""
        mock_redis.keys.return_value = [b"task:123", b"task:456", b"task:789"]

        store = RedisTaskStore(mock_redis)
        result = await store.list_task_ids()

        assert result == ["123", "456", "789"]
        mock_redis.keys.assert_called_once_with("task:*")

    @pytest.mark.asyncio
    async def test_list_task_ids_with_pattern(self, mock_redis):
        """Test listing task IDs with pattern."""
        mock_redis.keys.return_value = [b"task:user_123", b"task:user_456"]

        store = RedisTaskStore(mock_redis)
        result = await store.list_task_ids("user_*")

        assert result == ["user_123", "user_456"]
        mock_redis.keys.assert_called_once_with("task:user_*")

    @pytest.mark.asyncio
    async def test_task_exists(self, mock_redis):
        """Test checking if task exists."""
        mock_redis.exists.return_value = True

        store = RedisTaskStore(mock_redis)
        result = await store.task_exists("task_123")

        assert result is True
        mock_redis.exists.assert_called_once_with("task:task_123")


class TestRedisTaskStoreIntegration:
    """Integration tests for RedisTaskStore with real Redis."""

    @pytest.mark.asyncio
    async def test_full_task_lifecycle(self, task_store, sample_task_data):
        """Test complete task lifecycle with real Redis."""
        from a2a.types import Task, TaskStatus, TaskState

        # Create task with different ID to avoid conflicts
        task_data = sample_task_data.copy()
        task_data["id"] = "integration_test_task"
        task = Task(**task_data)
        task_id = task.id

        # Task should not exist initially
        assert not await task_store.task_exists(task_id)
        assert await task_store.get(task_id) is None

        # Save task
        await task_store.save(task)
        assert await task_store.task_exists(task_id)

        # Retrieve task
        retrieved_task = await task_store.get(task_id)
        assert retrieved_task is not None
        assert retrieved_task.id == task.id
        assert retrieved_task.status.state == task.status.state
        assert retrieved_task.metadata == task.metadata

        # Update task
        updates = {
            "status": TaskStatus(state=TaskState.working),
            "metadata": {"progress": 50},
        }
        assert await task_store.update_task(task_id, updates)

        updated_task = await task_store.get(task_id)
        assert updated_task is not None
        assert updated_task.status.state == TaskState.working
        # Check that metadata was updated
        assert updated_task.metadata["progress"] == 50

        # List tasks
        task_list = await task_store.list_task_ids()
        assert task_id in task_list

        # Delete task
        await task_store.delete(task_id)
        assert not await task_store.task_exists(task_id)
        assert await task_store.get(task_id) is None


class TestRedisJSONTaskStore:
    """Tests for RedisJSONTaskStore."""

    def test_init(self, mock_redis):
        """Test RedisJSONTaskStore initialization."""
        store = RedisJSONTaskStore(mock_redis, prefix="json:")
        assert store.redis == mock_redis
        assert store.prefix == "json:"

    @pytest.mark.asyncio
    async def test_save_task(self, mock_redis, sample_task_data):
        """Test task saving with JSON."""
        from a2a.types import Task

        # Create Task object from sample data
        task = Task(**sample_task_data)

        store = RedisJSONTaskStore(mock_redis)
        await store.save(task)

        mock_redis.json.assert_called_once()
        # The save method serializes the task using model_dump()
        expected_data = task.model_dump()
        # Get the mock json object that was already set up in conftest
        mock_json = mock_redis.json.return_value
        mock_json.set.assert_called_once_with("task:task_123", "$", expected_data)

    @pytest.mark.asyncio
    async def test_get_task_exists(self, mock_redis, sample_task_data):
        """Test retrieving an existing task with JSON."""
        from a2a.types import Task

        # Get the mock json object that was already set up in conftest
        mock_json = mock_redis.json.return_value
        mock_json.get.return_value = sample_task_data

        store = RedisJSONTaskStore(mock_redis)
        result = await store.get("task_123")

        assert isinstance(result, Task)
        assert result.id == "task_123"
        assert result.context_id == "context_456"
        mock_json.get.assert_called_once_with("task:task_123")

    @pytest.mark.asyncio
    async def test_get_task_redis_error(self, mock_redis):
        """Test retrieving task when Redis JSON operation fails."""
        mock_json = MagicMock()
        mock_json.get.side_effect = Exception("Redis error")
        mock_redis.json.return_value = mock_json

        store = RedisJSONTaskStore(mock_redis)
        result = await store.get("task_123")

        assert result is None

    @pytest.mark.asyncio
    async def test_delete_task(self, mock_redis):
        """Test task deletion with JSON."""
        mock_redis.delete.return_value = 1

        store = RedisJSONTaskStore(mock_redis)
        await store.delete("task_123")
        mock_redis.delete.assert_called_once_with("task:task_123")

    @pytest.mark.asyncio
    async def test_update_task_exists(self, mock_redis, sample_task_data):
        """Test updating an existing task with JSON."""
        # Get the mock json object that was already set up in conftest
        mock_json = mock_redis.json.return_value
        mock_json.get.return_value = sample_task_data

        from a2a.types import TaskStatus, TaskState

        store = RedisJSONTaskStore(mock_redis)
        updates = {"status": TaskStatus(state=TaskState.completed)}
        result = await store.update_task("task_123", updates)

        assert result is True
        # Should fetch, update, and save
        mock_json.get.assert_called_once_with("task:task_123")
        mock_json.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_task_not_exists(self, mock_redis):
        """Test updating a non-existent task with JSON."""
        # Get the mock json object that was already set up in conftest
        mock_json = mock_redis.json.return_value
        mock_json.get.return_value = None

        store = RedisJSONTaskStore(mock_redis)
        result = await store.update_task("nonexistent", {"status": "completed"})

        assert result is False

    @pytest.mark.asyncio
    async def test_list_task_ids(self, mock_redis):
        """Test listing task IDs."""
        mock_redis.keys.return_value = [b"task:123", b"task:456"]

        store = RedisJSONTaskStore(mock_redis)
        result = await store.list_task_ids()

        assert result == ["123", "456"]
        mock_redis.keys.assert_called_once_with("task:*")

    @pytest.mark.asyncio
    async def test_task_exists(self, mock_redis):
        """Test checking if task exists."""
        mock_redis.exists.return_value = True

        store = RedisJSONTaskStore(mock_redis)
        result = await store.task_exists("task_123")

        assert result is True
        mock_redis.exists.assert_called_once_with("task:task_123")
