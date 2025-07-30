"""Tests for RedisQueueManager and RedisEventQueue."""

import json
import pytest
from unittest.mock import MagicMock

from a2a_redis.queue_manager import RedisQueueManager, RedisEventQueue


class TestRedisEventQueue:
    """Tests for RedisEventQueue."""

    def test_init(self, mock_redis):
        """Test RedisEventQueue initialization."""
        queue = RedisEventQueue(mock_redis, "task_123", prefix="test:")
        assert queue.redis == mock_redis
        assert queue.task_id == "task_123"
        assert queue.prefix == "test:"
        assert queue._queue_key == "test:task_123"
        assert not queue._closed

    def test_enqueue_event_simple(self, mock_redis):
        """Test enqueueing a simple event."""
        queue = RedisEventQueue(mock_redis, "task_123")

        # Mock event data
        event = {"type": "test", "data": "sample"}
        queue.enqueue_event(event)

        # Should call LPUSH with serialized event
        mock_redis.lpush.assert_called_once()
        call_args = mock_redis.lpush.call_args
        assert call_args[0][0] == "queue:task_123"  # queue key

        # Verify serialized data
        serialized_data = json.loads(call_args[0][1])
        assert serialized_data["type"] == "dict"
        assert serialized_data["data"] == event

    def test_enqueue_event_with_model_dump(self, mock_redis):
        """Test enqueueing event with model_dump method."""
        queue = RedisEventQueue(mock_redis, "task_123")

        # Mock Pydantic-like object
        mock_event = MagicMock()
        mock_event.model_dump.return_value = {"field": "value"}
        type(mock_event).__name__ = "MockEvent"

        queue.enqueue_event(mock_event)

        mock_redis.lpush.assert_called_once()
        call_args = mock_redis.lpush.call_args
        serialized_data = json.loads(call_args[0][1])
        assert serialized_data["type"] == "MockEvent"
        assert serialized_data["data"] == {"field": "value"}

    def test_enqueue_event_closed_queue(self, mock_redis):
        """Test enqueueing to closed queue raises error."""
        queue = RedisEventQueue(mock_redis, "task_123")
        queue.close()

        with pytest.raises(RuntimeError, match="Cannot enqueue to closed queue"):
            queue.enqueue_event({"test": "data"})

    def test_dequeue_event_no_wait(self, mock_redis):
        """Test dequeueing with no_wait=True."""
        queue = RedisEventQueue(mock_redis, "task_123")

        # Mock Redis response
        mock_event_data = json.dumps({"type": "TestEvent", "data": {"field": "value"}})
        mock_redis.rpop.return_value = mock_event_data.encode()

        result = queue.dequeue_event(no_wait=True)

        assert result == {"field": "value"}
        mock_redis.rpop.assert_called_once_with("queue:task_123")

    def test_dequeue_event_no_wait_empty(self, mock_redis):
        """Test dequeueing with no_wait=True when queue is empty."""
        queue = RedisEventQueue(mock_redis, "task_123")
        mock_redis.rpop.return_value = None

        with pytest.raises(RuntimeError, match="No events available"):
            queue.dequeue_event(no_wait=True)

    def test_dequeue_event_blocking(self, mock_redis):
        """Test dequeueing with blocking."""
        queue = RedisEventQueue(mock_redis, "task_123")

        # Mock Redis BRPOP response
        mock_event_data = json.dumps({"type": "TestEvent", "data": {"field": "value"}})
        mock_redis.brpop.return_value = ("queue:task_123", mock_event_data.encode())

        result = queue.dequeue_event(no_wait=False)

        assert result == {"field": "value"}
        mock_redis.brpop.assert_called_once_with("queue:task_123", timeout=1)

    def test_dequeue_event_blocking_timeout(self, mock_redis):
        """Test dequeueing with blocking timeout."""
        queue = RedisEventQueue(mock_redis, "task_123")
        mock_redis.brpop.return_value = None

        with pytest.raises(RuntimeError, match="No events available"):
            queue.dequeue_event(no_wait=False)

    def test_dequeue_event_closed_queue(self, mock_redis):
        """Test dequeueing from closed queue raises error."""
        queue = RedisEventQueue(mock_redis, "task_123")
        queue.close()

        with pytest.raises(RuntimeError, match="Cannot dequeue from closed queue"):
            queue.dequeue_event()

    def test_close_queue(self, mock_redis):
        """Test closing queue."""
        queue = RedisEventQueue(mock_redis, "task_123")
        assert not queue.is_closed()

        queue.close()
        assert queue.is_closed()

    def test_tap_queue(self, mock_redis):
        """Test creating a tap of the queue."""
        queue = RedisEventQueue(mock_redis, "task_123", prefix="test:")
        tap = queue.tap()

        assert isinstance(tap, RedisEventQueue)
        assert tap.task_id == "task_123"
        assert tap.prefix == "test:"
        assert tap.redis == mock_redis
        assert not tap.is_closed()

    def test_task_done(self, mock_redis):
        """Test task_done method (no-op for Redis)."""
        queue = RedisEventQueue(mock_redis, "task_123")
        queue.task_done()  # Should not raise any errors


class TestRedisQueueManager:
    """Tests for RedisQueueManager."""

    def test_init(self, mock_redis):
        """Test RedisQueueManager initialization."""
        manager = RedisQueueManager(mock_redis, prefix="test:")
        assert manager.redis == mock_redis
        assert manager.prefix == "test:"
        assert manager._queues == {}

    @pytest.mark.asyncio
    async def test_add_queue(self, mock_redis):
        """Test adding a queue."""
        manager = RedisQueueManager(mock_redis)
        mock_queue = MagicMock()

        await manager.add("task_123", mock_queue)

        assert "task_123" in manager._queues
        assert isinstance(manager._queues["task_123"], RedisEventQueue)

    @pytest.mark.asyncio
    async def test_create_or_tap_new_queue(self, mock_redis):
        """Test creating a new queue."""
        manager = RedisQueueManager(mock_redis)

        queue = await manager.create_or_tap("task_123")

        assert isinstance(queue, RedisEventQueue)
        assert queue.task_id == "task_123"
        assert "task_123" in manager._queues

    @pytest.mark.asyncio
    async def test_create_or_tap_existing_queue(self, mock_redis):
        """Test getting existing queue."""
        manager = RedisQueueManager(mock_redis)

        # Create queue first
        queue1 = await manager.create_or_tap("task_123")
        queue2 = await manager.create_or_tap("task_123")

        assert queue1 is queue2  # Should return same instance

    @pytest.mark.asyncio
    async def test_get_existing_queue(self, mock_redis):
        """Test getting existing queue."""
        manager = RedisQueueManager(mock_redis)

        # Create queue first
        await manager.create_or_tap("task_123")
        queue = await manager.get("task_123")

        assert isinstance(queue, RedisEventQueue)
        assert queue.task_id == "task_123"

    @pytest.mark.asyncio
    async def test_get_nonexistent_queue(self, mock_redis):
        """Test getting non-existent queue."""
        manager = RedisQueueManager(mock_redis)

        queue = await manager.get("nonexistent")

        assert queue is None

    @pytest.mark.asyncio
    async def test_tap_existing_queue(self, mock_redis):
        """Test tapping existing queue."""
        manager = RedisQueueManager(mock_redis)

        # Create queue first
        await manager.create_or_tap("task_123")
        tap = await manager.tap("task_123")

        assert isinstance(tap, RedisEventQueue)
        assert tap.task_id == "task_123"

    @pytest.mark.asyncio
    async def test_tap_nonexistent_queue(self, mock_redis):
        """Test tapping non-existent queue."""
        manager = RedisQueueManager(mock_redis)

        tap = await manager.tap("nonexistent")

        assert tap is None

    @pytest.mark.asyncio
    async def test_close_queue(self, mock_redis):
        """Test closing a queue."""
        manager = RedisQueueManager(mock_redis)

        # Create queue first
        await manager.create_or_tap("task_123")
        assert "task_123" in manager._queues

        await manager.close("task_123")

        assert "task_123" not in manager._queues

    @pytest.mark.asyncio
    async def test_close_nonexistent_queue(self, mock_redis):
        """Test closing non-existent queue."""
        manager = RedisQueueManager(mock_redis)

        # Should not raise error
        await manager.close("nonexistent")

        assert "nonexistent" not in manager._queues


class TestRedisQueueManagerIntegration:
    """Integration tests for RedisQueueManager with real Redis."""

    @pytest.mark.asyncio
    async def test_full_queue_lifecycle(self, redis_client):
        """Test complete queue lifecycle with real Redis."""
        manager = RedisQueueManager(redis_client, prefix="test_queue:")
        task_id = "integration_test"

        # Create queue
        queue = await manager.create_or_tap(task_id)
        assert isinstance(queue, RedisEventQueue)

        # Enqueue event
        test_event = {"type": "test", "message": "Hello Redis!"}
        queue.enqueue_event(test_event)

        # Dequeue event
        retrieved_event = queue.dequeue_event(no_wait=True)
        assert retrieved_event == test_event

        # Close queue
        await manager.close(task_id)
        assert await manager.get(task_id) is None

    @pytest.mark.asyncio
    async def test_queue_persistence(self, redis_client):
        """Test that events persist in Redis."""
        manager1 = RedisQueueManager(redis_client, prefix="persist_test:")
        task_id = "persist_task"

        # Create queue and enqueue event
        queue1 = await manager1.create_or_tap(task_id)
        test_event = {"message": "Persistent event"}
        queue1.enqueue_event(test_event)

        # Create new manager instance (simulating restart)
        manager2 = RedisQueueManager(redis_client, prefix="persist_test:")
        queue2 = await manager2.create_or_tap(task_id)

        # Event should still be there
        retrieved_event = queue2.dequeue_event(no_wait=True)
        assert retrieved_event == test_event

        # Cleanup
        await manager2.close(task_id)
