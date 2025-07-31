"""Tests for RedisQueueManager and RedisEventQueue."""

import json
import pytest
from unittest.mock import MagicMock

from a2a_redis.queue_manager import RedisQueueManager, RedisEventQueue
from a2a_redis.streams_consumer_strategy import (
    ConsumerGroupStrategy,
    ConsumerGroupConfig,
)


class TestRedisEventQueue:
    """Tests for RedisEventQueue."""

    def test_init(self, mock_redis):
        """Test RedisEventQueue initialization."""
        # Mock xgroup_create to avoid actual Redis calls during init
        mock_redis.xgroup_create = MagicMock()

        queue = RedisEventQueue(mock_redis, "task_123", prefix="test:")
        assert queue.redis == mock_redis
        assert queue.task_id == "task_123"
        assert queue.prefix == "test:"
        assert queue._stream_key == "test:task_123"
        assert not queue._closed
        assert queue.consumer_group == "processors-task_123"

        # Should have tried to create consumer group
        mock_redis.xgroup_create.assert_called_once()

    def test_enqueue_event_simple(self, mock_redis):
        """Test enqueueing a simple event."""
        # Mock xgroup_create to avoid actual Redis calls during init
        mock_redis.xgroup_create = MagicMock()
        queue = RedisEventQueue(mock_redis, "task_123")

        # Mock event data
        event = {"type": "test", "data": "sample"}
        queue.enqueue_event(event)

        # Should call XADD with event data
        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        assert call_args[0][0] == "stream:task_123"  # stream key

        # Verify event data structure
        fields = call_args[0][1]
        assert fields["event_type"] == "dict"
        assert json.loads(fields["event_data"]) == event

    def test_enqueue_event_with_model_dump(self, mock_redis):
        """Test enqueueing event with model_dump method."""
        # Mock xgroup_create to avoid actual Redis calls during init
        mock_redis.xgroup_create = MagicMock()
        queue = RedisEventQueue(mock_redis, "task_123")

        # Mock Pydantic-like object
        mock_event = MagicMock()
        mock_event.model_dump.return_value = {"field": "value"}
        type(mock_event).__name__ = "MockEvent"

        queue.enqueue_event(mock_event)

        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        fields = call_args[0][1]
        assert fields["event_type"] == "MockEvent"
        assert json.loads(fields["event_data"]) == {"field": "value"}

    def test_enqueue_event_closed_queue(self, mock_redis):
        """Test enqueueing to closed queue raises error."""
        # Mock xgroup_create and xpending_range for close operation
        mock_redis.xgroup_create = MagicMock()
        mock_redis.xpending_range = MagicMock(return_value=[])

        queue = RedisEventQueue(mock_redis, "task_123")
        queue.close()

        with pytest.raises(RuntimeError, match="Cannot enqueue to closed queue"):
            queue.enqueue_event({"test": "data"})

    def test_dequeue_event_no_wait(self, mock_redis):
        """Test dequeueing with no_wait=True."""
        # Mock xgroup_create to avoid actual Redis calls during init
        mock_redis.xgroup_create = MagicMock()
        queue = RedisEventQueue(mock_redis, "task_123")

        # Mock Redis streams response
        mock_event_data = json.dumps({"field": "value"})
        mock_redis.xreadgroup.return_value = [
            (
                b"stream:task_123",
                [
                    (
                        b"1234567890-0",
                        {
                            b"event_type": b"TestEvent",
                            b"event_data": mock_event_data.encode(),
                        },
                    )
                ],
            )
        ]
        mock_redis.xack = MagicMock()

        result = queue.dequeue_event(no_wait=True)

        assert result == {"field": "value"}
        mock_redis.xreadgroup.assert_called_once()
        mock_redis.xack.assert_called_once()

    def test_dequeue_event_no_wait_empty(self, mock_redis):
        """Test dequeueing with no_wait=True when queue is empty."""
        # Mock xgroup_create to avoid actual Redis calls during init
        mock_redis.xgroup_create = MagicMock()
        queue = RedisEventQueue(mock_redis, "task_123")

        # Mock empty streams response
        mock_redis.xreadgroup.return_value = []

        with pytest.raises(RuntimeError, match="No events available"):
            queue.dequeue_event(no_wait=True)

    def test_dequeue_event_blocking(self, mock_redis):
        """Test dequeueing with blocking."""
        # Mock xgroup_create to avoid actual Redis calls during init
        mock_redis.xgroup_create = MagicMock()
        queue = RedisEventQueue(mock_redis, "task_123")

        # Mock Redis streams response with blocking
        mock_event_data = json.dumps({"field": "value"})
        mock_redis.xreadgroup.return_value = [
            (
                b"stream:task_123",
                [
                    (
                        b"1234567890-0",
                        {
                            b"event_type": b"TestEvent",
                            b"event_data": mock_event_data.encode(),
                        },
                    )
                ],
            )
        ]
        mock_redis.xack = MagicMock()

        result = queue.dequeue_event(no_wait=False)

        assert result == {"field": "value"}
        # Verify blocking call with 1000ms timeout
        args, kwargs = mock_redis.xreadgroup.call_args
        assert kwargs.get("block") == 1000

    def test_dequeue_event_blocking_timeout(self, mock_redis):
        """Test dequeueing with blocking timeout."""
        # Mock xgroup_create to avoid actual Redis calls during init
        mock_redis.xgroup_create = MagicMock()
        queue = RedisEventQueue(mock_redis, "task_123")

        # Mock timeout response (empty result)
        mock_redis.xreadgroup.return_value = []

        with pytest.raises(RuntimeError, match="No events available"):
            queue.dequeue_event(no_wait=False)

    def test_dequeue_event_closed_queue(self, mock_redis):
        """Test dequeueing from closed queue raises error."""
        # Mock xgroup_create and xpending_range for close operation
        mock_redis.xgroup_create = MagicMock()
        mock_redis.xpending_range = MagicMock(return_value=[])

        queue = RedisEventQueue(mock_redis, "task_123")
        queue.close()

        with pytest.raises(RuntimeError, match="Cannot dequeue from closed queue"):
            queue.dequeue_event()

    def test_close_queue(self, mock_redis):
        """Test closing queue."""
        # Mock xgroup_create and xpending_range for close operation
        mock_redis.xgroup_create = MagicMock()
        mock_redis.xpending_range = MagicMock(return_value=[])

        queue = RedisEventQueue(mock_redis, "task_123")
        assert not queue.is_closed()

        queue.close()
        assert queue.is_closed()

    def test_tap_queue(self, mock_redis):
        """Test creating a tap of the queue."""
        # Mock xgroup_create to avoid actual Redis calls during init
        mock_redis.xgroup_create = MagicMock()

        queue = RedisEventQueue(mock_redis, "task_123", prefix="test:")
        tap = queue.tap()

        assert isinstance(tap, RedisEventQueue)
        assert tap.task_id == "task_123"
        assert tap.prefix == "test:"
        assert tap.redis == mock_redis
        assert not tap.is_closed()

    def test_task_done(self, mock_redis):
        """Test task_done method (no-op for Redis)."""
        # Mock xgroup_create to avoid actual Redis calls during init
        mock_redis.xgroup_create = MagicMock()

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
        assert manager.consumer_config is not None

    @pytest.mark.asyncio
    async def test_add_queue(self, mock_redis):
        """Test adding a queue."""
        # Mock xgroup_create to avoid actual Redis calls during queue creation
        mock_redis.xgroup_create = MagicMock()

        manager = RedisQueueManager(mock_redis)
        mock_queue = MagicMock()

        await manager.add("task_123", mock_queue)

        assert "task_123" in manager._queues
        assert isinstance(manager._queues["task_123"], RedisEventQueue)

    @pytest.mark.asyncio
    async def test_create_or_tap_new_queue(self, mock_redis):
        """Test creating a new queue."""
        # Mock xgroup_create to avoid actual Redis calls during queue creation
        mock_redis.xgroup_create = MagicMock()

        manager = RedisQueueManager(mock_redis)

        queue = await manager.create_or_tap("task_123")

        assert isinstance(queue, RedisEventQueue)
        assert queue.task_id == "task_123"
        assert "task_123" in manager._queues

    @pytest.mark.asyncio
    async def test_create_or_tap_existing_queue(self, mock_redis):
        """Test getting existing queue."""
        # Mock xgroup_create to avoid actual Redis calls during queue creation
        mock_redis.xgroup_create = MagicMock()

        manager = RedisQueueManager(mock_redis)

        # Create queue first
        queue1 = await manager.create_or_tap("task_123")
        queue2 = await manager.create_or_tap("task_123")

        assert queue1 is queue2  # Should return same instance

    @pytest.mark.asyncio
    async def test_get_existing_queue(self, mock_redis):
        """Test getting existing queue."""
        # Mock xgroup_create to avoid actual Redis calls during queue creation
        mock_redis.xgroup_create = MagicMock()

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
        # Mock xgroup_create to avoid actual Redis calls during queue creation
        mock_redis.xgroup_create = MagicMock()

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
        # Mock xgroup_create and xpending_range for queue operations
        mock_redis.xgroup_create = MagicMock()
        mock_redis.xpending_range = MagicMock(return_value=[])

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
        manager = RedisQueueManager(redis_client, prefix="test_stream:")
        task_id = "integration_test"

        # Create queue
        queue = await manager.create_or_tap(task_id)
        assert isinstance(queue, RedisEventQueue)

        # Enqueue event
        test_event = {"type": "test", "message": "Hello Redis Streams!"}
        queue.enqueue_event(test_event)

        # Dequeue event
        retrieved_event = queue.dequeue_event(no_wait=True)
        assert retrieved_event == test_event

        # Close queue
        await manager.close(task_id)
        assert await manager.get(task_id) is None

    @pytest.mark.asyncio
    async def test_queue_persistence(self, redis_client):
        """Test that events persist in Redis Streams."""
        manager1 = RedisQueueManager(redis_client, prefix="persist_stream:")
        task_id = "persist_task"

        # Create queue and enqueue event
        queue1 = await manager1.create_or_tap(task_id)
        test_event = {"message": "Persistent stream event"}
        queue1.enqueue_event(test_event)

        # Create new manager instance (simulating restart)
        # Use different consumer config to ensure we get a different consumer ID
        config = ConsumerGroupConfig(
            strategy=ConsumerGroupStrategy.SHARED_LOAD_BALANCING
        )
        manager2 = RedisQueueManager(
            redis_client, prefix="persist_stream:", consumer_config=config
        )
        queue2 = await manager2.create_or_tap(task_id)

        # Event should still be there (from the stream)
        retrieved_event = queue2.dequeue_event(no_wait=True)
        assert retrieved_event == test_event

        # Cleanup
        await manager2.close(task_id)
