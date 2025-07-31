"""Tests for Redis Streams queue manager and event queue implementations."""

import pytest
from unittest.mock import MagicMock

from a2a_redis.streams_queue_manager import RedisStreamsQueueManager
from a2a_redis.streams_queue import RedisStreamsEventQueue
from a2a_redis.streams_consumer_strategy import (
    ConsumerGroupStrategy,
    ConsumerGroupConfig,
)


class TestRedisStreamsEventQueue:
    """Tests for RedisStreamsEventQueue."""

    def test_init(self, mock_redis):
        """Test RedisStreamsEventQueue initialization."""
        queue = RedisStreamsEventQueue(mock_redis, "task_123")
        assert queue.redis == mock_redis
        assert queue.task_id == "task_123"
        assert queue.prefix == "stream:"
        assert queue._stream_key == "stream:task_123"
        assert not queue._closed

        # Consumer group is created on first use, not during initialization
        mock_redis.xgroup_create.assert_not_called()

    def test_init_with_custom_prefix(self, mock_redis):
        """Test RedisStreamsEventQueue with custom prefix."""
        queue = RedisStreamsEventQueue(mock_redis, "task_123", prefix="custom:")
        assert queue.prefix == "custom:"
        assert queue._stream_key == "custom:task_123"

    def test_init_with_consumer_config(self, mock_redis):
        """Test RedisStreamsEventQueue with consumer configuration."""
        config = ConsumerGroupConfig(
            strategy=ConsumerGroupStrategy.SHARED_LOAD_BALANCING
        )
        queue = RedisStreamsEventQueue(mock_redis, "task_123", consumer_config=config)
        assert queue.consumer_config == config

    @pytest.mark.asyncio
    async def test_enqueue_event_simple(self, mock_redis):
        """Test enqueueing a simple event."""
        queue = RedisStreamsEventQueue(mock_redis, "task_123")

        event_data = {"type": "test", "data": "sample"}
        await queue.enqueue_event(event_data)

        # Verify xadd was called with proper structure
        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        assert call_args[0][0] == "stream:task_123"  # stream key

        fields = call_args[0][1]
        assert "event_type" in fields
        assert "event_data" in fields
        assert fields["event_type"] == "dict"

    @pytest.mark.asyncio
    async def test_enqueue_event_with_model_dump(self, mock_redis, sample_task_data):
        """Test enqueueing an event with model_dump method."""
        queue = RedisStreamsEventQueue(mock_redis, "task_123")

        # Create a mock object with model_dump
        event = MagicMock()
        event.model_dump.return_value = sample_task_data

        await queue.enqueue_event(event)

        # Verify model_dump was called and data was serialized
        event.model_dump.assert_called_once()
        mock_redis.xadd.assert_called_once()

    @pytest.mark.asyncio
    async def test_enqueue_event_closed_queue(self, mock_redis):
        """Test enqueueing to a closed queue raises error."""
        queue = RedisStreamsEventQueue(mock_redis, "task_123")
        queue._closed = True

        with pytest.raises(RuntimeError, match="Cannot enqueue to closed queue"):
            await queue.enqueue_event({"test": "data"})

    @pytest.mark.asyncio
    async def test_dequeue_event_success(self, mock_redis):
        """Test successful event dequeuing."""
        queue = RedisStreamsEventQueue(mock_redis, "task_123")

        # Mock xreadgroup response
        mock_redis.xreadgroup.return_value = [
            (
                b"stream:task_123",
                [
                    (
                        b"1234567890-0",
                        {
                            b"event_type": b"dict",
                            b"event_data": b'{"type": "test", "data": "sample"}',
                        },
                    )
                ],
            )
        ]

        result = await queue.dequeue_event(no_wait=True)

        # Verify proper calls were made
        mock_redis.xreadgroup.assert_called_once()
        mock_redis.xack.assert_called_once_with(
            "stream:task_123", queue.consumer_group, b"1234567890-0"
        )

        # Verify returned data
        assert result == {"type": "test", "data": "sample"}

    @pytest.mark.asyncio
    async def test_dequeue_event_no_wait_timeout(self, mock_redis):
        """Test dequeuing with no_wait timeout."""
        queue = RedisStreamsEventQueue(mock_redis, "task_123")
        mock_redis.xreadgroup.return_value = []

        with pytest.raises(RuntimeError, match="No events available"):
            await queue.dequeue_event(no_wait=True)

        # Verify timeout was set to 0 (non-blocking)
        call_args = mock_redis.xreadgroup.call_args
        assert call_args[1]["block"] == 0

    @pytest.mark.asyncio
    async def test_dequeue_event_closed_queue(self, mock_redis):
        """Test dequeuing from a closed queue raises error."""
        queue = RedisStreamsEventQueue(mock_redis, "task_123")
        queue._closed = True

        with pytest.raises(RuntimeError, match="Cannot dequeue from closed queue"):
            await queue.dequeue_event()

    @pytest.mark.asyncio
    async def test_close_queue(self, mock_redis):
        """Test closing the queue."""
        queue = RedisStreamsEventQueue(mock_redis, "task_123")

        # Mock pending messages
        mock_redis.xpending_range.return_value = [
            {"message_id": b"123-0"},
            {"message_id": b"124-0"},
        ]

        await queue.close()

        assert queue._closed
        # Verify pending messages were acknowledged
        mock_redis.xpending_range.assert_called_once()
        mock_redis.xack.assert_called_once_with(
            "stream:task_123", queue.consumer_group, b"123-0", b"124-0"
        )

    def test_tap_queue(self, mock_redis):
        """Test creating a tap of the queue."""
        queue = RedisStreamsEventQueue(mock_redis, "task_123")
        tap = queue.tap()

        assert isinstance(tap, RedisStreamsEventQueue)
        assert tap.redis == mock_redis
        assert tap.task_id == "task_123"
        assert tap.prefix == queue.prefix
        assert tap is not queue  # Should be a different instance

    def test_task_done(self, mock_redis):
        """Test task_done method (no-op for streams)."""
        queue = RedisStreamsEventQueue(mock_redis, "task_123")
        queue.task_done()  # Should not raise any errors


class TestRedisStreamsQueueManager:
    """Tests for RedisStreamsQueueManager."""

    def test_init(self, mock_redis):
        """Test RedisStreamsQueueManager initialization."""
        manager = RedisStreamsQueueManager(mock_redis)
        assert manager.redis == mock_redis
        assert manager.prefix == "stream:"
        assert isinstance(manager.consumer_config, ConsumerGroupConfig)
        assert manager._queues == {}

    def test_init_with_custom_prefix(self, mock_redis):
        """Test initialization with custom prefix."""
        manager = RedisStreamsQueueManager(mock_redis, prefix="custom:")
        assert manager.prefix == "custom:"

    def test_init_with_consumer_config(self, mock_redis):
        """Test initialization with consumer configuration."""
        config = ConsumerGroupConfig(
            strategy=ConsumerGroupStrategy.SHARED_LOAD_BALANCING
        )
        manager = RedisStreamsQueueManager(mock_redis, consumer_config=config)
        assert manager.consumer_config == config

    @pytest.mark.asyncio
    async def test_add_queue(self, mock_redis):
        """Test adding a queue for a task."""
        manager = RedisStreamsQueueManager(mock_redis)

        # Add should create a new queue
        await manager.add("task_123", None)

        assert "task_123" in manager._queues
        assert isinstance(manager._queues["task_123"], RedisStreamsEventQueue)

    @pytest.mark.asyncio
    async def test_create_or_tap_new_queue(self, mock_redis):
        """Test creating a new queue."""
        manager = RedisStreamsQueueManager(mock_redis)

        queue = await manager.create_or_tap("task_123")

        assert isinstance(queue, RedisStreamsEventQueue)
        assert queue.task_id == "task_123"
        assert "task_123" in manager._queues

    @pytest.mark.asyncio
    async def test_create_or_tap_existing_queue(self, mock_redis):
        """Test getting existing queue."""
        manager = RedisStreamsQueueManager(mock_redis)

        # Create initial queue
        queue1 = await manager.create_or_tap("task_123")
        queue2 = await manager.create_or_tap("task_123")

        assert queue1 is queue2  # Should return same instance

    @pytest.mark.asyncio
    async def test_get_existing_queue(self, mock_redis):
        """Test getting existing queue."""
        manager = RedisStreamsQueueManager(mock_redis)

        # Create queue first
        await manager.create_or_tap("task_123")

        queue = await manager.get("task_123")
        assert isinstance(queue, RedisStreamsEventQueue)

    @pytest.mark.asyncio
    async def test_get_nonexistent_queue(self, mock_redis):
        """Test getting non-existent queue."""
        manager = RedisStreamsQueueManager(mock_redis)

        queue = await manager.get("nonexistent")
        assert queue is None

    @pytest.mark.asyncio
    async def test_tap_existing_queue(self, mock_redis):
        """Test tapping existing queue."""
        manager = RedisStreamsQueueManager(mock_redis)

        # Create queue first
        await manager.create_or_tap("task_123")

        tap = await manager.tap("task_123")
        assert isinstance(tap, RedisStreamsEventQueue)
        assert tap.task_id == "task_123"

    @pytest.mark.asyncio
    async def test_tap_nonexistent_queue(self, mock_redis):
        """Test tapping non-existent queue."""
        manager = RedisStreamsQueueManager(mock_redis)

        tap = await manager.tap("nonexistent")
        assert tap is None

    @pytest.mark.asyncio
    async def test_close_queue(self, mock_redis):
        """Test closing a queue."""
        manager = RedisStreamsQueueManager(mock_redis)

        # Create queue first
        await manager.create_or_tap("task_123")

        # Close it
        await manager.close("task_123")

        assert "task_123" not in manager._queues

    @pytest.mark.asyncio
    async def test_close_nonexistent_queue(self, mock_redis):
        """Test closing non-existent queue."""
        manager = RedisStreamsQueueManager(mock_redis)

        # Should not raise error
        await manager.close("nonexistent")


class TestRedisStreamsQueueManagerIntegration:
    """Integration tests for RedisStreamsQueueManager with real Redis."""

    @pytest.mark.asyncio
    async def test_queue_lifecycle(self, redis_client):
        """Test complete queue lifecycle with real Redis."""
        manager = RedisStreamsQueueManager(redis_client, prefix="test_stream:")

        # Create queue
        queue = await manager.create_or_tap("integration_test")
        assert isinstance(queue, RedisStreamsEventQueue)

        # Enqueue event
        test_event = {"type": "test", "data": "integration"}
        await queue.enqueue_event(test_event)

        # Create another consumer (tap) to test consumer groups
        tap = await manager.tap("integration_test")

        # Dequeue event
        result = await tap.dequeue_event(no_wait=True)
        assert result == test_event

        # Close queue
        await manager.close("integration_test")

        # Verify queue is removed
        assert await manager.get("integration_test") is None

    @pytest.mark.asyncio
    async def test_multiple_consumers(self, redis_client):
        """Test multiple consumers sharing work."""
        manager = RedisStreamsQueueManager(redis_client, prefix="test_stream:")

        # Create queue and tap
        queue = await manager.create_or_tap("multi_consumer_test")
        tap = await manager.tap("multi_consumer_test")

        # Enqueue multiple events
        for i in range(5):
            await queue.enqueue_event({"id": i, "data": f"event_{i}"})

        # Both consumers should be able to get events
        events = []
        for _ in range(5):
            try:
                event = await queue.dequeue_event(no_wait=True)
                events.append(event)
            except RuntimeError:
                try:
                    event = await tap.dequeue_event(no_wait=True)
                    events.append(event)
                except RuntimeError:
                    break

        assert len(events) == 5

        # Clean up
        await manager.close("multi_consumer_test")
