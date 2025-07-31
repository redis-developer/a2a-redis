"""Tests for RedisPubSubEventQueue."""

import json
import time
import pytest
from unittest.mock import MagicMock

from a2a_redis.pubsub_queue import RedisPubSubEventQueue
from a2a_redis.queue_manager import RedisQueueManager
from a2a_redis.queue_types import QueueType


class TestRedisPubSubEventQueue:
    """Tests for RedisPubSubEventQueue."""

    def test_init(self, mock_redis):
        """Test RedisPubSubEventQueue initialization."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123", prefix="test:")
        assert queue.redis == mock_redis
        assert queue.task_id == "task_123"
        assert queue.prefix == "test:"
        assert queue._channel == "test:task_123"
        assert not queue._closed

        # Should have set up subscription
        mock_redis.pubsub.assert_called_once()
        mock_pubsub.subscribe.assert_called_once_with("test:task_123")

    def test_enqueue_event_simple(self, mock_redis):
        """Test enqueueing a simple event."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123")

        # Mock event data
        event = {"type": "test", "data": "sample"}
        queue.enqueue_event(event)

        # Should call PUBLISH with event data
        mock_redis.publish.assert_called_once()
        call_args = mock_redis.publish.call_args
        assert call_args[0][0] == "pubsub:task_123"  # channel

        # Verify event data structure
        message = json.loads(call_args[0][1])
        assert message["event_type"] == "dict"
        assert message["event_data"] == event

    def test_enqueue_event_with_model_dump(self, mock_redis):
        """Test enqueueing event with model_dump method."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123")

        # Mock Pydantic-like object
        mock_event = MagicMock()
        mock_event.model_dump.return_value = {"field": "value"}
        type(mock_event).__name__ = "MockEvent"

        queue.enqueue_event(mock_event)

        mock_redis.publish.assert_called_once()
        call_args = mock_redis.publish.call_args
        message = json.loads(call_args[0][1])
        assert message["event_type"] == "MockEvent"
        assert message["event_data"] == {"field": "value"}

    def test_enqueue_event_closed_queue(self, mock_redis):
        """Test enqueueing to closed queue raises error."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123")
        queue.close()

        with pytest.raises(RuntimeError, match="Cannot enqueue to closed queue"):
            queue.enqueue_event({"test": "data"})

    def test_dequeue_event_no_wait_timeout(self, mock_redis):
        """Test dequeueing with no_wait=True when no messages available."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123")

        with pytest.raises(RuntimeError, match="No events available"):
            queue.dequeue_event(no_wait=True)

    def test_dequeue_event_closed_queue(self, mock_redis):
        """Test dequeueing from closed queue raises error."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123")
        queue.close()

        with pytest.raises(RuntimeError, match="Cannot dequeue from closed queue"):
            queue.dequeue_event()

    def test_close_queue(self, mock_redis):
        """Test closing queue."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123")
        assert not queue.is_closed()

        queue.close()
        assert queue.is_closed()

        # Should have cleaned up subscription
        mock_pubsub.unsubscribe.assert_called_once_with("pubsub:task_123")
        mock_pubsub.close.assert_called_once()

    def test_tap_queue(self, mock_redis):
        """Test creating a tap of the queue."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123", prefix="test:")
        tap = queue.tap()

        assert isinstance(tap, RedisPubSubEventQueue)
        assert tap.task_id == "task_123"
        assert tap.prefix == "test:"
        assert tap.redis == mock_redis
        assert not tap.is_closed()

    def test_task_done(self, mock_redis):
        """Test task_done method (no-op for pub/sub)."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123")
        queue.task_done()  # Should not raise any errors


class TestRedisQueueManagerPubSub:
    """Tests for RedisQueueManager with pub/sub queues."""

    def test_init_pubsub(self, mock_redis):
        """Test RedisQueueManager initialization with pub/sub."""
        manager = RedisQueueManager(mock_redis, queue_type=QueueType.PUBSUB)
        assert manager.redis == mock_redis
        assert manager.queue_type == QueueType.PUBSUB
        assert manager.prefix == "pubsub:"
        assert manager.consumer_config is None
        assert manager._queues == {}

    def test_init_streams_default(self, mock_redis):
        """Test RedisQueueManager defaults to streams."""
        manager = RedisQueueManager(mock_redis)
        assert manager.queue_type == QueueType.STREAMS
        assert manager.prefix == "stream:"
        assert manager.consumer_config is not None

    @pytest.mark.asyncio
    async def test_create_pubsub_queue(self, mock_redis):
        """Test creating a pub/sub queue."""
        # Mock pubsub to avoid actual Redis calls during queue creation
        mock_pubsub = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub

        manager = RedisQueueManager(mock_redis, queue_type=QueueType.PUBSUB)
        queue = await manager.create_or_tap("task_123")

        assert isinstance(queue, RedisPubSubEventQueue)
        assert queue.task_id == "task_123"
        assert "task_123" in manager._queues

    @pytest.mark.asyncio
    async def test_create_streams_queue(self, mock_redis):
        """Test creating a streams queue (default)."""
        # Mock xgroup_create to avoid actual Redis calls during queue creation
        mock_redis.xgroup_create = MagicMock()

        manager = RedisQueueManager(mock_redis)  # Defaults to STREAMS
        queue = await manager.create_or_tap("task_123")

        # Should be streams implementation
        from a2a_redis.queue_manager import RedisEventQueue

        assert isinstance(queue, RedisEventQueue)
        assert queue.task_id == "task_123"


class TestRedisQueueManagerPubSubIntegration:
    """Integration tests for pub/sub queue manager with real Redis."""

    @pytest.mark.asyncio
    async def test_pubsub_queue_lifecycle(self, redis_client):
        """Test complete pub/sub queue lifecycle with real Redis."""
        manager = RedisQueueManager(
            redis_client, queue_type=QueueType.PUBSUB, prefix="test_pubsub:"
        )
        task_id = "pubsub_integration_test"

        # Create queue
        queue = await manager.create_or_tap(task_id)
        assert isinstance(queue, RedisPubSubEventQueue)

        # Give subscription time to set up
        time.sleep(0.1)

        # Enqueue event
        test_event = {"type": "test", "message": "Hello Pub/Sub!"}
        queue.enqueue_event(test_event)

        # Give message time to be delivered
        time.sleep(0.1)

        # Dequeue event
        retrieved_event = queue.dequeue_event(no_wait=True)
        assert retrieved_event == test_event

        # Close queue
        await manager.close(task_id)
        assert await manager.get(task_id) is None

    @pytest.mark.asyncio
    async def test_pubsub_broadcast_behavior(self, redis_client):
        """Test that pub/sub broadcasts to multiple subscribers."""
        task_id = "broadcast_test"

        # Create two separate queue instances for the same task (simulating multiple subscribers)
        queue1 = RedisPubSubEventQueue(redis_client, task_id, prefix="test_broadcast:")
        queue2 = RedisPubSubEventQueue(redis_client, task_id, prefix="test_broadcast:")

        # Give subscriptions time to set up
        time.sleep(0.1)

        # Publish an event from one queue
        test_event = {"message": "Broadcast message"}
        queue1.enqueue_event(test_event)

        # Give message time to be delivered
        time.sleep(0.1)

        # Both queues should receive the same message
        retrieved_event1 = queue1.dequeue_event(no_wait=True)
        retrieved_event2 = queue2.dequeue_event(no_wait=True)

        assert retrieved_event1 == test_event
        assert retrieved_event2 == test_event

        # Cleanup
        queue1.close()
        queue2.close()
