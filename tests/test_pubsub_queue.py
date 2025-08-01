"""Tests for RedisPubSubEventQueue."""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock

from a2a_redis.pubsub_queue import RedisPubSubEventQueue


class TestRedisPubSubEventQueue:
    """Tests for RedisPubSubEventQueue."""

    def test_init(self, mock_redis):
        """Test RedisPubSubEventQueue initialization."""
        queue = RedisPubSubEventQueue(mock_redis, "task_123", prefix="test:")
        assert queue.redis == mock_redis
        assert queue.task_id == "task_123"
        assert queue.prefix == "test:"
        assert queue._channel == "test:task_123"
        assert not queue._closed
        assert queue._pubsub is None  # Lazy initialization
        assert not queue._setup_complete

    @pytest.mark.asyncio
    async def test_enqueue_event_simple(self, mock_redis):
        """Test enqueueing a simple event."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_redis.pubsub.return_value = mock_pubsub
        mock_redis.publish = AsyncMock()

        queue = RedisPubSubEventQueue(mock_redis, "task_123")

        # Mock event data
        event = {"type": "test", "data": "sample"}
        await queue.enqueue_event(event)

        # Should call PUBLISH with event data
        mock_redis.publish.assert_called_once()
        call_args = mock_redis.publish.call_args
        assert call_args[0][0] == "pubsub:task_123"  # channel

        # Verify event data structure
        message = json.loads(call_args[0][1])
        assert message["event_type"] == "dict"
        assert message["event_data"] == event

    @pytest.mark.asyncio
    async def test_enqueue_event_with_model_dump(self, mock_redis):
        """Test enqueueing event with model_dump method."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_redis.pubsub.return_value = mock_pubsub
        mock_redis.publish = AsyncMock()

        queue = RedisPubSubEventQueue(mock_redis, "task_123")

        # Mock Pydantic-like object
        mock_event = MagicMock()
        mock_event.model_dump.return_value = {"field": "value"}
        type(mock_event).__name__ = "MockEvent"

        await queue.enqueue_event(mock_event)

        mock_redis.publish.assert_called_once()
        call_args = mock_redis.publish.call_args
        message = json.loads(call_args[0][1])
        assert message["event_type"] == "MockEvent"
        assert message["event_data"] == {"field": "value"}

    @pytest.mark.asyncio
    async def test_enqueue_event_closed_queue(self, mock_redis):
        """Test enqueueing to closed queue raises error."""
        queue = RedisPubSubEventQueue(mock_redis, "task_123")
        queue._closed = True

        with pytest.raises(RuntimeError, match="Cannot enqueue to closed queue"):
            await queue.enqueue_event({"test": "data"})

    @pytest.mark.asyncio
    async def test_dequeue_event_no_wait_timeout(self, mock_redis):
        """Test dequeueing with no_wait=True when no messages available."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.get_message = AsyncMock(return_value=None)
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123")

        with pytest.raises(RuntimeError, match="No events available"):
            await queue.dequeue_event(no_wait=True)

    @pytest.mark.asyncio
    async def test_dequeue_event_closed_queue(self, mock_redis):
        """Test dequeueing from closed queue raises error."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123")
        await queue.close()

        with pytest.raises(RuntimeError, match="Cannot dequeue from closed queue"):
            await queue.dequeue_event()

    @pytest.mark.asyncio
    async def test_close_queue(self, mock_redis):
        """Test closing queue."""
        # Mock pubsub to avoid actual Redis calls during init
        mock_pubsub = MagicMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()
        mock_redis.pubsub.return_value = mock_pubsub

        queue = RedisPubSubEventQueue(mock_redis, "task_123")
        assert not queue.is_closed()

        # Set up pubsub to simulate it being initialized
        queue._pubsub = mock_pubsub
        queue._setup_complete = True

        await queue.close()
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
