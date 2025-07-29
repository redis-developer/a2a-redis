"""Tests for RedisQueueManager."""

import json
import pytest
from unittest.mock import MagicMock, patch
import redis

from a2a_redis.queue_manager import RedisQueueManager


class TestRedisQueueManager:
    """Tests for RedisQueueManager."""
    
    def test_init(self, mock_redis):
        """Test RedisQueueManager initialization."""
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(
                mock_redis,
                stream_name="test_stream",
                consumer_group="test_group",
                max_len=1000
            )
            assert manager.redis == mock_redis
            assert manager.stream_name == "test_stream"
            assert manager.consumer_group == "test_group"
            assert manager.max_len == 1000
    
    def test_ensure_consumer_group_success(self, mock_redis):
        """Test consumer group creation success."""
        mock_redis.xgroup_create.return_value = True
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group', wraps=RedisQueueManager._ensure_consumer_group):
            manager = RedisQueueManager(mock_redis)
            manager._ensure_consumer_group()
            
            mock_redis.xgroup_create.assert_called_once_with(
                "a2a:events", "a2a-agents", id="0", mkstream=True
            )
    
    def test_ensure_consumer_group_already_exists(self, mock_redis):
        """Test consumer group creation when group already exists."""
        mock_redis.xgroup_create.side_effect = redis.ResponseError("BUSYGROUP")
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group', wraps=RedisQueueManager._ensure_consumer_group):
            manager = RedisQueueManager(mock_redis)
            # Should not raise exception
            manager._ensure_consumer_group()
    
    def test_serialize_event(self, mock_redis, sample_event_data):
        """Test event serialization."""
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            serialized = manager._serialize_event(sample_event_data)
            
            assert serialized["type"] == "task_created"
            assert serialized["task_id"] == "task_123"
            assert isinstance(serialized["data"], str)  # Should be JSON string
            assert json.loads(serialized["data"]) == sample_event_data["data"]
    
    def test_deserialize_event(self, mock_redis):
        """Test event deserialization."""
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            # Mock Redis response format (bytes)
            redis_data = {
                b"type": b"task_created",
                b"task_id": b"task_123",
                b"data": b'{"status": "pending", "priority": "high"}'
            }
            
            deserialized = manager._deserialize_event(redis_data)
            
            assert deserialized["type"] == "task_created"
            assert deserialized["task_id"] == "task_123"
            assert deserialized["data"] == {"status": "pending", "priority": "high"}
    
    def test_enqueue_simple(self, mock_redis, sample_event_data):
        """Test enqueueing an event."""
        mock_redis.xadd.return_value = b"1234567890-0"
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis, max_len=None)
            
            event_id = manager.enqueue(sample_event_data)
            
            assert event_id == "1234567890-0"
            mock_redis.xadd.assert_called_once()
            call_args = mock_redis.xadd.call_args
            assert call_args[0][0] == "a2a:events"  # stream name
    
    def test_enqueue_with_maxlen(self, mock_redis, sample_event_data):
        """Test enqueueing with max length trimming."""
        mock_redis.xadd.return_value = b"1234567890-0"
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis, max_len=1000)
            
            event_id = manager.enqueue(sample_event_data)
            
            assert event_id == "1234567890-0"
            mock_redis.xadd.assert_called_once()
            call_args = mock_redis.xadd.call_args
            assert call_args[1]["maxlen"] == 1000
            assert call_args[1]["approximate"] is True
    
    def test_dequeue_success(self, mock_redis):
        """Test successful event dequeuing."""
        # Mock Redis XREADGROUP response
        mock_redis.xreadgroup.return_value = [
            (b"a2a:events", [
                (b"1234567890-0", {b"type": b"task_created", b"task_id": b"task_123"}),
                (b"1234567890-1", {b"type": b"task_updated", b"task_id": b"task_456"})
            ])
        ]
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            events = manager.dequeue("consumer1", count=2, block=1000)
            
            assert len(events) == 2
            assert events[0][0] == "1234567890-0"
            assert events[0][1]["type"] == "task_created"
            assert events[1][0] == "1234567890-1"
            assert events[1][1]["type"] == "task_updated"
            
            mock_redis.xreadgroup.assert_called_once_with(
                "a2a-agents", "consumer1", {"a2a:events": ">"}, count=2, block=1000
            )
    
    def test_dequeue_no_messages(self, mock_redis):
        """Test dequeuing when no messages available."""
        mock_redis.xreadgroup.return_value = []
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            events = manager.dequeue("consumer1")
            
            assert events == []
    
    def test_dequeue_with_auto_claim(self, mock_redis):
        """Test dequeuing with auto-claim."""
        # Mock auto-claim response
        mock_redis.xautoclaim.return_value = [
            None,  # next_id
            [(b"1234567890-0", {b"type": b"task_created", b"task_id": b"task_123"})]
        ]
        mock_redis.xreadgroup.return_value = []  # No new messages
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            events = manager.dequeue("consumer1", auto_claim_min_idle=30000)
            
            assert len(events) == 1
            assert events[0][0] == "1234567890-0"
            assert events[0][1]["type"] == "task_created"
            
            mock_redis.xautoclaim.assert_called_once()
    
    def test_dequeue_nogroup_error(self, mock_redis):
        """Test dequeuing when consumer group doesn't exist."""
        mock_redis.xreadgroup.side_effect = [
            redis.ResponseError("NOGROUP"),
            []  # Second call after group creation
        ]
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group') as mock_ensure:
            manager = RedisQueueManager(mock_redis)
            mock_ensure.reset_mock()  # Reset call from __init__
            
            events = manager.dequeue("consumer1")
            
            assert events == []
            assert mock_ensure.call_count == 1  # Called once to recreate group
    
    def test_ack_success(self, mock_redis):
        """Test acknowledging events."""
        mock_redis.xack.return_value = 2
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            result = manager.ack("consumer1", ["1234567890-0", "1234567890-1"])
            
            assert result == 2
            mock_redis.xack.assert_called_once_with(
                "a2a:events", "a2a-agents", "1234567890-0", "1234567890-1"
            )
    
    def test_ack_empty_list(self, mock_redis):
        """Test acknowledging empty event list."""
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            result = manager.ack("consumer1", [])
            
            assert result == 0
            mock_redis.xack.assert_not_called()
    
    def test_pending_count_specific_consumer(self, mock_redis):
        """Test getting pending count for specific consumer."""
        mock_redis.xpending_range.return_value = [
            ("msg1", "consumer1", 1000, 1),
            ("msg2", "consumer1", 2000, 1)
        ]
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            count = manager.pending_count("consumer1")
            
            assert count == 2
            mock_redis.xpending_range.assert_called_once()
    
    def test_pending_count_all_consumers(self, mock_redis):
        """Test getting pending count for all consumers."""
        mock_redis.xpending.return_value = {"pending": 5}
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            count = manager.pending_count()
            
            assert count == 5
            mock_redis.xpending.assert_called_once()
    
    def test_get_consumer_info(self, mock_redis):
        """Test getting consumer information."""
        mock_redis.xinfo_consumers.return_value = [
            {b"name": b"consumer1", b"pending": 2, b"idle": 1000},
            {b"name": b"consumer2", b"pending": 0, b"idle": 5000}
        ]
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            info = manager.get_consumer_info()
            
            assert len(info) == 2
            assert info[0]["name"] == "consumer1"
            assert info[0]["pending"] == 2
            assert info[1]["name"] == "consumer2"
            assert info[1]["idle"] == 5000
    
    def test_trim_stream(self, mock_redis):
        """Test stream trimming."""
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis, max_len=1000)
            
            manager.trim_stream()
            
            mock_redis.xtrim.assert_called_once_with(
                "a2a:events", maxlen=1000, approximate=True
            )
    
    def test_delete_consumer(self, mock_redis):
        """Test consumer deletion."""
        mock_redis.xgroup_delconsumer.return_value = 1
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            result = manager.delete_consumer("consumer1")
            
            assert result is True
            mock_redis.xgroup_delconsumer.assert_called_once_with(
                "a2a:events", "a2a-agents", "consumer1"
            )
    
    def test_get_stream_info(self, mock_redis):
        """Test getting stream information."""
        mock_redis.xinfo_stream.return_value = {
            b"length": 100,
            b"first-entry": [b"1234567890-0", {b"field": b"value"}],
            b"last-entry": [b"1234567899-0", {b"field": b"value2"}],
            b"groups": 1
        }
        
        with patch.object(RedisQueueManager, '_ensure_consumer_group'):
            manager = RedisQueueManager(mock_redis)
            
            info = manager.get_stream_info()
            
            assert info["length"] == 100
            assert info["groups"] == 1


class TestRedisQueueManagerIntegration:
    """Integration tests for RedisQueueManager with real Redis."""
    
    def test_full_queue_lifecycle(self, queue_manager, sample_event_data):
        """Test complete queue lifecycle with real Redis."""
        consumer_id = "test_consumer"
        
        # Enqueue events
        event_id1 = queue_manager.enqueue(sample_event_data)
        event_id2 = queue_manager.enqueue({
            "type": "task_updated",
            "task_id": "task_456",
            "status": "completed"
        })
        
        assert event_id1 is not None
        assert event_id2 is not None
        assert event_id1 != event_id2
        
        # Dequeue events
        events = queue_manager.dequeue(consumer_id, count=5, block=100)
        
        assert len(events) == 2
        event_ids = [event[0] for event in events]
        assert event_id1 in event_ids
        assert event_id2 in event_ids
        
        # Check event data
        for event_id, event_data in events:
            assert "type" in event_data
            assert event_data["type"] in ["task_created", "task_updated"]
        
        # Acknowledge events
        ack_count = queue_manager.ack(consumer_id, event_ids)
        assert ack_count == 2
        
        # No more events should be available
        more_events = queue_manager.dequeue(consumer_id, count=1, block=100)
        assert len(more_events) == 0
    
    def test_consumer_groups(self, redis_client, sample_event_data):
        """Test multiple consumers in the same group."""
        manager = RedisQueueManager(
            redis_client,
            stream_name="test_multi_consumer",
            consumer_group="test_group"
        )
        
        # Enqueue events
        for i in range(5):
            manager.enqueue({**sample_event_data, "sequence": i})
        
        # Two consumers read from the same group
        consumer1_events = manager.dequeue("consumer1", count=3, block=100)
        consumer2_events = manager.dequeue("consumer2", count=3, block=100)
        
        # Each consumer should get different events
        total_events = len(consumer1_events) + len(consumer2_events)
        assert total_events == 5
        
        # Get all event IDs
        all_event_ids = [e[0] for e in consumer1_events + consumer2_events]
        assert len(set(all_event_ids)) == 5  # All unique
        
        # Acknowledge events
        if consumer1_events:
            manager.ack("consumer1", [e[0] for e in consumer1_events])
        if consumer2_events:
            manager.ack("consumer2", [e[0] for e in consumer2_events])