import pytest
from unittest.mock import AsyncMock
from a2a.types import Message, Task, TaskStatusUpdateEvent
from a2a_redis.model_utils import serialize_event, deserialize_event
from a2a_redis import RedisStreamsEventQueue, RedisPubSubEventQueue


@pytest.mark.asyncio
async def test_message_serialization_roundtrip():
    """Test that Message objects survive serialization/deserialization."""
    original_message = Message(
        messageId="test-123",
        role="user", 
        parts=[{"kind": "text", "text": "Hello"}],
        kind="message"
    )
    
    # Serialize then deserialize
    serialized = serialize_event(original_message)
    reconstructed = deserialize_event(serialized)
    
    # Should be same type and data
    assert type(reconstructed) == type(original_message)
    assert reconstructed.messageId == original_message.messageId
    assert reconstructed.role == original_message.role


@pytest.mark.asyncio 
async def test_streams_queue_pydantic_model_preservation():
    """Test that Redis Streams queue preserves Pydantic models."""
    redis_mock = AsyncMock()
    redis_mock.xadd = AsyncMock()
    redis_mock.xreadgroup = AsyncMock(return_value=[
        ["stream:test", [[b"123-0", {b"event_type": b"Message", b"event_data": b'{"messageId": "test", "role": "user", "parts": [], "kind": "message"}'}]]]
    ])
    
    queue = RedisStreamsEventQueue(redis_mock, "test")
    result = await queue.dequeue_event()
    
    # Should return actual Message instance, not dict
    assert isinstance(result, Message)
    assert result.messageId == "test"