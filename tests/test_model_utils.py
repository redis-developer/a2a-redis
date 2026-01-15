import pytest
from unittest.mock import AsyncMock
from a2a.types import Message
from a2a_redis.model_utils import serialize_event, deserialize_event
from a2a_redis import RedisStreamsEventQueue


@pytest.mark.asyncio
async def test_message_serialization_roundtrip():
    """Test that Message objects survive serialization/deserialization."""
    original_message = Message(
        messageId="test-123",
        role="user",
        parts=[{"kind": "text", "text": "Hello"}],
        kind="message",
    )

    # Serialize then deserialize
    serialized = serialize_event(original_message)
    reconstructed = deserialize_event(serialized)

    # Should be same type and data
    assert isinstance(reconstructed, type(original_message))
    assert reconstructed.message_id == original_message.message_id
    assert reconstructed.role == original_message.role


@pytest.mark.asyncio
async def test_streams_queue_pydantic_model_preservation():
    """Test that Redis Streams queue preserves Pydantic models."""
    redis_mock = AsyncMock()
    redis_mock.xadd = AsyncMock()
    redis_mock.xreadgroup = AsyncMock(
        return_value=[
            [
                "stream:test",
                [
                    [
                        b"123-0",
                        {
                            b"event_type": b"Message",
                            b"event_data": b'{"messageId": "test", "role": "user", "parts": [], "kind": "message"}',
                        },
                    ]
                ],
            ]
        ]
    )

    queue = RedisStreamsEventQueue(redis_mock, "test")
    result = await queue.dequeue_event()

    # Should return actual Message instance, not dict
    assert isinstance(result, Message)
    assert result.message_id == "test"


def test_deserialize_event_non_dict_passthrough():
    """Test that deserialize_event returns non-dict values as-is."""
    # Test string passthrough
    result = deserialize_event("just a string")
    assert result == "just a string"

    # Test list passthrough
    result = deserialize_event([1, 2, 3])
    assert result == [1, 2, 3]

    # Test None passthrough
    result = deserialize_event(None)
    assert result is None


def test_deserialize_event_dict_without_event_data():
    """Test that deserialize_event returns dict without event_data as-is."""
    # Dict without event_data key
    data = {"some_key": "some_value"}
    result = deserialize_event(data)
    assert result == data


def test_deserialize_event_unknown_type():
    """Test that deserialize_event returns event_data when type is unknown."""
    event_structure = {"event_type": "UnknownType", "event_data": {"field": "value"}}
    result = deserialize_event(event_structure)
    assert result == {"field": "value"}


def test_deserialize_event_model_reconstruction_failure():
    """Test that deserialize_event falls back to event_data when model reconstruction fails."""
    # Provide a known type but invalid data that will fail model validation
    event_structure = {
        "event_type": "Message",
        "event_data": {"invalid": "data"},  # Missing required fields
    }
    result = deserialize_event(event_structure)
    # Should fall back to returning the raw event_data
    assert result == {"invalid": "data"}
