"""Test configuration and fixtures for a2a-redis tests."""

import pytest
import pytest_asyncio
import redis
import redis.asyncio as redis_async
from unittest.mock import MagicMock, AsyncMock

from a2a_redis import (
    RedisTaskStore,
    RedisStreamsQueueManager,
    RedisPubSubQueueManager,
    RedisPushNotificationConfigStore,
)


@pytest.fixture
def mock_redis():
    """Mock async Redis client for testing."""
    mock_client = AsyncMock(spec=redis_async.Redis)
    # Ensure all Redis methods are async mocks
    mock_client.hset = AsyncMock()
    mock_client.hgetall = AsyncMock()
    mock_client.exists = AsyncMock()
    mock_client.delete = AsyncMock()
    mock_client.keys = AsyncMock()
    mock_client.hdel = AsyncMock()
    mock_client.xadd = AsyncMock()
    mock_client.xreadgroup = AsyncMock()
    mock_client.xgroup_create = AsyncMock()
    mock_client.xgroup_destroy = AsyncMock()
    mock_client.xack = AsyncMock()
    mock_client.xlen = AsyncMock()
    mock_client.xdel = AsyncMock()
    mock_client.xpending_range = AsyncMock()
    mock_client.xpending = AsyncMock()
    mock_client.publish = AsyncMock()
    # Mock pubsub operations
    mock_pubsub = AsyncMock()
    mock_pubsub.subscribe = AsyncMock()
    mock_pubsub.unsubscribe = AsyncMock()
    mock_pubsub.close = AsyncMock()
    mock_pubsub.get_message = AsyncMock()
    mock_client.pubsub = MagicMock(return_value=mock_pubsub)
    # Mock JSON operations
    mock_json = AsyncMock()
    mock_json.set = AsyncMock()
    mock_json.get = AsyncMock()
    mock_client.json = MagicMock(return_value=mock_json)
    return mock_client


@pytest_asyncio.fixture
async def redis_client():
    """Real async Redis client for integration tests."""
    try:
        client = redis_async.Redis(
            host="localhost", port=6379, db=15, decode_responses=False
        )
        await client.ping()
        # Clean the test database
        await client.flushdb()
        yield client
        # Clean up after tests
        await client.flushdb()
        await client.aclose()
    except redis.ConnectionError:
        pytest.skip("Redis server not available")


@pytest.fixture
def task_store(redis_client):
    """RedisTaskStore instance for testing."""
    return RedisTaskStore(redis_client, prefix="test_task:")


@pytest.fixture
def streams_queue_manager(redis_client):
    """RedisStreamsQueueManager instance for testing."""
    return RedisStreamsQueueManager(redis_client, prefix="test_stream:")


@pytest.fixture
def pubsub_queue_manager(redis_client):
    """RedisPubSubQueueManager instance for testing."""
    return RedisPubSubQueueManager(redis_client, prefix="test_pubsub:")


@pytest.fixture
def push_config_store(redis_client):
    """RedisPushNotificationConfigStore instance for testing."""
    return RedisPushNotificationConfigStore(redis_client, prefix="test_push:")


@pytest.fixture
def sample_task_data():
    """Sample task data for testing."""
    from a2a.types import TaskStatus, TaskState

    return {
        "id": "task_123",
        "context_id": "context_456",
        "status": TaskStatus(state=TaskState.submitted),
        "metadata": {
            "user_id": "user_456",
            "priority": "high",
            "tags": ["test", "sample"],
            "description": "Test task",
            "created_at": "2024-01-01T00:00:00Z",
        },
    }


@pytest.fixture
def sample_event_data():
    """Sample event data for testing."""
    return {
        "type": "task_created",
        "task_id": "task_123",
        "agent_id": "agent_456",
        "timestamp": "2024-01-01T00:00:00Z",
        "data": {"status": "pending", "priority": "high"},
    }


@pytest.fixture
def sample_push_config():
    """Sample push notification config for testing."""
    return {
        "endpoint": "https://fcm.googleapis.com/fcm/send",
        "auth_token": "test_token_123",
        "enabled": True,
        "preferences": {
            "task_updates": True,
            "reminders": False,
            "daily_summary": True,
        },
    }
