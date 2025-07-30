"""Test configuration and fixtures for a2a-redis tests."""

import pytest
import redis
from unittest.mock import MagicMock

from a2a_redis import (
    RedisTaskStore,
    RedisQueueManager,
    RedisPushNotificationConfigStore,
)


@pytest.fixture
def mock_redis():
    """Mock Redis client for testing."""
    mock_client = MagicMock(spec=redis.Redis)
    return mock_client


@pytest.fixture
def redis_client():
    """Real Redis client for integration tests."""
    try:
        client = redis.Redis(host="localhost", port=6379, db=15, decode_responses=False)
        client.ping()
        # Clean the test database
        client.flushdb()
        yield client
        # Clean up after tests
        client.flushdb()
        client.close()
    except redis.ConnectionError:
        pytest.skip("Redis server not available")


@pytest.fixture
def task_store(redis_client):
    """RedisTaskStore instance for testing."""
    return RedisTaskStore(redis_client, prefix="test_task:")


@pytest.fixture
def queue_manager(redis_client):
    """RedisQueueManager instance for testing."""
    return RedisQueueManager(
        redis_client, stream_name="test_stream", consumer_group="test_group"
    )


@pytest.fixture
def push_config_store(redis_client):
    """RedisPushNotificationConfigStore instance for testing."""
    return RedisPushNotificationConfigStore(redis_client, prefix="test_push:")


@pytest.fixture
def sample_task_data():
    """Sample task data for testing."""
    return {
        "id": "task_123",
        "status": "pending",
        "description": "Test task",
        "metadata": {
            "user_id": "user_456",
            "priority": "high",
            "tags": ["test", "sample"],
        },
        "created_at": "2024-01-01T00:00:00Z",
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
