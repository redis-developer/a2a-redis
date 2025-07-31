"""Tests for RedisPushNotificationConfigStore."""

import json
import pytest

from a2a_redis.push_notification_config_store import RedisPushNotificationConfigStore
from a2a.types import PushNotificationConfig


class TestRedisPushNotificationConfigStore:
    """Tests for RedisPushNotificationConfigStore."""

    def test_init(self, mock_redis):
        """Test RedisPushNotificationConfigStore initialization."""
        store = RedisPushNotificationConfigStore(mock_redis, prefix="push:")
        assert store.redis == mock_redis
        assert store.prefix == "push:"

    def test_task_key_generation(self, mock_redis):
        """Test task key generation."""
        store = RedisPushNotificationConfigStore(mock_redis, prefix="push:")
        key = store._task_key("task_123")
        assert key == "push:task_123"

    @pytest.mark.asyncio
    async def test_get_info_empty(self, mock_redis):
        """Test getting configs when none exist."""
        mock_redis.hgetall.return_value = {}

        store = RedisPushNotificationConfigStore(mock_redis)
        configs = await store.get_info("task_123")

        assert configs == []
        mock_redis.hgetall.assert_called_once_with("push_config:task_123")

    @pytest.mark.asyncio
    async def test_get_info_with_configs(self, mock_redis):
        """Test getting existing configs."""
        # Mock Redis response
        config_data = {"url": "https://example.com/webhook", "token": "test_token"}
        mock_redis.hgetall.return_value = {
            b"config_1": json.dumps(config_data).encode()
        }

        store = RedisPushNotificationConfigStore(mock_redis)
        configs = await store.get_info("task_123")

        assert len(configs) == 1
        assert isinstance(configs[0], PushNotificationConfig)
        assert configs[0].url == "https://example.com/webhook"
        assert configs[0].token == "test_token"
        assert configs[0].id == "config_1"

    @pytest.mark.asyncio
    async def test_get_info_invalid_json(self, mock_redis):
        """Test getting configs with invalid JSON data."""
        mock_redis.hgetall.return_value = {
            b"config_1": b"invalid json data",
            b"config_2": json.dumps({"url": "https://valid.com"}).encode(),
        }

        store = RedisPushNotificationConfigStore(mock_redis)
        configs = await store.get_info("task_123")

        # Should only return valid configs
        assert len(configs) == 1
        assert configs[0].url == "https://valid.com"

    @pytest.mark.asyncio
    async def test_set_info_new_config(self, mock_redis):
        """Test setting a new config."""
        mock_redis.hgetall.return_value = {}  # No existing configs

        config = PushNotificationConfig(
            url="https://example.com/webhook", token="test_token", id="my_config"
        )

        store = RedisPushNotificationConfigStore(mock_redis)
        await store.set_info("task_123", config)

        # Should call hset with the config data
        mock_redis.hset.assert_called_once()
        call_args = mock_redis.hset.call_args
        assert call_args[0][0] == "push_config:task_123"  # key
        assert call_args[0][1] == "my_config"  # field (config id)

        # Check serialized config data
        config_json = call_args[0][2]
        config_data = json.loads(config_json)
        assert config_data["url"] == "https://example.com/webhook"
        assert config_data["token"] == "test_token"
        assert "id" not in config_data  # ID should be excluded from stored data

    @pytest.mark.asyncio
    async def test_set_info_auto_generate_id(self, mock_redis):
        """Test setting config with auto-generated ID."""
        mock_redis.hgetall.return_value = {}  # No existing configs

        config = PushNotificationConfig(url="https://example.com/webhook")

        store = RedisPushNotificationConfigStore(mock_redis)
        await store.set_info("task_123", config)

        mock_redis.hset.assert_called_once()
        call_args = mock_redis.hset.call_args
        assert call_args[0][1] == "config_0"  # Auto-generated ID

    @pytest.mark.asyncio
    async def test_delete_info_specific_config(self, mock_redis):
        """Test deleting a specific config."""
        store = RedisPushNotificationConfigStore(mock_redis)
        await store.delete_info("task_123", "config_1")

        mock_redis.hdel.assert_called_once_with("push_config:task_123", "config_1")
        mock_redis.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_info_all_configs(self, mock_redis):
        """Test deleting all configs for a task."""
        store = RedisPushNotificationConfigStore(mock_redis)
        await store.delete_info("task_123")

        mock_redis.delete.assert_called_once_with("push_config:task_123")
        mock_redis.hdel.assert_not_called()


class TestRedisPushNotificationConfigStoreIntegration:
    """Integration tests for RedisPushNotificationConfigStore with real Redis."""

    @pytest.mark.asyncio
    async def test_full_config_lifecycle(self, push_config_store):
        """Test complete config lifecycle with real Redis."""
        task_id = "integration_test_task"

        # Should have no configs initially
        configs = await push_config_store.get_info(task_id)
        assert len(configs) == 0

        # Create config
        config1 = PushNotificationConfig(
            url="https://webhook1.example.com", token="token1", id="config1"
        )
        await push_config_store.set_info(task_id, config1)

        # Retrieve configs
        configs = await push_config_store.get_info(task_id)
        assert len(configs) == 1
        assert configs[0].url == "https://webhook1.example.com"
        assert configs[0].token == "token1"
        assert configs[0].id == "config1"

        # Add another config
        config2 = PushNotificationConfig(
            url="https://webhook2.example.com", id="config2"
        )
        await push_config_store.set_info(task_id, config2)

        configs = await push_config_store.get_info(task_id)
        assert len(configs) == 2

        # Delete specific config
        await push_config_store.delete_info(task_id, "config1")
        configs = await push_config_store.get_info(task_id)
        assert len(configs) == 1
        assert configs[0].id == "config2"

        # Delete all configs
        await push_config_store.delete_info(task_id)
        configs = await push_config_store.get_info(task_id)
        assert len(configs) == 0

    @pytest.mark.asyncio
    async def test_config_persistence(self, redis_client):
        """Test that configs persist across store instances."""
        task_id = "persist_test"

        # Create config with first store instance
        store1 = RedisPushNotificationConfigStore(redis_client, prefix="persist:")
        config = PushNotificationConfig(
            url="https://persistent.example.com", token="persist_token"
        )
        await store1.set_info(task_id, config)

        # Create new store instance (simulating restart)
        store2 = RedisPushNotificationConfigStore(redis_client, prefix="persist:")
        configs = await store2.get_info(task_id)

        assert len(configs) == 1
        assert configs[0].url == "https://persistent.example.com"
        assert configs[0].token == "persist_token"

        # Cleanup
        await store2.delete_info(task_id)
