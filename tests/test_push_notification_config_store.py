"""Tests for RedisPushNotificationConfigStore."""

import json
import pytest
from unittest.mock import MagicMock

from a2a_redis.push_notification_config_store import RedisPushNotificationConfigStore


class TestRedisPushNotificationConfigStore:
    """Tests for RedisPushNotificationConfigStore."""
    
    def test_init(self, mock_redis):
        """Test RedisPushNotificationConfigStore initialization."""
        store = RedisPushNotificationConfigStore(mock_redis, prefix="push:")
        assert store.redis == mock_redis
        assert store.prefix == "push:"
    
    def test_config_key_generation(self, mock_redis):
        """Test config key generation."""
        store = RedisPushNotificationConfigStore(mock_redis, prefix="push:")
        key = store._config_key("agent123", "user456")
        assert key == "push:agent123:user456"
    
    def test_agent_pattern_generation(self, mock_redis):
        """Test agent pattern generation."""
        store = RedisPushNotificationConfigStore(mock_redis, prefix="push:")
        pattern = store._agent_pattern("agent123")
        assert pattern == "push:agent123:*"
    
    def test_serialize_config(self, mock_redis, sample_push_config):
        """Test config serialization."""
        store = RedisPushNotificationConfigStore(mock_redis)
        serialized = store._serialize_config(sample_push_config)
        
        assert serialized["endpoint"] == sample_push_config["endpoint"]
        assert serialized["auth_token"] == sample_push_config["auth_token"]
        assert serialized["enabled"] == "True"  # String due to serialization
        assert isinstance(serialized["preferences"], str)  # JSON string
        assert json.loads(serialized["preferences"]) == sample_push_config["preferences"]
    
    def test_deserialize_config(self, mock_redis, sample_push_config):
        """Test config deserialization."""
        store = RedisPushNotificationConfigStore(mock_redis)
        
        # Mock Redis response format (bytes)
        redis_data = {
            b"endpoint": b"https://fcm.googleapis.com/fcm/send",
            b"auth_token": b"test_token_123",
            b"enabled": b"True",
            b"preferences": json.dumps(sample_push_config["preferences"]).encode()
        }
        
        deserialized = store._deserialize_config(redis_data)
        
        assert deserialized["endpoint"] == "https://fcm.googleapis.com/fcm/send"
        assert deserialized["auth_token"] == "test_token_123"
        assert deserialized["enabled"] == "True"
        assert deserialized["preferences"] == sample_push_config["preferences"]
    
    def test_create_config(self, mock_redis, sample_push_config):
        """Test config creation."""
        store = RedisPushNotificationConfigStore(mock_redis)
        store.create_config("agent123", "user456", sample_push_config)
        
        mock_redis.hset.assert_called_once()
        call_args = mock_redis.hset.call_args
        assert call_args[0][0] == "push_config:agent123:user456"
        
        # Check serialization occurred
        mapping = call_args[1]["mapping"]
        assert "preferences" in mapping
        assert isinstance(mapping["preferences"], str)
    
    def test_get_config_exists(self, mock_redis, sample_push_config):
        """Test retrieving an existing config."""
        # Mock Redis response
        mock_redis.hgetall.return_value = {
            b"endpoint": b"https://fcm.googleapis.com/fcm/send",
            b"auth_token": b"test_token_123",
            b"enabled": b"True",
            b"preferences": json.dumps(sample_push_config["preferences"]).encode()
        }
        
        store = RedisPushNotificationConfigStore(mock_redis)
        result = store.get_config("agent123", "user456")
        
        assert result is not None
        assert result["endpoint"] == "https://fcm.googleapis.com/fcm/send"
        assert result["preferences"] == sample_push_config["preferences"]
        mock_redis.hgetall.assert_called_once_with("push_config:agent123:user456")
    
    def test_get_config_not_exists(self, mock_redis):
        """Test retrieving a non-existent config."""
        mock_redis.hgetall.return_value = {}
        
        store = RedisPushNotificationConfigStore(mock_redis)
        result = store.get_config("agent123", "user456")
        
        assert result is None
        mock_redis.hgetall.assert_called_once_with("push_config:agent123:user456")
    
    def test_update_config_exists(self, mock_redis):
        """Test updating an existing config."""
        mock_redis.exists.return_value = True
        
        store = RedisPushNotificationConfigStore(mock_redis)
        updates = {"enabled": False, "preferences": {"task_updates": False}}
        result = store.update_config("agent123", "user456", updates)
        
        assert result is True
        mock_redis.exists.assert_called_once_with("push_config:agent123:user456")
        mock_redis.hset.assert_called_once()
    
    def test_update_config_not_exists(self, mock_redis):
        """Test updating a non-existent config."""
        mock_redis.exists.return_value = False
        
        store = RedisPushNotificationConfigStore(mock_redis)
        result = store.update_config("agent123", "user456", {"enabled": False})
        
        assert result is False
        mock_redis.exists.assert_called_once_with("push_config:agent123:user456")
        mock_redis.hset.assert_not_called()
    
    def test_delete_config(self, mock_redis):
        """Test config deletion."""
        mock_redis.delete.return_value = 1
        
        store = RedisPushNotificationConfigStore(mock_redis)
        result = store.delete_config("agent123", "user456")
        
        assert result is True
        mock_redis.delete.assert_called_once_with("push_config:agent123:user456")
    
    def test_delete_config_not_exists(self, mock_redis):
        """Test deleting a non-existent config."""
        mock_redis.delete.return_value = 0
        
        store = RedisPushNotificationConfigStore(mock_redis)
        result = store.delete_config("agent123", "user456")
        
        assert result is False
        mock_redis.delete.assert_called_once_with("push_config:agent123:user456")
    
    def test_list_configs_for_agent(self, mock_redis, sample_push_config):
        """Test listing configs for an agent."""
        mock_redis.keys.return_value = [
            b"push_config:agent123:user1",
            b"push_config:agent123:user2"
        ]
        
        # Mock data for both configs
        mock_data = {
            b"enabled": b"True",
            b"endpoint": b"https://fcm.googleapis.com/fcm/send",
            b"preferences": json.dumps({"task_updates": True}).encode()
        }
        mock_redis.hgetall.return_value = mock_data
        
        store = RedisPushNotificationConfigStore(mock_redis)
        configs = store.list_configs_for_agent("agent123")
        
        assert len(configs) == 2
        for config in configs:
            assert config["agent_id"] == "agent123"
            assert config["user_id"] in ["user1", "user2"]
            assert config["enabled"] == "True"
        
        mock_redis.keys.assert_called_once_with("push_config:agent123:*")
        assert mock_redis.hgetall.call_count == 2
    
    def test_list_configs_for_user(self, mock_redis):
        """Test listing configs for a user."""
        mock_redis.keys.return_value = [
            b"push_config:agent1:user123",
            b"push_config:agent2:user123"
        ]
        
        mock_data = {
            b"enabled": b"True",
            b"endpoint": b"https://example.com"
        }
        mock_redis.hgetall.return_value = mock_data
        
        store = RedisPushNotificationConfigStore(mock_redis)
        configs = store.list_configs_for_user("user123")
        
        assert len(configs) == 2
        for config in configs:
            assert config["user_id"] == "user123"
            assert config["agent_id"] in ["agent1", "agent2"]
        
        mock_redis.keys.assert_called_once_with("push_config:*:user123")
    
    def test_config_exists(self, mock_redis):
        """Test checking if config exists."""
        mock_redis.exists.return_value = True
        
        store = RedisPushNotificationConfigStore(mock_redis)
        result = store.config_exists("agent123", "user456")
        
        assert result is True
        mock_redis.exists.assert_called_once_with("push_config:agent123:user456")
    
    def test_get_users_for_agent(self, mock_redis):
        """Test getting users for an agent."""
        mock_redis.keys.return_value = [
            b"push_config:agent123:user1",
            b"push_config:agent123:user2",
            b"push_config:agent123:user3"
        ]
        
        store = RedisPushNotificationConfigStore(mock_redis)
        users = store.get_users_for_agent("agent123")
        
        assert users == ["user1", "user2", "user3"]
        mock_redis.keys.assert_called_once_with("push_config:agent123:*")
    
    def test_get_agents_for_user(self, mock_redis):
        """Test getting agents for a user."""
        mock_redis.keys.return_value = [
            b"push_config:agent1:user123",
            b"push_config:agent2:user123",
            b"push_config:agent3:user123"
        ]
        
        store = RedisPushNotificationConfigStore(mock_redis)
        agents = store.get_agents_for_user("user123")
        
        assert agents == ["agent1", "agent2", "agent3"]
        mock_redis.keys.assert_called_once_with("push_config:*:user123")
    
    def test_cleanup_configs_for_agent(self, mock_redis):
        """Test cleaning up configs for an agent."""
        mock_redis.keys.return_value = [
            b"push_config:agent123:user1",
            b"push_config:agent123:user2"
        ]
        mock_redis.delete.return_value = 2
        
        store = RedisPushNotificationConfigStore(mock_redis)
        result = store.cleanup_configs_for_agent("agent123")
        
        assert result == 2
        mock_redis.keys.assert_called_once_with("push_config:agent123:*")
        mock_redis.delete.assert_called_once()
    
    def test_cleanup_configs_for_agent_none_exist(self, mock_redis):
        """Test cleaning up configs when none exist."""
        mock_redis.keys.return_value = []
        
        store = RedisPushNotificationConfigStore(mock_redis)
        result = store.cleanup_configs_for_agent("agent123")
        
        assert result == 0
        mock_redis.delete.assert_not_called()
    
    def test_cleanup_configs_for_user(self, mock_redis):
        """Test cleaning up configs for a user."""
        mock_redis.keys.return_value = [
            b"push_config:agent1:user123",
            b"push_config:agent2:user123"
        ]
        mock_redis.delete.return_value = 2
        
        store = RedisPushNotificationConfigStore(mock_redis)
        result = store.cleanup_configs_for_user("user123")
        
        assert result == 2
        mock_redis.keys.assert_called_once_with("push_config:*:user123")
        mock_redis.delete.assert_called_once()


class TestRedisPushNotificationConfigStoreIntegration:
    """Integration tests for RedisPushNotificationConfigStore with real Redis."""
    
    def test_full_config_lifecycle(self, push_config_store, sample_push_config):
        """Test complete config lifecycle with real Redis."""
        agent_id = "test_agent"
        user_id = "test_user"
        
        # Config should not exist initially
        assert not push_config_store.config_exists(agent_id, user_id)
        assert push_config_store.get_config(agent_id, user_id) is None
        
        # Create config
        push_config_store.create_config(agent_id, user_id, sample_push_config)
        assert push_config_store.config_exists(agent_id, user_id)
        
        # Retrieve config
        retrieved_config = push_config_store.get_config(agent_id, user_id)
        assert retrieved_config is not None
        assert retrieved_config["endpoint"] == sample_push_config["endpoint"]
        assert retrieved_config["preferences"] == sample_push_config["preferences"]
        
        # Update config
        updates = {"enabled": False, "auth_token": "new_token"}
        assert push_config_store.update_config(agent_id, user_id, updates)
        
        updated_config = push_config_store.get_config(agent_id, user_id)
        assert updated_config["enabled"] == "False"  # String due to Redis storage
        assert updated_config["auth_token"] == "new_token"
        
        # Test listing operations
        agent_configs = push_config_store.list_configs_for_agent(agent_id)
        assert len(agent_configs) == 1
        assert agent_configs[0]["user_id"] == user_id
        
        user_configs = push_config_store.list_configs_for_user(user_id)
        assert len(user_configs) == 1
        assert user_configs[0]["agent_id"] == agent_id
        
        # Test user/agent listing
        users = push_config_store.get_users_for_agent(agent_id)
        assert user_id in users
        
        agents = push_config_store.get_agents_for_user(user_id)
        assert agent_id in agents
        
        # Delete config
        assert push_config_store.delete_config(agent_id, user_id)
        assert not push_config_store.config_exists(agent_id, user_id)
        assert push_config_store.get_config(agent_id, user_id) is None
    
    def test_multiple_configs_management(self, push_config_store, sample_push_config):
        """Test managing multiple configs."""
        agents = ["agent1", "agent2", "agent3"]
        users = ["user1", "user2"]
        
        # Create configs for multiple agent-user combinations
        for agent_id in agents:
            for user_id in users:
                config = {**sample_push_config, "agent_specific": agent_id}
                push_config_store.create_config(agent_id, user_id, config)
        
        # Test agent-specific operations
        for agent_id in agents:
            agent_configs = push_config_store.list_configs_for_agent(agent_id)
            assert len(agent_configs) == len(users)
            
            agent_users = push_config_store.get_users_for_agent(agent_id)
            assert set(agent_users) == set(users)
        
        # Test user-specific operations
        for user_id in users:
            user_configs = push_config_store.list_configs_for_user(user_id)
            assert len(user_configs) == len(agents)
            
            user_agents = push_config_store.get_agents_for_user(user_id)
            assert set(user_agents) == set(agents)
        
        # Test cleanup operations
        deleted_count = push_config_store.cleanup_configs_for_agent("agent1")
        assert deleted_count == len(users)
        
        # Verify cleanup worked
        remaining_configs = push_config_store.list_configs_for_agent("agent1")
        assert len(remaining_configs) == 0
        
        # Cleanup remaining configs by user
        deleted_count = push_config_store.cleanup_configs_for_user("user1")
        assert deleted_count == 2  # agent2 and agent3