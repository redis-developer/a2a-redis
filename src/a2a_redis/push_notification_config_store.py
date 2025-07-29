"""Redis-backed push notification config store implementation for the A2A Python SDK."""

import json
from typing import Any, Dict, List, Optional

import redis
from a2a.server.tasks.push_notification_config_store import PushNotificationConfigStore


class RedisPushNotificationConfigStore(PushNotificationConfigStore):
    """Redis-backed implementation of the A2A PushNotificationConfigStore interface."""
    
    def __init__(self, redis_client: redis.Redis, prefix: str = "push_config:"):
        """Initialize the Redis push notification config store.
        
        Args:
            redis_client: Redis client instance
            prefix: Key prefix for config storage
        """
        self.redis = redis_client
        self.prefix = prefix
    
    def _config_key(self, agent_id: str, user_id: str) -> str:
        """Generate the Redis key for a push notification config."""
        return f"{self.prefix}{agent_id}:{user_id}"
    
    def _agent_pattern(self, agent_id: str) -> str:
        """Generate the pattern for all configs for an agent."""
        return f"{self.prefix}{agent_id}:*"
    
    def _serialize_config(self, config: Dict[str, Any]) -> Dict[str, str]:
        """Serialize config data for Redis storage."""
        serialized = {}
        for key, value in config.items():
            if isinstance(value, (dict, list)):
                serialized[key] = json.dumps(value)
            else:
                serialized[key] = str(value)
        return serialized
    
    def _deserialize_config(self, config_data: Dict[bytes, bytes]) -> Dict[str, Any]:
        """Deserialize config data from Redis."""
        if not config_data:
            return {}
        
        result = {}
        for key, value in config_data.items():
            key_str = key.decode() if isinstance(key, bytes) else key
            value_str = value.decode() if isinstance(value, bytes) else value
            
            # Try to deserialize JSON data
            try:
                result[key_str] = json.loads(value_str)
            except json.JSONDecodeError:
                result[key_str] = value_str
        
        return result
    
    def create_config(
        self,
        agent_id: str,
        user_id: str,
        config: Dict[str, Any]
    ) -> None:
        """Create a new push notification config.
        
        Args:
            agent_id: Agent identifier
            user_id: User identifier
            config: Push notification configuration
        """
        serialized_config = self._serialize_config(config)
        self.redis.hset(self._config_key(agent_id, user_id), mapping=serialized_config)
    
    def get_config(
        self,
        agent_id: str,
        user_id: str
    ) -> Optional[Dict[str, Any]]:
        """Retrieve a push notification config.
        
        Args:
            agent_id: Agent identifier
            user_id: User identifier
            
        Returns:
            Configuration dictionary or None if not found
        """
        config_data = self.redis.hgetall(self._config_key(agent_id, user_id))
        if not config_data:
            return None
        
        return self._deserialize_config(config_data)
    
    def update_config(
        self,
        agent_id: str,
        user_id: str,
        updates: Dict[str, Any]
    ) -> bool:
        """Update an existing push notification config.
        
        Args:
            agent_id: Agent identifier
            user_id: User identifier
            updates: Dictionary of fields to update
            
        Returns:
            True if config was updated, False if config doesn't exist
        """
        if not self.redis.exists(self._config_key(agent_id, user_id)):
            return False
        
        serialized_updates = self._serialize_config(updates)
        self.redis.hset(self._config_key(agent_id, user_id), mapping=serialized_updates)
        return True
    
    def delete_config(self, agent_id: str, user_id: str) -> bool:
        """Delete a push notification config.
        
        Args:
            agent_id: Agent identifier
            user_id: User identifier
            
        Returns:
            True if config was deleted, False if config didn't exist
        """
        return bool(self.redis.delete(self._config_key(agent_id, user_id)))
    
    def list_configs_for_agent(self, agent_id: str) -> List[Dict[str, Any]]:
        """List all push notification configs for an agent.
        
        Args:
            agent_id: Agent identifier
            
        Returns:
            List of configuration dictionaries with user_id included
        """
        pattern = self._agent_pattern(agent_id)
        keys = self.redis.keys(pattern)
        
        configs = []
        for key in keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            # Extract user_id from key
            user_id = key_str.replace(f"{self.prefix}{agent_id}:", "")
            
            config_data = self.redis.hgetall(key)
            if config_data:
                config = self._deserialize_config(config_data)
                config["user_id"] = user_id
                config["agent_id"] = agent_id
                configs.append(config)
        
        return configs
    
    def list_configs_for_user(self, user_id: str) -> List[Dict[str, Any]]:
        """List all push notification configs for a user across all agents.
        
        Args:
            user_id: User identifier
            
        Returns:
            List of configuration dictionaries with agent_id included
        """
        pattern = f"{self.prefix}*:{user_id}"
        keys = self.redis.keys(pattern)
        
        configs = []
        for key in keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            # Extract agent_id from key
            agent_id = key_str.replace(self.prefix, "").replace(f":{user_id}", "")
            
            config_data = self.redis.hgetall(key)
            if config_data:
                config = self._deserialize_config(config_data)
                config["user_id"] = user_id
                config["agent_id"] = agent_id
                configs.append(config)
        
        return configs
    
    def config_exists(self, agent_id: str, user_id: str) -> bool:
        """Check if a push notification config exists.
        
        Args:
            agent_id: Agent identifier
            user_id: User identifier
            
        Returns:
            True if config exists, False otherwise
        """
        return bool(self.redis.exists(self._config_key(agent_id, user_id)))
    
    def get_users_for_agent(self, agent_id: str) -> List[str]:
        """Get all user IDs that have push notification configs for an agent.
        
        Args:
            agent_id: Agent identifier
            
        Returns:
            List of user IDs
        """
        pattern = self._agent_pattern(agent_id)
        keys = self.redis.keys(pattern)
        
        user_ids = []
        for key in keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            user_id = key_str.replace(f"{self.prefix}{agent_id}:", "")
            user_ids.append(user_id)
        
        return user_ids
    
    def get_agents_for_user(self, user_id: str) -> List[str]:
        """Get all agent IDs that have push notification configs for a user.
        
        Args:
            user_id: User identifier
            
        Returns:
            List of agent IDs
        """
        pattern = f"{self.prefix}*:{user_id}"
        keys = self.redis.keys(pattern)
        
        agent_ids = []
        for key in keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            agent_id = key_str.replace(self.prefix, "").replace(f":{user_id}", "")
            agent_ids.append(agent_id)
        
        return agent_ids
    
    def cleanup_configs_for_agent(self, agent_id: str) -> int:
        """Remove all push notification configs for an agent.
        
        Args:
            agent_id: Agent identifier
            
        Returns:
            Number of configs deleted
        """
        pattern = self._agent_pattern(agent_id)
        keys = self.redis.keys(pattern)
        
        if not keys:
            return 0
        
        return self.redis.delete(*keys)
    
    def cleanup_configs_for_user(self, user_id: str) -> int:
        """Remove all push notification configs for a user.
        
        Args:
            user_id: User identifier
            
        Returns:
            Number of configs deleted
        """
        pattern = f"{self.prefix}*:{user_id}"
        keys = self.redis.keys(pattern)
        
        if not keys:
            return 0
        
        return self.redis.delete(*keys)