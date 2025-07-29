# a2a-redis

Redis components for the Agent-to-Agent (A2A) Python SDK.

This package provides Redis-backed implementations of core A2A components for storing and retrieving task-related data and managing an event queue using Redis.

## Features

- **RedisTaskStore**: Redis-backed task storage using Redis hashes or RedisJSON
- **RedisQueueManager**: Event queue management using Redis Streams with consumer groups
- **RedisPushNotificationConfigStore**: Redis-backed push notification configuration storage
- **Connection Management**: Robust Redis connection handling with automatic retry and health monitoring

## Installation

```bash
pip install a2a-redis
```

## Quick Start

```python
import redis
from a2a_redis import RedisTaskStore, RedisQueueManager, RedisPushNotificationConfigStore
from a2a.server.request_handler import DefaultRequestHandler
from a2a.server.application import A2AStarletteApplication

# Create Redis client
redis_client = redis.from_url("redis://localhost:6379/0")

# Initialize Redis components
task_store = RedisTaskStore(redis_client)
queue_manager = RedisQueueManager(redis_client)
push_config_store = RedisPushNotificationConfigStore(redis_client)

# Use with A2A request handler
request_handler = DefaultRequestHandler(
    agent_executor=YourAgentExecutor(),
    task_store=task_store,
    queue_manager=queue_manager,
    push_notification_config_store=push_config_store,
)

# Create A2A server
server = A2AStarletteApplication(
    agent_card=your_agent_card,
    http_handler=request_handler
)
```

## Components

### RedisTaskStore

Stores task data in Redis using hashes for simple data or RedisJSON for complex nested data.

```python
from a2a_redis import RedisTaskStore, RedisJSONTaskStore

# Using Redis hashes (works with any Redis server)
task_store = RedisTaskStore(redis_client, prefix="mytasks:")

# Using RedisJSON (requires RedisJSON module)
json_task_store = RedisJSONTaskStore(redis_client, prefix="mytasks:")

# Basic operations
task_store.create_task("task123", {"status": "pending", "data": {"key": "value"}})
task = task_store.get_task("task123")
task_store.update_task("task123", {"status": "completed"})
task_store.delete_task("task123")
```

### RedisQueueManager

Manages event queues using Redis Streams with consumer groups for reliable message delivery.

```python
from a2a_redis import RedisQueueManager

queue_manager = RedisQueueManager(
    redis_client,
    stream_name="a2a:events",
    consumer_group="a2a-agents",
    max_len=10000  # Limit stream length
)

# Enqueue events
event_id = queue_manager.enqueue({
    "type": "task_created",
    "task_id": "task123",
    "agent_id": "agent456"
})

# Dequeue events for a consumer
events = queue_manager.dequeue("consumer1", count=5, block=1000)
for event_id, event_data in events:
    # Process event
    print(f"Processing event {event_id}: {event_data}")
    
    # Acknowledge processing
    queue_manager.ack("consumer1", [event_id])
```

### RedisPushNotificationConfigStore

Stores push notification configurations for agents and users.

```python
from a2a_redis import RedisPushNotificationConfigStore

config_store = RedisPushNotificationConfigStore(redis_client)

# Create push notification config
config_store.create_config(
    agent_id="agent123",
    user_id="user456", 
    config={
        "endpoint": "https://fcm.googleapis.com/fcm/send",
        "auth_key": "your-auth-key",
        "enabled": True
    }
)

# Retrieve config
config = config_store.get_config("agent123", "user456")

# List all configs for an agent
agent_configs = config_store.list_configs_for_agent("agent123")
```

## Connection Management

The package includes robust connection management utilities:

```python
from a2a_redis.utils import RedisConnectionManager, create_redis_client

# Create managed Redis client
redis_client = create_redis_client(
    url="redis://localhost:6379/0",
    max_connections=50
)

# Or use connection manager directly
connection_manager = RedisConnectionManager(
    host="localhost",
    port=6379,
    db=0,
    max_connections=50,
    retry_on_timeout=True
)
redis_client = connection_manager.client
```

## Requirements

- Python 3.8+
- redis >= 4.0.0
- a2a-python (Agent-to-Agent Python SDK)

## Optional Dependencies

- RedisJSON module for `RedisJSONTaskStore` (enhanced nested data support)

## Development

```bash
# Clone the repository
git clone https://github.com/a2aproject/a2a-redis.git
cd a2a-redis

# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check .
black --check .
mypy .
```

## License

MIT License