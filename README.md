# a2a-redis

Redis components for the Agent-to-Agent (A2A) Python SDK.

This package provides Redis-backed implementations of core A2A components for persistent task storage, event queue management, and push notification configuration using Redis.

## Features

- **RedisTaskStore**: Redis-backed task storage using Redis hashes or RedisJSON
- **RedisQueueManager**: Event queue management using Redis Lists with async support
- **RedisPushNotificationConfigStore**: Task-based push notification configuration storage
- **Connection Management**: Robust Redis connection handling with automatic retry and health monitoring

## Installation

```bash
pip install a2a-redis
```

## Quick Start

```python
from a2a_redis import RedisTaskStore, RedisQueueManager, RedisPushNotificationConfigStore
from a2a_redis.utils import create_redis_client
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.apps import A2AStarletteApplication

# Create Redis client with connection management
redis_client = create_redis_client(url="redis://localhost:6379/0", max_connections=50)

# Initialize Redis components
task_store = RedisTaskStore(redis_client, prefix="myapp:tasks:")
queue_manager = RedisQueueManager(redis_client, prefix="myapp:queues:")
push_config_store = RedisPushNotificationConfigStore(redis_client, prefix="myapp:push:")

# Use with A2A request handler
request_handler = DefaultRequestHandler(
    agent_executor=YourAgentExecutor(),
    task_store=task_store,
    queue_manager=queue_manager,
    push_config_store=push_config_store,
)

# Create A2A server
server = A2AStarletteApplication(
    agent_card=your_agent_card,
    http_handler=request_handler
)
```

## Components

### RedisTaskStore

Stores task data in Redis using hashes for simple data or RedisJSON for complex nested data. Implements the A2A TaskStore interface.

```python
from a2a_redis import RedisTaskStore, RedisJSONTaskStore

# Using Redis hashes (works with any Redis server)
task_store = RedisTaskStore(redis_client, prefix="mytasks:")

# Using RedisJSON (requires RedisJSON module)
json_task_store = RedisJSONTaskStore(redis_client, prefix="mytasks:")

# A2A TaskStore interface methods
task_store.save("task123", {"status": "pending", "data": {"key": "value"}})
task = task_store.get("task123")
success = task_store.delete("task123")

# List all task IDs (utility method)
task_ids = task_store.list_task_ids()
```

### RedisQueueManager

Manages event queues using Redis Lists for task-based event queues. Implements the A2A QueueManager interface with async support.

```python
import asyncio
from a2a_redis import RedisQueueManager

queue_manager = RedisQueueManager(redis_client, prefix="myapp:queues:")

async def main():
    # Create or get existing queue for a task
    queue = await queue_manager.create_or_tap("task123")

    # Enqueue events
    queue.enqueue_event({"type": "progress", "message": "Task started"})
    queue.enqueue_event({"type": "progress", "message": "50% complete"})

    # Dequeue events
    try:
        event = queue.dequeue_event(no_wait=True)  # Non-blocking
        print(f"Got event: {event}")
    except RuntimeError:
        print("No events available")

    # Close queue when done
    await queue_manager.close("task123")

asyncio.run(main())
```

### RedisPushNotificationConfigStore

Stores push notification configurations per task. Implements the A2A PushNotificationConfigStore interface.

```python
from a2a_redis import RedisPushNotificationConfigStore
from a2a.types import PushNotificationConfig

config_store = RedisPushNotificationConfigStore(redis_client, prefix="myapp:push:")

# Create push notification config
config = PushNotificationConfig(
    url="https://webhook.example.com/notify",
    token="secret_token",
    id="webhook_1"
)

# A2A interface methods
config_store.set_info("task123", config)

# Get all configs for a task
configs = config_store.get_info("task123")
for config in configs:
    print(f"Config {config.id}: {config.url}")

# Delete specific config or all configs for a task
config_store.delete_info("task123", "webhook_1")  # Delete specific
config_store.delete_info("task123")  # Delete all
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

- Python 3.11+
- redis >= 4.0.0
- a2a-sdk >= 0.2.16 (Agent-to-Agent Python SDK)
- uvicorn >= 0.35.0

## Optional Dependencies

- RedisJSON module for `RedisJSONTaskStore` (enhanced nested data support)

## Development

```bash
# Clone the repository
git clone https://github.com/a2aproject/a2a-redis.git
cd a2a-redis

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
uv sync --dev

# Run tests with coverage
uv run pytest --cov=a2a_redis --cov-report=term-missing

# Run linting and formatting
uv run ruff check src/ tests/
uv run ruff format src/ tests/
uv run pyright src/

# Install pre-commit hooks
uv run pre-commit install

# Run example
uv run python examples/redis_travel_agent.py
```

## License

MIT License
