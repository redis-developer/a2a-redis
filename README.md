# a2a-redis

Redis components for the Agent-to-Agent (A2A) Python SDK.

This package provides Redis-backed implementations of core A2A components for persistent task storage, reliable event queue management, and push notification configuration using Redis.

## Features

- **RedisTaskStore & RedisJSONTaskStore**: Redis-backed task storage using hashes or JSON
- **RedisStreamsQueueManager & RedisStreamsEventQueue**: Persistent, reliable event queues with consumer groups
- **RedisPubSubQueueManager & RedisPubSubEventQueue**: Real-time, low-latency event broadcasting
- **RedisPushNotificationConfigStore**: Task-based push notification configuration storage
- **Consumer Group Strategies for Streams**: Flexible load balancing and instance isolation patterns

## Installation

```bash
pip install a2a-redis
```

## Quick Start

```python
from a2a_redis import RedisTaskStore, RedisStreamsQueueManager, RedisPushNotificationConfigStore
from a2a_redis.utils import create_redis_client
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.apps import A2AStarletteApplication

# Create Redis client with connection management
redis_client = create_redis_client(url="redis://localhost:6379/0", max_connections=50)

# Initialize Redis components
task_store = RedisTaskStore(redis_client, prefix="myapp:tasks:")
queue_manager = RedisStreamsQueueManager(redis_client, prefix="myapp:queues:")
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

## Queue Components

The package provides both high-level queue managers and direct queue implementations:

### Queue Managers
- `RedisStreamsQueueManager` - Manages Redis Streams-based queues
- `RedisPubSubQueueManager` - Manages Redis Pub/Sub-based queues
- Both implement the A2A SDK's `QueueManager` interface

### Event Queues
- `RedisStreamsEventQueue` - Direct Redis Streams queue implementation
- `RedisPubSubEventQueue` - Direct Redis Pub/Sub queue implementation
- Both implement the `EventQueue` interface through protocol compliance

## Queue Manager Types: Streams vs Pub/Sub

### RedisStreamsQueueManager

**Key Features:**
- **Persistent storage**: Events remain in streams until explicitly trimmed
- **Guaranteed delivery**: Consumer groups with acknowledgments prevent message loss
- **Load balancing**: Multiple consumers can share work via consumer groups
- **Failure recovery**: Unacknowledged messages can be reclaimed by other consumers
- **Event replay**: Historical events can be re-read from any point in time
- **Ordering**: Maintains strict insertion order with unique message IDs

**Use Cases:**
- Task event queues requiring reliability
- Audit trails and event history
- Work distribution systems
- Systems requiring failure recovery
- Multi-consumer load balancing

**Trade-offs:**
- Higher memory usage (events persist)
- More complex setup (consumer groups)
- Slightly higher latency than pub/sub

### RedisPubSubQueueManager

**Key Features:**
- **Real-time delivery**: Events delivered immediately to active subscribers
- **No persistence**: Events not stored, only delivered to active consumers
- **Fire-and-forget**: No acknowledgments or delivery guarantees
- **Broadcasting**: All subscribers receive all events
- **Low latency**: Minimal overhead for immediate delivery
- **Minimal memory usage**: No storage of events

**Use Cases:**
- Live status updates and notifications
- Real-time dashboard updates
- System event broadcasting
- Non-critical event distribution
- Low-latency requirements
- Simple fan-out scenarios

**Not suitable for:**
- Critical event processing requiring guarantees
- Systems requiring event replay or audit trails
- Offline-capable applications
- Work queues requiring load balancing

## Components

### Task Storage

#### RedisTaskStore
Stores task data in Redis using hashes with JSON serialization. Works with any Redis server.

```python
from a2a_redis import RedisTaskStore

task_store = RedisTaskStore(redis_client, prefix="mytasks:")

# A2A TaskStore interface methods
await task_store.save("task123", {"status": "pending", "data": {"key": "value"}})
task = await task_store.get("task123")
success = await task_store.delete("task123")

# List all task IDs (utility method)
task_ids = await task_store.list_task_ids()
```

#### RedisJSONTaskStore
Stores task data using Redis's JSON module for native JSON operations and complex nested data.

```python
from a2a_redis import RedisJSONTaskStore

# Requires Redis 8 or RedisJSON module
json_task_store = RedisJSONTaskStore(redis_client, prefix="mytasks:")

# Same interface as RedisTaskStore but with native JSON support
await json_task_store.save("task123", {"complex": {"nested": {"data": "value"}}})
```

### Queue Managers

Both queue managers implement the A2A QueueManager interface with full async support:

```python
import asyncio
from a2a_redis import RedisStreamsQueueManager, RedisPubSubQueueManager
from a2a_redis.streams_consumer_strategy import ConsumerGroupConfig, ConsumerGroupStrategy

# Choose based on your requirements:

# For reliable, persistent processing
streams_manager = RedisStreamsQueueManager(redis_client, prefix="myapp:streams:")

# For real-time, low-latency broadcasting
pubsub_manager = RedisPubSubQueueManager(redis_client, prefix="myapp:pubsub:")

# With custom consumer group configuration (streams only)
config = ConsumerGroupConfig(strategy=ConsumerGroupStrategy.SHARED_LOAD_BALANCING)
streams_manager = RedisStreamsQueueManager(redis_client, consumer_config=config)

async def main():
    # Same interface for both managers
    queue = await streams_manager.create_or_tap("task123")

    # Enqueue events
    await queue.enqueue_event({"type": "progress", "message": "Task started"})
    await queue.enqueue_event({"type": "progress", "message": "50% complete"})

    # Dequeue events
    try:
        event = await queue.dequeue_event(no_wait=True)  # Non-blocking
        print(f"Got event: {event}")
        await queue.task_done()  # Acknowledge the message (streams only)
    except RuntimeError:
        print("No events available")

    # Close queue when done
    await queue.close()

asyncio.run(main())
```

### Consumer Group Strategies

The Streams queue manager supports different consumer group strategies:

```python
from a2a_redis.streams_consumer_strategy import ConsumerGroupStrategy, ConsumerGroupConfig

# Multiple instances share work across a single consumer group
config = ConsumerGroupConfig(strategy=ConsumerGroupStrategy.SHARED_LOAD_BALANCING)

# Each instance gets its own consumer group
config = ConsumerGroupConfig(strategy=ConsumerGroupStrategy.INSTANCE_ISOLATED)

# Custom consumer group name
config = ConsumerGroupConfig(strategy=ConsumerGroupStrategy.CUSTOM, group_name="my_group")

streams_manager = RedisStreamsQueueManager(redis_client, consumer_config=config)
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
await config_store.set_info("task123", config)

# Get all configs for a task
configs = await config_store.get_info("task123")
for config in configs:
    print(f"Config {config.id}: {config.url}")

# Delete specific config or all configs for a task
await config_store.delete_info("task123", "webhook_1")  # Delete specific
await config_store.delete_info("task123")  # Delete all
```

## Connection Management

The package includes robust connection management utilities with retry logic and health monitoring:

```python
from a2a_redis.utils import RedisConnectionManager, create_redis_client, redis_retry, safe_redis_operation

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

# Use retry decorator for custom operations
@redis_retry(max_retries=3, backoff_factor=1.0)
async def my_redis_operation(redis_client):
    return await redis_client.get("mykey")

# Safe operations with fallback values
result = await safe_redis_operation(
    lambda: redis_client.get("mykey"),
    fallback="default_value"
)
```

## Requirements

- Python 3.11+
- redis >= 4.0.0
- a2a-sdk >= 0.2.16 (Agent-to-Agent Python SDK)
- uvicorn >= 0.35.0

## Optional Dependencies

- RedisJSON module for `RedisJSONTaskStore` (enhanced nested data support)
- Redis Stack or Redis with modules for full feature support

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

# Run examples
uv run python examples/basic_usage.py
uv run python examples/redis_travel_agent.py
```

## Testing

Tests use Redis database 15 for isolation and include both mock and real Redis integration tests:

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_streams_queue_manager.py -v

# Run with coverage
uv run pytest --cov=a2a_redis --cov-report=term-missing
```

## License

MIT License
