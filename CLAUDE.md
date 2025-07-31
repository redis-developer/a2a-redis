# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.


IMPORTANT: ALWAYS USE THE VIRTUALENV AT .venv WHEN RUNNING COMMANDS


## Development Environment

This project uses **Redis 8** with all modules included (Search, JSON, etc.) as the backing store.

### Essential Commands

```bash
# Development setup
uv venv && source .venv/bin/activate && uv sync --dev

# Testing with coverage
uv run pytest --cov=a2a_redis --cov-report=term-missing

# Run single test file
uv run pytest tests/test_specific_file.py -v

# Code quality checks
uv run ruff check src/ tests/        # Linting
uv run ruff format src/ tests/       # Formatting
uv run pyright src/                  # Type checking

# Pre-commit hooks
uv run pre-commit run --all-files
```

## Architecture Overview

This project provides Redis-backed implementations for the Agent-to-Agent (A2A) Python SDK with a **two-tier queue architecture** that was recently restructured for better maintainability.

### Core Design Pattern: Two-Tier Queue Architecture

**Tier 1 - Queue Managers** (High-level abstractions):
- `RedisStreamsQueueManager` - Manages Redis Streams-based queues
- `RedisPubSubQueueManager` - Manages Redis Pub/Sub-based queues
- Both implement the A2A SDK's `QueueManager` interface

**Tier 2 - Event Queues** (Direct implementations):
- `RedisStreamsEventQueue` - Direct Redis Streams queue implementation
- `RedisPubSubEventQueue` - Direct Redis Pub/Sub queue implementation
- Both implement the A2A SDK's `EventQueue` interface

### Storage Strategy Pattern

**Task Storage** offers multiple backends:
- `RedisTaskStore` - Uses Redis hashes for general-purpose storage
- `RedisJSONTaskStore` - Uses RedisJSON module for native JSON operations

**Configuration Storage**:
- `RedisPushNotificationConfigStore` - Push notification configurations

### Consumer Group Strategy Pattern

The `ConsumerGroupStrategy` enum defines load balancing behavior:
- `SHARED_LOAD_BALANCING` - Multiple instances share work across a single consumer group
- `INSTANCE_ISOLATED` - Each instance gets its own consumer group
- `CUSTOM` - User provides custom consumer group name

## Key Architectural Decisions

### Redis Streams vs Pub/Sub Trade-offs

**Redis Streams (Default)**:
- ✅ Persistent storage, guaranteed delivery, consumer groups, failure recovery
- ❌ Higher memory usage, more complex setup
- **Use for**: Task queues, audit trails, reliable processing

**Redis Pub/Sub**:
- ✅ Low-latency, minimal memory footprint, natural broadcasting
- ❌ No persistence, no delivery guarantees, events lost without active subscribers
- **Use for**: Live notifications, real-time updates, system events

### Connection Resilience

The `utils.py` module provides comprehensive Redis connection management:
- `RedisConnectionManager` - Health checks and auto-reconnection logic
- `@redis_retry` decorator - Exponential backoff for connection errors
- `safe_redis_operation` - Wrapper for operations with fallback values

### Configuration Patterns

All components support:
- **Prefix-based namespacing** for Redis key organization
- **Flexible consumer group strategies** for load balancing vs isolation
- **Connection pooling** with configurable limits and timeouts

## Testing Patterns

- **Test isolation**: Uses Redis database 15 for testing
- **Dual testing approach**: Both mock Redis tests and real Redis integration tests
- **Async support**: Full pytest-asyncio integration for async method testing
- **Comprehensive fixtures**: Shared test setup in `conftest.py`

## Recent Restructuring (Important Context)

The codebase recently underwent major architectural improvements:
- **Removed**: Unified `queue_manager.py` that contained duplicate implementations
- **Added**: Clear separation between queue managers and direct implementations
- **Result**: Better maintainability and clearer component boundaries

This restructuring eliminated code duplication while maintaining backward compatibility through the package's `__init__.py` exports.
