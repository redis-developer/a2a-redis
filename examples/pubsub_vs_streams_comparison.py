"""Comparison example demonstrating Redis Streams vs Pub/Sub queue implementations.

This example shows the key differences between the two Redis queue implementations
and helps you choose the right one for your use case.
"""

import asyncio
from typing import Dict, Any

from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentSkill,
)

from a2a_redis import (
    RedisTaskStore,
    RedisStreamsQueueManager,
    RedisPubSubQueueManager,
    RedisPushNotificationConfigStore,
    ConsumerGroupStrategy,
    ConsumerGroupConfig,
)
from a2a_redis.utils import create_redis_client


class ExampleAgentExecutor:
    """Simple agent executor for demonstration."""

    def __init__(self, name: str):
        self.name = name

    async def execute(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Execute agent request."""
        return {
            "response": f"{self.name} processed: {request}",
            "status": "completed",
        }


def create_streams_components():
    """Create Redis Streams-based components (default, recommended).

    **Use Redis Streams when you need**:
    - Reliable event processing with guaranteed delivery
    - Event persistence and replay capability
    - Audit trails and event history
    - Consumer groups for load balancing
    - Failure recovery (events survive crashes)

    **Trade-offs**:
    - Higher memory usage (events persist until trimmed)
    - Slightly higher latency than pub/sub
    - More complex setup (consumer groups)
    """
    redis_client = create_redis_client(url="redis://localhost:6379/1")

    # Configure consumer group strategy
    consumer_config = ConsumerGroupConfig(
        strategy=ConsumerGroupStrategy.SHARED_LOAD_BALANCING,
        consumer_prefix="reliable_agent",
    )

    task_store = RedisTaskStore(redis_client, prefix="streams_agent:tasks:")
    queue_manager = RedisStreamsQueueManager(
        redis_client,
        prefix="streams_agent:events:",
        consumer_config=consumer_config,
    )
    push_config_store = RedisPushNotificationConfigStore(
        redis_client, prefix="streams_agent:push:"
    )

    return task_store, queue_manager, push_config_store


def create_pubsub_components():
    """Create Redis Pub/Sub-based components.

    **Use Redis Pub/Sub when you need**:
    - Real-time, low-latency event delivery
    - Fire-and-forget broadcasting to multiple listeners
    - Live notifications and dashboard updates
    - System-wide event announcements
    - Minimal memory usage

    **Trade-offs**:
    - No persistence (events lost if no active subscribers)
    - No delivery guarantees (fire-and-forget)
    - No replay capability
    - Limited error recovery
    """
    redis_client = create_redis_client(url="redis://localhost:6379/2")

    task_store = RedisTaskStore(redis_client, prefix="pubsub_agent:tasks:")
    queue_manager = RedisPubSubQueueManager(
        redis_client,
        prefix="pubsub_agent:events:",
        # Note: Pub/Sub doesn't use consumer configs
    )
    push_config_store = RedisPushNotificationConfigStore(
        redis_client, prefix="pubsub_agent:push:"
    )

    return task_store, queue_manager, push_config_store


def create_agent_card(name: str, port: int) -> AgentCard:
    """Create agent card configuration."""
    skill = AgentSkill(
        id=f"{name.lower()}_processor",
        name=f"{name} Event Processor",
        description=f"Demonstrates {name} event processing patterns",
        tags=["example", "demo", name.lower()],
        examples=[
            f"Process events with {name}",
            f"Handle notifications via {name}",
        ],
    )

    return AgentCard(
        name=f"{name} Agent Example",
        description=f"Example A2A agent using {name} for event queuing",
        url=f"http://localhost:{port}/",
        version="1.0.0",
        default_input_modes=["text"],
        default_output_modes=["text"],
        capabilities=AgentCapabilities(streaming=True),
        skills=[skill],
    )


async def run_streams_agent():
    """Run agent with Redis Streams (reliable, persistent events)."""
    print("🔄 Starting Streams-based Agent...")
    print("✅ Features: Persistent events, guaranteed delivery, consumer groups")
    print("⚠️  Trade-offs: Higher memory usage, slightly higher latency")
    print("🎯 Best for: Task queues, audit trails, reliable processing\n")

    # Create components
    task_store, queue_manager, push_config_store = create_streams_components()

    # Create request handler
    request_handler = DefaultRequestHandler(
        agent_executor=ExampleAgentExecutor("StreamsAgent"),
        task_store=task_store,
        queue_manager=queue_manager,
        push_config_store=push_config_store,
    )

    # Create server
    agent_card = create_agent_card("Streams", 10001)
    server = A2AStarletteApplication(
        agent_card=agent_card, http_handler=request_handler
    )

    print("🚀 Streams Agent running on http://localhost:10001/")
    print("📊 Events are persistent and recoverable")
    print("👥 Supports multiple consumers with load balancing")
    return server


async def run_pubsub_agent():
    """Run agent with Redis Pub/Sub (real-time, fire-and-forget events)."""
    print("\n🔄 Starting Pub/Sub-based Agent...")
    print("✅ Features: Real-time delivery, broadcasting, minimal memory")
    print("⚠️  Trade-offs: No persistence, no delivery guarantees")
    print("🎯 Best for: Live notifications, real-time updates, broadcasting\n")

    # Create components
    task_store, queue_manager, push_config_store = create_pubsub_components()

    # Create request handler
    request_handler = DefaultRequestHandler(
        agent_executor=ExampleAgentExecutor("PubSubAgent"),
        task_store=task_store,
        queue_manager=queue_manager,
        push_config_store=push_config_store,
    )

    # Create server
    agent_card = create_agent_card("PubSub", 10002)
    server = A2AStarletteApplication(
        agent_card=agent_card, http_handler=request_handler
    )

    print("🚀 Pub/Sub Agent running on http://localhost:10002/")
    print("⚡ Events are delivered in real-time")
    print("📢 Supports broadcasting to multiple subscribers")
    return server


def demonstrate_differences():
    """Demonstrate key differences between the two approaches."""
    print("\n" + "=" * 60)
    print("REDIS STREAMS vs PUB/SUB COMPARISON")
    print("=" * 60)
    print()

    print("📊 REDIS STREAMS (Default - Recommended for most use cases)")
    print("   ✅ Persistent event storage")
    print("   ✅ Guaranteed delivery with acknowledgments")
    print("   ✅ Consumer groups for load balancing")
    print("   ✅ Event replay and audit trails")
    print("   ✅ Automatic failure recovery")
    print("   ❌ Higher memory usage")
    print("   ❌ Slightly higher latency")
    print("   🎯 Use for: Task queues, reliable processing, audit logs")
    print()

    print("⚡ REDIS PUB/SUB (For real-time, low-latency scenarios)")
    print("   ✅ Real-time, low-latency delivery")
    print("   ✅ Natural broadcasting pattern")
    print("   ✅ Minimal memory usage")
    print("   ✅ Simple fire-and-forget semantics")
    print("   ❌ No persistence (offline consumers miss events)")
    print("   ❌ No delivery guarantees")
    print("   ❌ No replay capability")
    print("   🎯 Use for: Live notifications, real-time updates, broadcasting")
    print()

    print("💡 CHOOSING THE RIGHT APPROACH:")
    print("   • Need reliability & persistence? → Use Redis Streams")
    print("   • Need real-time broadcasting? → Use Redis Pub/Sub")
    print("   • Task/work queues? → Use Redis Streams")
    print("   • Live dashboards/notifications? → Use Redis Pub/Sub")
    print("   • Audit trails required? → Use Redis Streams")
    print("   • Fire-and-forget events? → Use Redis Pub/Sub")


async def main():
    """Run the comparison demonstration."""
    demonstrate_differences()

    print("\n🔧 Setup Instructions:")
    print("1. Start Redis: redis-server")
    print("2. Run this script: python examples/pubsub_vs_streams_comparison.py")
    print("3. Test Streams Agent: curl http://localhost:10001/")
    print("4. Test Pub/Sub Agent: curl http://localhost:10002/")
    print("\n⚠️  Note: This example shows setup only. For full server operation,")
    print("    uncomment the uvicorn.run() calls below and run each agent separately.")
    print("\n" + "=" * 60)

    # Uncomment to actually run the servers (run each in separate processes)
    # import uvicorn
    #
    # # Run streams agent
    # streams_server = await run_streams_agent()
    # uvicorn.run(streams_server.build(), host="0.0.0.0", port=10001)
    #
    # # Or run pub/sub agent
    # pubsub_server = await run_pubsub_agent()
    # uvicorn.run(pubsub_server.build(), host="0.0.0.0", port=10002)


if __name__ == "__main__":
    asyncio.run(main())
