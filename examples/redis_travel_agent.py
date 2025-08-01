"""Example A2A agent using Redis components for storage and queue management.

This example uses Redis Streams for reliable event queuing (default).
For real-time, fire-and-forget scenarios, see pubsub_vs_streams_comparison.py
which demonstrates both Redis Streams and Redis Pub/Sub implementations.
"""

from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentSkill,
)

# Import our Redis components
from a2a_redis import (
    RedisTaskStore,
    RedisStreamsQueueManager,
    RedisPushNotificationConfigStore,
)
from a2a_redis.utils import create_redis_client


# Example agent executor - you would implement this with your actual agent logic
class TravelPlannerAgentExecutor:
    """Example travel planner agent executor."""

    def __init__(self):
        self.name = "TravelPlannerAgent"

    async def execute(self, request):
        """Execute travel planning request."""
        # This would contain your actual agent logic
        # For now, just return a simple response
        return {
            "response": f"Planning your trip! Request: {request}",
            "status": "completed",
        }


def create_redis_components():
    """Create Redis-backed A2A components."""
    # Create Redis client with connection retry and health monitoring
    redis_client = create_redis_client(
        url="redis://localhost:6379/0",
        # Alternative: specify individual parameters
        # host="localhost",
        # port=6379,
        # db=0,
        # password=None,
        max_connections=50,
    )

    # Create Redis-backed components (all working with a2a-sdk interfaces)
    task_store = RedisTaskStore(redis_client, prefix="travel_agent:tasks:")
    queue_manager = RedisStreamsQueueManager(
        redis_client, prefix="travel_agent:streams:"
    )
    push_config_store = RedisPushNotificationConfigStore(
        redis_client, prefix="travel_agent:push:"
    )

    return task_store, queue_manager, push_config_store


def main():
    """Main function to set up and run the A2A agent with Redis components."""

    # Create agent skill definition
    skill = AgentSkill(
        id="travel_planner",
        name="travel planner agent",
        description="An intelligent travel planning agent that helps users plan trips, find accommodations, and create itineraries",
        tags=["travel", "planning", "hotels", "flights", "itinerary"],
        examples=[
            "Plan a 5-day trip to Paris",
            "Find hotels in Tokyo for next week",
            "Create an itinerary for a family vacation to Hawaii",
        ],
    )

    # Create agent card with metadata
    agent_card = AgentCard(
        name="Travel Planner Agent (Redis-backed)",
        description="AI-powered travel planning agent with persistent Redis storage",
        url="http://localhost:10001/",
        version="1.0.0",
        default_input_modes=["text"],
        default_output_modes=["text"],
        capabilities=AgentCapabilities(streaming=True),
        skills=[skill],
    )

    # Create Redis components
    task_store, queue_manager, push_config_store = create_redis_components()

    # Create request handler with Redis components
    request_handler = DefaultRequestHandler(
        agent_executor=TravelPlannerAgentExecutor(),
        task_store=task_store,
        queue_manager=queue_manager,
        push_config_store=push_config_store,
    )

    # Create A2A server application
    server = A2AStarletteApplication(
        agent_card=agent_card, http_handler=request_handler
    )

    print("🚀 Starting Travel Planner Agent with full Redis backend...")
    print("📍 Agent URL: http://localhost:10001/")
    print("🔄 Using Redis for:")
    print("   • Task storage (persistent) ✅")
    print("   • Event queues (Redis Streams with consumer groups) ✅")
    print("   • Push notification configs (Redis Hashes) ✅")
    print("\n💡 Make sure Redis is running on localhost:6379")
    print("📝 All agent data is now persisted in Redis - fully stateless agent!")
    print("🔄 Agent can be restarted without losing any state")

    # Start the server
    import uvicorn

    uvicorn.run(server.build(), host="0.0.0.0", port=10001)


if __name__ == "__main__":
    main()
