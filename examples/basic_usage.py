"""Basic usage example for a2a-redis components."""

import asyncio
from a2a.types import Task, TaskStatus, TaskState, PushNotificationConfig
from a2a_redis import (
    RedisTaskStore,
    RedisStreamsQueueManager,
    RedisPubSubQueueManager,
    RedisPushNotificationConfigStore,
    RedisStreamsEventQueue,
    RedisPubSubEventQueue,
)
from a2a_redis.utils import create_redis_client


async def main():
    """Demonstrate basic usage of a2a-redis components."""

    print("=== a2a-redis Components Demo ===")
    print("Demonstrating practical usage patterns...\n")

    # Create Redis client with connection handling
    try:
        redis_client = create_redis_client(url="redis://localhost:6379/0")
        print("✓ Redis connection established")
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        print("Make sure Redis is running on localhost:6379")
        return

    print("\n=== RedisTaskStore Example ===")

    # Initialize task store
    task_store = RedisTaskStore(redis_client, prefix="example:")

    # Create a task
    task_id = "task_001"
    task = Task(
        id=task_id,
        context_id="ctx_001",
        status=TaskStatus(state=TaskState.submitted),
        metadata={
            "user_id": "user123",
            "priority": "high",
            "description": "Process user request",
        },
    )

    print(f"Creating task {task_id}")
    await task_store.save(task)

    # Retrieve the task
    retrieved_task = await task_store.get(task_id)
    if retrieved_task:
        print(f"Retrieved task: {retrieved_task.id} - {retrieved_task.status.state}")
        print(f"  Metadata: {retrieved_task.metadata}")

        # Update the task
        updates = {
            "status": TaskStatus(state=TaskState.working),
            "metadata": {**retrieved_task.metadata, "progress": 50},
        }
        await task_store.update_task(task_id, updates)
        updated_task = await task_store.get(task_id)
        if updated_task:
            print(f"Updated task: {updated_task.id} - {updated_task.status.state}")
            print(f"  Progress: {updated_task.metadata.get('progress', 0)}%")

    # List tasks
    all_task_ids = await task_store.list_task_ids()
    print(f"All task IDs: {all_task_ids}")

    print("\n=== RedisStreamsQueueManager Example ===")

    # Initialize queue manager for reliable event processing
    streams_manager = RedisStreamsQueueManager(redis_client, prefix="example:streams:")

    # Add a queue for our task
    await streams_manager.add(task_id, None)
    queue = await streams_manager.get(task_id)

    if queue:
        # Enqueue some events
        events_to_send = [
            {"type": "task_created", "task_id": task_id, "user_id": "user123"},
            {"type": "task_updated", "task_id": task_id, "status": "working"},
            {"type": "notification", "message": "Hello from A2A Streams!"},
        ]

        for event in events_to_send:
            await queue.enqueue_event(event)
            print(f"Enqueued to streams: {event}")

        # Dequeue events (streams provide guaranteed delivery)
        print("\nProcessing events from streams queue:")
        for _ in range(len(events_to_send)):
            try:
                event_data = await queue.dequeue_event(no_wait=True)
                print(f"Received from streams: {event_data}")
            except RuntimeError:
                print("No more events available in streams")
                break

    print("\n=== RedisPubSubQueueManager Example ===")

    # Initialize pub/sub manager for real-time messaging
    pubsub_manager = RedisPubSubQueueManager(redis_client, prefix="example:pubsub:")

    # Add a queue for real-time notifications
    await pubsub_manager.add(task_id, None)
    pubsub_queue = await pubsub_manager.get(task_id)

    if pubsub_queue:
        # Create a subscriber tap for broadcasting
        subscriber = await pubsub_manager.tap(task_id)

        if subscriber:
            # Give the subscription time to establish
            await asyncio.sleep(0.1)

            # Enqueue real-time events
            realtime_events = [
                {"type": "status_update", "task_id": task_id, "status": "working"},
                {"type": "progress_update", "task_id": task_id, "progress": 75},
                {"type": "user_notification", "message": "Task is almost complete!"},
            ]

            for event in realtime_events:
                await pubsub_queue.enqueue_event(event)
                print(f"Published to pub/sub: {event}")

            # Brief pause for message propagation
            await asyncio.sleep(0.1)

            # Try to receive messages (pub/sub is fire-and-forget)
            print("\nAttempting to receive pub/sub events:")
            for _ in range(len(realtime_events)):
                try:
                    event_data = await subscriber.dequeue_event(no_wait=True)
                    print(f"Received from pub/sub: {event_data}")
                except RuntimeError:
                    # This is expected in pub/sub if timing doesn't align
                    print("No events available (pub/sub timing)")
                    break

    print("\n=== RedisPushNotificationConfigStore Example ===")

    # Initialize push notification config store
    config_store = RedisPushNotificationConfigStore(
        redis_client, prefix="example:push:"
    )

    # Create push notification configs
    configs_to_create = [
        PushNotificationConfig(
            id="travel_config",
            url="https://fcm.googleapis.com/fcm/send",
            token="fcm_token_123",
        ),
        PushNotificationConfig(
            id="weather_config",
            url="https://api.pushover.net/1/messages.json",
            token="pushover_token_456",
        ),
    ]

    for config in configs_to_create:
        await config_store.set_info(task_id, config)
        print(f"Created push config: {config.id}")

    # Retrieve configs
    stored_configs = await config_store.get_info(task_id)
    print(f"Retrieved {len(stored_configs)} push notification configs:")
    for config in stored_configs:
        print(f"  - {config.id}: {config.url}")

    print("\n=== Direct Queue Usage Example ===")

    # Sometimes you want direct queue access for fine-grained control
    direct_streams = RedisStreamsEventQueue(
        redis_client, "direct_demo", prefix="example:direct:streams:"
    )
    direct_pubsub = RedisPubSubEventQueue(
        redis_client, "direct_demo", prefix="example:direct:pubsub:"
    )

    # Direct streams usage
    await direct_streams.enqueue_event(
        {"type": "direct_streams", "message": "Direct streams access"}
    )
    direct_event = await direct_streams.dequeue_event(no_wait=True)
    print(f"Direct streams event: {direct_event}")

    # Direct pub/sub usage
    await direct_pubsub.enqueue_event(
        {"type": "direct_pubsub", "message": "Direct pub/sub access"}
    )
    await asyncio.sleep(0.1)  # Brief pause for message propagation
    try:
        direct_pubsub_event = await direct_pubsub.dequeue_event(no_wait=True)
        print(f"Direct pub/sub event: {direct_pubsub_event}")
    except RuntimeError:
        print("Direct pub/sub event: (no subscriber was listening)")

    print("\n=== Architecture Overview ===")
    print("Queue Managers (High-level):")
    print(
        "  • RedisStreamsQueueManager - Persistent, reliable queues with delivery guarantees"
    )
    print(
        "  • RedisPubSubQueueManager - Real-time, fire-and-forget broadcast messaging"
    )
    print("\nDirect Implementations (Low-level):")
    print(
        "  • RedisStreamsEventQueue - Direct streams queue access with consumer groups"
    )
    print("  • RedisPubSubEventQueue - Direct pub/sub queue access for broadcasting")
    print("\nStorage & Configuration:")
    print("  • RedisTaskStore - Task persistence with Redis hashes or JSON")
    print("  • RedisPushNotificationConfigStore - Push notification endpoint configs")

    print("\n=== Usage Guidelines ===")
    print("Use Streams when you need:")
    print("  ✓ Guaranteed delivery and message persistence")
    print("  ✓ Consumer groups for load balancing")
    print("  ✓ Message replay and audit trails")
    print("  ✓ Failure recovery and acknowledgments")

    print("\nUse Pub/Sub when you need:")
    print("  ✓ Real-time, low-latency messaging")
    print("  ✓ Broadcasting to multiple subscribers")
    print("  ✓ Fire-and-forget event notifications")
    print("  ✓ Minimal memory usage")

    # Cleanup
    print("\n=== Cleanup ===")
    await task_store.delete(task_id)
    await config_store.delete_info(task_id)
    await streams_manager.close(task_id)
    await pubsub_manager.close(task_id)
    await direct_streams.close()
    await direct_pubsub.close()
    print("✅ Cleaned up example data")

    print("\n✅ Demo complete! All components ready for production use.")


if __name__ == "__main__":
    asyncio.run(main())
