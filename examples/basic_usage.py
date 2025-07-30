"""Basic usage example for a2a-redis components."""

import redis
from a2a_redis import (
    RedisTaskStore,
    RedisQueueManager,
    RedisPushNotificationConfigStore,
)


def main():
    """Demonstrate basic usage of a2a-redis components."""

    # Create Redis client
    redis_client = redis.from_url("redis://localhost:6379/0")

    print("=== RedisTaskStore Example ===")

    # Initialize task store
    task_store = RedisTaskStore(redis_client, prefix="example:")

    # Create a task
    task_id = "task_001"
    task_data = {
        "status": "pending",
        "description": "Process user request",
        "metadata": {"user_id": "user123", "priority": "high"},
    }

    print(f"Creating task {task_id}")
    task_store.create_task(task_id, task_data)

    # Retrieve the task
    retrieved_task = task_store.get_task(task_id)
    print(f"Retrieved task: {retrieved_task}")

    # Update the task
    task_store.update_task(task_id, {"status": "in_progress"})
    updated_task = task_store.get_task(task_id)
    print(f"Updated task: {updated_task}")

    # List tasks
    all_tasks = task_store.list_tasks()
    print(f"All tasks: {all_tasks}")

    print("\n=== RedisQueueManager Example ===")

    # Initialize queue manager
    queue_manager = RedisQueueManager(
        redis_client, stream_name="example:events", consumer_group="example-group"
    )

    # Enqueue some events
    events_to_send = [
        {"type": "task_created", "task_id": "task_001", "user_id": "user123"},
        {"type": "task_updated", "task_id": "task_001", "status": "in_progress"},
        {"type": "notification", "message": "Hello from A2A!"},
    ]

    for event in events_to_send:
        event_id = queue_manager.enqueue(event)
        print(f"Enqueued event {event_id}: {event}")

    # Dequeue events as a consumer
    consumer_id = "consumer_001"
    print(f"\nReading events as consumer {consumer_id}:")

    events = queue_manager.dequeue(consumer_id, count=5, block=1000)
    for event_id, event_data in events:
        print(f"Received event {event_id}: {event_data}")

        # Acknowledge the event
        queue_manager.ack(consumer_id, [event_id])
        print(f"Acknowledged event {event_id}")

    print("\n=== RedisPushNotificationConfigStore Example ===")

    # Initialize push notification config store
    config_store = RedisPushNotificationConfigStore(redis_client, prefix="push:")

    # Create push notification configs
    configs = [
        {
            "agent_id": "travel_agent",
            "user_id": "user123",
            "config": {
                "endpoint": "https://fcm.googleapis.com/fcm/send",
                "auth_token": "fcm_token_123",
                "enabled": True,
                "preferences": {"task_updates": True, "reminders": False},
            },
        },
        {
            "agent_id": "weather_agent",
            "user_id": "user123",
            "config": {
                "endpoint": "https://api.pushover.net/1/messages.json",
                "user_key": "pushover_user_123",
                "enabled": True,
            },
        },
    ]

    for config_data in configs:
        config_store.create_config(
            config_data["agent_id"], config_data["user_id"], config_data["config"]
        )
        print(
            f"Created config for {config_data['agent_id']} - {config_data['user_id']}"
        )

    # Retrieve specific config
    travel_config = config_store.get_config("travel_agent", "user123")
    print(f"Travel agent config: {travel_config}")

    # List all configs for a user
    user_configs = config_store.list_configs_for_user("user123")
    print(f"All configs for user123: {len(user_configs)} configs")
    for config in user_configs:
        print(f"  - {config['agent_id']}: enabled={config.get('enabled', False)}")

    # List all configs for an agent
    agent_configs = config_store.list_configs_for_agent("travel_agent")
    print(f"All configs for travel_agent: {len(agent_configs)} configs")

    # Cleanup
    print("\n=== Cleanup ===")
    task_store.delete_task(task_id)
    config_store.cleanup_configs_for_user("user123")
    print("Cleaned up example data")


if __name__ == "__main__":
    main()
