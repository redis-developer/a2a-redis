"""Protocol definition for EventQueue implementations.

This protocol defines the interface that all event queue implementations must satisfy,
decoupling our implementations from the A2A SDK's concrete EventQueue class.
"""

from typing import Protocol, Union
from a2a.types import Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent


class EventQueueProtocol(Protocol):
    """Protocol defining the EventQueue interface.

    This protocol describes the methods that must be implemented by any event queue
    to be compatible with the A2A SDK's queue manager expectations.
    """

    async def enqueue_event(
        self,
        event: Union[Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent],
    ) -> None:
        """Add an event to the queue.

        Args:
            event: Event to add to the queue

        Raises:
            RuntimeError: If queue is closed or operation fails
        """
        ...

    async def dequeue_event(
        self, no_wait: bool = False
    ) -> Union[Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent]:
        """Remove and return an event from the queue.

        Args:
            no_wait: If True, return immediately if no events available

        Returns:
            Event data

        Raises:
            RuntimeError: If queue is closed or no events available
        """
        ...

    async def close(self) -> None:
        """Close the queue and clean up resources."""
        ...

    def is_closed(self) -> bool:
        """Check if the queue is closed.

        Returns:
            True if the queue is closed, False otherwise
        """
        ...

    def tap(self) -> "EventQueueProtocol":
        """Create a tap (copy) of this queue.

        Returns:
            A new queue instance that can receive the same events
        """
        ...

    def task_done(self) -> None:
        """Mark a task as done.

        For compatibility with queue-based systems that track task completion.
        May be a no-op for some implementations.
        """
        ...
