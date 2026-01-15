"""Utilities for handling Pydantic model serialization/deserialization in Redis queues."""

import json
from typing import Any, Union, cast

import a2a.types


def serialize_event(event: Any) -> dict[str, Any]:
    """Serialize an event to a dictionary structure for Redis storage.

    Args:
        event: Event object to serialize

    Returns:
        Dictionary with event_type and event_data
    """
    if hasattr(event, "model_dump"):
        event_data = event.model_dump()
    else:
        event_data = event

    return {"event_type": type(event).__name__, "event_data": event_data}


def deserialize_event(event_structure: Any) -> Any:
    """Deserialize an event from a dictionary structure back to a Pydantic model.

    Args:
        event_structure: Dictionary containing event_type and event_data

    Returns:
        Reconstructed Pydantic model instance or raw data as fallback
    """
    if not isinstance(event_structure, dict) or "event_data" not in event_structure:
        return cast(Any, event_structure)

    # Cast to typed dict for type safety
    typed_structure: dict[str, Any] = cast(dict[str, Any], event_structure)
    event_data: Any = typed_structure["event_data"]
    event_type: str | None = typed_structure.get("event_type")

    # Re-construct the Pydantic model if type info is available
    if event_type and hasattr(a2a.types, event_type):
        ModelClass: type[Any] = getattr(a2a.types, event_type)
        try:
            return ModelClass(**event_data)
        except Exception:
            # Fallback if model reconstruction fails
            return event_data

    return event_data


def serialize_to_json(data: dict[str, Any], **json_kwargs: Any) -> str:
    """Serialize dictionary to JSON string with default str conversion."""
    return json.dumps(data, default=str, **json_kwargs)


def deserialize_from_json(json_str: Union[str, bytes]) -> dict[str, Any]:
    """Deserialize JSON string/bytes to dictionary."""
    if isinstance(json_str, bytes):
        json_str = json_str.decode()
    result: dict[str, Any] = json.loads(json_str)
    return result
