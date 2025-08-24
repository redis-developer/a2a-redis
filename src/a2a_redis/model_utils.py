"""Utilities for handling Pydantic model serialization/deserialization in Redis queues."""

import json
from typing import Any, Union, Optional
import a2a.types


def serialize_event(event: Any) -> dict:
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
    
    return {
        "event_type": type(event).__name__,
        "event_data": event_data
    }


def deserialize_event(event_structure: dict) -> Any:
    """Deserialize an event from a dictionary structure back to a Pydantic model.
    
    Args:
        event_structure: Dictionary containing event_type and event_data
        
    Returns:
        Reconstructed Pydantic model instance or raw data as fallback
    """
    if not isinstance(event_structure, dict) or "event_data" not in event_structure:
        return event_structure
    
    event_data = event_structure["event_data"]
    event_type = event_structure.get("event_type")
    
    # Re-construct the Pydantic model if type info is available
    if event_type and hasattr(a2a.types, event_type):
        ModelClass = getattr(a2a.types, event_type)
        try:
            return ModelClass(**event_data)
        except Exception:
            # Fallback if model reconstruction fails
            return event_data
    
    return event_data


def serialize_to_json(data: dict, **json_kwargs) -> str:
    """Serialize dictionary to JSON string with default str conversion."""
    return json.dumps(data, default=str, **json_kwargs)


def deserialize_from_json(json_str: Union[str, bytes]) -> dict:
    """Deserialize JSON string/bytes to dictionary."""
    if isinstance(json_str, bytes):
        json_str = json_str.decode()
    return json.loads(json_str)