"""Utility functions and classes for a2a-redis components."""

import logging
import time
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, Union, Tuple, Dict, Type

import redis
import redis.asyncio as redis_async
from redis.connection import ConnectionPool
from redis.exceptions import ConnectionError, RedisError, TimeoutError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RedisConnectionManager:
    """Manages Redis connections with automatic reconnection and health checking."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        username: Optional[str] = None,
        ssl: bool = False,
        max_connections: int = 50,
        retry_on_timeout: bool = True,
        health_check_interval: int = 30,
        socket_connect_timeout: float = 5.0,
        socket_timeout: float = 5.0,
        **kwargs: Any,
    ):
        """Initialize the Redis connection manager.

        Args:
            host: Redis server host
            port: Redis server port
            db: Redis database number
            password: Redis password
            username: Redis username
            ssl: Use SSL connection
            max_connections: Maximum number of connections in pool
            retry_on_timeout: Retry operations on timeout
            health_check_interval: Health check interval in seconds
            socket_connect_timeout: Connection timeout in seconds
            socket_timeout: Socket timeout in seconds
            **kwargs: Additional Redis client arguments
        """
        self.connection_params: dict[str, Any] = {
            "host": host,
            "port": port,
            "db": db,
            "password": password,
            "username": username,
            "ssl": ssl,
            "socket_connect_timeout": socket_connect_timeout,
            "socket_timeout": socket_timeout,
            "retry_on_timeout": retry_on_timeout,
            "health_check_interval": health_check_interval,
            **kwargs,
        }

        pool_params = {
            k: v
            for k, v in self.connection_params.items()
            if k != "connection_class" and k != "cache_factory"
        }
        self.pool = ConnectionPool(max_connections=max_connections, **pool_params)
        self._client: Optional[redis.Redis] = None

    @property
    def client(self) -> redis.Redis:
        """Get or create Redis client."""
        if self._client is None:
            self._client = redis.Redis(connection_pool=self.pool)
        return self._client

    def health_check(self) -> bool:
        """Check if Redis connection is healthy."""
        try:
            self.client.ping()  # type: ignore[misc]
            return True
        except RedisError as e:
            logger.warning(f"Redis health check failed: {e}")
            return False

    def reconnect(self) -> bool:
        """Force reconnection to Redis."""
        try:
            if self._client:
                try:
                    self._client.close()
                except Exception as e:
                    # Log the close error but don't fail the reconnect
                    logger.error(f"Failed to reconnect to Redis: {e}")
            self._client = None
            return self.health_check()
        except Exception as e:
            logger.error(f"Failed to reconnect to Redis: {e}")
            return False


def redis_retry(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (ConnectionError, TimeoutError),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for retrying Redis operations with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff_factor: Multiplier for delay after each retry
        exceptions: Tuple of exceptions to catch and retry on
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Optional[Exception] = None
            current_delay = delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not isinstance(e, exceptions):
                        logger.error(f"Non-retryable error in Redis operation: {e}")
                        raise
                    last_exception = e
                    if attempt == max_retries:
                        break

                    logger.warning(
                        f"Redis operation failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                        f"Retrying in {current_delay:.1f}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff_factor

            logger.error(f"Redis operation failed after {max_retries + 1} attempts")
            if last_exception:
                raise last_exception
            raise RuntimeError("Redis operation failed")

        return wrapper

    return decorator


def safe_redis_operation(
    operation: Callable[..., T],
    default_value: Optional[T] = None,
    log_errors: bool = True,
) -> Callable[..., Union[T, Any]]:
    """Wrapper for safe Redis operations that won't crash the application.

    Args:
        operation: Redis operation function
        default_value: Value to return on error
        log_errors: Whether to log errors
    """

    @wraps(operation)
    def wrapper(*args: Any, **kwargs: Any) -> Union[T, Any]:
        try:
            return operation(*args, **kwargs)
        except RedisError as e:
            if log_errors:
                logger.error(f"Redis operation failed: {e}")
            return default_value
        except Exception as e:
            if log_errors:
                logger.error(f"Unexpected error in Redis operation: {e}")
            return default_value

    return wrapper


class RedisHealthMonitor:
    """Monitors Redis health and provides alerts."""

    def __init__(self, connection_manager: RedisConnectionManager):
        """Initialize health monitor.

        Args:
            connection_manager: Redis connection manager to monitor
        """
        self.connection_manager = connection_manager
        self.last_check = 0
        self.is_healthy = True
        self.consecutive_failures = 0
        self.max_failures_before_alert = 3

    def check_health(self, force: bool = False) -> bool:
        """Check Redis health.

        Args:
            force: Force health check even if recently checked

        Returns:
            True if healthy, False otherwise
        """
        now = time.time()

        # Skip check if recently performed (unless forced)
        if not force and (now - self.last_check) < 30:
            return self.is_healthy

        self.last_check = now
        was_healthy = self.is_healthy
        self.is_healthy = self.connection_manager.health_check()

        if not self.is_healthy:
            self.consecutive_failures += 1
            if was_healthy:
                logger.warning("Redis connection became unhealthy")

            # Try to reconnect after multiple failures
            if self.consecutive_failures >= self.max_failures_before_alert:
                logger.error(
                    f"Redis unhealthy for {self.consecutive_failures} consecutive checks. "
                    "Attempting reconnection..."
                )
                if self.connection_manager.reconnect():
                    logger.info("Successfully reconnected to Redis")
                    self.is_healthy = True
                    self.consecutive_failures = 0
        else:
            if not was_healthy and self.consecutive_failures > 0:
                logger.info("Redis connection restored")
            self.consecutive_failures = 0

        return self.is_healthy

    def get_status(self) -> Dict[str, Any]:
        """Get current health status.

        Returns:
            Dictionary with health status information
        """
        return {
            "healthy": self.is_healthy,
            "last_check": self.last_check,
            "consecutive_failures": self.consecutive_failures,
        }


def create_redis_client(url: Optional[str] = None, **kwargs: Any) -> redis_async.Redis:
    """Create a Redis client with sensible defaults.

    Args:
        url: Redis URL (redis://host:port/db)
        **kwargs: Additional Redis client arguments

    Returns:
        Configured Redis client
    """
    if url:
        return redis_async.from_url(  # type: ignore[misc]
            url,
            retry_on_timeout=True,
            health_check_interval=30,
            socket_connect_timeout=5.0,
            socket_timeout=5.0,
            **kwargs,
        )

    return redis_async.Redis(
        host=kwargs.get("host", "localhost"),
        port=kwargs.get("port", 6379),
        db=kwargs.get("db", 0),
        password=kwargs.get("password"),
        username=kwargs.get("username"),
        ssl=kwargs.get("ssl", False),
        retry_on_timeout=True,
        health_check_interval=30,
        socket_connect_timeout=5.0,
        socket_timeout=5.0,
        **{
            k: v
            for k, v in kwargs.items()
            if k not in ["host", "port", "db", "password", "username", "ssl"]
        },
    )


def create_sync_redis_client(url: Optional[str] = None, **kwargs: Any) -> redis.Redis:
    """Create a synchronous Redis client with sensible defaults.

    Args:
        url: Redis URL (redis://host:port/db)
        **kwargs: Additional Redis client arguments

    Returns:
        Configured sync Redis client
    """
    if url:
        return redis.from_url(  # type: ignore[misc]
            url,
            retry_on_timeout=True,
            health_check_interval=30,
            socket_connect_timeout=5.0,
            socket_timeout=5.0,
            **kwargs,
        )

    return redis.Redis(
        host=kwargs.get("host", "localhost"),
        port=kwargs.get("port", 6379),
        db=kwargs.get("db", 0),
        password=kwargs.get("password"),
        username=kwargs.get("username"),
        ssl=kwargs.get("ssl", False),
        retry_on_timeout=True,
        health_check_interval=30,
        socket_connect_timeout=5.0,
        socket_timeout=5.0,
        **{
            k: v
            for k, v in kwargs.items()
            if k not in ["host", "port", "db", "password", "username", "ssl"]
        },
    )
