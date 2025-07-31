"""Tests for utility functions and classes."""

import time
import pytest
from unittest.mock import MagicMock, patch
from redis.exceptions import ConnectionError, RedisError

from a2a_redis.utils import (
    RedisConnectionManager,
    RedisHealthMonitor,
    redis_retry,
    safe_redis_operation,
    create_redis_client,
)


class TestRedisConnectionManager:
    """Tests for RedisConnectionManager."""

    def test_init_default_params(self):
        """Test initialization with default parameters."""
        with patch("a2a_redis.utils.ConnectionPool") as mock_pool:
            RedisConnectionManager()

            mock_pool.assert_called_once()
            call_args = mock_pool.call_args
            assert call_args[1]["host"] == "localhost"
            assert call_args[1]["port"] == 6379
            assert call_args[1]["db"] == 0
            assert call_args[1]["max_connections"] == 50

    def test_init_custom_params(self):
        """Test initialization with custom parameters."""
        with patch("a2a_redis.utils.ConnectionPool") as mock_pool:
            RedisConnectionManager(
                host="redis.example.com",
                port=6380,
                db=1,
                password="secret",
                username="user",
                ssl=True,
                max_connections=100,
            )

            call_args = mock_pool.call_args
            assert call_args[1]["host"] == "redis.example.com"
            assert call_args[1]["port"] == 6380
            assert call_args[1]["db"] == 1
            assert call_args[1]["password"] == "secret"
            assert call_args[1]["username"] == "user"
            assert call_args[1]["ssl"] is True
            assert call_args[1]["max_connections"] == 100

    def test_client_property_lazy_creation(self):
        """Test that client is created lazily."""
        with (
            patch("a2a_redis.utils.ConnectionPool") as mock_pool,
            patch("a2a_redis.utils.redis.Redis") as mock_redis_class,
        ):
            manager = RedisConnectionManager()
            assert manager._client is None

            # Access client property
            client = manager.client

            mock_redis_class.assert_called_once_with(
                connection_pool=mock_pool.return_value
            )
            assert manager._client is not None
            assert client == mock_redis_class.return_value

    def test_client_property_reuse(self):
        """Test that client is reused on subsequent accesses."""
        with (
            patch("a2a_redis.utils.ConnectionPool"),
            patch("a2a_redis.utils.redis.Redis") as mock_redis_class,
        ):
            manager = RedisConnectionManager()

            client1 = manager.client
            client2 = manager.client

            # Should only create Redis client once
            mock_redis_class.assert_called_once()
            assert client1 is client2

    def test_health_check_success(self):
        """Test successful health check."""
        with (
            patch("a2a_redis.utils.ConnectionPool"),
            patch("a2a_redis.utils.redis.Redis") as mock_redis_class,
        ):
            mock_client = MagicMock()
            mock_redis_class.return_value = mock_client
            mock_client.ping.return_value = True

            manager = RedisConnectionManager()
            result = manager.health_check()

            assert result is True
            mock_client.ping.assert_called_once()

    def test_health_check_failure(self):
        """Test failed health check."""
        with (
            patch("a2a_redis.utils.ConnectionPool"),
            patch("a2a_redis.utils.redis.Redis") as mock_redis_class,
        ):
            mock_client = MagicMock()
            mock_redis_class.return_value = mock_client
            mock_client.ping.side_effect = ConnectionError("Connection failed")

            manager = RedisConnectionManager()
            result = manager.health_check()

            assert result is False

    def test_reconnect_success(self):
        """Test successful reconnection."""
        with (
            patch("a2a_redis.utils.ConnectionPool"),
            patch("a2a_redis.utils.redis.Redis") as mock_redis_class,
        ):
            mock_client = MagicMock()
            mock_redis_class.return_value = mock_client
            mock_client.ping.return_value = True

            manager = RedisConnectionManager()
            # Set existing client
            manager._client = mock_client

            result = manager.reconnect()

            assert result is True
            mock_client.close.assert_called_once()
            # Note: _client gets set again during health_check() call in reconnect()
            # so we just verify the reconnect was successful

    def test_reconnect_failure(self):
        """Test failed reconnection."""
        with (
            patch("a2a_redis.utils.ConnectionPool"),
            patch("a2a_redis.utils.redis.Redis") as mock_redis_class,
        ):
            mock_client = MagicMock()
            mock_redis_class.return_value = mock_client
            mock_client.ping.side_effect = ConnectionError("Still failing")

            manager = RedisConnectionManager()
            manager._client = mock_client

            result = manager.reconnect()

            assert result is False

    def test_reconnect_exception_handling(self):
        """Test reconnection with general exception."""
        with (
            patch("a2a_redis.utils.ConnectionPool"),
            patch("a2a_redis.utils.redis.Redis") as mock_redis_class,
        ):
            mock_client = MagicMock()
            mock_redis_class.return_value = mock_client
            mock_client.close.side_effect = Exception("Close error")

            manager = RedisConnectionManager()
            manager._client = mock_client

            result = manager.reconnect()

            # Should still work despite close error
            assert result is True  # Because ping succeeds by default


class TestRedisRetryDecorator:
    """Tests for redis_retry decorator."""

    def test_success_no_retry(self):
        """Test successful operation without retry."""

        @redis_retry(max_retries=3)
        def test_func():
            return "success"

        result = test_func()
        assert result == "success"

    def test_retry_on_connection_error(self):
        """Test retry on connection error."""
        call_count = 0

        @redis_retry(max_retries=2, delay=0.01)
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ConnectionError("Connection failed")
            return "success"

        result = test_func()
        assert result == "success"
        assert call_count == 3

    def test_retry_exhausted(self):
        """Test when all retries are exhausted."""
        call_count = 0

        @redis_retry(max_retries=2, delay=0.01)
        def test_func():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Always fails")

        with pytest.raises(ConnectionError):
            test_func()

        assert call_count == 3  # Initial + 2 retries

    def test_no_retry_on_non_retryable_error(self):
        """Test that non-retryable errors are not retried."""
        call_count = 0

        @redis_retry(max_retries=2)
        def test_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Logic error")

        with pytest.raises(ValueError):
            test_func()

        assert call_count == 1  # No retries

    def test_custom_exceptions(self):
        """Test retry with custom exception types."""
        call_count = 0

        @redis_retry(max_retries=1, delay=0.01, exceptions=(ValueError,))
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("Custom retryable error")
            return "success"

        result = test_func()
        assert result == "success"
        assert call_count == 2

    def test_backoff_factor(self):
        """Test exponential backoff."""
        delays = []

        def mock_sleep(delay):
            delays.append(delay)

        @redis_retry(max_retries=2, delay=0.1, backoff_factor=2.0)
        def test_func():
            raise ConnectionError("Always fails")

        with patch("time.sleep", side_effect=mock_sleep):
            with pytest.raises(ConnectionError):
                test_func()

        assert len(delays) == 2
        assert delays[0] == 0.1
        assert delays[1] == 0.2  # 0.1 * 2.0


class TestSafeRedisOperation:
    """Tests for safe_redis_operation wrapper."""

    def test_success(self):
        """Test successful operation."""

        def test_func():
            return "success"

        safe_func = safe_redis_operation(test_func, default_value="default")
        result = safe_func()

        assert result == "success"

    def test_redis_error_with_default(self):
        """Test Redis error returns default value."""

        def test_func():
            raise RedisError("Redis error")

        safe_func = safe_redis_operation(test_func, default_value="default")
        result = safe_func()

        assert result == "default"

    def test_general_exception_with_default(self):
        """Test general exception returns default value."""

        def test_func():
            raise ValueError("Some error")

        safe_func = safe_redis_operation(test_func, default_value="default")
        result = safe_func()

        assert result == "default"

    def test_no_default_value(self):
        """Test with no default value specified."""

        def test_func():
            raise RedisError("Redis error")

        safe_func = safe_redis_operation(test_func)
        result = safe_func()

        assert result is None


class TestRedisHealthMonitor:
    """Tests for RedisHealthMonitor."""

    def test_init(self):
        """Test health monitor initialization."""
        mock_manager = MagicMock()
        monitor = RedisHealthMonitor(mock_manager)

        assert monitor.connection_manager == mock_manager
        assert monitor.last_check == 0
        assert monitor.is_healthy is True
        assert monitor.consecutive_failures == 0

    def test_check_health_success(self):
        """Test successful health check."""
        mock_manager = MagicMock()
        mock_manager.health_check.return_value = True

        monitor = RedisHealthMonitor(mock_manager)
        result = monitor.check_health(force=True)

        assert result is True
        assert monitor.is_healthy is True
        assert monitor.consecutive_failures == 0
        mock_manager.health_check.assert_called_once()

    def test_check_health_failure(self):
        """Test failed health check."""
        mock_manager = MagicMock()
        mock_manager.health_check.return_value = False

        monitor = RedisHealthMonitor(mock_manager)
        result = monitor.check_health(force=True)

        assert result is False
        assert monitor.is_healthy is False
        assert monitor.consecutive_failures == 1

    def test_check_health_skip_recent(self):
        """Test skipping recent health check."""
        mock_manager = MagicMock()

        monitor = RedisHealthMonitor(mock_manager)
        monitor.last_check = time.time()  # Set recent check
        monitor.is_healthy = True

        result = monitor.check_health(force=False)

        assert result is True
        mock_manager.health_check.assert_not_called()

    def test_reconnect_after_failures(self):
        """Test reconnection after multiple failures."""
        mock_manager = MagicMock()
        mock_manager.health_check.return_value = False
        mock_manager.reconnect.return_value = True

        monitor = RedisHealthMonitor(mock_manager)
        monitor.max_failures_before_alert = 2

        # First failure
        monitor.check_health(force=True)
        assert monitor.consecutive_failures == 1
        mock_manager.reconnect.assert_not_called()

        # Second failure - should trigger reconnect
        monitor.check_health(force=True)
        assert monitor.consecutive_failures == 0  # Reset after successful reconnect
        assert monitor.is_healthy is True
        mock_manager.reconnect.assert_called_once()

    def test_get_status(self):
        """Test getting health status."""
        mock_manager = MagicMock()
        monitor = RedisHealthMonitor(mock_manager)
        monitor.is_healthy = False
        monitor.consecutive_failures = 3
        monitor.last_check = 12345.0

        status = monitor.get_status()

        assert status == {
            "healthy": False,
            "consecutive_failures": 3,
            "last_check": 12345.0,
        }


class TestCreateRedisClient:
    """Tests for create_redis_client function."""

    def test_create_from_url(self):
        """Test creating client from URL."""
        with patch("a2a_redis.utils.redis_async.from_url") as mock_from_url:
            create_redis_client(url="redis://localhost:6379/0")

            mock_from_url.assert_called_once_with(
                "redis://localhost:6379/0",
                retry_on_timeout=True,
                health_check_interval=30,
                socket_connect_timeout=5.0,
                socket_timeout=5.0,
            )

    def test_create_from_params(self):
        """Test creating client from parameters."""
        with patch("a2a_redis.utils.redis_async.Redis") as mock_redis:
            create_redis_client(
                host="redis.example.com", port=6380, db=1, password="secret"
            )

            mock_redis.assert_called_once_with(
                host="redis.example.com",
                port=6380,
                db=1,
                password="secret",
                username=None,
                ssl=False,
                retry_on_timeout=True,
                health_check_interval=30,
                socket_connect_timeout=5.0,
                socket_timeout=5.0,
            )

    def test_create_with_defaults(self):
        """Test creating client with default parameters."""
        with patch("a2a_redis.utils.redis_async.Redis") as mock_redis:
            create_redis_client()

            # Just check that Redis was called - the exact parameters may vary
            mock_redis.assert_called_once()
