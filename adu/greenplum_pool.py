#!/usr/bin/env python3
"""
Global Greenplum Connection Pool Manager with Circuit Breaker
Prevents connection exhaustion by limiting concurrent connections to Greenplum
"""

import threading
import time
import psycopg2
import vertica_python
from psycopg2 import pool
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
import uuid

from adu.enhanced_logger import logger


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"       # Normal operation
    OPEN = "open"          # Rejecting requests
    HALF_OPEN = "half_open" # Testing if service is back


@dataclass
class ConnectionConfig:
    """Database connection configuration"""
    db_type: str
    host: str
    port: int
    username: str
    password: str
    database: str
    connect_timeout: int = 30
    command_timeout: int = 300


class CircuitBreaker:
    """Circuit breaker to prevent cascade failures when GP rejects connections"""
    
    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0, 
                 success_threshold: int = 3):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.success_threshold = success_threshold
        
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0
        self.lock = threading.Lock()
    
    def can_proceed(self) -> bool:
        """Check if circuit breaker allows the operation"""
        with self.lock:
            if self.state == CircuitBreakerState.CLOSED:
                return True
            elif self.state == CircuitBreakerState.OPEN:
                # Check if timeout has expired
                if time.time() - self.last_failure_time >= self.timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.success_count = 0
                    logger.info("Circuit breaker transitioning to HALF_OPEN state")
                    return True
                return False
            elif self.state == CircuitBreakerState.HALF_OPEN:
                return True
    
    def record_success(self):
        """Record successful operation"""
        with self.lock:
            self.failure_count = 0
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    self.state = CircuitBreakerState.CLOSED
                    logger.circuit_breaker_closed(self.success_count)
    
    def record_failure(self, error: Exception):
        """Record failed operation"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if (self.state == CircuitBreakerState.CLOSED and 
                self.failure_count >= self.failure_threshold):
                self.state = CircuitBreakerState.OPEN
                logger.circuit_breaker_opened(self.failure_count, self.timeout)
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                logger.warning("Circuit breaker returned to OPEN state after failure in HALF_OPEN")
    
    def get_state(self) -> str:
        """Get current circuit breaker state"""
        return self.state.value


class GreenplumConnectionPool:
    """
    Global connection pool for Greenplum/PostgreSQL with circuit breaker protection
    """
    
    def __init__(self, config: ConnectionConfig, min_connections: int = 2, 
                 max_connections: int = 6):
        self.config = config
        self.min_connections = min_connections
        self.max_connections = max_connections
        
        # Circuit breaker for connection failures
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,    # Open after 5 failures
            timeout=30.0,           # Stay open for 30 seconds
            success_threshold=2     # Close after 2 successes
        )
        
        # Connection pool
        self._pool = None
        self._pool_lock = threading.Lock()
        self._connection_count = 0
        self._active_connections = {}  # connection_id -> (connection, start_time)
        
        # Statistics
        self._stats = {
            'total_acquired': 0,
            'total_released': 0,
            'total_errors': 0,
            'peak_connections': 0,
            'average_hold_time': 0,
            'current_active': 0
        }
        
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool"""
        try:
            if self.config.db_type.lower() in ['postgresql', 'greenplum']:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=self.min_connections,
                    maxconn=self.max_connections,
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.username,
                    password=self.config.password,
                    connect_timeout=self.config.connect_timeout
                )
                logger.info(f"Initialized PostgreSQL/Greenplum connection pool: "
                           f"{self.min_connections}-{self.max_connections} connections")
            else:
                # For Vertica, we'll manage connections manually
                logger.info(f"Initialized manual connection management for {self.config.db_type}")
            
            # Update logger connection stats
            logger._connection_stats['max_connections'] = self.max_connections
            
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            self.circuit_breaker.record_failure(e)
            raise
    
    def _create_vertica_connection(self):
        """Create a Vertica connection"""
        return vertica_python.connect(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.username,
            password=self.config.password,
            connection_timeout=self.config.connect_timeout
        )
    
    @contextmanager
    def get_connection(self, timeout: float = 30.0):
        """
        Get a connection from the pool with circuit breaker protection
        
        Args:
            timeout: Maximum time to wait for a connection
            
        Yields:
            Database connection object
        """
        # Check circuit breaker
        if not self.circuit_breaker.can_proceed():
            raise Exception(f"Circuit breaker is OPEN - connections rejected for "
                          f"{self.circuit_breaker.timeout} seconds")
        
        connection = None
        connection_id = str(uuid.uuid4())[:8]
        start_time = time.time()
        
        try:
            # Acquire connection
            with self._pool_lock:
                if self.config.db_type.lower() in ['postgresql', 'greenplum']:
                    if self._pool is None:
                        raise Exception("Connection pool not initialized")
                    connection = self._pool.getconn()
                else:
                    # Manual connection for Vertica
                    connection = self._create_vertica_connection()
                
                # Track connection
                self._active_connections[connection_id] = (connection, start_time)
                self._connection_count += 1
                self._stats['total_acquired'] += 1
                self._stats['current_active'] = len(self._active_connections)
                self._stats['peak_connections'] = max(self._stats['peak_connections'], 
                                                    self._stats['current_active'])
            
            logger.connection_acquired(connection_id, self._stats['current_active'])
            
            # Test connection health
            self._test_connection(connection)
            
            yield connection
            
            # Record successful operation
            self.circuit_breaker.record_success()
            
        except Exception as e:
            logger.connection_error(str(e))
            self._stats['total_errors'] += 1
            self.circuit_breaker.record_failure(e)
            
            # Close broken connection
            if connection:
                try:
                    connection.close()
                except:
                    pass
                connection = None
            
            raise
            
        finally:
            # Return connection to pool
            if connection:
                duration = time.time() - start_time
                
                try:
                    with self._pool_lock:
                        if self.config.db_type.lower() in ['postgresql', 'greenplum']:
                            self._pool.putconn(connection)
                        else:
                            connection.close()
                        
                        # Update tracking
                        if connection_id in self._active_connections:
                            del self._active_connections[connection_id]
                        self._connection_count -= 1
                        self._stats['total_released'] += 1
                        self._stats['current_active'] = len(self._active_connections)
                        
                        # Update average hold time
                        if self._stats['total_released'] > 0:
                            current_avg = self._stats['average_hold_time']
                            self._stats['average_hold_time'] = (
                                (current_avg * (self._stats['total_released'] - 1) + duration) / 
                                self._stats['total_released']
                            )
                    
                    logger.connection_released(connection_id, duration)
                    
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")
    
    def _test_connection(self, connection):
        """Test if connection is healthy"""
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            
            if not result or result[0] != 1:
                raise Exception("Connection health check failed")
                
        except Exception as e:
            raise Exception(f"Connection health check failed: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics"""
        with self._pool_lock:
            stats = dict(self._stats)
            stats.update({
                'circuit_breaker_state': self.circuit_breaker.get_state(),
                'circuit_failure_count': self.circuit_breaker.failure_count,
                'max_connections': self.max_connections,
                'active_connection_ids': list(self._active_connections.keys())
            })
            return stats
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on the connection pool"""
        health = {
            'status': 'healthy',
            'circuit_breaker_state': self.circuit_breaker.get_state(),
            'active_connections': len(self._active_connections),
            'max_connections': self.max_connections,
            'pool_available': True,
            'errors': []
        }
        
        try:
            # Test getting a connection
            with self.get_connection(timeout=5.0) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT version()")
                version = cursor.fetchone()
                cursor.close()
                health['database_version'] = str(version[0]) if version else 'Unknown'
                
        except Exception as e:
            health['status'] = 'unhealthy'
            health['errors'].append(str(e))
            health['pool_available'] = False
        
        return health
    
    def close_all_connections(self):
        """Close all connections in the pool"""
        logger.info("Closing all connections in pool")
        
        with self._pool_lock:
            # Close active connections
            for conn_id, (conn, start_time) in self._active_connections.items():
                try:
                    conn.close()
                    logger.debug(f"Forcefully closed connection {conn_id}")
                except Exception as e:
                    logger.error(f"Error closing connection {conn_id}: {e}")
            
            self._active_connections.clear()
            self._connection_count = 0
            
            # Close pool
            if self._pool:
                try:
                    self._pool.closeall()
                except Exception as e:
                    logger.error(f"Error closing connection pool: {e}")
        
        logger.info("All connections closed")


# Global connection pool instance
_connection_pool = None
_pool_lock = threading.Lock()


def get_connection_pool(config: ConnectionConfig = None) -> GreenplumConnectionPool:
    """Get the global connection pool instance"""
    global _connection_pool
    
    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                if config is None:
                    raise ValueError("Connection config required for first initialization")
                _connection_pool = GreenplumConnectionPool(config)
    
    return _connection_pool


def initialize_connection_pool(db_type: str, host: str, port: int, username: str, 
                             password: str, database: str, max_connections: int = 6):
    """Initialize the global connection pool with configuration"""
    config = ConnectionConfig(
        db_type=db_type,
        host=host, 
        port=port,
        username=username,
        password=password,
        database=database
    )
    
    global _connection_pool
    with _pool_lock:
        _connection_pool = GreenplumConnectionPool(config, max_connections=max_connections)
    
    logger.info(f"Initialized global connection pool for {db_type} at {host}:{port}")
    return _connection_pool


@contextmanager  
def get_database_connection(timeout: float = 30.0):
    """
    Convenience function to get a database connection from the global pool
    
    Usage:
        with get_database_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
            results = cursor.fetchall()
    """
    pool = get_connection_pool()
    with pool.get_connection(timeout=timeout) as conn:
        yield conn


def get_pool_stats() -> Dict[str, Any]:
    """Get connection pool statistics"""
    try:
        pool = get_connection_pool()
        return pool.get_stats()
    except:
        return {'error': 'Connection pool not initialized'}


def pool_health_check() -> Dict[str, Any]:
    """Perform health check on connection pool"""
    try:
        pool = get_connection_pool()
        return pool.health_check()
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e),
            'pool_initialized': False
        }


def close_connection_pool():
    """Close the global connection pool"""
    global _connection_pool
    
    if _connection_pool:
        _connection_pool.close_all_connections()
        with _pool_lock:
            _connection_pool = None
        logger.info("Global connection pool closed")