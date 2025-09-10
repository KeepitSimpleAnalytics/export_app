#!/usr/bin/env python3
"""
Enhanced structured logging system for ADU Export Application
Provides consistent, trackable progress and performance metrics
"""

import logging
import time
import threading
import psutil
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class JobContext:
    """Job-level context for structured logging"""
    job_id: str
    start_time: float
    total_tables: int = 0
    completed_tables: int = 0
    failed_tables: int = 0
    total_rows: int = 0
    processed_rows: int = 0


@dataclass
class TableContext:
    """Table-level context for structured logging"""
    table_name: str
    start_time: float
    total_rows: int = 0
    processed_rows: int = 0
    total_chunks: int = 0
    completed_chunks: int = 0
    method: str = "Unknown"


class EnhancedLogger:
    """
    Structured logger with progress tracking, performance metrics, and connection monitoring
    """
    
    def __init__(self, name: str = "ADU"):
        self.logger = logging.getLogger(name)
        self._setup_logger()
        
        # Thread-local storage for context
        self._local = threading.local()
        
        # Global state tracking
        self._job_contexts: Dict[str, JobContext] = {}
        self._table_contexts: Dict[str, TableContext] = {}
        self._connection_stats = {
            'active_connections': 0,
            'max_connections': 6,
            'total_acquired': 0,
            'total_released': 0,
            'connection_errors': 0,
            'circuit_breaker_state': 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        }
        self._lock = threading.Lock()
    
    def _setup_logger(self):
        """Configure structured logging format"""
        if not self.logger.handlers:
            # Console handler (stdout)
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '[%(asctime)s] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            
            # File handler for web interface logs
            try:
                import os
                log_file_path = '/tmp/worker.log'
                os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
                
                file_handler = logging.FileHandler(log_file_path, mode='a')
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
            except Exception as e:
                # If file logging fails, continue with console logging only
                console_handler.emit(logging.LogRecord(
                    name=self.logger.name, level=logging.WARNING, pathname='', lineno=0,
                    msg=f"Failed to setup file logging to {log_file_path}: {e}",
                    args=(), exc_info=None
                ))
            
            self.logger.setLevel(logging.INFO)
    
    def set_job_context(self, job_id: str, total_tables: int = 0, total_rows: int = 0):
        """Set job-level context for current thread"""
        with self._lock:
            context = JobContext(
                job_id=job_id,
                start_time=time.time(),
                total_tables=total_tables,
                total_rows=total_rows
            )
            self._job_contexts[job_id] = context
            self._local.job_context = context
    
    def set_table_context(self, table_name: str, total_rows: int = 0, 
                         total_chunks: int = 0, method: str = "Unknown"):
        """Set table-level context for current thread"""
        with self._lock:
            context = TableContext(
                table_name=table_name,
                start_time=time.time(),
                total_rows=total_rows,
                total_chunks=total_chunks,
                method=method
            )
            self._table_contexts[table_name] = context
            self._local.table_context = context
    
    def get_job_context(self) -> Optional[JobContext]:
        """Get current job context"""
        return getattr(self._local, 'job_context', None)
    
    def get_table_context(self) -> Optional[TableContext]:
        """Get current table context"""
        return getattr(self._local, 'table_context', None)
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable form"""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            return f"{seconds/3600:.1f}h"
    
    def _format_rows(self, count: int) -> str:
        """Format row count in human-readable form"""
        if count < 1000:
            return str(count)
        elif count < 1000000:
            return f"{count/1000:.1f}K"
        elif count < 1000000000:
            return f"{count/1000000:.1f}M"
        else:
            return f"{count/1000000000:.1f}B"
    
    def _get_memory_usage(self) -> str:
        """Get current memory usage"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            return f"{memory_mb:.1f}MB"
        except:
            return "Unknown"
    
    def _build_context_prefix(self) -> str:
        """Build context prefix for log messages"""
        parts = []
        
        # Add job context
        job_ctx = self.get_job_context()
        if job_ctx:
            elapsed = time.time() - job_ctx.start_time
            if job_ctx.total_tables > 0:
                progress = (job_ctx.completed_tables / job_ctx.total_tables) * 100
                parts.append(f"JOB:{job_ctx.job_id}")
                parts.append(f"TABLES:{job_ctx.completed_tables}/{job_ctx.total_tables}")
                parts.append(f"JOB-PROGRESS:{progress:.1f}%")
            else:
                parts.append(f"JOB:{job_ctx.job_id}")
        
        # Add table context
        table_ctx = self.get_table_context()
        if table_ctx:
            parts.append(f"TABLE:{table_ctx.table_name}")
            if table_ctx.total_rows > 0:
                progress = (table_ctx.processed_rows / table_ctx.total_rows) * 100
                parts.append(f"PROGRESS:{progress:.1f}%")
                parts.append(f"ROWS:{self._format_rows(table_ctx.processed_rows)}/{self._format_rows(table_ctx.total_rows)}")
                
                # Calculate ETA
                if table_ctx.processed_rows > 0:
                    elapsed = time.time() - table_ctx.start_time
                    rate = table_ctx.processed_rows / elapsed
                    remaining_rows = table_ctx.total_rows - table_ctx.processed_rows
                    if rate > 0:
                        eta_seconds = remaining_rows / rate
                        parts.append(f"ETA:{self._format_duration(eta_seconds)}")
            
            if table_ctx.total_chunks > 0:
                parts.append(f"CHUNKS:{table_ctx.completed_chunks}/{table_ctx.total_chunks}")
            
            parts.append(f"METHOD:{table_ctx.method}")
        
        # Add connection info
        with self._lock:
            conn_stats = self._connection_stats
            parts.append(f"CONN:{conn_stats['active_connections']}/{conn_stats['max_connections']}")
            if conn_stats['circuit_breaker_state'] != 'CLOSED':
                parts.append(f"CIRCUIT:{conn_stats['circuit_breaker_state']}")
        
        # Add memory usage
        parts.append(f"MEM:{self._get_memory_usage()}")
        
        return "[" + "] [".join(parts) + "]" if parts else ""
    
    def info(self, message: str, **kwargs):
        """Log info message with context"""
        prefix = self._build_context_prefix()
        full_message = f"{prefix} {message}" if prefix else message
        self.logger.info(full_message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with context"""
        prefix = self._build_context_prefix()
        full_message = f"{prefix} {message}" if prefix else message
        self.logger.warning(full_message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message with context"""
        prefix = self._build_context_prefix()
        full_message = f"{prefix} {message}" if prefix else message
        self.logger.error(full_message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with context"""
        prefix = self._build_context_prefix()
        full_message = f"{prefix} {message}" if prefix else message
        self.logger.debug(full_message, **kwargs)
    
    # Job-level logging methods
    def job_started(self, job_id: str, total_tables: int, total_rows: int = 0):
        """Log job start"""
        self.set_job_context(job_id, total_tables, total_rows)
        if total_rows > 0:
            self.info(f"Started export job with {total_tables} tables, {self._format_rows(total_rows)} total rows")
        else:
            self.info(f"Started export job with {total_tables} tables")
    
    def job_completed(self, job_id: str, duration: float, success_count: int, error_count: int):
        """Log job completion"""
        job_ctx = self._job_contexts.get(job_id)
        if job_ctx:
            job_ctx.completed_tables = success_count
            job_ctx.failed_tables = error_count
        
        self.info(f"Export job completed in {self._format_duration(duration)} - "
                 f"{success_count} succeeded, {error_count} failed")
    
    def job_progress_update(self, job_id: str, completed_tables: int, failed_tables: int = 0):
        """Update job progress context"""
        job_ctx = self._job_contexts.get(job_id)
        if job_ctx:
            job_ctx.completed_tables = completed_tables
            job_ctx.failed_tables = failed_tables
    
    def job_failed(self, job_id: str, error: str):
        """Log job failure"""
        self.error(f"Export job failed: {error}")
    
    # Table-level logging methods
    def table_started(self, table_name: str, total_rows: int, method: str, 
                     total_chunks: int = 0, chunk_size: int = 0):
        """Log table export start"""
        self.set_table_context(table_name, total_rows, total_chunks, method)
        
        size_info = f"{self._format_rows(total_rows)} rows"
        if total_chunks > 0:
            size_info += f" in {total_chunks} chunks"
        if chunk_size > 0:
            size_info += f" ({self._format_rows(chunk_size)} rows/chunk)"
        
        self.info(f"Starting table export: {size_info}")
    
    def table_progress(self, table_name: str, processed_rows: int, 
                      completed_chunks: int = 0, throughput: int = 0):
        """Log table progress"""
        table_ctx = self._table_contexts.get(table_name)
        if table_ctx:
            table_ctx.processed_rows = processed_rows
            table_ctx.completed_chunks = completed_chunks
        
        parts = []
        if throughput > 0:
            parts.append(f"SPEED:{self._format_rows(throughput)}/sec")
        
        extra_info = " ".join(parts)
        if completed_chunks > 0:
            self.info(f"Progress update - chunk {completed_chunks} completed {extra_info}")
        else:
            self.info(f"Progress update {extra_info}")
    
    def table_completed(self, table_name: str, total_rows: int, duration: float, 
                       file_size_mb: float = 0):
        """Log table completion"""
        throughput = int(total_rows / duration) if duration > 0 else 0
        
        parts = [
            f"exported {self._format_rows(total_rows)} rows",
            f"in {self._format_duration(duration)}",
            f"at {self._format_rows(throughput)}/sec"
        ]
        
        if file_size_mb > 0:
            parts.append(f"({file_size_mb:.1f}MB file)")
        
        self.info(f"Table export completed: {', '.join(parts)}")
    
    def table_failed(self, table_name: str, error: str, retry_count: int = 0):
        """Log table failure"""
        retry_info = f" (retry {retry_count})" if retry_count > 0 else ""
        self.error(f"Table export failed{retry_info}: {error}")
    
    # Connection management logging
    def connection_acquired(self, connection_id: str, pool_size: int):
        """Log connection acquisition"""
        with self._lock:
            self._connection_stats['active_connections'] += 1
            self._connection_stats['total_acquired'] += 1
        
        self.debug(f"Connection acquired: {connection_id} (pool: {pool_size})")
    
    def connection_released(self, connection_id: str, duration: float):
        """Log connection release"""
        with self._lock:
            self._connection_stats['active_connections'] = max(0, 
                self._connection_stats['active_connections'] - 1)
            self._connection_stats['total_released'] += 1
        
        self.debug(f"Connection released: {connection_id} (held: {self._format_duration(duration)})")
    
    def connection_error(self, error: str, retry_count: int = 0):
        """Log connection error"""
        with self._lock:
            self._connection_stats['connection_errors'] += 1
        
        retry_info = f" (retry {retry_count})" if retry_count > 0 else ""
        self.error(f"Connection error{retry_info}: {error}")
    
    def circuit_breaker_opened(self, failure_count: int, backoff_seconds: int):
        """Log circuit breaker opening"""
        with self._lock:
            self._connection_stats['circuit_breaker_state'] = 'OPEN'
        
        self.warning(f"Circuit breaker opened after {failure_count} failures - "
                    f"backing off for {backoff_seconds}s")
    
    def circuit_breaker_closed(self, success_count: int):
        """Log circuit breaker closing"""
        with self._lock:
            self._connection_stats['circuit_breaker_state'] = 'CLOSED'
        
        self.info(f"Circuit breaker closed after {success_count} successful connections")
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get current connection statistics"""
        with self._lock:
            return dict(self._connection_stats)
    
    def log_system_stats(self):
        """Log current system resource usage"""
        try:
            # Memory usage
            memory = psutil.virtual_memory()
            memory_pct = memory.percent
            memory_available = memory.available / 1024 / 1024 / 1024  # GB
            
            # CPU usage
            cpu_pct = psutil.cpu_percent(interval=1)
            
            # Disk usage for temp directory
            disk_usage = psutil.disk_usage('/tmp')
            disk_free = disk_usage.free / 1024 / 1024 / 1024  # GB
            
            with self._lock:
                conn_stats = self._connection_stats
            
            self.info(f"System stats - CPU:{cpu_pct:.1f}% MEM:{memory_pct:.1f}% "
                     f"MEM-FREE:{memory_available:.1f}GB DISK-FREE:{disk_free:.1f}GB "
                     f"CONNECTIONS:{conn_stats['active_connections']}/{conn_stats['max_connections']}")
        
        except Exception as e:
            self.debug(f"Could not collect system stats: {e}")


# Global logger instance
logger = EnhancedLogger("ADU")


# Convenience functions for backwards compatibility
def info(message: str, **kwargs):
    logger.info(message, **kwargs)

def warning(message: str, **kwargs):
    logger.warning(message, **kwargs)

def error(message: str, **kwargs):
    logger.error(message, **kwargs)

def debug(message: str, **kwargs):
    logger.debug(message, **kwargs)