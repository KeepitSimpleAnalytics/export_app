#!/usr/bin/env python3
"""
Single SQLite Writer Queue System for ADU Export Application
Eliminates SQLite file lock contention by serializing all database operations
"""

import sqlite3
import threading
import queue
import time
import json
import atexit
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from contextlib import contextmanager

from adu.enhanced_logger import logger


class SQLiteOperationType(Enum):
    """Types of SQLite operations"""
    JOB_START = "job_start"
    JOB_UPDATE = "job_update" 
    JOB_COMPLETE = "job_complete"
    JOB_FAIL = "job_fail"
    TABLE_START = "table_start"
    TABLE_UPDATE = "table_update"
    TABLE_COMPLETE = "table_complete"
    TABLE_FAIL = "table_fail"
    ERROR_LOG = "error_log"
    PROGRESS_UPDATE = "progress_update"
    QUERY = "query"  # For SELECT operations
    BATCH = "batch"  # For batch operations


@dataclass
class SQLiteOperation:
    """Represents a queued SQLite operation"""
    operation_type: SQLiteOperationType
    data: Dict[str, Any]
    callback: Optional[Callable] = None
    result_queue: Optional[queue.Queue] = None
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


class SQLiteWriterQueue:
    """
    Single-threaded SQLite writer that processes all database operations from a queue.
    Eliminates lock contention and improves performance through batching.
    """
    
    def __init__(self, db_path: str, batch_size: int = 50, batch_timeout: float = 5.0):
        self.db_path = db_path
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        self._queue = queue.Queue()
        self._shutdown_event = threading.Event()
        self._worker_thread = None
        self._connection = None
        self._cursor = None
        self._batch_operations = []
        self._last_batch_time = time.time()
        self._stats = {
            'operations_processed': 0,
            'batch_operations': 0,
            'queue_size_max': 0,
            'average_queue_size': 0,
            'total_queries': 0
        }
        
        # Initialize database and start worker thread
        self._init_database()
        self._start_worker()
        
        # Register cleanup on exit
        atexit.register(self.shutdown)
    
    def _init_database(self):
        """Initialize database connection and tables"""
        try:
            self._connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self._connection.row_factory = sqlite3.Row
            self._cursor = self._connection.cursor()
            
            # Enable WAL mode for better concurrency
            self._cursor.execute("PRAGMA journal_mode=WAL")
            self._cursor.execute("PRAGMA synchronous=NORMAL")
            self._cursor.execute("PRAGMA cache_size=10000")
            self._cursor.execute("PRAGMA temp_store=MEMORY")
            
            self._create_tables()
            logger.info("SQLite writer initialized with WAL mode enabled")
            
        except Exception as e:
            logger.error(f"Failed to initialize SQLite database: {e}")
            raise
    
    def _create_tables(self):
        """Create necessary tables if they don't exist"""
        tables = [
            '''CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                db_username TEXT,
                status TEXT DEFAULT 'queued',
                overall_status TEXT DEFAULT 'queued',
                celery_task_id TEXT,
                start_time DATETIME,
                end_time DATETIME,
                error_message TEXT,
                progress_percent INTEGER DEFAULT 0,
                tables_total INTEGER DEFAULT 0,
                tables_completed INTEGER DEFAULT 0,
                tables_failed INTEGER DEFAULT 0,
                rows_total BIGINT DEFAULT 0,
                rows_processed BIGINT DEFAULT 0,
                throughput_rows_per_sec INTEGER DEFAULT 0,
                estimated_completion DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )''',
            
            '''CREATE TABLE IF NOT EXISTS errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                timestamp DATETIME,
                error_message TEXT,
                traceback TEXT,
                context TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs (job_id)
            )''',
            
            '''CREATE TABLE IF NOT EXISTS job_configs (
                job_id TEXT PRIMARY KEY,
                config TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs (job_id)
            )''',
            
            '''CREATE TABLE IF NOT EXISTS table_exports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                table_name TEXT,
                status TEXT,
                row_count INTEGER,
                rows_processed INTEGER DEFAULT 0,
                chunk_count INTEGER DEFAULT 1,
                file_path TEXT,
                file_size_mb REAL DEFAULT 0,
                throughput_rows_per_sec INTEGER DEFAULT 0,
                start_time DATETIME,
                end_time DATETIME,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                validation_status TEXT DEFAULT 'pending',
                checksum TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs (job_id)
            )'''
        ]
        
        for table_sql in tables:
            self._cursor.execute(table_sql)
        
        self._connection.commit()
    
    def _start_worker(self):
        """Start the background worker thread"""
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        logger.info("SQLite writer worker thread started")
    
    def _worker_loop(self):
        """Main worker loop that processes queued operations"""
        logger.info("SQLite writer worker loop started")
        
        while not self._shutdown_event.is_set():
            try:
                # Process operations with timeout
                self._process_operations()
                
                # Handle batching timeout
                if (time.time() - self._last_batch_time > self.batch_timeout and 
                    self._batch_operations):
                    self._flush_batch()
                    
            except Exception as e:
                logger.error(f"Error in SQLite worker loop: {e}")
                time.sleep(0.1)  # Brief pause to prevent tight loop on errors
        
        # Process any remaining operations on shutdown
        self._flush_remaining_operations()
        logger.info("SQLite writer worker loop stopped")
    
    def _process_operations(self):
        """Process queued operations, batching when possible"""
        try:
            # Get operation with timeout
            operation = self._queue.get(timeout=1.0)
            self._stats['operations_processed'] += 1
            
            # Update queue size stats
            current_queue_size = self._queue.qsize()
            self._stats['queue_size_max'] = max(self._stats['queue_size_max'], current_queue_size)
            
            # Handle different operation types
            if operation.operation_type == SQLiteOperationType.BATCH:
                self._process_batch_operation(operation)
            elif operation.operation_type == SQLiteOperationType.QUERY:
                self._process_query_operation(operation)
            else:
                # Add to batch for regular operations
                self._batch_operations.append(operation)
                
                # Flush batch if it's full
                if len(self._batch_operations) >= self.batch_size:
                    self._flush_batch()
                    
            self._queue.task_done()
            
        except queue.Empty:
            # No operations to process
            pass
    
    def _process_batch_operation(self, operation: SQLiteOperation):
        """Process a batch operation immediately"""
        try:
            self._execute_operation(operation)
            self._connection.commit()
            self._stats['batch_operations'] += 1
            
            if operation.callback:
                operation.callback()
                
        except Exception as e:
            logger.error(f"Error processing batch operation: {e}")
            if operation.result_queue:
                operation.result_queue.put(('error', str(e)))
    
    def _process_query_operation(self, operation: SQLiteOperation):
        """Process a SELECT query operation"""
        try:
            result = self._execute_query(operation)
            self._stats['total_queries'] += 1
            
            if operation.result_queue:
                operation.result_queue.put(('success', result))
            
            if operation.callback:
                operation.callback(result)
                
        except Exception as e:
            logger.error(f"Error processing query operation: {e}")
            if operation.result_queue:
                operation.result_queue.put(('error', str(e)))
    
    def _flush_batch(self):
        """Flush accumulated batch operations"""
        if not self._batch_operations:
            return
        
        try:
            self._connection.execute("BEGIN")
            
            for operation in self._batch_operations:
                try:
                    self._execute_operation(operation)
                except Exception as e:
                    logger.error(f"Error in batch operation {operation.operation_type}: {e}")
                    if operation.result_queue:
                        operation.result_queue.put(('error', str(e)))
                    continue
                
                # Handle callbacks and result queues
                if operation.result_queue:
                    operation.result_queue.put(('success', None))
                if operation.callback:
                    operation.callback()
            
            self._connection.commit()
            logger.debug(f"Flushed batch of {len(self._batch_operations)} operations")
            
            self._batch_operations.clear()
            self._last_batch_time = time.time()
            self._stats['batch_operations'] += 1
            
        except Exception as e:
            logger.error(f"Error flushing batch operations: {e}")
            try:
                self._connection.rollback()
            except:
                pass
            self._batch_operations.clear()
    
    def _execute_operation(self, operation: SQLiteOperation):
        """Execute a single SQLite operation"""
        op_type = operation.operation_type
        data = operation.data
        
        if op_type == SQLiteOperationType.JOB_START:
            # Check if job already exists and is completed
            self._cursor.execute("SELECT status, overall_status FROM jobs WHERE job_id = ?", (data['job_id'],))
            existing_job = self._cursor.fetchone()
            
            if existing_job and existing_job[1] in ('completed', 'completed_with_errors'):
                # Job is already completed, don't overwrite
                return
            elif existing_job:
                # Job exists but not completed, update only non-completion fields
                self._cursor.execute(
                    """UPDATE jobs SET db_username = ?, celery_task_id = ?, start_time = ?, tables_total = ?
                       WHERE job_id = ? AND overall_status NOT IN ('completed', 'completed_with_errors')""",
                    (data.get('db_username'), data.get('celery_task_id'), data['start_time'], 
                     data.get('tables_total', 0), data['job_id'])
                )
            else:
                # New job, insert normally
                self._cursor.execute(
                    """INSERT INTO jobs 
                       (job_id, db_username, status, overall_status, celery_task_id, start_time, tables_total) 
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (data['job_id'], data.get('db_username'), data['status'], data['status'],
                     data.get('celery_task_id'), data['start_time'], data.get('tables_total', 0))
                )
        
        elif op_type == SQLiteOperationType.JOB_UPDATE:
            # Build dynamic UPDATE query based on provided fields
            fields = []
            values = []
            for field, value in data.items():
                if field != 'job_id':
                    fields.append(f"{field} = ?")
                    values.append(value)
            
            if fields:
                values.append(data['job_id'])
                self._cursor.execute(
                    f"UPDATE jobs SET {', '.join(fields)} WHERE job_id = ?",
                    values
                )
        
        elif op_type == SQLiteOperationType.JOB_COMPLETE:
            self._cursor.execute(
                "UPDATE jobs SET status = ?, overall_status = ?, end_time = ? WHERE job_id = ?",
                ('completed', 'completed', data['end_time'], data['job_id'])
            )
        
        elif op_type == SQLiteOperationType.JOB_FAIL:
            self._cursor.execute(
                "UPDATE jobs SET status = ?, overall_status = ?, end_time = ?, error_message = ? WHERE job_id = ?",
                ('failed', 'failed', data['end_time'], data['error_message'], data['job_id'])
            )
        
        elif op_type == SQLiteOperationType.TABLE_START:
            self._cursor.execute(
                """INSERT OR REPLACE INTO table_exports 
                   (job_id, table_name, status, start_time, row_count) 
                   VALUES (?, ?, ?, ?, ?)""",
                (data['job_id'], data['table_name'], data['status'], 
                 data['start_time'], data.get('row_count', 0))
            )
        
        elif op_type == SQLiteOperationType.TABLE_UPDATE:
            # Dynamic update for table exports
            fields = []
            values = []
            for field, value in data.items():
                if field not in ['job_id', 'table_name']:
                    fields.append(f"{field} = ?")
                    values.append(value)
            
            if fields:
                values.extend([data['job_id'], data['table_name']])
                self._cursor.execute(
                    f"UPDATE table_exports SET {', '.join(fields)} WHERE job_id = ? AND table_name = ?",
                    values
                )
        
        elif op_type == SQLiteOperationType.TABLE_COMPLETE:
            self._cursor.execute(
                """UPDATE table_exports 
                   SET status = ?, end_time = ?, rows_processed = ?, file_path = ?, 
                       file_size_mb = ?, throughput_rows_per_sec = ?
                   WHERE job_id = ? AND table_name = ?""",
                ('completed', data['end_time'], data.get('rows_processed', 0),
                 data.get('file_path'), data.get('file_size_mb', 0), 
                 data.get('throughput_rows_per_sec', 0), data['job_id'], data['table_name'])
            )
        
        elif op_type == SQLiteOperationType.TABLE_FAIL:
            self._cursor.execute(
                "UPDATE table_exports SET status = ?, end_time = ?, error_message = ? WHERE job_id = ? AND table_name = ?",
                ('failed', data['end_time'], data['error_message'], data['job_id'], data['table_name'])
            )
        
        elif op_type == SQLiteOperationType.ERROR_LOG:
            self._cursor.execute(
                "INSERT INTO errors (job_id, timestamp, error_message, traceback, context) VALUES (?, ?, ?, ?, ?)",
                (data['job_id'], data['timestamp'], data['error_message'], 
                 data.get('traceback'), data.get('context'))
            )
    
    def _execute_query(self, operation: SQLiteOperation):
        """Execute a SELECT query and return results"""
        query = operation.data['query']
        params = operation.data.get('params', ())
        
        self._cursor.execute(query, params)
        
        if operation.data.get('fetchone'):
            return self._cursor.fetchone()
        elif operation.data.get('fetchall'):
            return self._cursor.fetchall()
        else:
            return self._cursor.fetchall()  # Default to fetchall
    
    def _flush_remaining_operations(self):
        """Flush any remaining operations during shutdown"""
        logger.info("Flushing remaining SQLite operations...")
        
        # Process any remaining batched operations
        if self._batch_operations:
            self._flush_batch()
        
        # Process any remaining queued operations
        remaining_count = 0
        while True:
            try:
                operation = self._queue.get_nowait()
                self._execute_operation(operation)
                self._queue.task_done()
                remaining_count += 1
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error processing remaining operation: {e}")
        
        if remaining_count > 0:
            self._connection.commit()
            logger.info(f"Processed {remaining_count} remaining operations")
    
    # Public API methods
    
    def job_started(self, job_id: str, db_username: str = None, celery_task_id: str = None, 
                   tables_total: int = 0):
        """Queue job start operation"""
        operation = SQLiteOperation(
            operation_type=SQLiteOperationType.JOB_START,
            data={
                'job_id': job_id,
                'db_username': db_username,
                'status': 'running',
                'celery_task_id': celery_task_id,
                'start_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'tables_total': tables_total
            }
        )
        self._queue.put(operation)
    
    def job_update(self, job_id: str, **kwargs):
        """Queue job update operation"""
        data = {'job_id': job_id}
        data.update(kwargs)
        
        operation = SQLiteOperation(
            operation_type=SQLiteOperationType.JOB_UPDATE,
            data=data
        )
        self._queue.put(operation)
    
    def job_completed(self, job_id: str):
        """Queue job completion operation"""
        operation = SQLiteOperation(
            operation_type=SQLiteOperationType.JOB_COMPLETE,
            data={
                'job_id': job_id,
                'end_time': time.strftime('%Y-%m-%d %H:%M:%S')
            }
        )
        self._queue.put(operation)
    
    def job_failed(self, job_id: str, error_message: str):
        """Queue job failure operation"""
        operation = SQLiteOperation(
            operation_type=SQLiteOperationType.JOB_FAIL,
            data={
                'job_id': job_id,
                'end_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'error_message': error_message
            }
        )
        self._queue.put(operation)
    
    def table_started(self, job_id: str, table_name: str, row_count: int = 0):
        """Queue table start operation"""
        operation = SQLiteOperation(
            operation_type=SQLiteOperationType.TABLE_START,
            data={
                'job_id': job_id,
                'table_name': table_name,
                'status': 'processing',
                'start_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'row_count': row_count
            }
        )
        self._queue.put(operation)
    
    def table_update(self, job_id: str, table_name: str, **kwargs):
        """Queue table update operation"""
        data = {'job_id': job_id, 'table_name': table_name}
        data.update(kwargs)
        
        operation = SQLiteOperation(
            operation_type=SQLiteOperationType.TABLE_UPDATE,
            data=data
        )
        self._queue.put(operation)
    
    def table_completed(self, job_id: str, table_name: str, rows_processed: int = 0,
                       file_path: str = None, file_size_mb: float = 0, 
                       throughput_rows_per_sec: int = 0):
        """Queue table completion operation"""
        operation = SQLiteOperation(
            operation_type=SQLiteOperationType.TABLE_COMPLETE,
            data={
                'job_id': job_id,
                'table_name': table_name,
                'end_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'rows_processed': rows_processed,
                'file_path': file_path,
                'file_size_mb': file_size_mb,
                'throughput_rows_per_sec': throughput_rows_per_sec
            }
        )
        self._queue.put(operation)
    
    def table_failed(self, job_id: str, table_name: str, error_message: str):
        """Queue table failure operation"""
        operation = SQLiteOperation(
            operation_type=SQLiteOperationType.TABLE_FAIL,
            data={
                'job_id': job_id,
                'table_name': table_name,
                'end_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'error_message': error_message
            }
        )
        self._queue.put(operation)
    
    def log_error(self, job_id: str, error_message: str, traceback: str = None, 
                  context: str = None):
        """Queue error logging operation"""
        operation = SQLiteOperation(
            operation_type=SQLiteOperationType.ERROR_LOG,
            data={
                'job_id': job_id,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'error_message': error_message,
                'traceback': traceback,
                'context': context
            }
        )
        self._queue.put(operation)
    
    def query(self, query: str, params: tuple = (), fetchone: bool = False, 
              timeout: float = 10.0):
        """Execute a SELECT query and return results"""
        result_queue = queue.Queue()
        operation = SQLiteOperation(
            operation_type=SQLiteOperationType.QUERY,
            data={
                'query': query,
                'params': params,
                'fetchone': fetchone,
                'fetchall': not fetchone
            },
            result_queue=result_queue
        )
        
        self._queue.put(operation)
        
        try:
            status, result = result_queue.get(timeout=timeout)
            if status == 'error':
                raise Exception(f"Query failed: {result}")
            return result
        except queue.Empty:
            raise TimeoutError(f"Query timed out after {timeout} seconds")
    
    @contextmanager
    def batch_context(self):
        """Context manager for batching multiple operations"""
        batch_operations = []
        
        class BatchCollector:
            def __init__(self, writer):
                self.writer = writer
                self.operations = batch_operations
            
            def add_operation(self, op_type: SQLiteOperationType, data: Dict[str, Any]):
                operation = SQLiteOperation(operation_type=op_type, data=data)
                self.operations.append(operation)
        
        collector = BatchCollector(self)
        try:
            yield collector
            
            # Execute all collected operations as a batch
            if batch_operations:
                batch_operation = SQLiteOperation(
                    operation_type=SQLiteOperationType.BATCH,
                    data={'operations': batch_operations}
                )
                self._queue.put(batch_operation)
                
        except Exception as e:
            logger.error(f"Error in batch context: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current queue and processing statistics"""
        stats = dict(self._stats)
        stats.update({
            'queue_size': self._queue.qsize(),
            'batch_pending': len(self._batch_operations),
            'worker_active': self._worker_thread.is_alive() if self._worker_thread else False
        })
        return stats
    
    def shutdown(self, timeout: float = 30.0):
        """Shutdown the writer queue and flush all operations"""
        if self._shutdown_event.is_set():
            return
        
        logger.info("Shutting down SQLite writer queue...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Wait for worker thread to finish
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)
            if self._worker_thread.is_alive():
                logger.warning("SQLite writer thread did not shut down cleanly")
        
        # Close database connection
        if self._connection:
            try:
                self._connection.close()
            except Exception as e:
                logger.error(f"Error closing SQLite connection: {e}")
        
        logger.info("SQLite writer queue shut down")


# Global SQLite writer instance
_sqlite_writer = None
_writer_lock = threading.Lock()


def get_sqlite_writer(db_path: str = None) -> SQLiteWriterQueue:
    """Get the global SQLite writer instance"""
    global _sqlite_writer
    
    if _sqlite_writer is None:
        with _writer_lock:
            if _sqlite_writer is None:
                import os
                if db_path is None:
                    db_path = os.environ.get('ADU_DB_PATH', '/tmp/adu.db')
                _sqlite_writer = SQLiteWriterQueue(db_path)
    
    return _sqlite_writer


# Convenience functions for backwards compatibility
def job_started(job_id: str, **kwargs):
    writer = get_sqlite_writer()
    writer.job_started(job_id, **kwargs)

def job_completed(job_id: str):
    writer = get_sqlite_writer()
    writer.job_completed(job_id)

def job_failed(job_id: str, error_message: str):
    writer = get_sqlite_writer()
    writer.job_failed(job_id, error_message)

def table_started(job_id: str, table_name: str, **kwargs):
    writer = get_sqlite_writer()
    writer.table_started(job_id, table_name, **kwargs)

def table_completed(job_id: str, table_name: str, **kwargs):
    writer = get_sqlite_writer()
    writer.table_completed(job_id, table_name, **kwargs)

def table_failed(job_id: str, table_name: str, error_message: str):
    writer = get_sqlite_writer()
    writer.table_failed(job_id, table_name, error_message)

def log_error(job_id: str, error_message: str, **kwargs):
    writer = get_sqlite_writer()
    writer.log_error(job_id, error_message, **kwargs)

def query(query_str: str, params: tuple = (), **kwargs):
    writer = get_sqlite_writer()
    return writer.query(query_str, params, **kwargs)