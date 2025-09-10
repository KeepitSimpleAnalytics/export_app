#!/usr/bin/env python3
"""
Server-Side Cursor Streaming for Large Table Exports
Eliminates OFFSET performance issues by using PostgreSQL/Greenplum native cursors
"""

import time
import polars as pl
import psutil
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, Generator
from contextlib import contextmanager
import uuid

from adu.enhanced_logger import logger
from adu.greenplum_pool import get_database_connection
from adu.database_type_mappings import create_polars_schema_from_database_metadata


class CursorStreamer:
    """
    Handles server-side cursor streaming for extremely large table exports
    Eliminates OFFSET-based performance degradation
    """
    
    def __init__(self, table_name: str, job_id: str):
        self.table_name = table_name
        self.job_id = job_id
        self.cursor_name = f"export_cursor_{uuid.uuid4().hex[:8]}"
        self.fetch_size = 50000  # Rows per fetch - configurable
        self.memory_limit_mb = 2048  # Max memory per chunk
        
        # Statistics tracking
        self.total_rows_streamed = 0
        self.chunks_created = 0
        self.start_time = time.time()
    
    @contextmanager
    def streaming_cursor(self, db_conn, select_query: str):
        """
        Create and manage a server-side cursor for streaming large result sets
        
        Args:
            db_conn: Database connection from connection pool
            select_query: SELECT query to stream
        """
        cursor = None
        try:
            cursor = db_conn.cursor()
            cursor.arraysize = self.fetch_size  # Optimize fetch size
            
            # Declare server-side cursor with HOLD to survive transaction boundaries
            declare_sql = f"""
                DECLARE {self.cursor_name} CURSOR WITH HOLD FOR {select_query}
            """
            
            logger.info(f"Declaring server-side cursor for streaming: {self.cursor_name}")
            cursor.execute(declare_sql)
            
            yield cursor
            
        except Exception as e:
            logger.error(f"Error in streaming cursor: {e}")
            raise
        finally:
            # Clean up cursor
            if cursor:
                try:
                    cursor.execute(f"CLOSE {self.cursor_name}")
                    logger.debug(f"Closed cursor: {self.cursor_name}")
                except Exception as e:
                    logger.warning(f"Error closing cursor {self.cursor_name}: {e}")
                finally:
                    cursor.close()
    
    def fetch_batch(self, cursor) -> Optional[list]:
        """
        Fetch a batch of rows from the server-side cursor
        
        Args:
            cursor: Database cursor with active server-side cursor
            
        Returns:
            List of rows or None if no more data
        """
        try:
            fetch_sql = f"FETCH {self.fetch_size} FROM {self.cursor_name}"
            cursor.execute(fetch_sql)
            rows = cursor.fetchall()
            
            if not rows:
                return None
            
            self.total_rows_streamed += len(rows)
            return rows
            
        except Exception as e:
            logger.error(f"Error fetching from cursor: {e}")
            raise
    
    def get_memory_usage_mb(self) -> float:
        """Get current process memory usage in MB"""
        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except:
            return 0.0
    
    def should_create_new_chunk(self, current_rows: int, memory_mb: float) -> bool:
        """
        Determine if we should create a new chunk based on memory usage or row count
        
        Args:
            current_rows: Number of rows in current chunk
            memory_mb: Current memory usage in MB
            
        Returns:
            True if new chunk should be created
        """
        # Memory-based chunking - prevent OOM
        if memory_mb > self.memory_limit_mb:
            return True
            
        # Row-based chunking - ensure reasonable chunk sizes
        if current_rows >= 1000000:  # 1M rows per chunk max
            return True
            
        return False
    
    def stream_to_parquet_chunks(self, select_query: str, output_dir: Path, 
                                polars_schema: Optional[pl.Schema] = None) -> Tuple[bool, int, list]:
        """
        Stream table data to multiple Parquet files using server-side cursor
        
        Args:
            select_query: SELECT query to execute
            output_dir: Directory to write Parquet chunks
            polars_schema: Optional Polars schema for type safety
            
        Returns:
            Tuple of (success: bool, total_rows: int, chunk_files: list)
        """
        chunk_files = []
        current_chunk_data = []
        
        logger.table_started(self.table_name, 0, "Cursor-Streaming", 0)
        
        try:
            with get_database_connection() as db_conn:
                with self.streaming_cursor(db_conn, select_query) as cursor:
                    
                    # Get column names from cursor description
                    if not polars_schema:
                        column_names = [desc[0] for desc in cursor.description]
                    else:
                        column_names = list(polars_schema.names())
                    
                    logger.info(f"Starting cursor streaming with {len(column_names)} columns")
                    
                    batch_count = 0
                    while True:
                        # Check memory usage before fetching
                        memory_before = self.get_memory_usage_mb()
                        
                        # Fetch next batch
                        batch_rows = self.fetch_batch(cursor)
                        if not batch_rows:
                            break  # No more data
                        
                        batch_count += 1
                        current_chunk_data.extend(batch_rows)
                        
                        # Check if we should create a chunk
                        memory_after = self.get_memory_usage_mb()
                        
                        if self.should_create_new_chunk(len(current_chunk_data), memory_after):
                            # Write current chunk to Parquet
                            success = self._write_chunk_to_parquet(
                                current_chunk_data, column_names, output_dir, 
                                self.chunks_created, polars_schema
                            )
                            
                            if success:
                                chunk_files.append(output_dir / f"part_{self.chunks_created:04d}.parquet")
                                self.chunks_created += 1
                                
                                # Log progress
                                elapsed = time.time() - self.start_time
                                throughput = int(self.total_rows_streamed / elapsed) if elapsed > 0 else 0
                                logger.table_progress(
                                    self.table_name, 
                                    self.total_rows_streamed,
                                    self.chunks_created,
                                    throughput
                                )
                            else:
                                return False, 0, []
                            
                            # Clear chunk data for next chunk
                            current_chunk_data = []
                        
                        # Log every 10 batches for progress tracking
                        if batch_count % 10 == 0:
                            logger.info(f"Processed {batch_count} batches, {self.total_rows_streamed:,} rows streamed")
                    
                    # Write any remaining data as final chunk
                    if current_chunk_data:
                        success = self._write_chunk_to_parquet(
                            current_chunk_data, column_names, output_dir,
                            self.chunks_created, polars_schema
                        )
                        
                        if success:
                            chunk_files.append(output_dir / f"part_{self.chunks_created:04d}.parquet")
                            self.chunks_created += 1
            
            # Final logging
            elapsed = time.time() - self.start_time
            throughput = int(self.total_rows_streamed / elapsed) if elapsed > 0 else 0
            
            logger.table_completed(
                self.table_name,
                self.total_rows_streamed,
                elapsed,
                sum(f.stat().st_size for f in chunk_files if f.exists()) / 1024 / 1024
            )
            
            return True, self.total_rows_streamed, chunk_files
            
        except Exception as e:
            logger.table_failed(self.table_name, f"Cursor streaming failed: {str(e)}")
            return False, 0, []
    
    def _write_chunk_to_parquet(self, chunk_data: list, column_names: list, 
                               output_dir: Path, chunk_num: int,
                               polars_schema: Optional[pl.Schema] = None) -> bool:
        """
        Write chunk data to Parquet file using Polars
        
        Args:
            chunk_data: List of row tuples
            column_names: List of column names
            output_dir: Output directory
            chunk_num: Chunk number for filename
            polars_schema: Optional schema for type safety
            
        Returns:
            True if successful
        """
        try:
            chunk_file = output_dir / f"part_{chunk_num:04d}.parquet"
            
            # Convert to Polars DataFrame
            if chunk_data:
                # Create DataFrame from list of tuples
                df_dict = {}
                for i, col_name in enumerate(column_names):
                    df_dict[col_name] = [row[i] if i < len(row) else None for row in chunk_data]
                
                if polars_schema:
                    df = pl.DataFrame(df_dict, schema=polars_schema)
                else:
                    df = pl.DataFrame(df_dict)
                
                # Write to Parquet with compression
                df.write_parquet(
                    chunk_file,
                    compression="snappy",
                    use_pyarrow=True
                )
                
                rows_written = len(chunk_data)
                file_size_mb = chunk_file.stat().st_size / 1024 / 1024
                
                logger.info(f"Wrote chunk {chunk_num}: {rows_written:,} rows, {file_size_mb:.1f}MB")
                return True
            else:
                logger.warning(f"No data to write for chunk {chunk_num}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to write chunk {chunk_num}: {str(e)}")
            return False


def export_large_table_with_cursor_streaming(
    job_id: str,
    table_name: str, 
    output_dir: Path,
    db_config: Dict[str, Any],
    select_query: Optional[str] = None,
    polars_schema: Optional[pl.Schema] = None
) -> Tuple[bool, int]:
    """
    Export extremely large table using server-side cursor streaming
    Eliminates OFFSET performance issues entirely
    
    Args:
        job_id: Job identifier for logging
        table_name: Name of table to export
        output_dir: Directory to write Parquet chunks
        db_config: Database connection configuration (not used with pool)
        select_query: Optional custom SELECT query, defaults to SELECT * FROM table
        polars_schema: Optional Polars schema for type safety
        
    Returns:
        Tuple of (success: bool, total_rows_exported: int)
    """
    
    # Default to SELECT * if no custom query provided
    if not select_query:
        select_query = f"SELECT * FROM {table_name}"
    
    logger.info(f"Starting cursor streaming export for {table_name}")
    logger.info(f"Query: {select_query}")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize streamer
    streamer = CursorStreamer(table_name, job_id)
    
    # Adjust fetch size based on available memory
    available_memory_gb = psutil.virtual_memory().available / 1024 / 1024 / 1024
    if available_memory_gb > 16:
        streamer.fetch_size = 100000  # Larger batches for high-memory systems
        streamer.memory_limit_mb = 4096
    elif available_memory_gb < 4:
        streamer.fetch_size = 25000   # Smaller batches for low-memory systems
        streamer.memory_limit_mb = 1024
    
    logger.info(f"Configured streaming: fetch_size={streamer.fetch_size:,}, memory_limit={streamer.memory_limit_mb}MB")
    
    # Execute streaming export
    success, total_rows, chunk_files = streamer.stream_to_parquet_chunks(
        select_query, output_dir, polars_schema
    )
    
    if success:
        total_size_mb = sum(f.stat().st_size for f in chunk_files if f.exists()) / 1024 / 1024
        logger.info(f"Cursor streaming completed: {len(chunk_files)} chunks, {total_rows:,} rows, {total_size_mb:.1f}MB total")
        return True, total_rows
    else:
        logger.error(f"Cursor streaming failed for {table_name}")
        return False, 0


def can_use_cursor_streaming(db_type: str) -> bool:
    """
    Check if cursor streaming is supported for the database type
    
    Args:
        db_type: Database type (postgresql, greenplum, vertica)
        
    Returns:
        True if cursor streaming is supported
    """
    supported_types = {'postgresql', 'greenplum'}
    return db_type.lower() in supported_types


def estimate_streaming_benefit(table_row_count: int, chunk_size: int) -> str:
    """
    Estimate performance benefit of cursor streaming vs OFFSET chunking
    
    Args:
        table_row_count: Total rows in table
        chunk_size: Chunk size for comparison
        
    Returns:
        Human-readable benefit estimate
    """
    if table_row_count < 1000000:  # 1M rows
        return "minimal (small table)"
    
    total_chunks = (table_row_count + chunk_size - 1) // chunk_size
    
    if total_chunks < 50:
        return "low (< 50 chunks)"
    elif total_chunks < 200:
        return "moderate (50-200 chunks)"
    elif total_chunks < 1000:
        return "high (200-1000 chunks)" 
    else:
        return "extreme (> 1000 chunks - OFFSET would be unusable)"