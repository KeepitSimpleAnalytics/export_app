"""
Parallel DuckDB export functions
"""
import logging
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from adu.duckdb_exporter import export_table_chunk_duckdb

logger = logging.getLogger(__name__)


def export_chunk_with_duckdb_worker(
    db_config: Dict[str, Any],
    table_name: str,
    table_dir: Path,
    chunk_num: int,
    offset: int,
    chunk_size: int,
    polars_schema: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str, int, str]:
    """
    Worker function to export a single chunk using dedicated DuckDB connection
    
    Returns:
        Tuple of (success: bool, error_message: str, rows_exported: int, chunk_filename: str)
    """
    chunk_filename = f"part_{chunk_num:04d}.parquet"
    chunk_file = table_dir / chunk_filename
    
    try:
        # Export this chunk using dedicated DuckDB connection
        success, message, rows_exported = export_table_chunk_duckdb(
            db_config, table_name, chunk_file, offset, chunk_size, polars_schema
        )
        
        if success:
            return True, "", rows_exported, chunk_filename
        else:
            return False, message, 0, chunk_filename
            
    except Exception as e:
        error_msg = f"Chunk {chunk_num} failed: {str(e)}"
        return False, error_msg, 0, chunk_filename


def export_chunk_with_duckdb_worker_with_retry(
    db_config: Dict[str, Any],
    table_name: str,
    table_dir: Path,
    chunk_num: int,
    offset: int,
    chunk_size: int,
    polars_schema: Optional[Dict[str, Any]] = None,
    max_retries: int = 3
) -> Tuple[bool, str, int, str]:
    """
    Worker function with retry logic for DuckDB chunk export - handles LDAP auth timeouts
    
    Returns:
        Tuple of (success: bool, error_message: str, rows_exported: int, chunk_filename: str)
    """
    chunk_filename = f"part_{chunk_num:04d}.parquet"
    
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                import time
                backoff_delay = 2 ** (attempt - 1)  # 1s, 2s, 4s delays
                logger.info(f"Retrying DuckDB chunk {chunk_num} (attempt {attempt + 1}/{max_retries + 1}) after {backoff_delay}s delay")
                time.sleep(backoff_delay)
            
            # Attempt the chunk export
            success, message, rows_exported, chunk_filename = export_chunk_with_duckdb_worker(
                db_config, table_name, table_dir, chunk_num, offset, chunk_size, polars_schema
            )
            
            if success:
                if attempt > 0:
                    logger.info(f"DuckDB chunk {chunk_num} succeeded on retry attempt {attempt + 1}")
                return True, "", rows_exported, chunk_filename
            else:
                error_msg = message
                
                # Check if this is a retryable error
                if attempt < max_retries:
                    retryable_errors = [
                        'connection', 'timeout', 'segment', 'network', 'temporary',
                        'ldap', 'authentication', 'login', 'attach', 'failed to attach',
                        'unable to connect', 'authentication failed'
                    ]
                    is_retryable = any(keyword in error_msg.lower() for keyword in retryable_errors)
                    
                    if is_retryable:
                        logger.warning(f"DuckDB chunk {chunk_num} failed (attempt {attempt + 1}), will retry: {error_msg}")
                        continue
                    else:
                        logger.error(f"DuckDB chunk {chunk_num} failed with non-retryable error: {error_msg}")
                        break
                else:
                    logger.error(f"DuckDB chunk {chunk_num} failed after {max_retries + 1} attempts: {error_msg}")
                    return False, error_msg, 0, chunk_filename
                    
        except Exception as e:
            error_msg = str(e)
            
            if attempt < max_retries:
                # Check if this is a retryable exception
                retryable_errors = [
                    'connection', 'timeout', 'segment', 'network', 'temporary',
                    'ldap', 'authentication', 'login', 'attach', 'failed to attach',
                    'unable to connect', 'authentication failed'
                ]
                is_retryable = any(keyword in error_msg.lower() for keyword in retryable_errors)
                
                if is_retryable:
                    logger.warning(f"DuckDB chunk {chunk_num} exception (attempt {attempt + 1}), will retry: {error_msg}")
                    continue
                else:
                    logger.error(f"DuckDB chunk {chunk_num} failed with non-retryable exception: {error_msg}")
                    break
            else:
                logger.error(f"DuckDB chunk {chunk_num} failed after {max_retries + 1} attempts: {error_msg}")
                
        return False, error_msg, 0, chunk_filename


def export_large_table_with_duckdb_parallel(
    db_config: Dict[str, Any],
    table_name: str,
    table_dir: Path,
    source_row_count: int,
    chunk_size: int,
    max_workers: int = 8
) -> Tuple[bool, int]:
    """
    Export large table using parallel DuckDB chunked approach
    
    Args:
        db_config: Database connection configuration
        table_name: Table name to export
        table_dir: Directory to export files to
        source_row_count: Total number of rows in source table
        chunk_size: Number of rows per chunk
        max_workers: Maximum number of concurrent workers
        
    Returns:
        Tuple of (success: bool, total_exported_rows: int)
    """
    logger.info(f"Starting parallel DuckDB export for {table_name} ({source_row_count:,} rows)")
    
    # Get table schema once to use across all chunks
    polars_schema = None
    try:
        # Import here to avoid circular imports
        from adu.database_utils import create_data_source_connection, get_table_schema
        
        # Create temporary connection for schema detection
        db_type = db_config.get('db_type', 'postgresql')
        temp_conn = create_data_source_connection(db_config, db_type)
        polars_schema = get_table_schema(temp_conn, db_type, table_name)
        temp_conn.close()
        
        if polars_schema:
            logger.info(f"Retrieved schema for {table_name}: {len(polars_schema)} columns")
        else:
            logger.warning(f"Could not retrieve schema for {table_name}, using DuckDB inference")
    except Exception as e:
        logger.warning(f"Schema retrieval failed for {table_name}: {str(e)}, using DuckDB inference")
    
    # Calculate chunks needed
    total_chunks = (source_row_count + chunk_size - 1) // chunk_size
    max_workers = min(max_workers, total_chunks)  # Don't create more workers than chunks
    
    logger.info(f"Processing {total_chunks} chunks with {max_workers} parallel workers")
    
    exported_files = []
    total_exported_rows = 0
    failed_chunks = []
    
    try:
        # Create thread pool and submit chunk export tasks
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chunk export tasks
            future_to_chunk = {}
            for chunk_num in range(total_chunks):
                offset = chunk_num * chunk_size
                actual_chunk_size = min(chunk_size, source_row_count - offset)
                
                future = executor.submit(
                    export_chunk_with_duckdb_worker_with_retry,
                    db_config,
                    table_name,
                    table_dir,
                    chunk_num,
                    offset,
                    actual_chunk_size,
                    polars_schema
                )
                future_to_chunk[future] = chunk_num
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_chunk):
                chunk_num = future_to_chunk[future]
                completed += 1
                
                try:
                    success, error_message, rows_exported, chunk_filename = future.result()
                    
                    if success:
                        exported_files.append(chunk_filename)
                        total_exported_rows += rows_exported
                        logger.info(f"Chunk {chunk_num:2d}/{total_chunks} completed: {rows_exported:,} rows ({completed}/{total_chunks} total)")
                    else:
                        failed_chunks.append((chunk_num, error_message))
                        logger.error(f"Chunk {chunk_num:2d}/{total_chunks} failed: {error_message}")
                        
                except Exception as e:
                    failed_chunks.append((chunk_num, str(e)))
                    logger.error(f"Chunk {chunk_num:2d}/{total_chunks} failed with exception: {str(e)}")
        
        # Check if all chunks succeeded
        if failed_chunks:
            failure_summary = "; ".join([f"Chunk {num}: {msg}" for num, msg in failed_chunks])
            logger.error(f"Parallel DuckDB export failed: {len(failed_chunks)} chunks failed - {failure_summary}")
            return False, total_exported_rows
        
        logger.info(f"Parallel DuckDB export completed successfully: {total_exported_rows:,} rows in {len(exported_files)} files")
        return True, total_exported_rows
        
    except Exception as e:
        error_msg = f"Parallel DuckDB export failed for {table_name}: {str(e)}"
        logger.error(error_msg)
        return False, total_exported_rows