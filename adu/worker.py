import sqlite3
import time
import traceback
import json
import os
import tempfile
import re
import logging
from pathlib import Path
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import vertica_python
from adu.database import get_db_connection  # Keep for backwards compatibility, but avoid using
from adu.enhanced_logger import logger
from adu.sqlite_writer import get_sqlite_writer
from adu.greenplum_pool import (
    initialize_connection_pool,
    get_database_connection as get_pooled_connection,
    get_pool_stats,
    ConnectionConfig
)
from adu.database_type_mappings import create_polars_schema_from_database_metadata
from adu.duckdb_exporter import (
    export_small_table_duckdb,
    export_table_chunk_duckdb,
    validate_duckdb_export,
    get_table_row_count_duckdb,
    create_duckdb_connection,
    export_large_table_with_duckdb
)
from adu.parallel_duckdb_functions import export_large_table_with_duckdb_parallel
from adu.smart_export import smart_export_table

def create_data_source_connection(db_config, db_type):
    """
    Create database connection for data source (PostgreSQL/Greenplum/Vertica)
    
    Args:
        db_config: Database configuration dictionary
        db_type: Database type ('postgresql', 'greenplum', 'vertica')
        
    Returns:
        Database connection object
    """
    if db_type.lower() in ['postgresql', 'greenplum']:
        return psycopg2.connect(
            host=db_config['host'],
            port=db_config.get('port', 5432),
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password']
        )
    elif db_type.lower() == 'vertica':
        return vertica_python.connect(
            host=db_config['host'],
            port=db_config.get('port', 5433),
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password']
        )
    else:
        raise ValueError(f"Unsupported database type for data source: {db_type}")

class ProgressManager:
    """Manages progress reporting and logging for large table exports"""
    def __init__(self, job_id, table_name, total_rows, total_chunks):
        self.job_id = job_id
        self.table_name = table_name
        self.total_rows = total_rows
        self.total_chunks = total_chunks
        self.start_time = time.time()
        self.completed_chunks = 0
        self.processed_rows = 0
        self.last_memory_mb = 0
        self.last_log_time = 0
        self.chunk_times = []
        self.export_method = "Unknown"
        
    def set_export_method(self, method):
        """Set the export method being used"""
        self.export_method = method
        logging.info(f"Job {self.job_id}: Starting {self.table_name} export using {method}")
        logging.info(f"Table {self.table_name}: {self.total_rows:,} rows → {self.total_chunks} chunks")
        
    def should_log_progress(self):
        """Determine if we should log progress (every 25% or 10 minutes)"""
        progress = self.completed_chunks / self.total_chunks if self.total_chunks > 0 else 0
        time_since_last_log = time.time() - self.last_log_time
        
        # Log at 25%, 50%, 75% completion or every 10 minutes
        progress_thresholds = [0.25, 0.50, 0.75]
        current_threshold = int(progress * 4) / 4  # Round to nearest 25%
        
        return (current_threshold in progress_thresholds and 
                self.last_log_time < time.time() - 300) or time_since_last_log > 600
    
    def update_chunk_completed(self, chunk_num, rows_exported, memory_mb=None, chunk_duration=None):
        """Update progress for a completed chunk"""
        self.completed_chunks += 1
        self.processed_rows += rows_exported
        
        if chunk_duration:
            self.chunk_times.append(chunk_duration)
        
        # Smart memory logging - only log if significant change
        memory_changed = memory_mb and abs(memory_mb - self.last_memory_mb) > 500  # 500MB threshold
        
        if self.should_log_progress() or memory_changed:
            self.log_progress(memory_mb)
            self.last_log_time = time.time()
            
        if memory_mb:
            self.last_memory_mb = memory_mb
    
    def log_progress(self, memory_mb=None):
        """Log structured progress update"""
        progress_pct = (self.completed_chunks / self.total_chunks) * 100
        elapsed_time = time.time() - self.start_time
        
        # Calculate ETA based on actual performance
        if self.completed_chunks > 0:
            avg_time_per_chunk = elapsed_time / self.completed_chunks
            remaining_chunks = self.total_chunks - self.completed_chunks
            eta_seconds = remaining_chunks * avg_time_per_chunk
            eta = str(timedelta(seconds=int(eta_seconds)))
        else:
            eta = "calculating..."
            
        # Calculate throughput
        if elapsed_time > 0:
            rows_per_sec = self.processed_rows / elapsed_time
            throughput_msg = f"{rows_per_sec:.0f} rows/sec"
            
            # Performance intelligence and warnings
            if self.export_method.startswith("Polars"):
                if rows_per_sec < 500:
                    throughput_msg += " 🚨 VERY SLOW (DuckDB should provide 10-50x improvement)"
                elif rows_per_sec < 1000:
                    throughput_msg += " ⚠️ SLOW (DuckDB recommended for performance)"
                elif rows_per_sec < 2000:
                    throughput_msg += " 💡 Consider DuckDB for faster exports"
            elif self.export_method.startswith("DuckDB"):
                if rows_per_sec < 2000:
                    throughput_msg += " ⚠️ Slower than expected for DuckDB"
                elif rows_per_sec > 10000:
                    throughput_msg += " ⚡ Excellent DuckDB performance"
        else:
            throughput_msg = "calculating..."
        
        # Memory info (only if provided and significant)
        memory_msg = f" [Memory: {memory_mb:.0f}MB]" if memory_mb else ""
        
        elapsed_str = str(timedelta(seconds=int(elapsed_time)))
        
        logging.info(f"Progress {self.table_name}: {progress_pct:.0f}% complete "
                    f"({self.completed_chunks:,}/{self.total_chunks:,} chunks) - "
                    f"{self.processed_rows:,}/{self.total_rows:,} rows "
                    f"[{elapsed_str} elapsed, {eta} remaining] "
                    f"({throughput_msg}){memory_msg}")
    
    def log_completion(self):
        """Log comprehensive final completion summary"""
        total_time = time.time() - self.start_time
        total_time_str = str(timedelta(seconds=int(total_time)))
        avg_throughput = self.processed_rows / total_time if total_time > 0 else 0
        
        # Performance classification
        if avg_throughput > 20000:
            perf_rating = "⚡ EXCELLENT"
        elif avg_throughput > 10000:
            perf_rating = "✅ GOOD"
        elif avg_throughput > 5000:
            perf_rating = "🟡 FAIR"
        elif avg_throughput > 1000:
            perf_rating = "🟠 SLOW"
        else:
            perf_rating = "🔴 VERY SLOW"
        
        # Calculate efficiency metrics
        chunks_per_sec = self.completed_chunks / total_time if total_time > 0 else 0
        avg_chunk_size = self.processed_rows / self.completed_chunks if self.completed_chunks > 0 else 0
        
        logging.info(f"✅ COMPLETED {self.table_name}: {self.processed_rows:,} rows "
                    f"({self.completed_chunks:,} chunks) exported in {total_time_str}")
        
        logging.info(f"📊 PERFORMANCE SUMMARY: {avg_throughput:.0f} rows/sec avg, "
                    f"{chunks_per_sec:.1f} chunks/sec, {avg_chunk_size:.0f} rows/chunk "
                    f"using {self.export_method} - {perf_rating}")
    
    def log_performance_warning(self, current_throughput):
        """Log performance warning if throughput is poor"""
        if current_throughput < 1000 and self.export_method == "Polars":
            logging.warning(f"Poor performance detected on {self.table_name}: {current_throughput:.0f} rows/sec. "
                           f"DuckDB method typically achieves 50,000+ rows/sec for similar tables.")

def redact_sensitive_data(text):
    """Redact sensitive information from log messages"""
    if not isinstance(text, str):
        text = str(text)
    
    # Patterns to redact
    patterns = [
        (r'gAAAAA[A-Za-z0-9_\-=]+', r'***ENCRYPTED_PASSWORD***'),
        (r'(password|pwd|pass|secret|token|api_key|access_key)\s*[:=]\s*[^\s,]+', r'\1=***REDACTED***'),
        (r'://([^:]+):([^@]+)@', r'://\1:***REDACTED***@'),
    ]
    
    redacted_text = text
    for pattern, replacement in patterns:
        redacted_text = re.sub(pattern, replacement, redacted_text, flags=re.IGNORECASE)
    
    return redacted_text

class SensitiveDataFilter(logging.Filter):
    """Logging filter to redact sensitive information"""
    def filter(self, record):
        try:
            # Redact the log message
            if hasattr(record, 'msg') and record.msg:
                record.msg = redact_sensitive_data(record.msg)
            
            # Redact any args that might contain sensitive data
            if hasattr(record, 'args') and record.args:
                record.args = tuple(redact_sensitive_data(str(arg)) if arg is not None else arg for arg in record.args)
        except Exception:
            # If redaction fails, just continue with the original record
            pass
        
        return True

# No encryption needed for airgapped environment

def get_database_connection(db_type, host, port, username, password, database=None):
    """
    DEPRECATED: Create database connection - use get_pooled_connection instead
    This function is kept for backward compatibility but should be avoided for new code
    """
    logger.warning("Using deprecated get_database_connection - consider using connection pool")
    
    if db_type.lower() in ['postgresql', 'greenplum']:
        return psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database or 'postgres'
        )
    elif db_type.lower() == 'vertica':
        return vertica_python.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database or 'defaultdb'
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def initialize_global_connection_pool(db_type, host, port, username, password, database):
    """Initialize the global connection pool for efficient connection management"""
    try:
        pool = initialize_connection_pool(
            db_type=db_type,
            host=host,
            port=port,
            username=username,
            password=password,
            database=database or ('postgres' if db_type.lower() in ['postgresql', 'greenplum'] else 'defaultdb'),
            max_connections=6  # Limit concurrent connections to prevent GP exhaustion
        )
        
        logger.info(f"Initialized global connection pool with max 6 connections to {host}:{port}")
        return pool
        
    except Exception as e:
        logger.error(f"Failed to initialize connection pool: {e}")
        raise

def discover_schemas(db_conn, db_type):
    """Discover all user schemas in the database"""
    cursor = db_conn.cursor()
    
    if db_type.lower() in ['postgresql', 'greenplum']:
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast_temp_1', 'pg_temp_1')
            AND schema_name NOT LIKE 'pg_%'
            AND schema_name NOT LIKE 'gp_%'
            ORDER BY schema_name
        """)
    elif db_type.lower() == 'vertica':
        cursor.execute("""
            SELECT schema_name 
            FROM v_catalog.schemata 
            WHERE schema_name NOT IN ('v_catalog', 'v_monitor', 'v_internal')
            ORDER BY schema_name
        """)
    
    schemas = [row[0] for row in cursor.fetchall()]
    cursor.close()
    logging.info(f"Discovered {len(schemas)} schemas: {schemas}")
    return schemas

def discover_tables_by_schema(db_conn, db_type, schema_name=None):
    """Discover tables in a specific schema or all schemas"""
    cursor = db_conn.cursor()
    
    if db_type.lower() in ['postgresql', 'greenplum']:
        if schema_name:
            # Get tables for specific schema
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (schema_name,))
            tables = [{'schema': schema_name, 'table': row[0], 'full_name': f"{schema_name}.{row[0]}"} for row in cursor.fetchall()]
        else:
            # Get all tables with schema info
            cursor.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast_temp_1', 'pg_temp_1')
                AND table_schema NOT LIKE 'pg_%'
                AND table_schema NOT LIKE 'gp_%'
                ORDER BY table_schema, table_name
            """)
            tables = [{'schema': row[0], 'table': row[1], 'full_name': f"{row[0]}.{row[1]}"} for row in cursor.fetchall()]
            
    elif db_type.lower() == 'vertica':
        if schema_name:
            cursor.execute("""
                SELECT table_name 
                FROM v_catalog.tables 
                WHERE schema_name = %s
                ORDER BY table_name
            """, (schema_name,))
            tables = [{'schema': schema_name, 'table': row[0], 'full_name': f"{schema_name}.{row[0]}"} for row in cursor.fetchall()]
        else:
            cursor.execute("""
                SELECT schema_name, table_name 
                FROM v_catalog.tables 
                WHERE schema_name NOT IN ('v_catalog', 'v_monitor', 'v_internal')
                ORDER BY schema_name, table_name
            """)
            tables = [{'schema': row[0], 'table': row[1], 'full_name': f"{row[0]}.{row[1]}"} for row in cursor.fetchall()]
    
    cursor.close()
    logging.info(f"Discovered {len(tables)} tables in schema '{schema_name or 'all'}': {[t['full_name'] for t in tables[:10]]}{'...' if len(tables) > 10 else ''}")
    return tables

def discover_tables(db_conn, db_type):
    """Legacy function for backward compatibility - discovers all tables with full names"""
    tables_info = discover_tables_by_schema(db_conn, db_type)
    return [table['full_name'] for table in tables_info]

def get_table_schema(db_conn, db_type, table_name):
    """
    Get the schema (column information) for a specific table
    
    Args:
        db_conn: Database connection
        db_type: Database type ('postgresql', 'greenplum', 'vertica')
        table_name: Full table name (schema.table or just table)
        
    Returns:
        dict: Polars schema mapping column names to types
    """
    cursor = db_conn.cursor()
    
    try:
        # Parse schema and table name
        if '.' in table_name:
            schema_name, table_only = table_name.split('.', 1)
        else:
            schema_name = 'public'
            table_only = table_name
        
        logging.info(f"Getting schema for {table_name} (parsed as schema='{schema_name}', table='{table_only}')")
        
        # First, verify table exists
        table_exists = _verify_table_exists(cursor, db_type, schema_name, table_only, table_name)
        if not table_exists:
            return None
        
        # Try primary schema query (case-insensitive)
        columns_metadata = _get_schema_primary(cursor, db_type, schema_name, table_only, table_name)
        
        # If primary failed, try fallback query
        if not columns_metadata and db_type.lower() in ['postgresql', 'greenplum']:
            logging.warning(f"Primary schema query failed for {table_name}, trying fallback method")
            columns_metadata = _get_schema_fallback(cursor, schema_name, table_only, table_name)
        
        if not columns_metadata:
            logging.error(f"No schema information found for table {table_name} using any method")
            return None
        
        # Create Polars schema from database metadata
        polars_schema = create_polars_schema_from_database_metadata(columns_metadata, db_type)
        
        logging.info(f"Retrieved schema for {table_name}: {len(columns_metadata)} columns")
        return polars_schema
        
    except Exception as e:
        logging.error(f"Failed to get schema for table {table_name}: {str(e)}")
        logging.error(f"Exception details: {type(e).__name__}: {str(e)}")
        return None
    finally:
        cursor.close()

def _verify_table_exists(cursor, db_type, schema_name, table_only, full_table_name):
    """Verify that the table exists before attempting schema detection"""
    try:
        logging.info(f"Verifying table existence: {full_table_name}")
        
        if db_type.lower() in ['postgresql', 'greenplum']:
            # Check if table exists (case-insensitive)
            cursor.execute("""
                SELECT 1 FROM information_schema.tables 
                WHERE LOWER(table_schema) = LOWER(%s) AND LOWER(table_name) = LOWER(%s)
                LIMIT 1
            """, (schema_name, table_only))
        elif db_type.lower() == 'vertica':
            cursor.execute("""
                SELECT 1 FROM v_catalog.tables 
                WHERE LOWER(schema_name) = LOWER(%s) AND LOWER(table_name) = LOWER(%s)
                LIMIT 1
            """, (schema_name, table_only))
        
        exists = cursor.fetchone() is not None
        if not exists:
            logging.error(f"Table {full_table_name} does not exist in database")
        else:
            logging.debug(f"Table {full_table_name} exists")
        return exists
        
    except Exception as e:
        logging.error(f"Error verifying table existence for {full_table_name}: {str(e)}")
        return False

def _get_schema_primary(cursor, db_type, schema_name, table_only, full_table_name):
    """Primary schema query using information_schema (case-insensitive)"""
    try:
        if db_type.lower() in ['postgresql', 'greenplum']:
            query = """
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM information_schema.columns 
                WHERE LOWER(table_schema) = LOWER(%s) AND LOWER(table_name) = LOWER(%s)
                ORDER BY ordinal_position
            """
            logging.debug(f"Executing primary schema query for {full_table_name}")
            cursor.execute(query, (schema_name, table_only))
            
        elif db_type.lower() == 'vertica':
            query = """
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM v_catalog.columns 
                WHERE LOWER(schema_name) = LOWER(%s) AND LOWER(table_name) = LOWER(%s)
                ORDER BY ordinal_position
            """
            logging.debug(f"Executing primary schema query for {full_table_name}")
            cursor.execute(query, (schema_name, table_only))
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        columns_metadata = cursor.fetchall()
        logging.debug(f"Primary schema query returned {len(columns_metadata)} columns for {full_table_name}")
        return columns_metadata
        
    except Exception as e:
        logging.error(f"Primary schema query failed for {full_table_name}: {str(e)}")
        return []

def _get_schema_fallback(cursor, schema_name, table_only, full_table_name):
    """Fallback schema query using pg_class and pg_attribute"""
    try:
        query = """
            SELECT a.attname as column_name,
                   pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
                   CASE WHEN a.atttypmod > 4 THEN a.atttypmod - 4 ELSE NULL END as character_maximum_length,
                   CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END as is_nullable
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE LOWER(n.nspname) = LOWER(%s) 
            AND LOWER(c.relname) = LOWER(%s)
            AND a.attnum > 0 
            AND NOT a.attisdropped
            ORDER BY a.attnum
        """
        logging.debug(f"Executing fallback schema query for {full_table_name}")
        cursor.execute(query, (schema_name, table_only))
        
        columns_metadata = cursor.fetchall()
        logging.debug(f"Fallback schema query returned {len(columns_metadata)} columns for {full_table_name}")
        return columns_metadata
        
    except Exception as e:
        logging.error(f"Fallback schema query failed for {full_table_name}: {str(e)}")
        return []

def verify_table_integrity_duckdb(table_dir, exported_files, total_exported_rows, expected_rows, table_name):
    """Verify data integrity for DuckDB exports without loading into memory"""
    try:
        # Check for chunk gaps using file naming
        chunk_numbers = []
        for filename in exported_files:
            if filename.startswith('part_') and filename.endswith('.parquet'):
                try:
                    chunk_num_str = filename[5:9]  # Extract XXXX from part_XXXX.parquet
                    chunk_numbers.append(int(chunk_num_str))
                except ValueError:
                    logging.warning(f"Could not parse chunk number from {filename}")

        if chunk_numbers:
            chunk_numbers.sort()
            expected_chunks = list(range(1, len(chunk_numbers) + 1))

            if chunk_numbers != expected_chunks:
                missing_chunks = set(expected_chunks) - set(chunk_numbers)
                if missing_chunks:
                    logging.error(f"Missing chunks detected for {table_name}: {sorted(missing_chunks)}")
                    raise ValueError(f"Missing chunks: {sorted(missing_chunks)}")

        # Quick integrity check using DuckDB without loading full files
        import duckdb
        conn = duckdb.connect(':memory:')

        try:
            total_verified_rows = 0
            for filename in exported_files:
                chunk_file = table_dir / filename
                if not chunk_file.exists():
                    raise ValueError(f"Chunk file missing: {filename}")

                # Quick row count check using DuckDB's parquet reader
                result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{chunk_file}')").fetchone()
                chunk_rows = result[0] if result else 0
                total_verified_rows += chunk_rows
                logging.debug(f"Verified chunk {filename}: {chunk_rows:,} rows")

            # Validate total row count
            if abs(total_verified_rows - expected_rows) > max(100, expected_rows * 0.001):  # Allow 0.1% discrepancy
                error_msg = f"Row count validation failed: expected {expected_rows:,}, got {total_verified_rows:,}"
                logging.error(error_msg)
                return False, error_msg

        finally:
            conn.close()

        logging.info(f"Data integrity verification passed for {table_name}: {len(exported_files)} chunks, {total_verified_rows:,} total rows")
        return True, f"Integrity verified - {total_verified_rows:,} rows in {len(exported_files)} chunks"

    except Exception as e:
        error_msg = f"Integrity verification failed for {table_name}: {str(e)}"
        logging.error(error_msg)
        return False, error_msg

def check_table_export_status(cursor, job_id, table_name):
    """Check if table is already being exported or completed to prevent duplicates"""
    cursor.execute("""
        SELECT status, start_time, end_time 
        FROM table_exports 
        WHERE job_id = ? AND table_name = ?
        ORDER BY start_time DESC 
        LIMIT 1
    """, (job_id, table_name))
    
    result = cursor.fetchone()
    if result:
        status = result[0]
        if status == 'processing':
            logging.warning(f"Table {table_name} is already being processed in job {job_id}")
            return 'processing'
        elif status == 'completed':
            logging.info(f"Table {table_name} already completed in job {job_id}")
            return 'completed'
        elif status == 'failed':
            logging.info(f"Table {table_name} previously failed in job {job_id}, will retry")
            return 'retry'
    
    return 'new'

def check_global_existing_export(output_path, table_name, expected_row_count):
    """
    Check if a complete export already exists anywhere in the output directory
    Scans all possible locations where this table might have been exported
    """
    try:
        base_path = Path(output_path)
        safe_table_name = table_name.replace('.', '_').replace('/', '_').replace('\\', '_')
        
        # Possible locations to check based on different export strategies
        possible_locations = [
            # clean_with_archive strategy
            base_path / safe_table_name,
            # schema_first strategy 
            base_path / table_name.split('.')[0] / table_name.split('.')[1] if '.' in table_name else None,
            # direct strategy (same as clean)
            base_path / safe_table_name,
        ]
        
        # Also check for versioned exports (table_v2, table_v3, etc.)
        for version in range(2, 10):  # Check up to v9
            possible_locations.append(base_path / f"{safe_table_name}_v{version}")
        
        # Filter out None values and check each location
        for location in filter(None, possible_locations):
            if location and location.exists():
                metadata_file = location / "_export_metadata.json"
                if metadata_file.exists():
                    try:
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                        
                        if (metadata.get('total_rows') == expected_row_count and 
                            metadata.get('status') == 'complete' and
                            metadata.get('table_name') == table_name):
                            logging.info(f"Found existing complete export for {table_name} at {location}, skipping duplicate export")
                            return True, str(location)
                    except (json.JSONDecodeError, KeyError):
                        continue
        
        return False, None
        
    except Exception as e:
        logging.warning(f"Error checking global existing exports for {table_name}: {str(e)}")
        return False, None

def check_existing_export(table_dir, expected_row_count, table_name):
    """Check if a complete export already exists for this table"""
    try:
        # Look for metadata file that indicates complete export
        metadata_file = table_dir / "_export_metadata.json"
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            
            if metadata.get('total_rows') == expected_row_count and metadata.get('status') == 'complete':
                logging.info(f"Valid existing export found for {table_name} with {expected_row_count} rows, skipping re-export")
                return True
        
        # If no metadata or incomplete, check if we have parquet files to clean up
        parquet_files = list(table_dir.glob("*.parquet"))
        if parquet_files:
            logging.warning(f"Found incomplete export for {table_name}, cleaning up {len(parquet_files)} files")
            for file in parquet_files:
                file.unlink()
            if metadata_file.exists():
                metadata_file.unlink()
        
        return False
        
    except Exception as e:
        logging.warning(f"Error checking existing export for {table_name}: {str(e)}")
        return False

def write_export_metadata(table_dir, table_name, total_rows, chunk_info=None, job_id=None):
    """Write metadata about the export for validation and resumption purposes"""
    metadata = {
        'table_name': table_name,
        'total_rows': total_rows,
        'export_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'complete',
        'job_id': job_id  # Track which job created this export
    }
    
    if chunk_info:
        metadata['partitioned'] = True
        metadata['chunk_count'] = chunk_info['chunk_count']
        metadata['chunk_size'] = chunk_info['chunk_size']
        metadata['files'] = chunk_info['files']
    else:
        metadata['partitioned'] = False
        metadata['files'] = ['data.parquet']
    
    metadata_file = table_dir / "_export_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)

def get_export_paths(output_path, job_id, table_name, config=None):
    """
    Get temporary and final export paths based on organization strategy
    
    Returns:
        tuple: (temp_path, final_path, archive_path)
    """
    safe_table_name = table_name.replace('.', '_').replace('/', '_').replace('\\', '_')
    base_path = Path(output_path)
    
    # Get configurable temp directory path
    temp_base_path = os.environ.get('ADU_TEMP_PATH')
    if temp_base_path:
        temp_base = Path(temp_base_path)
    else:
        temp_base = base_path  # Default to output path for backward compatibility
    
    # Get strategy from config or use default
    strategy = 'clean_with_archive'  # Default strategy
    if config and 'export_organization' in config:
        strategy = config['export_organization'].get('strategy', 'clean_with_archive')
    
    if strategy == 'clean_with_archive':
        # Use configurable temporary directory during export, clean final structure
        temp_path = temp_base / '.temp' / job_id / safe_table_name
        final_path = base_path / safe_table_name
        archive_path = base_path / '.archive' / 'jobs'
    elif strategy == 'direct':
        # Export directly to final location (may overwrite) - temp same as final
        temp_path = base_path / safe_table_name
        final_path = base_path / safe_table_name
        archive_path = base_path / '.jobs'
    elif strategy == 'schema_first':
        # Organize by schema first with configurable temp directory
        schema_name = table_name.split('.')[0] if '.' in table_name else 'public'
        table_only = table_name.split('.')[1] if '.' in table_name else table_name
        temp_path = temp_base / '.temp' / job_id / schema_name / table_only
        final_path = base_path / schema_name / table_only
        archive_path = base_path / '.archive' / 'jobs'
    else:
        # Legacy format as fallback with configurable temp directory
        temp_path = temp_base / job_id / safe_table_name
        final_path = base_path / job_id / safe_table_name
        archive_path = base_path / '.archive'
    
    return temp_path, final_path, archive_path

def resolve_naming_conflicts(final_path, conflict_strategy='version'):
    """
    Resolve naming conflicts for export directories
    
    Args:
        final_path: Desired final path
        conflict_strategy: 'version', 'timestamp', 'overwrite'
        
    Returns:
        Path: Actual path to use (may be versioned)
    """
    if not final_path.exists():
        return final_path
    
    if conflict_strategy == 'overwrite':
        # Remove existing directory
        import shutil
        shutil.rmtree(final_path)
        return final_path
    
    elif conflict_strategy == 'timestamp':
        # Add timestamp to name
        timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
        new_name = f"{final_path.name}_{timestamp}"
        return final_path.parent / new_name
    
    else:  # version strategy (default)
        # Find next available version number
        base_name = final_path.name
        parent = final_path.parent
        version = 2
        
        while True:
            versioned_name = f"{base_name}_v{version}"
            versioned_path = parent / versioned_name
            if not versioned_path.exists():
                return versioned_path
            version += 1

def organize_completed_export(job_id, output_path, config=None):
    """
    Reorganize exports after job completion to clean structure
    
    This function:
    1. Moves tables from temporary job directories to clean final structure
    2. Resolves naming conflicts
    3. Archives job metadata
    4. Cleans up temporary directories
    """
    try:
        base_path = Path(output_path)
        temp_job_path = base_path / '.temp' / job_id
        
        if not temp_job_path.exists():
            logging.info(f"No temporary exports found for job {job_id}, skipping organization")
            return True
        
        # Get organization strategy
        strategy = 'clean_with_archive'
        conflict_strategy = 'version'
        preserve_history = True
        
        if config and 'export_organization' in config:
            org_config = config['export_organization']
            strategy = org_config.get('strategy', 'clean_with_archive')
            conflict_strategy = org_config.get('conflict_resolution', 'version')
            preserve_history = org_config.get('preserve_job_history', True)
        
        moved_tables = []
        failed_tables = []
        
        # Process each table directory in the temp job path
        for table_temp_dir in temp_job_path.iterdir():
            if table_temp_dir.is_dir():
                table_name = table_temp_dir.name
                
                try:
                    # Get final destination path
                    _, final_path, _ = get_export_paths(output_path, job_id, table_name.replace('_', '.'), config)
                    
                    # Resolve conflicts if final path exists
                    actual_final_path = resolve_naming_conflicts(final_path, conflict_strategy)
                    
                    # Ensure parent directory exists
                    actual_final_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Move the entire table directory
                    import shutil
                    shutil.move(str(table_temp_dir), str(actual_final_path))
                    
                    # Update metadata with final location
                    metadata_file = actual_final_path / "_export_metadata.json"
                    if metadata_file.exists():
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                        metadata['final_path'] = str(actual_final_path)
                        metadata['organized_timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
                        with open(metadata_file, 'w') as f:
                            json.dump(metadata, f, indent=2)
                    
                    moved_tables.append({
                        'table': table_name,
                        'final_path': str(actual_final_path),
                        'was_renamed': str(actual_final_path) != str(final_path)
                    })
                    
                    logging.info(f"Moved {table_name} to {actual_final_path}")
                    
                except Exception as e:
                    error_msg = f"Failed to move table {table_name}: {str(e)}"
                    logging.error(error_msg)
                    failed_tables.append({'table': table_name, 'error': error_msg})
        
        # Archive job metadata if requested
        if preserve_history and moved_tables:
            archive_job_metadata(job_id, {
                'completion_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'strategy': strategy,
                'conflict_resolution': conflict_strategy,
                'moved_tables': moved_tables,
                'failed_tables': failed_tables,
                'total_tables': len(moved_tables) + len(failed_tables)
            }, output_path)
        
        # Clean up temporary directory
        try:
            import shutil
            if temp_job_path.exists():
                shutil.rmtree(temp_job_path)
            logging.info(f"Cleaned up temporary directory for job {job_id}")
        except Exception as e:
            logging.warning(f"Failed to clean up temp directory for job {job_id}: {str(e)}")
        
        # Additional cleanup for any orphaned temp files from connection failures
        cleanup_orphaned_temp_files(base_path / '.temp', job_id)
        
        # Update database with new file paths
        update_database_paths_after_organization(job_id, moved_tables)
        
        if failed_tables:
            logging.warning(f"Job {job_id} organization completed with {len(failed_tables)} failures")
            return False
        else:
            logging.info(f"Job {job_id} organization completed successfully, moved {len(moved_tables)} tables")
            return True
            
    except Exception as e:
        logging.error(f"Failed to organize exports for job {job_id}: {str(e)}")
        return False

def verify_table_integrity(table_dir, exported_files, total_exported_rows, expected_rows, table_name):
    """Verify data integrity across all chunks"""
    try:
        # Check for chunk gaps
        chunk_numbers = []
        for filename in exported_files:
            if filename.startswith('part_') and filename.endswith('.parquet'):
                chunk_num_str = filename[5:9]  # Extract XXXX from part_XXXX.parquet
                try:
                    chunk_numbers.append(int(chunk_num_str))
                except ValueError:
                    logging.warning(f"Could not parse chunk number from {filename}")
        
        chunk_numbers.sort()
        
        # Check for gaps in chunk sequence
        if chunk_numbers:
            expected_chunks = list(range(1, len(chunk_numbers) + 1))
            if chunk_numbers != expected_chunks:
                missing_chunks = set(expected_chunks) - set(chunk_numbers)
                if missing_chunks:
                    logging.error(f"Missing chunks detected for {table_name}: {sorted(missing_chunks)}")
                    raise ValueError(f"Missing chunks: {sorted(missing_chunks)}")
                
                extra_chunks = set(chunk_numbers) - set(expected_chunks)
                if extra_chunks:
                    logging.warning(f"Extra chunks detected for {table_name}: {sorted(extra_chunks)}")
        
        # Verify all chunk files exist and are readable
        for filename in exported_files:
            chunk_file = table_dir / filename
            if not chunk_file.exists():
                raise ValueError(f"Chunk file missing: {filename}")
            
            # Quick read test - disabled for DuckDB-only mode  
            # try:
            #     chunk_df = pl.scan_parquet(chunk_file).select(pl.len()).collect()
            #     chunk_rows = chunk_df[0, 0]
            #     logging.debug(f"Verified chunk {filename}: {chunk_rows} rows")
            # except Exception as e:
            #     raise ValueError(f"Chunk file corrupted: {filename} - {str(e)}")
            logging.debug(f"Chunk file exists: {filename}")
        
        logging.info(f"Data integrity verification passed for {table_name}: {len(exported_files)} chunks, {total_exported_rows} total rows")
        
    except Exception as e:
        logging.error(f"Data integrity verification failed for {table_name}: {str(e)}")
        raise

class ChunkProgressManager:
    """Manages batched chunk progress updates to reduce I/O overhead"""
    
    def __init__(self, progress_file, batch_size=10, time_threshold=30):
        self.progress_file = progress_file
        self.batch_size = batch_size  # Write every N chunks
        self.time_threshold = time_threshold  # Write every N seconds
        self.last_write_time = time.time()
        self.pending_updates = {}
        self.completed_chunks = set()
        self.chunk_metadata = {}
        
    def add_completed_chunk(self, chunk_num, chunk_info):
        """Add a completed chunk to pending updates"""
        self.completed_chunks.add(chunk_num)
        self.chunk_metadata[str(chunk_num)] = chunk_info
        self.pending_updates[chunk_num] = chunk_info
        
        # Check if we should write based on batch size or time
        should_write = (
            len(self.pending_updates) >= self.batch_size or
            (time.time() - self.last_write_time) >= self.time_threshold
        )
        
        if should_write:
            self.flush()
    
    def flush(self, force=False):
        """Write all pending updates to disk"""
        if not self.pending_updates and not force:
            return
            
        try:
            progress_data = {
                'completed_chunks': sorted(list(self.completed_chunks)),
                'chunk_metadata': self.chunk_metadata,
                'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_completed': len(self.completed_chunks),
                'batch_info': {
                    'batch_size': self.batch_size,
                    'time_threshold': self.time_threshold,
                    'pending_count': len(self.pending_updates)
                }
            }
            
            # Write atomically using temp file
            temp_file = self.progress_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
            temp_file.rename(self.progress_file)
            
            # Clear pending updates
            self.pending_updates.clear()
            self.last_write_time = time.time()
            
            logging.debug(f"Saved chunk progress: {len(self.completed_chunks)} total chunks")
            
        except Exception as e:
            logging.warning(f"Failed to save chunk progress: {str(e)}")

def save_chunk_progress(progress_file, completed_chunks, chunk_metadata):
    """Legacy function for backward compatibility - use ChunkProgressManager instead"""
    try:
        progress_data = {
            'completed_chunks': sorted(list(completed_chunks)),
            'chunk_metadata': chunk_metadata,
            'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_completed': len(completed_chunks)
        }
        
        # Write atomically using temp file
        temp_file = progress_file.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(progress_data, f, indent=2)
        temp_file.rename(progress_file)
        
    except Exception as e:
        logging.warning(f"Failed to save chunk progress: {str(e)}")

def cleanup_orphaned_temp_files(temp_base_path, current_job_id=None):
    """
    Clean up orphaned temp files from failed jobs or connection issues.
    Keeps files for the current job if specified.
    """
    try:
        if not temp_base_path.exists():
            return
        
        import time
        current_time = time.time()
        cleanup_age_hours = 24  # Clean up temp files older than 24 hours
        
        for job_dir in temp_base_path.iterdir():
            if not job_dir.is_dir():
                continue
                
            job_id = job_dir.name
            
            # Skip current job
            if current_job_id and job_id == current_job_id:
                continue
            
            try:
                # Check if directory is old enough to clean up
                dir_age_hours = (current_time - job_dir.stat().st_mtime) / 3600
                
                if dir_age_hours > cleanup_age_hours:
                    shutil.rmtree(job_dir)
                    logging.info(f"Cleaned up orphaned temp directory for job {job_id} (age: {dir_age_hours:.1f}h)")
                elif dir_age_hours > 1:  # Log older directories that aren't cleaned yet
                    logging.info(f"Found temp directory for job {job_id} (age: {dir_age_hours:.1f}h)")
                    
            except Exception as e:
                logging.warning(f"Failed to process temp directory for job {job_id}: {str(e)}")
                
    except Exception as e:
        logging.warning(f"Failed to cleanup orphaned temp files: {str(e)}")

def archive_job_metadata(job_id, organization_info, output_path):
    """Archive job metadata for history and auditing"""
    try:
        archive_dir = Path(output_path) / '.archive' / 'jobs'
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
        archive_file = archive_dir / f"job_{job_id}_{timestamp}.json"
        
        with open(archive_file, 'w') as f:
            json.dump(organization_info, f, indent=2)
        
        logging.info(f"Archived job metadata to {archive_file}")
        
    except Exception as e:
        logging.warning(f"Failed to archive job metadata for {job_id}: {str(e)}")

def update_database_paths_after_organization(job_id, moved_tables):
    """Update database with new file paths after reorganization"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for table_info in moved_tables:
            table_name = table_info['table'].replace('_', '.')  # Convert back to original format
            new_path = table_info['final_path']
            
            cursor.execute(
                "UPDATE table_exports SET file_path = ? WHERE job_id = ? AND table_name = ?",
                (new_path, job_id, table_name)
            )
        
        conn.commit()
        conn.close()
        
        logging.info(f"Updated database paths for {len(moved_tables)} tables in job {job_id}")
        
    except Exception as e:
        logging.error(f"Failed to update database paths for job {job_id}: {str(e)}")

def export_small_table_single_file(db_config, table_name, table_dir, source_row_count, db_type='postgresql'):
    """Export small table as a single Parquet file"""
    try:
        # Create proper data source connection for schema detection
        data_conn = create_data_source_connection(db_config, db_type)
        
        # Get table schema to avoid inference issues
        polars_schema = get_table_schema(data_conn, db_type, table_name)
        
        # Read table data using Polars with explicit schema
        query = f"SELECT * FROM {table_name}"
        if not polars_schema:
            error_msg = (f"Failed to retrieve database schema for {table_name}. "
                        f"This could be due to: (1) table does not exist, (2) insufficient permissions, "
                        f"(3) case sensitivity issues in table name, or (4) database connection problems. "
                        f"Check logs above for specific schema detection errors.")
            logging.error(error_msg)
            data_conn.close()
            return False, error_msg
            
        try:
            # Always use database schema - never inference
            df = pl.read_database(query, data_conn, schema_overrides=polars_schema)
            logging.info(f"Using explicit schema for {table_name} with {len(polars_schema)} columns")
        except Exception as schema_error:
            # Schema enforcement failed - convert all columns to String and retry
            error_msg = str(schema_error)
            logging.error(f"Schema enforcement failed for {table_name}: {error_msg}")
            logging.error("Data type mismatch detected - actual data doesn't match database schema")
            
            # Create all-String schema as fallback
            string_schema = {col_name: pl.String for col_name in polars_schema.keys()}
            
            try:
                df = pl.read_database(query, db_conn, schema_overrides=string_schema)
                logging.warning(f"Successfully read {table_name} with all columns converted to String type")
            except Exception as retry_error:
                error_msg = f"Failed to read {table_name} even with String fallback schema: {str(retry_error)}"
                logging.error(error_msg)
                return False, error_msg
        
        # Validate that we read the expected number of rows
        df_row_count = len(df)
        if df_row_count != source_row_count:
            error_msg = f"Row count mismatch: source table has {source_row_count} rows, but DataFrame has {df_row_count} rows"
            logging.error(error_msg)
            return False, error_msg
        
        # Validate data before export
        validation_passed, validation_message = validate_data(df, table_name)
        if not validation_passed:
            logging.warning(f"Data validation failed for {table_name}: {validation_message}")
        
        # Handle problematic data types while preserving data integrity
        df = handle_mixed_types(df)
        
        # Write to single Parquet file
        parquet_file = table_dir / "data.parquet"
        temp_file = table_dir / "data.tmp"
        
        try:
            df.write_parquet(temp_file)
            temp_file.rename(parquet_file)
        except Exception as write_error:
            if temp_file.exists():
                temp_file.unlink()
            raise write_error
        
        # Post-export validation
        verification_df = pl.read_parquet(parquet_file)
        parquet_row_count = len(verification_df)
        
        if parquet_row_count != source_row_count:
            error_msg = f"Post-export validation failed: source has {source_row_count} rows, Parquet file has {parquet_row_count} rows"
            logging.error(error_msg)
            return False, error_msg
        
        # Write metadata
        write_export_metadata(table_dir, table_name, source_row_count, job_id=None)
        
        result_message = f"Exported and verified {source_row_count} rows to single file"
        if not validation_passed:
            result_message += f" (validation warning: {validation_message})"
        
        logging.info(f"Exported {table_name} to {table_dir} ({result_message})")
        return True, source_row_count
        
    except Exception as e:
        logging.error(f"Failed to export small table {table_name}: {str(e)}")
        return False, str(e)
    finally:
        # Ensure connection is closed
        try:
            data_conn.close()
        except:
            pass

def export_small_table_single_file_duckdb(db_config, table_name, table_dir, source_row_count, db_type='postgresql'):
    """Export small table as a single Parquet file using DuckDB streaming"""
    try:
        # Get table schema for validation
        polars_schema = None
        try:
            # Create data source connection for schema detection
            data_conn = create_data_source_connection(db_config, db_type)
            polars_schema = get_table_schema(data_conn, db_type, table_name)
            data_conn.close()
        except Exception as e:
            logging.warning(f"Could not get schema for {table_name}: {str(e)}")
        
        # Create output file path
        parquet_file = table_dir / "data.parquet"
        
        # Use DuckDB streaming export
        success, message, rows_exported = export_small_table_duckdb(
            db_config, table_name, parquet_file, polars_schema
        )
        
        if not success:
            return False, message
        
        # Validate the export
        validation_passed, validation_message = validate_duckdb_export(
            parquet_file, table_name, source_row_count, polars_schema
        )
        
        if not validation_passed:
            logging.error(f"DuckDB export validation failed for {table_name}: {validation_message}")
            return False, validation_message
        
        # Write metadata
        write_export_metadata(table_dir, table_name, rows_exported, job_id=None)
        
        result_message = f"DuckDB exported and verified {rows_exported} rows to single file"
        if "warning" in validation_message.lower():
            result_message += f" (validation warning)"
            
        logging.info(f"DuckDB export completed: {table_name} to {table_dir} ({result_message})")
        return True, rows_exported
        
    except Exception as e:
        logging.error(f"Failed to export small table {table_name} with DuckDB: {str(e)}")
        return False, str(e)

def export_large_table_partitioned_duckdb(db_config, table_name, table_dir, source_row_count, chunk_size, db_type='postgresql'):
    """Export large table as multiple partitioned Parquet files using DuckDB streaming"""
    try:
        # Get table schema for validation
        polars_schema = None
        try:
            # Create data source connection for schema detection
            data_conn = create_data_source_connection(db_config, db_type)
            polars_schema = get_table_schema(data_conn, db_type, table_name)
            data_conn.close()
        except Exception as e:
            logging.warning(f"Could not get schema for {table_name}: {str(e)}")
        
        chunk_count = (source_row_count + chunk_size - 1) // chunk_size
        exported_files = []
        total_exported_rows = 0
        
        logging.info(f"DuckDB exporting {table_name} in {chunk_count} chunks of up to {chunk_size} rows each")
        
        for chunk_num in range(chunk_count):
            offset = chunk_num * chunk_size
            
            # Create chunk file path
            chunk_file = table_dir / f"part_{chunk_num:04d}.parquet"
            
            logging.info(f"Processing DuckDB chunk {chunk_num + 1}/{chunk_count} (offset {offset})")
            
            # Use DuckDB streaming export for chunk
            success, message, rows_exported = export_table_chunk_duckdb(
                db_config, table_name, chunk_file, offset, chunk_size, polars_schema
            )
            
            if not success:
                logging.error(f"DuckDB chunk {chunk_num + 1} failed: {message}")
                return False, message
            
            if rows_exported == 0:
                logging.info(f"DuckDB chunk {chunk_num + 1} exported 0 rows, stopping")
                break
                
            # Validate chunk export
            validation_passed, validation_message = validate_duckdb_export(
                chunk_file, f"{table_name}_chunk_{chunk_num + 1}", rows_exported, polars_schema
            )
            
            if not validation_passed:
                logging.warning(f"DuckDB chunk {chunk_num + 1} validation warning: {validation_message}")
            
            exported_files.append(chunk_file.name)
            total_exported_rows += rows_exported
            
            logging.info(f"DuckDB chunk {chunk_num + 1} completed: {rows_exported} rows")
        
        # Final validation - check total rows
        if total_exported_rows != source_row_count:
            error_msg = f"DuckDB export row mismatch: expected {source_row_count}, got {total_exported_rows}"
            logging.error(error_msg)
            return False, error_msg
        
        # Write metadata
        chunk_info = {
            'files': exported_files,
            'chunk_count': len(exported_files),
            'partitioned': True
        }
        write_export_metadata(table_dir, table_name, total_exported_rows, chunk_info, job_id=None)
        
        logging.info(f"DuckDB export completed: {table_name} exported {total_exported_rows} rows in {len(exported_files)} chunks")
        return True, total_exported_rows
        
    except Exception as e:
        logging.error(f"Failed to export large table {table_name} with DuckDB: {str(e)}")
        return False, str(e)

def export_large_table_partitioned(db_config, table_name, table_dir, source_row_count, chunk_size, db_type='postgresql'):
    """Export large table as multiple partitioned Parquet files"""
    try:
        # Create data source connection for schema detection
        data_conn = create_data_source_connection(db_config, db_type)
        
        # Get table schema to avoid inference issues across chunks
        polars_schema = get_table_schema(data_conn, db_type, table_name)
        if polars_schema:
            logging.info(f"Using explicit schema for {table_name} chunks with {len(polars_schema)} columns")
        else:
            logging.warning(f"No schema available for {table_name}, chunks may have inference issues")
        
        chunk_count = (source_row_count + chunk_size - 1) // chunk_size  # Ceiling division
        exported_files = []
        total_exported_rows = 0
        
        # Initialize progress manager to reduce logging spam
        progress_manager = ProgressManager(None, table_name, source_row_count, chunk_count)
        progress_manager.set_export_method("Polars (legacy)")
        
        for chunk_num in range(chunk_count):
            offset = chunk_num * chunk_size
            limit = min(chunk_size, source_row_count - offset)
            
            # Reduced logging - progress manager handles this intelligently
            
            # Query with LIMIT and OFFSET for pagination with ORDER BY for deterministic results
            # ORDER BY is critical to ensure consistent, non-overlapping chunks
            query = f"SELECT * FROM {table_name} ORDER BY 1 LIMIT {limit} OFFSET {offset}"
            
            try:
                if not polars_schema:
                    error_msg = (f"Failed to retrieve database schema for {table_name}. "
                                f"This could be due to: (1) table does not exist, (2) insufficient permissions, "
                                f"(3) case sensitivity issues in table name, or (4) database connection problems. "
                                f"Check logs above for specific schema detection errors.")
                    logging.error(error_msg)
                    raise ValueError(error_msg)
                
                try:
                    # Always use database schema - never inference
                    df_chunk = pl.read_database(query, data_conn, schema_overrides=polars_schema)
                except Exception as schema_error:
                    # Schema enforcement failed - likely due to data/schema mismatch
                    error_msg = str(schema_error)
                    logging.error(f"Schema enforcement failed for {table_name} chunk {chunk_num + 1}: {error_msg}")
                    
                    # Extract column name from error if possible
                    problematic_column = None
                    if "could not append value" in error_msg:
                        # Try to extract column info from error message
                        logging.error(f"Data type mismatch detected - actual data doesn't match database schema")
                        logging.error(f"This usually means the database column contains mixed data types")
                    
                    # Create corrected schema with problematic columns as String
                    corrected_schema = polars_schema.copy()
                    
                    # For now, convert all non-string columns to String as a fallback
                    # This ensures export continues while preserving all data
                    string_converted = []
                    for col_name, col_type in corrected_schema.items():
                        if col_type != pl.String:
                            corrected_schema[col_name] = pl.String
                            string_converted.append(f"{col_name} ({col_type} -> String)")
                    
                    logging.warning(f"Converting {len(string_converted)} columns to String type to handle mixed data:")
                    for conversion in string_converted[:5]:  # Log first 5
                        logging.warning(f"  - {conversion}")
                    if len(string_converted) > 5:
                        logging.warning(f"  - ... and {len(string_converted) - 5} more columns")
                    
                    # Retry with corrected schema
                    try:
                        df_chunk = pl.read_database(query, data_conn, schema_overrides=corrected_schema)
                        logging.info(f"Successfully read chunk {chunk_num + 1} with corrected String schema")
                    except Exception as retry_error:
                        error_msg = f"Failed to read chunk {chunk_num + 1} even with String fallback schema: {str(retry_error)}"
                        logging.error(error_msg)
                        raise Exception(error_msg)
                
                chunk_row_count = len(df_chunk)
                
                if chunk_row_count != limit:
                    logging.warning(f"Expected {limit} rows in chunk {chunk_num + 1}, got {chunk_row_count}")
                
                # Validate and process data types for this chunk
                validation_passed, validation_message = validate_data(df_chunk, f"{table_name}_chunk_{chunk_num + 1}")
                if not validation_passed:
                    logging.warning(f"Data validation failed for {table_name} chunk {chunk_num + 1}: {validation_message}")
                
                df_chunk = handle_mixed_types(df_chunk)
                
                # Write chunk to Parquet
                chunk_filename = f"part_{chunk_num + 1:04d}.parquet"
                chunk_file = table_dir / chunk_filename
                temp_file = table_dir / f"part_{chunk_num + 1:04d}.tmp"
                
                try:
                    df_chunk.write_parquet(temp_file)
                    temp_file.rename(chunk_file)
                except Exception as write_error:
                    if temp_file.exists():
                        temp_file.unlink()
                    raise write_error
                
                # Verify chunk
                verification_df = pl.read_parquet(chunk_file)
                verified_count = len(verification_df)
                
                if verified_count != chunk_row_count:
                    error_msg = f"Chunk {chunk_num + 1} verification failed: expected {chunk_row_count}, got {verified_count}"
                    logging.error(error_msg)
                    return False, error_msg
                
                exported_files.append(chunk_filename)
                total_exported_rows += verified_count
                
                # Update progress manager instead of per-chunk logging
                progress_manager.update_chunk_completed(chunk_num, verified_count)
                
            except Exception as chunk_error:
                logging.error(f"Failed to export chunk {chunk_num + 1}: {str(chunk_error)}")
                return False, f"Chunk {chunk_num + 1} failed: {str(chunk_error)}"
        
        # Final validation - check total rows
        if total_exported_rows != source_row_count:
            error_msg = f"Total exported rows mismatch: expected {source_row_count}, got {total_exported_rows}"
            logging.error(error_msg)
            return False, error_msg
        
        # Write metadata
        chunk_info = {
            'chunk_count': chunk_count,
            'chunk_size': chunk_size,
            'files': exported_files
        }
        write_export_metadata(table_dir, table_name, source_row_count, chunk_info, job_id=None)
        
        # Log final completion with performance summary
        progress_manager.log_completion()
        return True, total_exported_rows
        
    except Exception as e:
        logging.error(f"Failed to export large table {table_name}: {str(e)}")
        return False, str(e)
    finally:
        # Ensure connection is closed
        try:
            data_conn.close()
        except:
            pass

def check_connection_health(db_conn):
    """
    Check if database connection is healthy and can execute queries.
    Returns True if healthy, False otherwise.
    """
    try:
        cursor = db_conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        return True
    except Exception as e:
        logging.warning(f"Connection health check failed: {str(e)}")
        return False


def get_table_partition_strategy_with_recovery(db_conn_params, table_name, target_chunk_size=1000000):
    """
    Get partition strategy with connection recovery for segment failures.
    """
    # Try with existing connection first
    try:
        conn = get_database_connection(
            db_type=db_conn_params['db_type'],
            host=db_conn_params['host'],
            port=db_conn_params['port'],
            username=db_conn_params['username'],
            password=db_conn_params['password'],
            database=db_conn_params['database']
        )
        if conn:
            result = get_table_partition_strategy(conn, table_name, target_chunk_size)
            conn.close()
            return result
    except Exception as e:
        logging.warning(f"Connection recovery failed for {table_name}: {str(e)}")
    
    # Fallback strategy if connection recovery fails
    logging.warning(f"Using fallback strategy for {table_name} due to connection issues")
    return {
        'strategy': 'offset_partition',
        'chunk_size': target_chunk_size,
        'estimated_chunks': 1,
        'total_rows': target_chunk_size
    }

def get_table_partition_strategy(db_conn, table_name, target_chunk_size=1000000):
    """
    Analyze table structure to determine optimal partitioning strategy for large Greenplum tables.
    Uses table statistics and primary keys for efficient chunking.
    """
    # Check connection health before proceeding
    if not check_connection_health(db_conn):
        logging.warning(f"Connection unhealthy for table {table_name}, using minimal strategy")
        return {
            'strategy': 'offset_partition',
            'chunk_size': target_chunk_size,
            'estimated_chunks': 1,
            'total_rows': target_chunk_size
        }
    
    cursor = db_conn.cursor()
    
    try:
        # Get table statistics from Greenplum
        schema, table = table_name.split('.') if '.' in table_name else ('public', table_name)
        
        # Check if table has a suitable numeric column for range partitioning
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            AND data_type IN ('integer', 'bigint', 'serial', 'bigserial', 'numeric')
            ORDER BY ordinal_position
        """, (schema, table))
        
        numeric_columns = cursor.fetchall()
        
        # Check for primary key or unique constraints
        cursor.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = %s AND tc.table_name = %s
            AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
            ORDER BY kcu.ordinal_position
        """, (schema, table))
        
        key_columns = [row[0] for row in cursor.fetchall()]
        
        # Find the best partitioning column (prefer primary key, then any numeric)
        partition_column = None
        if key_columns and any(col in [nc[0] for nc in numeric_columns] for col in key_columns):
            # Use numeric primary key if available
            partition_column = next(col for col in key_columns if col in [nc[0] for nc in numeric_columns])
        elif numeric_columns:
            # Use first numeric column
            partition_column = numeric_columns[0][0]
        
        if partition_column:
            # Get min/max values for range partitioning
            cursor.execute(f"SELECT MIN({partition_column}), MAX({partition_column}) FROM {table_name}")
            min_val, max_val = cursor.fetchone()
            
            if min_val is not None and max_val is not None:
                # Get total row count for chunk calculation
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                total_rows = cursor.fetchone()[0]
                
                # Calculate optimal number of chunks
                num_chunks = max(1, (total_rows + target_chunk_size - 1) // target_chunk_size)
                
                # Calculate range step
                range_step = (max_val - min_val) / num_chunks if num_chunks > 1 else max_val - min_val + 1
                
                return {
                    'strategy': 'offset_partition',  # Force offset-based partitioning for reliability
                    'chunk_size': target_chunk_size,
                    'estimated_chunks': num_chunks,
                    'total_rows': total_rows
                }
        
        # Fallback to offset-based chunking for tables without suitable numeric columns
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0]
        num_chunks = max(1, (total_rows + target_chunk_size - 1) // target_chunk_size)
        
        return {
            'strategy': 'offset_partition',
            'chunk_size': target_chunk_size,
            'estimated_chunks': num_chunks,
            'total_rows': total_rows
        }
        
    except Exception as e:
        logging.warning(f"Could not analyze partition strategy for {table_name}: {str(e)}")
        
        # Handle transaction rollback for connection/segment errors
        try:
            db_conn.rollback()
            logging.info("Transaction rolled back after partition strategy error")
        except Exception as rollback_error:
            logging.warning(f"Could not rollback transaction: {str(rollback_error)}")
        
        # Create new cursor for fallback query after potential connection issues
        try:
            cursor.close()
        except:
            pass
        
        cursor = db_conn.cursor()
        
        # Simple fallback with error handling
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]
        except Exception as fallback_error:
            logging.error(f"Fallback COUNT query also failed for {table_name}: {str(fallback_error)}")
            # Return minimal strategy if even COUNT fails
            return {
                'strategy': 'offset_partition',
                'chunk_size': target_chunk_size,
                'estimated_chunks': 1,
                'total_rows': target_chunk_size  # Assume one chunk worth of data
            }
        
        num_chunks = max(1, (total_rows + target_chunk_size - 1) // target_chunk_size)
        
        return {
            'strategy': 'offset_partition',
            'chunk_size': target_chunk_size,
            'estimated_chunks': num_chunks,
            'total_rows': total_rows
        }
    finally:
        cursor.close()

def export_large_table_parallel_chunks(db_conn_params, table_name, table_dir, partition_info, max_chunk_workers=4):
    """
    Export large table using parallel chunk processing with optimized Greenplum queries.
    Each chunk is processed in its own thread with its own database connection.
    """
    try:
        strategy = partition_info['strategy']
        total_rows = partition_info['total_rows']
        estimated_chunks = partition_info['estimated_chunks']
        db_type = db_conn_params.get('db_type', 'postgresql')
        
        # Initialize progress manager to reduce logging spam
        progress_manager = ProgressManager(None, table_name, total_rows, estimated_chunks)
        progress_manager.set_export_method("Polars (parallel)")
        
        # Get table schema once to use across all chunks
        temp_conn = create_data_source_connection(db_conn_params, db_type)
        polars_schema = get_table_schema(temp_conn, db_type, table_name)
        temp_conn.close()
        
        if polars_schema:
            logging.info(f"Using explicit schema for {table_name} parallel chunks with {len(polars_schema)} columns")
        else:
            logging.warning(f"No schema available for {table_name}, chunks may have inference issues")
        
        logging.info(f"Exporting {table_name} using {strategy} with {estimated_chunks} chunks in parallel (max {max_chunk_workers} concurrent)")
        
        # Check for existing chunks and generate resume plan
        chunk_progress_file = table_dir / "_chunk_progress.json"
        
        # Initialize progress manager with configurable thresholds
        progress_manager = ChunkProgressManager(
            chunk_progress_file, 
            batch_size=max(1, estimated_chunks // 20),  # Write every ~5% of chunks
            time_threshold=30  # Or every 30 seconds
        )
        
        # Initialize variables for backward compatibility
        completed_chunks = set()
        chunk_metadata = {}
        
        if chunk_progress_file.exists():
            try:
                with open(chunk_progress_file, 'r') as f:
                    progress_data = json.load(f)
                    completed_chunks = set(progress_data.get('completed_chunks', []))
                    chunk_metadata = progress_data.get('chunk_metadata', {})
                    
                # Load existing progress into manager
                progress_manager.completed_chunks = completed_chunks
                progress_manager.chunk_metadata = chunk_metadata
                    
                logging.info(f"Found existing progress for {table_name}: {len(completed_chunks)} completed chunks")
                
                # Check if this is an old format file (before batching)
                if 'batch_info' not in progress_data:
                    logging.info(f"Converting legacy progress file format for {table_name}")
                    # Force immediate save with new format
                    progress_manager.flush(force=True)
                
                # Verify existing chunks are still valid
                valid_chunks = set()
                for chunk_num in completed_chunks:
                    chunk_file = table_dir / f"part_{chunk_num:04d}.parquet"
                    if chunk_file.exists():
                        # Verify chunk integrity if metadata available
                        chunk_key = str(chunk_num)
                        if chunk_key in chunk_metadata:
                            expected_checksum = chunk_metadata[chunk_key].get('checksum')
                            if expected_checksum:
                                import hashlib
                                actual_hash = hashlib.sha256(chunk_file.read_bytes()).hexdigest()
                                if actual_hash == expected_checksum:
                                    valid_chunks.add(chunk_num)
                                    logging.info(f"Verified existing chunk {chunk_num} integrity")
                                else:
                                    logging.warning(f"Chunk {chunk_num} checksum mismatch, will re-download")
                            else:
                                valid_chunks.add(chunk_num)  # Assume valid if no checksum
                        else:
                            valid_chunks.add(chunk_num)  # Assume valid if no metadata
                    else:
                        logging.warning(f"Chunk {chunk_num} file missing, will re-download")
                
                # Update manager with validated chunks
                progress_manager.completed_chunks = valid_chunks
                completed_chunks = valid_chunks
                
                if len(valid_chunks) < len(progress_data.get('completed_chunks', [])):
                    logging.info(f"Validated {len(valid_chunks)} chunks, will retry {len(progress_data.get('completed_chunks', [])) - len(valid_chunks)} invalid chunks")
                    
            except Exception as e:
                logging.warning(f"Could not load chunk progress: {str(e)}, starting fresh")
                completed_chunks = set()
                chunk_metadata = {}

        # Prepare chunk tasks
        chunk_tasks = []
        
        if strategy == 'range_partition' and False:  # Temporarily disable range partitioning
            # Range-based partitioning (more efficient for Greenplum)
            column = partition_info['column']
            min_val = partition_info['min_value']
            max_val = partition_info['max_value']
            range_step = partition_info['range_step']
            
            for i in range(estimated_chunks):
                start_val = min_val + (i * range_step)
                end_val = min_val + ((i + 1) * range_step) if i < estimated_chunks - 1 else max_val + 1
                
                query = f"""
                    SELECT * FROM {table_name} 
                    WHERE {column} >= {start_val} AND {column} < {end_val}
                    ORDER BY {column}
                """
                
                if (i + 1) not in completed_chunks:  # Skip completed chunks
                    chunk_tasks.append({
                        'chunk_num': i + 1,
                        'query': query,
                        'expected_range': (start_val, end_val)
                    })
        else:
            # Offset-based partitioning (fallback)
            chunk_size = partition_info.get('chunk_size', 1000000)  # Use default 1M rows if not available
            for i in range(estimated_chunks):
                offset = i * chunk_size
                limit = min(chunk_size, total_rows - offset)
                
                query = f"SELECT * FROM {table_name} ORDER BY 1 LIMIT {limit} OFFSET {offset}"
                
                if (i + 1) not in completed_chunks:  # Skip completed chunks
                    chunk_tasks.append({
                        'chunk_num': i + 1,
                        'query': query,
                        'expected_rows': limit
                    })
                else:
                    logging.info(f"Skipping already completed chunk {i + 1}")
        
        # Process chunks in parallel
        exported_files = []
        total_exported_rows = 0
        failed_chunks = []
        
        def process_chunk_with_retry(chunk_task, max_retries=3):
            """Process a single chunk with retry logic and exponential backoff"""
            chunk_num = chunk_task['chunk_num']
            query = chunk_task['query']
            
            for attempt in range(max_retries + 1):
                try:
                    # Create dedicated connection for this chunk using existing function
                    chunk_conn = get_database_connection(
                        db_type=db_conn_params['db_type'],
                        host=db_conn_params['host'], 
                        port=db_conn_params['port'],
                        username=db_conn_params['username'],
                        password=db_conn_params['password'],
                        database=db_conn_params['database']
                    )
                    
                    if attempt > 0:
                        import time
                        backoff_delay = 2 ** (attempt - 1)  # 1s, 2s, 4s delays
                        logging.info(f"Retrying chunk {chunk_num} (attempt {attempt + 1}/{max_retries + 1}) after {backoff_delay}s delay")
                        time.sleep(backoff_delay)
                    
                    return process_chunk_attempt(chunk_task, chunk_conn)
                    
                except Exception as e:
                    error_msg = str(e)
                    if attempt < max_retries:
                        # Determine if error is retryable
                        retryable_errors = ['connection', 'timeout', 'segment', 'network', 'temporary']
                        is_retryable = any(keyword in error_msg.lower() for keyword in retryable_errors)
                        
                        if is_retryable:
                            logging.warning(f"Chunk {chunk_num} failed (attempt {attempt + 1}), will retry: {error_msg}")
                            continue
                        else:
                            logging.error(f"Chunk {chunk_num} failed with non-retryable error: {error_msg}")
                            break
                    else:
                        logging.error(f"Chunk {chunk_num} failed after {max_retries + 1} attempts: {error_msg}")
                
                return {
                    'success': False,
                    'chunk_num': chunk_num,
                    'error': error_msg,
                    'attempts': attempt + 1
                }

        def process_chunk_attempt(chunk_task, chunk_conn):
            """Process a single chunk attempt"""
            chunk_num = chunk_task['chunk_num']
            query = chunk_task['query']
            
            try:
                # Memory monitoring - with fallback if psutil fails (reduced logging)
                memory_before = 0
                try:
                    import psutil
                    process = psutil.Process()
                    memory_before = process.memory_info().rss / (1024 * 1024)  # MB
                except ImportError:
                    pass  # Memory monitoring disabled
                except Exception as e:
                    logging.debug(f"Memory monitoring failed: {str(e)}")
                
                # Read chunk with optimized settings and explicit schema
                if not polars_schema:
                    error_msg = (f"Failed to retrieve database schema for {table_name}. "
                                f"This could be due to: (1) table does not exist, (2) insufficient permissions, "
                                f"(3) case sensitivity issues in table name, or (4) database connection problems. "
                                f"Check logs above for specific schema detection errors.")
                    logging.error(error_msg)
                    raise ValueError(error_msg)
                
                try:
                    # Memory-efficient approach: Don't use iter_batches for chunks
                    # Let polars handle memory management internally
                    df_chunk = pl.read_database(
                        query, 
                        chunk_conn,
                        schema_overrides=polars_schema
                        # Removed iter_batches to prevent memory accumulation
                    )
                except Exception as schema_error:
                    # Schema enforcement failed - convert all columns to String and retry
                    error_msg = str(schema_error)
                    logging.error(f"Schema enforcement failed for {table_name} chunk {chunk_num}: {error_msg}")
                    logging.error("Data type mismatch detected - actual data doesn't match database schema")
                    
                    # Create all-String schema as fallback
                    string_schema = {col_name: pl.String for col_name in polars_schema.keys()}
                    
                    try:
                        df_chunk = pl.read_database(
                            query, 
                            chunk_conn,
                            schema_overrides=string_schema
                            # Removed iter_batches to prevent memory accumulation
                        )
                        logging.warning(f"Successfully read chunk {chunk_num} with all columns converted to String type")
                    except Exception as retry_error:
                        error_msg = f"Failed to read chunk {chunk_num} even with String fallback schema: {str(retry_error)}"
                        logging.error(error_msg)
                        raise Exception(error_msg)
                
                # Memory check: Log chunk memory usage for monitoring
                chunk_row_count = len(df_chunk)
                if chunk_row_count > 0:
                    # Estimate memory usage (rough calculation)
                    estimated_mb = (chunk_row_count * len(polars_schema) * 8) / (1024 * 1024)  # ~8 bytes per field
                    if estimated_mb > 1000:  # Warn if chunk > 1GB
                        logging.warning(f"Large chunk {chunk_num}: {chunk_row_count} rows, ~{estimated_mb:.1f}MB estimated")
                else:
                    logging.warning(f"Empty chunk {chunk_num} detected")
                
                # Reduced logging - detailed chunk info moved to debug level
                logging.debug(f"Chunk {chunk_num} loaded {chunk_row_count} rows")
                
                # Process data types for Parquet compatibility
                df_chunk = handle_mixed_types(df_chunk)
                
                # Write chunk with optimized Parquet settings
                chunk_filename = f"part_{chunk_num:04d}.parquet"
                chunk_file = table_dir / chunk_filename
                temp_file = table_dir / f"part_{chunk_num:04d}.tmp"
                
                # Optimized Parquet writing for large datasets
                df_chunk.write_parquet(
                    temp_file,
                    compression="zstd",  # Better compression ratio for large files
                    compression_level=3,  # Balanced speed/compression
                    row_group_size=100000,  # Optimize for query performance
                    use_pyarrow=True
                )
                
                temp_file.rename(chunk_file)
                
                # Verify chunk without reading entire file back
                parquet_info = pl.scan_parquet(chunk_file).select(pl.len()).collect()
                verified_count = parquet_info[0, 0]
                
                if verified_count != chunk_row_count:
                    raise ValueError(f"Chunk verification failed: expected {chunk_row_count}, got {verified_count}")
                
                # Memory monitoring after processing - with fallback
                try:
                    import psutil
                    process = psutil.Process()
                    memory_after = process.memory_info().rss / (1024 * 1024)  # MB
                    memory_delta = memory_after - memory_before
                    
                    # Circuit breaker: Warn if memory usage is excessive (optimized for 128GB system)
                    if memory_after > 80000:  # 80GB threshold for 128GB system
                        logging.error(f"🚨 CRITICAL MEMORY WARNING: Process using {memory_after:.1f}MB (delta: +{memory_delta:.1f}MB)")
                        logging.error(f"🚨 Approaching system limits on 128GB system!")
                    elif memory_after > 60000:  # 60GB warning threshold
                        logging.warning(f"⚠️ HIGH MEMORY: Process using {memory_after:.1f}MB (delta: +{memory_delta:.1f}MB)")
                    elif memory_delta > 5000:  # 5GB delta threshold (larger jumps expected with big tables)
                        logging.warning(f"Memory spike: Chunk {chunk_num} delta: +{memory_delta:.1f}MB (total: {memory_after:.1f}MB)")
                    
                    logging.debug(f"Successfully exported chunk {chunk_num} with {verified_count} rows (Memory: {memory_after:.1f}MB)")
                    
                    # Force garbage collection if memory usage is high (optimized for 128GB)
                    if memory_after > 40000:  # 40GB threshold
                        import gc
                        gc.collect()
                        logging.debug(f"Forced garbage collection after chunk {chunk_num}")
                except:
                    logging.debug(f"Successfully exported chunk {chunk_num} with {verified_count} rows")
                
                # Explicit memory cleanup
                del df_chunk
                
                # Calculate chunk checksum for integrity verification
                import hashlib
                chunk_hash = hashlib.sha256()
                chunk_content = chunk_file.read_bytes()
                chunk_hash.update(chunk_content)
                checksum = chunk_hash.hexdigest()
                file_size = len(chunk_content)
                
                # Cleanup checksum content from memory immediately
                del chunk_content
                
                return {
                    'chunk_num': chunk_num,
                    'filename': chunk_filename,
                    'row_count': verified_count,
                    'success': True,
                    'checksum': checksum,
                    'file_size': file_size
                }
                
            except Exception as e:
                error_msg = f"Chunk {chunk_num} failed: {str(e)}"
                logging.error(error_msg)
                return {
                    'chunk_num': chunk_num,
                    'error': error_msg,
                    'success': False
                }
            finally:
                if chunk_conn:
                    chunk_conn.close()
        
        # Global memory circuit breaker
        def check_memory_safety():
            """Check if system memory usage is safe for continued processing"""
            try:
                import psutil
                memory = psutil.virtual_memory()
                memory_usage_percent = memory.percent
                available_gb = memory.available / (1024**3)
                
                if memory_usage_percent > 90:
                    raise MemoryError(f"System memory critically low: {memory_usage_percent:.1f}% used, {available_gb:.1f}GB available")
                elif memory_usage_percent > 80:
                    logging.warning(f"High system memory usage: {memory_usage_percent:.1f}% used, {available_gb:.1f}GB available")
                    
                return True
            except ImportError:
                logging.debug("psutil not available for memory monitoring")
                return True  # Continue without monitoring
            except Exception as e:
                logging.warning(f"Memory check failed: {str(e)}")
                return True  # Continue on check failure
        
        # Pre-flight memory check
        check_memory_safety()
        
        # Execute chunks in parallel with retry
        current_session_chunks = set()
        
        with ThreadPoolExecutor(max_workers=max_chunk_workers, thread_name_prefix=f"Chunk-{table_name}") as executor:
            chunk_futures = {executor.submit(process_chunk_with_retry, task): task for task in chunk_tasks}
            
            for future in as_completed(chunk_futures):
                # Memory safety check before processing result
                try:
                    check_memory_safety()
                except MemoryError as e:
                    logging.error(f"Memory circuit breaker triggered: {str(e)}")
                    # Cancel remaining futures
                    for pending_future in chunk_futures:
                        if pending_future != future:
                            pending_future.cancel()
                    raise e
                
                result = future.result()
                current_session_chunks.add(result['chunk_num'])
                
                if result['success']:
                    exported_files.append(result['filename'])
                    total_exported_rows += result['row_count']
                    
                    # Use batched progress manager instead of immediate writes
                    chunk_info = {
                        'filename': result['filename'],
                        'row_count': result['row_count'],
                        'checksum': result.get('checksum'),
                        'file_size': result.get('file_size'),
                        'completed_at': time.strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    # This will only write to disk when batch/time threshold is met
                    progress_manager.add_completed_chunk(result['chunk_num'], chunk_info)
                else:
                    failed_chunks.append(result)
        
        # Ensure all pending progress is written before final validation
        progress_manager.flush(force=True)
        
        # Check if all chunks succeeded
        if failed_chunks:
            chunk_numbers = [f['chunk_num'] for f in failed_chunks]
            error_details = []
            for f in failed_chunks:
                error_details.append(f"Chunk {f['chunk_num']}: {f.get('error', 'Unknown error')}")
            
            error_msg = f"Failed chunks: {chunk_numbers}"
            logging.error(error_msg)
            logging.error("Chunk failure details:")
            for detail in error_details:
                logging.error(f"  - {detail}")
            return False, error_msg
        
        # Include rows from previously completed chunks using progress manager data
        completed_chunks = progress_manager.completed_chunks
        chunk_metadata = progress_manager.chunk_metadata
        
        for chunk_num in completed_chunks:
            if chunk_num not in current_session_chunks:
                # This chunk was completed in a previous run
                chunk_key = str(chunk_num)
                if chunk_key in chunk_metadata:
                    previous_rows = chunk_metadata[chunk_key].get('row_count', 0)
                    previous_file = chunk_metadata[chunk_key].get('filename')
                    if previous_file:
                        exported_files.append(previous_file)
                        total_exported_rows += previous_rows
        
        # Final data integrity validation
        verify_table_integrity(table_dir, exported_files, total_exported_rows, total_rows, table_name)
        
        # Final validation with stricter tolerance for resumed exports
        expected_tolerance = 0.001 if completed_chunks else 0.01  # 0.1% for resumed, 1% for fresh
        if abs(total_exported_rows - total_rows) > (total_rows * expected_tolerance):
            error_msg = f"Row count validation failed: expected {total_rows}, got {total_exported_rows} (tolerance: {expected_tolerance*100}%)"
            logging.error(error_msg)
            return False, error_msg
        
        # Clean up progress file on successful completion
        if chunk_progress_file.exists():
            try:
                chunk_progress_file.unlink()
                logging.info(f"Cleaned up chunk progress file for completed {table_name}")
            except Exception as e:
                logging.warning(f"Failed to clean up progress file: {str(e)}")

        # Write metadata
        chunk_info = {
            'chunk_count': len(exported_files),
            'chunk_strategy': strategy,
            'chunk_size': partition_info.get('chunk_size', 1000000),  # Add missing chunk_size
            'files': sorted(exported_files),
            'parallel_processing': True,
            'max_workers': max_chunk_workers,
            'resume_capable': True,
            'integrity_verified': True
        }
        write_export_metadata(table_dir, table_name, total_exported_rows, chunk_info)
        
        logging.info(f"Successfully exported {table_name} as {len(exported_files)} parallel chunks with {total_exported_rows} total rows")
        return True, total_exported_rows
        
    except Exception as e:
        logging.error(f"Failed to export large table {table_name} with parallel chunks: {str(e)}")
        return False, str(e)
    """Read a partitioned table back into a single DataFrame for verification"""
    try:
        metadata_file = table_dir / "_export_metadata.json"
        if not metadata_file.exists():
            raise ValueError("No metadata file found")
        
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        if metadata.get('partitioned', False):
            # Read all partition files
            parquet_files = sorted(table_dir.glob("part_*.parquet"))
            if not parquet_files:
                raise ValueError("No partition files found")
            
            # Read and concatenate all chunks
            chunks = []
            for file in parquet_files:
                chunk_df = pl.read_parquet(file)
                chunks.append(chunk_df)
            
            combined_df = pl.concat(chunks)
            return combined_df
        else:
            # Single file
            data_file = table_dir / "data.parquet"
            if not data_file.exists():
                raise ValueError("Single data file not found")
            return pl.read_parquet(data_file)
            
    except Exception as e:
        raise ValueError(f"Failed to read partitioned table: {str(e)}")

def export_table_to_parquet(db_conn, table_name, output_path, job_id, chunk_size=1000000, max_chunk_workers=None, config=None, db_type='postgresql', db_password=None):
    """
    Export a single table to Parquet format using optimized strategies for large Greenplum tables
    
    For very large tables (>10M rows), uses parallel chunk processing with range-based partitioning
    when possible for optimal Greenplum performance.
    
    Args:
        db_conn: Database connection
        table_name: Fully qualified table name (schema.table)
        output_path: Base output directory
        job_id: Unique job identifier
        chunk_size: Maximum rows per file for large tables (default: 1,000,000)
        max_chunk_workers: Max parallel workers for chunk processing (default: auto-detect)
        config: Configuration dict with export organization options
        db_type: Database type for schema handling
    
    Returns:
        tuple: (success: bool, row_count: int or error_message: str)
    """
    try:
        # First, get the source table row count and analyze partitioning strategy
        source_count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor = db_conn.cursor()
        cursor.execute(source_count_query)
        source_row_count = cursor.fetchone()[0]
        cursor.close()
        logging.info(f"Source table {table_name} has {source_row_count:,} rows")
        
        # Create table-specific directory structure using new path strategy
        table_temp_path, table_final_path, _ = get_export_paths(output_path, job_id, table_name, config)
        table_dir = table_temp_path
        table_dir.mkdir(parents=True, exist_ok=True)
        
        # First check if table export already exists globally in final location
        global_exists, existing_location = check_global_existing_export(output_path, table_name, source_row_count)
        if global_exists:
            logging.info(f"Skipping {table_name} - already exported to {existing_location}")
            return True, source_row_count
            
        # Then check if table export already exists in current temp location
        if check_existing_export(table_dir, source_row_count, table_name):
            return True, source_row_count
        
        # Check if DuckDB export is enabled in configuration
        use_duckdb = config.get('use_duckdb_export', False) if config else False
        
        # DEBUG: Log DuckDB configuration status
        logging.info(f"🔍 DEBUG - DuckDB config check for {table_name}:")
        logging.info(f"🔍 DEBUG - Config received: {config is not None}")
        logging.info(f"🔍 DEBUG - use_duckdb_export in config: {'use_duckdb_export' in (config or {})}")
        logging.info(f"🔍 DEBUG - use_duckdb value: {use_duckdb}")
        if config:
            logging.info(f"🔍 DEBUG - Full config keys: {list(config.keys())}")
        
        # Use DuckDB exclusively - no fallback to memory-heavy Polars
        logging.info(f"✅ Using DuckDB export method for {table_name} (memory-optimized)")
        # Prepare database configuration for DuckDB
        dsn_params = db_conn.get_dsn_parameters()
        db_config = {
            'db_type': db_type,
            'db_host': dsn_params.get('host', 'localhost'),
            'db_port': int(dsn_params.get('port', 5432)),
            'db_username': dsn_params.get('user', ''),
            'db_password': db_password or dsn_params.get('password', ''),
            'db_name': dsn_params.get('dbname', 'postgres')
        }

        # Determine export strategy based on table size (DuckDB version)
        if source_row_count <= chunk_size:
            logging.info(f"Table {table_name} has {source_row_count:,} rows, using DuckDB single file export")
            return export_small_table_duckdb(db_config, table_name, table_dir, source_row_count, db_type)
        else:
            logging.info(f"Table {table_name} has {source_row_count:,} rows, using DuckDB chunked export")
            return export_large_table_with_duckdb(db_config, table_name, table_dir, source_row_count, chunk_size, db_type)


        
    except Exception as e:
        logging.error(f"Failed to export table {table_name}: {str(e)}")
        return False, str(e)

def process_single_table(job_id, table_name, db_conn_params, output_path, chunk_size, max_chunk_workers=None, config=None):
    """Process a single table using smart export method selection"""
    table_start_time = time.strftime('%Y-%m-%d %H:%M:%S')
    db_type = db_conn_params.get('db_type', 'postgresql')
    
    # Get SQLite writer for thread-safe database operations
    sqlite_writer = get_sqlite_writer()
    
    # Set table context for enhanced logging
    logger.set_table_context(table_name, 0, 0, "Analyzing")
    
    try:
        # Check if table export is already in progress or completed
        export_status_query = """
            SELECT status FROM table_exports 
            WHERE job_id = ? AND table_name = ?
            ORDER BY start_time DESC LIMIT 1
        """
        
        try:
            result = sqlite_writer.query(export_status_query, (job_id, table_name), fetchone=True, timeout=5.0)
            export_status = result['status'] if result else 'new'
        except:
            export_status = 'new'
        
        if export_status == 'completed':
            logger.info("Table already completed, skipping")
            return {'table': table_name, 'status': 'already_completed', 'result': 'skipped'}
        elif export_status == 'processing':
            logger.warning("Table is already being processed, skipping to prevent duplicates")
            return {'table': table_name, 'status': 'already_processing', 'result': 'skipped'}
        
        # Start table processing - use SQLite writer queue
        sqlite_writer.table_started(job_id, table_name)
        
        # Create table directory using path strategy
        table_temp_path, table_final_path, _ = get_export_paths(output_path, job_id, table_name, config)
        table_dir = table_temp_path
        table_dir.mkdir(parents=True, exist_ok=True)
        
        # Check for existing exports to avoid re-processing
        with get_pooled_connection() as db_conn:
            cursor = db_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
        
        # Check if export already exists
        global_exists, existing_location = check_global_existing_export(output_path, table_name, row_count)
        if global_exists:
            logger.info(f"Skipping - already exported to {existing_location}")
            success, result = True, row_count
            method_used = "existing_export"
        elif check_existing_export(table_dir, row_count, table_name):
            logger.info("Using existing export in temp location")
            success, result = True, row_count  
            method_used = "existing_temp"
        else:
            # Use smart export method selection
            logger.info("Using smart export method selection")
            
            # Transform db_conn_params to standard db_config format for compatibility
            db_config = {
                'db_type': db_conn_params.get('db_type', 'postgresql'),
                'db_host': db_conn_params.get('host', 'localhost'),
                'db_port': int(db_conn_params.get('port', 5432)),
                'db_username': db_conn_params.get('username', ''),
                'db_password': db_conn_params.get('password', ''),
                'db_name': db_conn_params.get('database', 'postgres')
            }
            
            # Execute smart export
            success, result, method_used = smart_export_table(
                job_id, table_name, table_dir, db_type, db_config
            )
        
        # Update SQLite with results using queue system
        table_end_time = time.strftime('%Y-%m-%d %H:%M:%S')
        
        if success:
            # Calculate file size
            total_size_mb = 0
            try:
                for parquet_file in table_dir.glob("*.parquet"):
                    if parquet_file.exists():
                        total_size_mb += parquet_file.stat().st_size / 1024 / 1024
            except:
                total_size_mb = 0
            
            # Log success with method used
            logger.info(f"Table export completed successfully using {method_used}")
            
            # Update SQLite via queue
            sqlite_writer.table_completed(
                job_id=job_id,
                table_name=table_name,
                rows_processed=result,
                file_path=str(table_dir),
                file_size_mb=total_size_mb
            )
            
            return {'table': table_name, 'status': 'success', 'result': result, 'method': method_used}
        else:
            # Log failure
            error_msg = f"Smart export failed using method: {method_used}"
            logger.error(error_msg)
            
            # Update SQLite via queue
            sqlite_writer.table_failed(job_id, table_name, error_msg)
            
            return {'table': table_name, 'status': 'failed', 'result': error_msg}
    
    except Exception as e:
        error_msg = f"Table processing error: {str(e)}"
        logger.table_failed(table_name, error_msg)
        
        # Log error via SQLite queue
        sqlite_writer.table_failed(job_id, table_name, error_msg)
        sqlite_writer.log_error(job_id, error_msg, traceback.format_exc(), 
                               json.dumps({"table": table_name, "config": config}))
        
        return {'table': table_name, 'status': 'failed', 'result': error_msg}


def process_data(job_id, config):
    """Main data processing function with enhanced logging and connection pooling"""
    
    # Record job start time
    job_start_time = time.time()
    
    # Initialize enhanced logging for this job
    total_tables = len(config.get('tables', []))
    logger.job_started(job_id, total_tables)
    
    # Get SQLite writer for efficient database operations
    sqlite_writer = get_sqlite_writer()
    
    # Check if job is already completed before calling job_started
    existing_job = sqlite_writer.query("SELECT overall_status FROM jobs WHERE job_id = ?", (job_id,), fetchone=True)
    if existing_job and existing_job[0] in ('completed', 'completed_with_errors'):
        logger.info(f"Job {job_id} already completed, skipping execution")
        return
    
    sqlite_writer.job_started(job_id, config.get('db_username'), tables_total=total_tables)
    
    # Initialize progress tracking
    completed_tables = 0
    total_rows_processed = 0
    
    # Initialize connection pool
    try:
        initialize_global_connection_pool(
            config['db_type'], 
            config['db_host'], 
            config['db_port'],
            config['db_username'], 
            config['db_password'], 
            config.get('db_name')
        )
        logger.info("Global connection pool initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize connection pool: {e}")
        sqlite_writer.job_update(job_id=job_id, status='failed', error_message=f"Connection pool error: {str(e)}")
        return []
    
    # Process each table using smart export
    results = []
    for table_name in config['tables']:
        try:
            logger.info(f"Processing table: {table_name}")
            
            # Use smart export to automatically select optimal method
            # Create table-specific output directory
            table_output_dir = Path(config.get('output_path', '/app/exports')) / job_id / table_name
            
            success, rows_exported, method_used = smart_export_table(
                job_id=job_id,
                table_name=table_name,
                output_dir=table_output_dir,
                db_type=config['db_type'],
                db_config=config
            )
            
            if success:
                # Note: table completion tracking is now handled inside smart_export_table()
                # to avoid duplication and ensure proper file path and size calculation
                results.append({
                    'table': table_name, 
                    'status': 'completed', 
                    'rows': rows_exported, 
                    'method': method_used
                })
                logger.info(f"Table {table_name} completed: {rows_exported:,} rows using {method_used}")
                
                # Update progress tracking
                completed_tables += 1
                total_rows_processed += rows_exported
                progress_percent = int((completed_tables / total_tables) * 100)
                
                # Update enhanced logger job context progress
                logger.job_progress_update(job_id, completed_tables)
                
                # Update job progress in database
                current_time = time.time()
                elapsed_time = current_time - job_start_time
                throughput = int(total_rows_processed / elapsed_time) if elapsed_time > 0 else 0
                
                sqlite_writer.job_update(
                    job_id=job_id,
                    progress_percent=progress_percent,
                    tables_completed=completed_tables,
                    rows_processed=total_rows_processed,
                    throughput_rows_per_sec=throughput
                )
            else:
                # Note: table failure tracking is now handled inside smart_export_table()
                results.append({
                    'table': table_name, 
                    'status': 'failed', 
                    'method': method_used
                })
                logger.error(f"Table {table_name} failed using {method_used}")
                
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {str(e)}")
            # Handle processing errors that occur outside smart_export_table()
            try:
                sqlite_writer.table_update(job_id, table_name, status='failed', 
                                         error_message=f"Processing error: {str(e)}")
            except:
                pass  # Don't let SQLite errors propagate
                
            results.append({
                'table': table_name, 
                'status': 'failed', 
                'error': str(e)
            })
    
    # Update job completion status
    total_completed = sum(1 for r in results if r['status'] == 'completed')
    total_failed = sum(1 for r in results if r['status'] == 'failed')
    job_duration = time.time() - job_start_time
    
    # Final job completion with 100% progress
    if total_failed == 0:
        sqlite_writer.job_update(
            job_id=job_id, 
            status='completed', 
            overall_status='completed',
            progress_percent=100,
            tables_completed=total_completed,
            tables_failed=0,
            end_time=time.strftime('%Y-%m-%d %H:%M:%S')
        )
        logger.job_completed(job_id, job_duration, total_completed, 0)
    else:
        sqlite_writer.job_update(
            job_id=job_id, 
            status='completed_with_errors', 
            overall_status='completed_with_errors',
            progress_percent=100,
            tables_completed=total_completed,
            tables_failed=total_failed,
            end_time=time.strftime('%Y-%m-%d %H:%M:%S')
        )
        logger.job_completed(job_id, job_duration, total_completed, total_failed)
    
    return results


def poll_for_jobs():
    while True:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM jobs WHERE overall_status = 'queued' ORDER BY start_time LIMIT 1")
        job = cursor.fetchone()
        conn.close()

        if job:
            job_id = job['job_id']
            logging.info(f"Found queued job: {job_id}")
            
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT config FROM job_configs WHERE job_id = ?", (job_id,))
            config_json = cursor.fetchone()['config']
            config = json.loads(config_json)
            cursor.execute("UPDATE jobs SET overall_status = ? WHERE job_id = ?", ('running', job_id))
            conn.commit()
            conn.close()

            process_data(job_id, config)
            
            # Immediately update job status to prevent reprocessing (race condition fix)
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE jobs SET overall_status = 'completed' WHERE job_id = ? AND overall_status = 'running'", (job_id,))
            conn.commit()
            conn.close()
            logging.info(f"Job {job_id} processing completed and status updated")
        else:
            # Wait for a bit before polling again
            time.sleep(5)

if __name__ == '__main__':
    logging.info("Starting worker...")
    poll_for_jobs()
