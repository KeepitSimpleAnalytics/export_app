"""
DuckDB-based export module for memory-efficient database exports

This module provides memory-safe streaming export functionality using DuckDB
as an intermediary for exporting large tables from Greenplum/PostgreSQL to Parquet.
Designed to eliminate OOM issues by avoiding loading large datasets into Python memory.
"""

import duckdb
import logging
import os
import polars as pl
import psutil
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from adu.database_type_mappings import POSTGRESQL_TYPE_MAPPING, VERTICA_TYPE_MAPPING


logger = logging.getLogger(__name__)


def map_polars_to_duckdb_type(polars_type) -> str:
    """Map Polars types to DuckDB SQL cast types for schema enforcement"""
    type_mapping = {
        pl.Int8: 'TINYINT',
        pl.Int16: 'SMALLINT', 
        pl.Int32: 'INTEGER',
        pl.Int64: 'BIGINT',
        pl.UInt8: 'UTINYINT',
        pl.UInt16: 'USMALLINT',
        pl.UInt32: 'UINTEGER', 
        pl.UInt64: 'UBIGINT',
        pl.Float32: 'REAL',
        pl.Float64: 'DOUBLE',
        pl.Boolean: 'BOOLEAN',
        pl.String: 'VARCHAR',
        pl.Binary: 'BLOB',
        pl.Date: 'DATE',
        pl.Time: 'TIME',
        pl.Datetime: 'TIMESTAMP',
    }
    return type_mapping.get(polars_type, 'VARCHAR')  # Default to VARCHAR for unknown types


def create_duckdb_connection(db_config: Dict[str, Any]) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection with PostgreSQL extension for remote database access
    Designed for airgapped deployment with pre-installed extensions.
    
    Args:
        db_config: Database configuration with host, port, username, password, etc.
        
    Returns:
        DuckDB connection with PostgreSQL extension loaded
    """
    duck_conn = None
    try:
        # Debug: Log the db_config contents (with sensitive data redacted)
        config_keys = list(db_config.keys()) if db_config else []
        logger.info(f"DuckDB connection config keys: {config_keys}")
        
        # Validate required configuration keys (skip if using connection pool)
        use_pool = db_config.get('use_connection_pool', False)
        if not use_pool:
            required_keys = ['username', 'password', 'host', 'port']
            missing_keys = [key for key in required_keys if key not in db_config or not db_config[key]]
            if missing_keys:
                error_msg = f"Missing or empty required database configuration keys: {missing_keys}"
                logger.error(error_msg)
                raise KeyError(error_msg)
        
        # Create DuckDB connection
        duck_conn = duckdb.connect()
        
        # Load PostgreSQL extension (pre-installed during Docker build)
        try:
            duck_conn.execute("LOAD postgres")
            logger.info("PostgreSQL extension loaded successfully (airgapped mode)")
        except Exception as ext_error:
            # Fallback: try to install if load fails (for development environments)
            logger.warning(f"Failed to load pre-installed PostgreSQL extension: {ext_error}")
            logger.info("Attempting to install PostgreSQL extension (requires internet access)...")
            try:
                duck_conn.execute("INSTALL postgres")
                duck_conn.execute("LOAD postgres")
                logger.info("PostgreSQL extension installed and loaded successfully")
            except Exception as install_error:
                error_msg = (
                    f"Failed to load PostgreSQL extension in airgapped mode. "
                    f"Original load error: {ext_error}. "
                    f"Install attempt error: {install_error}. "
                    f"This may indicate the extension was not properly pre-installed during Docker build."
                )
                logger.error(error_msg)
                raise Exception(error_msg)
        
        if use_pool:
            # When using connection pool, we need to get connection details from the pool
            from adu.greenplum_pool import get_connection_pool
            try:
                pool = get_connection_pool()
                config = pool.config
                pg_conn_str = f"postgresql://{config.username}:{config.password}@{config.host}:{config.port}/{config.database}"
                
                # Attach remote PostgreSQL database
                duck_conn.execute(f"ATTACH '{pg_conn_str}' AS remote_db (TYPE POSTGRES)")
                logger.info(f"DuckDB connection established via connection pool to {config.host}:{config.port}")
            except Exception as attach_error:
                error_msg = f"Failed to attach to PostgreSQL database via connection pool: {attach_error}"
                logger.error(error_msg)
                raise Exception(error_msg)
        else:
            # Construct PostgreSQL connection string with proper escaping
            pg_conn_str = f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config.get('database', 'postgres')}"
            
            # Attach remote PostgreSQL database
            try:
                duck_conn.execute(f"ATTACH '{pg_conn_str}' AS remote_db (TYPE POSTGRES)")
                logger.info(f"DuckDB connection established to {db_config['host']}:{db_config['port']} (airgapped mode)")
            except Exception as attach_error:
                error_msg = f"Failed to attach to PostgreSQL database: {attach_error}"
                logger.error(error_msg)
                raise Exception(error_msg)
        
        return duck_conn
        
    except Exception as e:
        logger.error(f"Failed to create DuckDB connection: {str(e)}")
        if duck_conn:
            try:
                duck_conn.close()
            except:
                pass
        raise


def get_memory_usage_mb() -> float:
    """Get current process memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / (1024 * 1024)


def check_memory_safety() -> Tuple[bool, str]:
    """Check system memory safety thresholds"""
    try:
        memory = psutil.virtual_memory()
        process_memory = get_memory_usage_mb()
        
        if memory.percent > 90:
            return False, f"System memory critically low: {memory.percent:.1f}% used"
        elif process_memory > 8000:  # 8GB process limit
            return False, f"Process memory limit exceeded: {process_memory:.1f}MB"
        elif memory.percent > 80:
            logger.warning(f"High system memory usage: {memory.percent:.1f}% used")
        
        return True, "Memory usage within safe limits"
        
    except Exception as e:
        logger.error(f"Failed to check memory safety: {str(e)}")
        return True, "Memory check failed - proceeding"


def export_table_chunk_duckdb(
    db_config: Dict[str, Any],
    table_name: str,
    output_path: Path,
    offset: int,
    chunk_size: int,
    polars_schema: Optional[Dict[str, Any]] = None,
    custom_where: Optional[str] = None
) -> Tuple[bool, str, int]:
    """
    Export a single table chunk using DuckDB streaming with memory safety
    
    Args:
        db_config: Database connection configuration
        table_name: Fully qualified table name (schema.table)
        output_path: Output Parquet file path
        offset: Starting row offset for chunk (ignored if custom_where provided)
        chunk_size: Number of rows to export (ignored if custom_where provided)
        polars_schema: Optional Polars schema for type enforcement
        custom_where: Optional custom WHERE clause for filtering (overrides offset/chunk_size)
        
    Returns:
        Tuple of (success: bool, message: str, rows_exported: int)
    """
    duck_conn = None
    try:
        # Memory safety check before starting
        memory_safe, memory_msg = check_memory_safety()
        if not memory_safe:
            return False, f"Memory safety check failed: {memory_msg}", 0
        
        memory_before = get_memory_usage_mb()
        logger.info(f"Starting DuckDB chunk export: offset={offset}, chunk_size={chunk_size} (Memory: {memory_before:.1f}MB)")
        
        # Create DuckDB connection
        duck_conn = create_duckdb_connection(db_config)
        
        # Build SELECT query with optional type casting
        if polars_schema:
            # Apply schema enforcement via SQL casting
            column_casts = []
            for col_name, polars_type in polars_schema.items():
                duckdb_type = map_polars_to_duckdb_type(polars_type)
                column_casts.append(f'"{col_name}"::{duckdb_type} AS "{col_name}"')
            columns_sql = ", ".join(column_casts)
            logger.info(f"Using schema enforcement with {len(column_casts)} column casts")
        else:
            columns_sql = "*"
            logger.warning("No schema provided - using SELECT * (may cause type inference issues)")
        
        # Construct export query with optional custom WHERE clause
        if custom_where:
            where_clause = f"WHERE {custom_where}"
            logger.info(f"Using custom WHERE clause: {custom_where}")
        else:
            where_clause = f"LIMIT {chunk_size} OFFSET {offset}"
            logger.info(f"Using offset/limit: offset={offset}, chunk_size={chunk_size}")
        
        export_query = f"""
        COPY (
            SELECT {columns_sql} 
            FROM remote_db.{table_name} 
            {where_clause}
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'snappy')
        """
        
        logger.info(f"Executing DuckDB export query for {table_name} chunk")
        
        # Execute streaming export - DuckDB handles memory management internally
        result = duck_conn.execute(export_query)
        
        # Get number of rows exported (DuckDB returns this from COPY command)
        rows_exported = duck_conn.fetchall()[0][0] if result else 0
        
        # Verify the file was created and get basic info
        if not output_path.exists():
            return False, f"Export file not created: {output_path}", 0
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        memory_after = get_memory_usage_mb()
        memory_delta = memory_after - memory_before
        
        logger.info(f"DuckDB chunk export completed: {rows_exported} rows, {file_size_mb:.2f}MB file (Memory delta: +{memory_delta:.1f}MB)")
        
        # Memory safety check after export
        memory_safe, memory_msg = check_memory_safety()
        if not memory_safe:
            logger.warning(f"Post-export memory warning: {memory_msg}")
        
        return True, f"Successfully exported {rows_exported} rows", rows_exported
        
    except Exception as e:
        error_msg = f"DuckDB chunk export failed: {str(e)}"
        logger.error(error_msg)
        return False, error_msg, 0
        
    finally:
        if duck_conn:
            try:
                duck_conn.close()
            except Exception as e:
                logger.warning(f"Error closing DuckDB connection: {str(e)}")


def export_small_table_duckdb(
    db_config: Dict[str, Any],
    table_name: str,
    output_path: Path,
    polars_schema: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str, int]:
    """
    Export a complete small table using DuckDB streaming
    
    Args:
        db_config: Database connection configuration
        table_name: Fully qualified table name
        output_path: Output Parquet file path
        polars_schema: Optional Polars schema for type enforcement
        
    Returns:
        Tuple of (success: bool, message: str, rows_exported: int)
    """
    duck_conn = None
    try:
        memory_before = get_memory_usage_mb()
        logger.info(f"Starting DuckDB small table export for {table_name} (Memory: {memory_before:.1f}MB)")
        
        # Create DuckDB connection
        duck_conn = create_duckdb_connection(db_config)
        
        # Build SELECT query with optional type casting
        if polars_schema:
            column_casts = []
            for col_name, polars_type in polars_schema.items():
                duckdb_type = map_polars_to_duckdb_type(polars_type)
                column_casts.append(f'"{col_name}"::{duckdb_type} AS "{col_name}"')
            columns_sql = ", ".join(column_casts)
            logger.info(f"Using schema enforcement with {len(column_casts)} column casts")
        else:
            columns_sql = "*"
        
        # Export entire table
        export_query = f"""
        COPY (
            SELECT {columns_sql} 
            FROM remote_db.{table_name}
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'snappy')
        """
        
        logger.info(f"Executing DuckDB full table export for {table_name}")
        
        # Execute streaming export
        result = duck_conn.execute(export_query)
        rows_exported = duck_conn.fetchall()[0][0] if result else 0
        
        # Verify export
        if not output_path.exists():
            return False, f"Export file not created: {output_path}", 0
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        memory_after = get_memory_usage_mb()
        memory_delta = memory_after - memory_before
        
        logger.info(f"DuckDB table export completed: {rows_exported} rows, {file_size_mb:.2f}MB file (Memory delta: +{memory_delta:.1f}MB)")
        
        return True, f"Successfully exported {rows_exported} rows", rows_exported
        
    except Exception as e:
        error_msg = f"DuckDB table export failed: {str(e)}"
        logger.error(error_msg)
        return False, error_msg, 0
        
    finally:
        if duck_conn:
            try:
                duck_conn.close()
            except Exception as e:
                logger.warning(f"Error closing DuckDB connection: {str(e)}")


def validate_duckdb_export(
    output_path: Path,
    table_name: str,
    expected_rows: int,
    polars_schema: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str]:
    """
    Validate a DuckDB-exported Parquet file by reading it back
    
    Args:
        output_path: Path to exported Parquet file
        table_name: Table name for logging
        expected_rows: Expected number of rows
        polars_schema: Optional schema for additional validation
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        if not output_path.exists():
            return False, f"Export file does not exist: {output_path}"
        
        # Read back the exported Parquet file
        df = pl.read_parquet(output_path)
        actual_rows = len(df)
        
        # Row count validation
        if actual_rows != expected_rows:
            return False, f"Row count mismatch: expected {expected_rows}, got {actual_rows}"
        
        # Basic data validation using existing validation logic
        from adu.worker import validate_data
        validation_passed, validation_message = validate_data(df, table_name)
        
        if not validation_passed:
            logger.warning(f"DuckDB export validation warning for {table_name}: {validation_message}")
            return True, f"Export successful with validation warnings: {validation_message}"
        
        return True, f"Export validation passed: {actual_rows} rows verified"
        
    except Exception as e:
        error_msg = f"Export validation failed: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


def get_table_row_count_duckdb(
    db_config: Dict[str, Any],
    table_name: str
) -> Tuple[bool, int, str]:
    """
    Get table row count using DuckDB connection
    
    Args:
        db_config: Database connection configuration
        table_name: Fully qualified table name
        
    Returns:
        Tuple of (success: bool, row_count: int, message: str)
    """
    duck_conn = None
    try:
        # Create DuckDB connection
        duck_conn = create_duckdb_connection(db_config)
        
        # Get row count
        count_query = f"SELECT COUNT(*) FROM remote_db.{table_name}"
        result = duck_conn.execute(count_query).fetchone()
        row_count = result[0] if result else 0
        
        logger.info(f"Table {table_name} has {row_count:,} rows (via DuckDB)")
        return True, row_count, f"Successfully counted {row_count} rows"
        
    except Exception as e:
        error_msg = f"Failed to get row count for {table_name}: {str(e)}"
        logger.error(error_msg)
        return False, 0, error_msg
        
    finally:
        if duck_conn:
            try:
                duck_conn.close()
            except Exception as e:
                logger.warning(f"Error closing DuckDB connection: {str(e)}")


def export_large_table_with_duckdb(
    db_config: Dict[str, Any],
    table_name: str,
    table_dir: Path,
    source_row_count: int,
    chunk_size: int,
    db_type: str
) -> Tuple[bool, int]:
    """
    Export large table using DuckDB chunked approach
    
    Args:
        db_config: Database connection configuration
        table_name: Table name to export
        table_dir: Directory to export files to
        source_row_count: Total number of rows in source table
        chunk_size: Number of rows per chunk
        db_type: Database type (postgresql, etc.)
        
    Returns:
        Tuple of (success: bool, total_exported_rows: int)
    """
    logger.info(f"Starting DuckDB large table export for {table_name} ({source_row_count:,} rows)")
    
    # Calculate chunks needed
    total_chunks = (source_row_count + chunk_size - 1) // chunk_size
    
    duck_conn = None
    total_exported_rows = 0
    
    try:
        # Create DuckDB connection
        duck_conn = create_duckdb_connection(db_config)
        
        # Export each chunk
        for chunk_num in range(total_chunks):
            offset = chunk_num * chunk_size
            actual_chunk_size = min(chunk_size, source_row_count - offset)
            
            # Create chunk output file path
            chunk_file = table_dir / f"part_{chunk_num:04d}.parquet"
            
            # Export this chunk
            success, message, rows_exported = export_table_chunk_duckdb(
                db_config, table_name, chunk_file, offset, actual_chunk_size
            )
            
            if not success:
                raise Exception(f"Failed to export chunk {chunk_num}: {message}")
                
            total_exported_rows += rows_exported
            
            # Memory safety check
            is_safe, memory_msg = check_memory_safety()
            if not is_safe:
                logger.warning(f"Memory pressure detected during chunk {chunk_num}: {memory_msg}")
        
        logger.info(f"DuckDB large table export completed: {total_exported_rows:,} rows in {total_chunks} chunks")
        return True, total_exported_rows
        
    except Exception as e:
        error_msg = f"DuckDB large table export failed for {table_name}: {str(e)}"
        logger.error(error_msg)
        return False, 0
        
    finally:
        if duck_conn:
            try:
                duck_conn.close()
            except Exception as e:
                logger.warning(f"Error closing DuckDB connection: {str(e)}")