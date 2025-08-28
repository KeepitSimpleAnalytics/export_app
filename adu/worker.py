import sqlite3
import time
import traceback
import json
import os
import logging
import tempfile
from pathlib import Path
import polars as pl
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import vertica_python
import pandera as pa
import pandera.polars as pl_pa
import re
import multiprocessing
from adu.database import get_db_connection
from adu.database_type_mappings import create_polars_schema_from_database_metadata, get_type_mapping

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
        # Redact the log message
        record.msg = redact_sensitive_data(record.msg)
        
        # Redact any args that might contain sensitive data
        if hasattr(record, 'args') and record.args:
            record.args = tuple(redact_sensitive_data(arg) for arg in record.args)
        
        return True

# Configure logging with sensitive data filtering
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create file handler
file_handler = logging.FileHandler('/tmp/worker.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
file_handler.addFilter(SensitiveDataFilter())

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
console_handler.addFilter(SensitiveDataFilter())

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Prevent duplicate logs from the root logger's basic config
logger.propagate = False

# No encryption needed for airgapped environment

def get_database_connection(db_type, host, port, username, password, database=None):
    """Create database connection based on database type"""
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
        
        if db_type.lower() in ['postgresql', 'greenplum']:
            cursor.execute("""
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema_name, table_only))
        elif db_type.lower() == 'vertica':
            cursor.execute("""
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM v_catalog.columns 
                WHERE schema_name = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema_name, table_only))
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        columns_metadata = cursor.fetchall()
        
        if not columns_metadata:
            logging.warning(f"No schema information found for table {table_name}")
            return None
        
        # Create Polars schema from database metadata
        polars_schema = create_polars_schema_from_database_metadata(columns_metadata, db_type)
        
        logging.info(f"Retrieved schema for {table_name}: {len(columns_metadata)} columns")
        return polars_schema
        
    except Exception as e:
        logging.error(f"Failed to get schema for table {table_name}: {str(e)}")
        return None
    finally:
        cursor.close()

def create_basic_schema(df):
    """Create a basic Pandera schema from a DataFrame for validation"""
    try:
        schema_dict = {}
        
        for col_name in df.columns:
            col_data = df[col_name]
            dtype = col_data.dtype
            
            # Map Polars dtypes to Pandera checks
            if dtype == pl.Int64 or dtype == pl.Int32 or dtype == pl.Int16 or dtype == pl.Int8:
                schema_dict[col_name] = pl_pa.Column(pl.Int64, nullable=True)
            elif dtype == pl.Float64 or dtype == pl.Float32:
                schema_dict[col_name] = pl_pa.Column(pl.Float64, nullable=True)
            elif dtype == pl.Boolean:
                schema_dict[col_name] = pl_pa.Column(pl.Boolean, nullable=True)
            elif dtype == pl.Date:
                schema_dict[col_name] = pl_pa.Column(pl.Date, nullable=True)
            elif dtype == pl.Datetime:
                schema_dict[col_name] = pl_pa.Column(pl.Datetime, nullable=True)
            elif dtype == pl.Object:
                # Skip validation for Object types as they're often complex/mixed
                logging.info(f"Skipping validation for Object column '{col_name}'")
                continue
            else:  # String and other types
                schema_dict[col_name] = pl_pa.Column(pl.String, nullable=True)
        
        # Only create schema if we have columns to validate
        if schema_dict:
            return pl_pa.DataFrameSchema(schema_dict)
        else:
            logging.warning("No columns suitable for validation found")
            return None
        
    except Exception as e:
        logging.warning(f"Could not create schema for validation: {str(e)}")
        return None

def validate_data(df, table_name):
    """Validate DataFrame using Pandera with improved Object type handling"""
    try:
        # Check if DataFrame has any Object columns that need preprocessing
        object_columns = [col for col in df.columns if df[col].dtype == pl.Object]
        
        if object_columns:
            logging.info(f"Found Object columns in {table_name}: {object_columns}. Processing before validation...")
            # Process the DataFrame to handle Object types first
            df = handle_mixed_types(df)
        
        # Create basic schema
        schema = create_basic_schema(df)
        if schema is None:
            logging.warning(f"Skipping validation for {table_name} - could not create schema")
            return True, "Validation skipped - no suitable columns for validation"
        
        # Perform validation
        validated_df = schema.validate(df, lazy=True)
        logging.info(f"Data validation passed for {table_name}")
        return True, f"Validation passed - {len(df)} rows validated"
        
    except pa.errors.SchemaErrors as e:
        # Be more lenient with validation errors for complex data
        error_count = len(e.failure_cases)
        total_checks = len(df) * len(df.columns)
        error_rate = error_count / total_checks if total_checks > 0 else 0
        
        if error_rate < 0.1:  # Less than 10% error rate
            error_msg = f"Data validation passed with minor issues for {table_name}: {error_count} validation errors ({error_rate:.2%} error rate)"
            logging.warning(error_msg)
            return True, error_msg
        else:
            error_msg = f"Data validation failed for {table_name}: {error_count} validation errors ({error_rate:.2%} error rate)"
            logging.warning(error_msg)
            logging.debug(f"Validation errors for {table_name}: {e.failure_cases}")
            return False, error_msg
        
    except Exception as e:
        error_msg = f"Validation error for {table_name}: {str(e)}"
        logging.warning(error_msg)
        # For unexpected errors, allow the export to continue
        return True, f"Validation warning - {error_msg}"

def handle_mixed_types(df):
    """Handle mixed data types while preserving data integrity"""
    try:
        processed_columns = []
        
        for col_name in df.columns:
            col_data = df[col_name]
            dtype = col_data.dtype
            
            # Handle specific problematic data types
            if dtype == pl.Object:
                # For Object types, convert using map_elements to handle complex objects safely
                logging.warning(f"Column '{col_name}' has Object dtype, converting to string representation")
                try:
                    # First try to convert to string using map_elements for safer conversion
                    processed_columns.append(
                        pl.col(col_name).map_elements(
                            lambda x: str(x) if x is not None else None, 
                            return_dtype=pl.String
                        ).alias(col_name)
                    )
                except Exception as inner_e:
                    # If that fails, try to fill nulls first then convert
                    logging.warning(f"Standard Object conversion failed for '{col_name}', trying null-safe conversion: {str(inner_e)}")
                    processed_columns.append(
                        pl.col(col_name).fill_null("").map_elements(
                            lambda x: str(x) if x is not None else "", 
                            return_dtype=pl.String
                        ).alias(col_name)
                    )
            elif str(dtype).startswith('Decimal'):
                # Convert Decimal types to Float64 for Parquet compatibility
                logging.info(f"Column '{col_name}' has Decimal dtype, converting to Float64")
                processed_columns.append(pl.col(col_name).cast(pl.Float64))
            elif dtype == pl.List:
                # Convert List types to string representation
                logging.warning(f"Column '{col_name}' has List dtype, converting to string")
                processed_columns.append(pl.col(col_name).map_elements(lambda x: str(x) if x is not None else None, return_dtype=pl.String))
            elif dtype == pl.Struct:
                # Convert Struct types to JSON string
                logging.warning(f"Column '{col_name}' has Struct dtype, converting to JSON string")
                processed_columns.append(pl.col(col_name).map_elements(lambda x: str(x) if x is not None else None, return_dtype=pl.String))
            else:
                # Keep the original column for supported types
                processed_columns.append(pl.col(col_name))
        
        if processed_columns:
            df = df.with_columns(processed_columns)
        
        return df
        
    except Exception as e:
        logging.warning(f"Error in handle_mixed_types: {str(e)}, attempting safe fallback")
        # More robust fallback - convert problematic columns one by one
        try:
            safe_columns = []
            for col_name in df.columns:
                try:
                    # Try to keep the column as-is first
                    test_col = df.select(pl.col(col_name))
                    safe_columns.append(pl.col(col_name))
                except Exception:
                    # If that fails, force convert to string safely
                    logging.warning(f"Converting problematic column '{col_name}' to string")
                    safe_columns.append(
                        pl.col(col_name).map_elements(
                            lambda x: str(x) if x is not None else None,
                            return_dtype=pl.String
                        ).alias(col_name)
                    )
            
            return df.with_columns(safe_columns) if safe_columns else df
            
        except Exception as final_e:
            logging.error(f"All type conversion attempts failed: {str(final_e)}, using last resort conversion")
            # Absolute last resort - convert entire dataframe to strings column by column
            string_df_data = {}
            for col_name in df.columns:
                try:
                    col_values = df[col_name].to_list()
                    string_values = [str(val) if val is not None else None for val in col_values]
                    string_df_data[col_name] = string_values
                except Exception:
                    # If even that fails, create empty string column
                    string_df_data[col_name] = ["" for _ in range(df.height)]
            
            return pl.DataFrame(string_df_data)

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
    
    # Get strategy from config or use default
    strategy = 'clean_with_archive'  # Default strategy
    if config and 'export_organization' in config:
        strategy = config['export_organization'].get('strategy', 'clean_with_archive')
    
    if strategy == 'clean_with_archive':
        # Use temporary directory during export, clean final structure
        temp_path = base_path / '.temp' / job_id / safe_table_name
        final_path = base_path / safe_table_name
        archive_path = base_path / '.archive' / 'jobs'
    elif strategy == 'direct':
        # Export directly to final location (may overwrite)
        temp_path = base_path / safe_table_name
        final_path = base_path / safe_table_name
        archive_path = base_path / '.jobs'
    elif strategy == 'schema_first':
        # Organize by schema first
        schema_name = table_name.split('.')[0] if '.' in table_name else 'public'
        table_only = table_name.split('.')[1] if '.' in table_name else table_name
        temp_path = base_path / '.temp' / job_id / schema_name / table_only
        final_path = base_path / schema_name / table_only
        archive_path = base_path / '.archive' / 'jobs'
    else:
        # Legacy format as fallback
        temp_path = base_path / job_id / safe_table_name
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

def export_small_table_single_file(db_conn, table_name, table_dir, source_row_count, db_type='postgresql'):
    """Export small table as a single Parquet file"""
    try:
        # Get table schema to avoid inference issues
        polars_schema = get_table_schema(db_conn, db_type, table_name)
        
        # Read table data using Polars with explicit schema
        query = f"SELECT * FROM {table_name}"
        if not polars_schema:
            error_msg = f"No database schema available for {table_name}. Schema is required - inference is not allowed."
            logging.error(error_msg)
            return False, error_msg
            
        try:
            # Always use database schema - never inference
            df = pl.read_database(query, db_conn, schema_overrides=polars_schema)
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

def export_large_table_partitioned(db_conn, table_name, table_dir, source_row_count, chunk_size, db_type='postgresql'):
    """Export large table as multiple partitioned Parquet files"""
    try:
        # Get table schema to avoid inference issues across chunks
        polars_schema = get_table_schema(db_conn, db_type, table_name)
        if polars_schema:
            logging.info(f"Using explicit schema for {table_name} chunks with {len(polars_schema)} columns")
        else:
            logging.warning(f"No schema available for {table_name}, chunks may have inference issues")
        
        chunk_count = (source_row_count + chunk_size - 1) // chunk_size  # Ceiling division
        exported_files = []
        total_exported_rows = 0
        
        logging.info(f"Exporting {table_name} in {chunk_count} chunks of up to {chunk_size} rows each")
        
        for chunk_num in range(chunk_count):
            offset = chunk_num * chunk_size
            limit = min(chunk_size, source_row_count - offset)
            
            logging.info(f"Processing chunk {chunk_num + 1}/{chunk_count} (rows {offset + 1} to {offset + limit})")
            
            # Query with LIMIT and OFFSET for pagination with ORDER BY for deterministic results
            # ORDER BY is critical to ensure consistent, non-overlapping chunks
            query = f"SELECT * FROM {table_name} ORDER BY 1 LIMIT {limit} OFFSET {offset}"
            
            try:
                if not polars_schema:
                    error_msg = f"No database schema available for {table_name}. Schema is required - inference is not allowed."
                    logging.error(error_msg)
                    raise ValueError(error_msg)
                
                try:
                    # Always use database schema - never inference
                    df_chunk = pl.read_database(query, db_conn, schema_overrides=polars_schema)
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
                        df_chunk = pl.read_database(query, db_conn, schema_overrides=corrected_schema)
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
                
                logging.info(f"Successfully exported chunk {chunk_num + 1}/{chunk_count} with {verified_count} rows")
                
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
        
        logging.info(f"Successfully exported {table_name} as {chunk_count} partitioned files with {total_exported_rows} total rows")
        return True, total_exported_rows
        
    except Exception as e:
        logging.error(f"Failed to export large table {table_name}: {str(e)}")
        return False, str(e)

def get_table_partition_strategy(db_conn, table_name, target_chunk_size=1000000):
    """
    Analyze table structure to determine optimal partitioning strategy for large Greenplum tables.
    Uses table statistics and primary keys for efficient chunking.
    """
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
                    'strategy': 'range_partition',
                    'column': partition_column,
                    'min_value': min_val,
                    'max_value': max_val,
                    'range_step': range_step,
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
        # Simple fallback
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0]
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
        
        # Get table schema once to use across all chunks
        temp_conn = get_database_connection(**db_conn_params)
        polars_schema = get_table_schema(temp_conn, db_type, table_name)
        temp_conn.close()
        
        if polars_schema:
            logging.info(f"Using explicit schema for {table_name} parallel chunks with {len(polars_schema)} columns")
        else:
            logging.warning(f"No schema available for {table_name}, chunks may have inference issues")
        
        logging.info(f"Exporting {table_name} using {strategy} with {estimated_chunks} chunks in parallel (max {max_chunk_workers} concurrent)")
        
        # Prepare chunk tasks
        chunk_tasks = []
        
        if strategy == 'range_partition':
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
                
                chunk_tasks.append({
                    'chunk_num': i + 1,
                    'query': query,
                    'expected_range': (start_val, end_val)
                })
        else:
            # Offset-based partitioning (fallback)
            chunk_size = partition_info['chunk_size']
            for i in range(estimated_chunks):
                offset = i * chunk_size
                limit = min(chunk_size, total_rows - offset)
                
                query = f"SELECT * FROM {table_name} ORDER BY 1 LIMIT {limit} OFFSET {offset}"
                
                chunk_tasks.append({
                    'chunk_num': i + 1,
                    'query': query,
                    'expected_rows': limit
                })
        
        # Process chunks in parallel
        exported_files = []
        total_exported_rows = 0
        failed_chunks = []
        
        def process_chunk(chunk_task):
            """Process a single chunk in its own thread"""
            chunk_num = chunk_task['chunk_num']
            query = chunk_task['query']
            
            # Create dedicated connection for this chunk
            chunk_conn = get_database_connection(**db_conn_params)
            
            try:
                logging.info(f"Processing chunk {chunk_num}/{estimated_chunks} for {table_name}")
                
                # Read chunk with optimized settings and explicit schema
                if not polars_schema:
                    error_msg = f"No database schema available for {table_name}. Schema is required - inference is not allowed."
                    logging.error(error_msg)
                    raise ValueError(error_msg)
                
                try:
                    # Always use database schema - never inference
                    df_chunk = pl.read_database(
                        query, 
                        chunk_conn,
                        schema_overrides=polars_schema,
                        # Optimize for large datasets
                        iter_batches=True,
                        batch_size=50000  # Process in smaller batches to reduce memory
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
                            schema_overrides=string_schema,
                            iter_batches=True,
                            batch_size=50000
                        )
                        logging.warning(f"Successfully read chunk {chunk_num} with all columns converted to String type")
                    except Exception as retry_error:
                        error_msg = f"Failed to read chunk {chunk_num} even with String fallback schema: {str(retry_error)}"
                        logging.error(error_msg)
                        raise Exception(error_msg)
                
                # If iter_batches is used, we need to collect batches
                if hasattr(df_chunk, '__iter__'):
                    chunks = []
                    for batch in df_chunk:
                        chunks.append(batch)
                    df_chunk = pl.concat(chunks) if chunks else pl.DataFrame()
                
                chunk_row_count = len(df_chunk)
                logging.info(f"Chunk {chunk_num} loaded {chunk_row_count} rows")
                
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
                
                logging.info(f"Successfully exported chunk {chunk_num} with {verified_count} rows")
                
                return {
                    'chunk_num': chunk_num,
                    'filename': chunk_filename,
                    'row_count': verified_count,
                    'success': True
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
                chunk_conn.close()
        
        # Execute chunks in parallel
        with ThreadPoolExecutor(max_workers=max_chunk_workers, thread_name_prefix=f"Chunk-{table_name}") as executor:
            chunk_futures = {executor.submit(process_chunk, task): task for task in chunk_tasks}
            
            for future in as_completed(chunk_futures):
                result = future.result()
                
                if result['success']:
                    exported_files.append(result['filename'])
                    total_exported_rows += result['row_count']
                else:
                    failed_chunks.append(result)
        
        # Check if all chunks succeeded
        if failed_chunks:
            error_msg = f"Failed chunks: {[f['chunk_num'] for f in failed_chunks]}"
            logging.error(error_msg)
            return False, error_msg
        
        # Final validation
        if abs(total_exported_rows - total_rows) > (total_rows * 0.01):  # Allow 1% variance
            logging.warning(f"Row count variance detected: expected {total_rows}, got {total_exported_rows}")
        
        # Write metadata
        chunk_info = {
            'chunk_count': len(exported_files),
            'chunk_strategy': strategy,
            'chunk_size': partition_info.get('chunk_size', 1000000),  # Add missing chunk_size
            'files': sorted(exported_files),
            'parallel_processing': True,
            'max_workers': max_chunk_workers
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

def export_table_to_parquet(db_conn, table_name, output_path, job_id, chunk_size=1000000, max_chunk_workers=None, config=None, db_type='postgresql'):
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
        
        # Check if table export already exists and is complete
        if check_existing_export(table_dir, source_row_count, table_name):
            return True, source_row_count
        
        # Determine export strategy based on table size
        if source_row_count <= chunk_size:
            # Small table - single file export
            logging.info(f"Table {table_name} has {source_row_count:,} rows, exporting as single file")
            return export_small_table_single_file(db_conn, table_name, table_dir, source_row_count, db_type)
        
        elif source_row_count <= chunk_size * 2:
            # Small multi-chunk table - use sequential chunking only for very small tables
            logging.info(f"Table {table_name} has {source_row_count:,} rows, using sequential chunking")
            return export_large_table_partitioned(db_conn, table_name, table_dir, source_row_count, chunk_size, db_type)
        
        else:
            # Very large table - use optimized parallel chunking
            logging.info(f"Table {table_name} has {source_row_count:,} rows, using optimized parallel chunking")
            
            # Analyze optimal partitioning strategy
            partition_info = get_table_partition_strategy(db_conn, table_name, chunk_size)
            
            # Auto-detect optimal chunk workers if not specified
            if max_chunk_workers is None:
                # For very large tables, use more workers but cap based on system resources
                max_chunk_workers = min(10, max(4, multiprocessing.cpu_count() // 2))  # More aggressive for high-end systems
            
            # Get connection parameters for parallel processing
            dsn_params = db_conn.get_dsn_parameters()
            db_conn_params = {
                'db_type': 'postgresql',  # Greenplum uses PostgreSQL protocol
                'host': dsn_params.get('host', 'localhost'),
                'port': int(dsn_params.get('port', 5432)),
                'username': dsn_params.get('user', ''),
                'database': dsn_params.get('dbname', ''),
                'password': dsn_params.get('password', '')  # Extract password from connection if available
            }
            
            # If password is still empty, it might be a trusted connection or connection string auth
            if not db_conn_params['password']:
                logging.info("Using trusted authentication or connection string auth for parallel connections")
            
            # Use parallel processing for better performance
            return export_large_table_parallel_chunks(db_conn_params, table_name, table_dir, partition_info, max_chunk_workers)
        
    except Exception as e:
        logging.error(f"Failed to export table {table_name}: {str(e)}")
        return False, str(e)

def process_single_table(job_id, table_name, db_conn_params, output_path, chunk_size, max_chunk_workers=None, config=None):
    """Process a single table - designed to be thread-safe with optimized large table handling"""
    table_start_time = time.strftime('%Y-%m-%d %H:%M:%S')
    db_type = db_conn_params.get('db_type', 'postgresql')
    
    # Each thread gets its own database connections
    sqlite_conn = get_db_connection()
    cursor = sqlite_conn.cursor()
    
    # Create separate database connection for this thread
    db_conn = get_database_connection(**db_conn_params)
    
    try:
        # Check if table export is already in progress or completed (thread-safe check)
        cursor.execute("BEGIN IMMEDIATE")  # Lock for thread safety
        export_status = check_table_export_status(cursor, job_id, table_name)
        
        if export_status == 'completed':
            logging.info(f"Table {table_name} already completed, skipping")
            cursor.execute("COMMIT")
            sqlite_conn.close()
            db_conn.close()
            return {'table': table_name, 'status': 'already_completed', 'result': 'skipped'}
        elif export_status == 'processing':
            logging.warning(f"Table {table_name} is already being processed, skipping to prevent duplicates")
            cursor.execute("COMMIT")
            sqlite_conn.close()
            db_conn.close()
            return {'table': table_name, 'status': 'already_processing', 'result': 'skipped'}
        
        # Insert or update table export record
        if export_status == 'retry':
            cursor.execute(
                "UPDATE table_exports SET status = ?, start_time = ?, end_time = NULL, error_message = NULL WHERE job_id = ? AND table_name = ?",
                ('processing', table_start_time, job_id, table_name)
            )
        else:  # new
            cursor.execute(
                "INSERT INTO table_exports (job_id, table_name, status, start_time) VALUES (?, ?, ?, ?)",
                (job_id, table_name, 'processing', table_start_time)
            )
        cursor.execute("COMMIT")
        
        # Check table size to determine if we should use parallel chunking
        cursor_temp = db_conn.cursor()
        cursor_temp.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor_temp.fetchone()[0]
        cursor_temp.close()
        
        # For very large tables (>10M rows), use optimized parallel chunking
        if row_count > chunk_size * 10:
            logging.info(f"Table {table_name} has {row_count:,} rows - using optimized parallel processing")
            
            # Get table analysis
            partition_info = get_table_partition_strategy(db_conn, table_name, chunk_size)
            
            # Auto-detect optimal chunk workers if not specified
            if max_chunk_workers is None:
                max_chunk_workers = min(8, max(4, multiprocessing.cpu_count() // 2))  # More aggressive for high-end systems
            
            # Create table directory using new path strategy
            table_temp_path, table_final_path, _ = get_export_paths(output_path, job_id, table_name, config)
            table_dir = table_temp_path
            table_dir.mkdir(parents=True, exist_ok=True)
            
            # Check existing export
            if check_existing_export(table_dir, row_count, table_name):
                success, result = True, row_count
            else:
                # Use parallel chunking
                success, result = export_large_table_parallel_chunks(
                    db_conn_params, table_name, table_dir, partition_info, max_chunk_workers
                )
        else:
            # Use standard export for smaller tables
            success, result = export_table_to_parquet(
                db_conn, table_name, output_path, job_id, chunk_size, max_chunk_workers, config, db_type
            )
        
        table_end_time = time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Update results in database (thread-safe)
        cursor.execute("BEGIN IMMEDIATE")
        if success:
            row_count_result = result
            # Store temporary path in database, will be updated after job organization
            table_temp_path, _, _ = get_export_paths(output_path, job_id, table_name, config)
            
            cursor.execute(
                "UPDATE table_exports SET status = ?, row_count = ?, file_path = ?, end_time = ? WHERE job_id = ? AND table_name = ?",
                ('completed', row_count_result, str(table_temp_path), table_end_time, job_id, table_name)
            )
            logging.info(f"Successfully exported {table_name} ({result:,} rows)")
            cursor.execute("COMMIT")
            return {'table': table_name, 'status': 'success', 'result': row_count_result}
        else:
            cursor.execute(
                "UPDATE table_exports SET status = ?, error_message = ?, end_time = ? WHERE job_id = ? AND table_name = ?",
                ('failed', str(result), table_end_time, job_id, table_name)
            )
            cursor.execute(
                "INSERT INTO errors (job_id, timestamp, error_message, traceback, context) VALUES (?, ?, ?, ?, ?)",
                (job_id, time.strftime('%Y-%m-%d %H:%M:%S'), 
                 f"Table export failed: {table_name}", str(result), 
                 json.dumps({"table": table_name}))
            )
            cursor.execute("COMMIT")
            return {'table': table_name, 'status': 'failed', 'result': str(result)}
            
    except Exception as table_error:
        table_end_time = time.strftime('%Y-%m-%d %H:%M:%S')
        error_msg = f"Failed to process table {table_name}: {str(table_error)}"
        logging.error(error_msg)
        
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute(
                "UPDATE table_exports SET status = ?, error_message = ?, end_time = ? WHERE job_id = ? AND table_name = ?",
                ('failed', error_msg, table_end_time, job_id, table_name)
            )
            cursor.execute(
                "INSERT INTO errors (job_id, timestamp, error_message, traceback, context) VALUES (?, ?, ?, ?, ?)",
                (job_id, time.strftime('%Y-%m-%d %H:%M:%S'), 
                 error_msg, traceback.format_exc(), 
                 json.dumps({"table": table_name}))
            )
            cursor.execute("COMMIT")
        except:
            pass  # Ignore database errors during error handling
        
        return {'table': table_name, 'status': 'failed', 'result': error_msg}
    finally:
        sqlite_conn.close()
        db_conn.close()

def process_data(job_id, config):
    """Main data processing function with actual database connections"""
    logging.info(f"Starting job {job_id}")
    
    # Setup SQLite connection for job tracking
    sqlite_conn = get_db_connection()
    cursor = sqlite_conn.cursor()
    
    # Set up default export organization if not specified
    if 'export_organization' not in config:
        config['export_organization'] = {
            'strategy': 'clean_with_archive',
            'conflict_resolution': 'version',
            'preserve_job_history': True,
            'auto_cleanup_temp': True
        }
    
    # No decryption needed for airgapped environment
    password = config['db_password']
    
    # Extract configuration
    db_type = config['db_type']
    host = config['db_host']
    port = int(config['db_port'])
    username = config['db_username']
    database = config.get('db_name')
    tables_to_export = config.get('tables', [])
    output_path = config.get('output_path', '/app/exports')
    chunk_size = config.get('chunk_size', 1000000)  # Default 1M rows per chunk
    
    # Validate chunk_size
    if chunk_size < 10000:
        logging.warning(f"Chunk size {chunk_size} is very small, setting to minimum of 10,000")
        chunk_size = 10000
    elif chunk_size > 10000000:
        logging.warning(f"Chunk size {chunk_size} is very large, setting to maximum of 10,000,000")
        chunk_size = 10000000
    
    logging.info(f"Using chunk size of {chunk_size:,} rows for large table partitioning")
    
    total_tables = 0
    successful_tables = 0
    failed_tables = 0
    
    try:
        logging.info(f"Connecting to {db_type} at {host}:{port} with user {username}")
        
        # Connect to source database
        db_conn = get_database_connection(db_type, host, port, username, password, database)
        
        # If no tables specified, discover all tables
        if not tables_to_export:
            tables_to_export = discover_tables(db_conn, db_type)
            logging.info(f"Discovered {len(tables_to_export)} tables")
        
        total_tables = len(tables_to_export)
        
        # Close the initial connection as each thread will create its own
        db_conn.close()
        
        # Prepare database connection parameters for threads
        db_conn_params = {
            'db_type': db_type,
            'host': host,
            'port': port,
            'username': username,
            'password': password,
            'database': database
        }
        
        # Get number of threads from config or use default
        max_workers = config.get('max_threads', min(12, len(tables_to_export)))  # Default to 12 threads for high-end systems
        max_chunk_workers = config.get('max_chunk_workers', min(8, max(4, multiprocessing.cpu_count() // 2)))  # Default to 8 chunk workers for high-end systems
        
        logging.info(f"Processing {total_tables} tables using {max_workers} worker threads")
        logging.info(f"Large tables will use up to {max_chunk_workers} parallel chunk workers")
        
        # Process tables in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="TableExport") as executor:
            # Submit all table export tasks
            future_to_table = {
                executor.submit(process_single_table, job_id, table_name, db_conn_params, output_path, chunk_size, max_chunk_workers, config): table_name
                for table_name in tables_to_export
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    result = future.result()
                    if result['status'] in ['success', 'already_completed']:
                        successful_tables += 1
                    elif result['status'] == 'failed':
                        failed_tables += 1
                    # Skip counting 'already_processing' as they're handled by other threads
                    
                    logging.info(f"Thread completed for table {table_name}: {result['status']}")
                    
                except Exception as exc:
                    failed_tables += 1
                    error_msg = f"Thread failed for table {table_name}: {str(exc)}"
                    logging.error(error_msg)
                    
                    # Log the error to database
                    try:
                        cursor.execute(
                            "INSERT INTO errors (job_id, timestamp, error_message, traceback, context) VALUES (?, ?, ?, ?, ?)",
                            (job_id, time.strftime('%Y-%m-%d %H:%M:%S'), 
                             error_msg, traceback.format_exc(), 
                             json.dumps({"table": table_name, "thread_error": True}))
                        )
                        sqlite_conn.commit()
                    except:
                        pass  # Ignore database errors during error handling
        
        # Update job status based on results
        if failed_tables == 0:
            status = 'complete'
            logging.info(f"Job {job_id} completed successfully. {successful_tables}/{total_tables} tables exported.")
        else:
            status = 'complete_with_errors'
            logging.warning(f"Job {job_id} completed with errors. {successful_tables}/{total_tables} tables exported successfully.")
        
        # Organize exports to clean structure after job completion
        logging.info(f"Organizing exports for job {job_id} to clean directory structure...")
        organization_success = organize_completed_export(job_id, output_path, config)
        
        if organization_success:
            logging.info(f"Export organization completed successfully for job {job_id}")
        else:
            logging.warning(f"Export organization had issues for job {job_id}, but exports are still available")
        
        cursor.execute("UPDATE jobs SET overall_status = ?, end_time = ? WHERE job_id = ?",
                       (status, time.strftime('%Y-%m-%d %H:%M:%S'), job_id))
        sqlite_conn.commit()

    except Exception as e:
        # Log the error to the database
        error_message = str(e)
        tb = traceback.format_exc()
        logging.error(f"Job {job_id} failed: {error_message}")
        
        cursor.execute("INSERT INTO errors (job_id, timestamp, error_message, traceback, context) VALUES (?, ?, ?, ?, ?)",
                       (job_id, time.strftime('%Y-%m-%d %H:%M:%S'), error_message, tb, json.dumps(config)))
        cursor.execute("UPDATE jobs SET overall_status = ? WHERE job_id = ?",
                       ('complete_with_errors', job_id))
        sqlite_conn.commit()

    finally:
        sqlite_conn.close()
        logging.info(f"Finished job {job_id}")

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
        else:
            # Wait for a bit before polling again
            time.sleep(5)

if __name__ == '__main__':
    logging.info("Starting worker...")
    poll_for_jobs()