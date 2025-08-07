import sqlite3
import time
import traceback
import json
import os
import logging
from pathlib import Path
from cryptography.fernet import Fernet
import polars as pl
import psycopg2
import vertica_python
import pandera as pa
import pandera.polars as pl_pa
import re
from database import get_db_connection

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

# Get the Fernet key from the environment
key = os.environ.get('FERNET_KEY')
if key:
    fernet = Fernet(key.encode())
else:
    # Handle the case where the key is not set
    logging.error("FERNET_KEY environment variable not set.")
    exit(1)

def get_database_connection(db_type, host, port, username, password, database=None):
    """Create database connection based on database type"""
    if db_type.lower() == 'postgresql':
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
    
    if db_type.lower() == 'postgresql':
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
    
    if db_type.lower() == 'postgresql':
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
            else:  # String and other types
                schema_dict[col_name] = pl_pa.Column(pl.String, nullable=True)
        
        return pl_pa.DataFrameSchema(schema_dict)
        
    except Exception as e:
        logging.warning(f"Could not create schema for validation: {str(e)}")
        return None

def validate_data(df, table_name):
    """Validate DataFrame using Pandera"""
    try:
        # Create basic schema
        schema = create_basic_schema(df)
        if schema is None:
            logging.warning(f"Skipping validation for {table_name} - could not create schema")
            return True, "Validation skipped"
        
        # Perform validation
        validated_df = schema.validate(df, lazy=True)
        logging.info(f"Data validation passed for {table_name}")
        return True, f"Validation passed - {len(df)} rows validated"
        
    except pa.errors.SchemaErrors as e:
        error_msg = f"Data validation failed for {table_name}: {len(e.failure_cases)} validation errors"
        logging.warning(error_msg)
        logging.debug(f"Validation errors for {table_name}: {e.failure_cases}")
        return False, error_msg
        
    except Exception as e:
        error_msg = f"Validation error for {table_name}: {str(e)}"
        logging.warning(error_msg)
        return False, error_msg

def export_table_to_parquet(db_conn, table_name, output_path, job_id):
    """Export a single table to Parquet format using Polars with data validation"""
    try:
        # Read table data using Polars
        query = f"SELECT * FROM {table_name}"
        df = pl.read_database(query, db_conn)
        
        # Validate data before export
        validation_passed, validation_message = validate_data(df, table_name)
        if not validation_passed:
            logging.warning(f"Data validation failed for {table_name}: {validation_message}")
            # Continue with export but log the validation failure
        
        # Create output directory if it doesn't exist
        output_dir = Path(output_path) / job_id
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Write to Parquet (replace dots in table names for file safety)
        safe_table_name = table_name.replace('.', '_')
        parquet_file = output_dir / f"{safe_table_name}.parquet"
        df.write_parquet(parquet_file)
        
        result_message = f"Exported {len(df)} rows"
        if not validation_passed:
            result_message += f" (validation warning: {validation_message})"
        
        logging.info(f"Exported {table_name} to {parquet_file} ({result_message})")
        return True, len(df)
        
    except Exception as e:
        logging.error(f"Failed to export table {table_name}: {str(e)}")
        return False, str(e)

def process_data(job_id, config):
    """Main data processing function with actual database connections"""
    logging.info(f"Starting job {job_id}")
    
    # Setup SQLite connection for job tracking
    sqlite_conn = get_db_connection()
    cursor = sqlite_conn.cursor()
    
    # Decrypt the password
    password = fernet.decrypt(config['db_password'].encode()).decode()
    
    # Extract configuration
    db_type = config['db_type']
    host = config['db_host']
    port = int(config['db_port'])
    username = config['db_username']
    database = config.get('db_name')
    tables_to_export = config.get('tables', [])
    output_path = config.get('output_path', '/app/exports')
    
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
        
        # Process each table
        for table_name in tables_to_export:
            table_start_time = time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Insert initial table export record
            cursor.execute(
                "INSERT INTO table_exports (job_id, table_name, status, start_time) VALUES (?, ?, ?, ?)",
                (job_id, table_name, 'processing', table_start_time)
            )
            sqlite_conn.commit()
            
            try:
                success, result = export_table_to_parquet(db_conn, table_name, output_path, job_id)
                table_end_time = time.strftime('%Y-%m-%d %H:%M:%S')
                
                if success:
                    successful_tables += 1
                    row_count = result
                    safe_table_name = table_name.replace('.', '_')
                    file_path = f"{output_path}/{job_id}/{safe_table_name}.parquet"
                    
                    # Update table export record with success
                    cursor.execute(
                        "UPDATE table_exports SET status = ?, row_count = ?, file_path = ?, end_time = ? WHERE job_id = ? AND table_name = ?",
                        ('completed', row_count, file_path, table_end_time, job_id, table_name)
                    )
                    logging.info(f"Successfully exported {table_name} ({result} rows)")
                else:
                    failed_tables += 1
                    # Update table export record with failure
                    cursor.execute(
                        "UPDATE table_exports SET status = ?, error_message = ?, end_time = ? WHERE job_id = ? AND table_name = ?",
                        ('failed', str(result), table_end_time, job_id, table_name)
                    )
                    
                    # Log table-specific error
                    cursor.execute(
                        "INSERT INTO errors (job_id, timestamp, error_message, traceback, context) VALUES (?, ?, ?, ?, ?)",
                        (job_id, time.strftime('%Y-%m-%d %H:%M:%S'), 
                         f"Table export failed: {table_name}", str(result), 
                         json.dumps({"table": table_name, "config": config}))
                    )
                    
            except Exception as table_error:
                failed_tables += 1
                table_end_time = time.strftime('%Y-%m-%d %H:%M:%S')
                error_msg = f"Failed to process table {table_name}: {str(table_error)}"
                logging.error(error_msg)
                
                # Update table export record with failure
                cursor.execute(
                    "UPDATE table_exports SET status = ?, error_message = ?, end_time = ? WHERE job_id = ? AND table_name = ?",
                    ('failed', error_msg, table_end_time, job_id, table_name)
                )
                
                cursor.execute(
                    "INSERT INTO errors (job_id, timestamp, error_message, traceback, context) VALUES (?, ?, ?, ?, ?)",
                    (job_id, time.strftime('%Y-%m-%d %H:%M:%S'), 
                     error_msg, traceback.format_exc(), 
                     json.dumps({"table": table_name, "config": config}))
                )
            
            sqlite_conn.commit()
        
        # Close database connection
        db_conn.close()
        
        # Update job status based on results
        if failed_tables == 0:
            status = 'complete'
            logging.info(f"Job {job_id} completed successfully. {successful_tables}/{total_tables} tables exported.")
        else:
            status = 'complete_with_errors'
            logging.warning(f"Job {job_id} completed with errors. {successful_tables}/{total_tables} tables exported successfully.")
        
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