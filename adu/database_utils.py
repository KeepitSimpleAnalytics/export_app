"""
Database utility functions for ADU Export Application.

This module contains shared database connection and schema detection functions
to avoid circular imports between worker.py and other modules.
"""

import logging
import psycopg2
import vertica_python
from adu.database_type_mappings import create_polars_schema_from_database_metadata


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