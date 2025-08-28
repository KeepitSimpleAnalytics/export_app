"""
Database Type Mappings for Polars DataFrame Schema Generation

This module provides comprehensive mappings between database types and Polars types
for PostgreSQL, Greenplum, and Vertica databases.
"""

import polars as pl

# PostgreSQL/Greenplum Type Mappings
POSTGRESQL_TYPE_MAPPING = {
    # Integer types
    'smallint': pl.Int16,
    'int2': pl.Int16,
    'integer': pl.Int32,
    'int': pl.Int32,
    'int4': pl.Int32,
    'bigint': pl.Int64,
    'int8': pl.Int64,
    'serial': pl.Int32,
    'bigserial': pl.Int64,
    
    # Floating point types
    'real': pl.Float32,
    'float4': pl.Float32,
    'double precision': pl.Float64,
    'float8': pl.Float64,
    'float': pl.Float64,
    
    # Decimal/Numeric types (use Float64 to avoid precision issues)
    'numeric': pl.Float64,
    'decimal': pl.Float64,
    'money': pl.Float64,
    
    # Boolean type
    'boolean': pl.Boolean,
    'bool': pl.Boolean,
    
    # Date/Time types
    'date': pl.Date,
    'time': pl.Time,
    'time without time zone': pl.Time,
    'time with time zone': pl.Time,
    'timestamp': pl.Datetime,
    'timestamp without time zone': pl.Datetime,
    'timestamp with time zone': pl.Datetime,
    'timestamptz': pl.Datetime,
    'interval': pl.String,  # Store as string for compatibility
    
    # String/Text types
    'character varying': pl.String,
    'varchar': pl.String,
    'character': pl.String,
    'char': pl.String,
    'text': pl.String,
    'name': pl.String,
    
    # Binary types
    'bytea': pl.Binary,
    
    # JSON types (store as string)
    'json': pl.String,
    'jsonb': pl.String,
    
    # Network types (store as string)
    'inet': pl.String,
    'cidr': pl.String,
    'macaddr': pl.String,
    'macaddr8': pl.String,
    
    # UUID type
    'uuid': pl.String,
    
    # Array types (store as string representation)
    'array': pl.String,
    
    # Geometric types (store as string)
    'point': pl.String,
    'line': pl.String,
    'lseg': pl.String,
    'box': pl.String,
    'path': pl.String,
    'polygon': pl.String,
    'circle': pl.String,
    
    # Range types (store as string)
    'int4range': pl.String,
    'int8range': pl.String,
    'numrange': pl.String,
    'tsrange': pl.String,
    'tstzrange': pl.String,
    'daterange': pl.String,
}

# Vertica Type Mappings
VERTICA_TYPE_MAPPING = {
    # Integer types
    'int': pl.Int64,
    'integer': pl.Int64,
    'bigint': pl.Int64,
    'smallint': pl.Int16,
    'tinyint': pl.Int8,
    'auto_increment': pl.Int64,
    
    # Floating point types
    'float': pl.Float64,
    'float8': pl.Float64,
    'real': pl.Float32,
    'double precision': pl.Float64,
    
    # Decimal/Numeric types
    'numeric': pl.Float64,
    'decimal': pl.Float64,
    'money': pl.Float64,
    
    # Boolean type
    'boolean': pl.Boolean,
    'bool': pl.Boolean,
    
    # Date/Time types
    'date': pl.Date,
    'time': pl.Time,
    'time with time zone': pl.Time,
    'timestamp': pl.Datetime,
    'timestamp with time zone': pl.Datetime,
    'timestamptz': pl.Datetime,
    'interval': pl.String,
    'interval day': pl.String,
    'interval day to hour': pl.String,
    'interval day to minute': pl.String,
    'interval day to second': pl.String,
    'interval hour': pl.String,
    'interval hour to minute': pl.String,
    'interval hour to second': pl.String,
    'interval minute': pl.String,
    'interval minute to second': pl.String,
    'interval second': pl.String,
    'interval year': pl.String,
    'interval year to month': pl.String,
    'interval month': pl.String,
    
    # String/Text types
    'varchar': pl.String,
    'char': pl.String,
    'long varchar': pl.String,
    'text': pl.String,
    
    # Binary types
    'binary': pl.Binary,
    'varbinary': pl.Binary,
    'long varbinary': pl.Binary,
    'bytea': pl.Binary,
    
    # UUID type
    'uuid': pl.String,
}

# Greenplum inherits from PostgreSQL with some additions
GREENPLUM_TYPE_MAPPING = POSTGRESQL_TYPE_MAPPING.copy()
GREENPLUM_TYPE_MAPPING.update({
    # Greenplum-specific types (if any)
    'gp_segment_id': pl.Int32,
})

def get_type_mapping(db_type):
    """
    Get the appropriate type mapping dictionary for a database type
    
    Args:
        db_type (str): Database type ('postgresql', 'greenplum', 'vertica')
        
    Returns:
        dict: Mapping from database types to Polars types
    """
    db_type_lower = db_type.lower()
    
    if db_type_lower in ['postgresql', 'postgres']:
        return POSTGRESQL_TYPE_MAPPING
    elif db_type_lower == 'greenplum':
        return GREENPLUM_TYPE_MAPPING
    elif db_type_lower == 'vertica':
        return VERTICA_TYPE_MAPPING
    else:
        # Default to PostgreSQL mapping
        return POSTGRESQL_TYPE_MAPPING

def map_database_type_to_polars(database_type, db_type='postgresql'):
    """
    Map a single database type to its corresponding Polars type
    
    Args:
        database_type (str): The database column type
        db_type (str): The database system type
        
    Returns:
        polars.DataType: The corresponding Polars data type
    """
    type_mapping = get_type_mapping(db_type)
    
    # Normalize the database type
    normalized_type = database_type.lower().strip()
    
    # Handle parameterized types (e.g., varchar(255), numeric(10,2))
    if '(' in normalized_type:
        base_type = normalized_type.split('(')[0].strip()
    else:
        base_type = normalized_type
    
    # Look up the type, with fallbacks
    if base_type in type_mapping:
        return type_mapping[base_type]
    
    # Handle types that start with known prefixes
    for db_type_prefix, polars_type in type_mapping.items():
        if base_type.startswith(db_type_prefix):
            return polars_type
    
    # Default fallback to String for unknown types
    return pl.String

def create_polars_schema_from_database_metadata(columns_metadata, db_type='postgresql'):
    """
    Create a Polars schema dictionary from database column metadata
    
    Args:
        columns_metadata (list): List of tuples (column_name, data_type, ...)
        db_type (str): Database type for proper type mapping
        
    Returns:
        dict: Dictionary mapping column names to Polars types
    """
    schema = {}
    
    for column_info in columns_metadata:
        column_name = column_info[0]
        data_type = column_info[1]
        
        polars_type = map_database_type_to_polars(data_type, db_type)
        schema[column_name] = polars_type
    
    return schema

def get_schema_override_hints(db_type='postgresql'):
    """
    Get schema override hints for common problematic type conversions
    
    Args:
        db_type (str): Database type
        
    Returns:
        dict: Common override patterns for schema issues
    """
    hints = {
        'string_fallbacks': [
            'json', 'jsonb', 'xml', 'inet', 'cidr', 'macaddr', 'uuid',
            'point', 'line', 'polygon', 'circle', 'path', 'box',
            'interval', 'tsrange', 'daterange'
        ],
        'float64_fallbacks': [
            'numeric', 'decimal', 'money'
        ],
        'problematic_types': {
            # Types that often cause inference issues
            'text': pl.String,
            'varchar': pl.String,
            'char': pl.String,
            'json': pl.String,
            'jsonb': pl.String,
            'uuid': pl.String,
        }
    }
    
    return hints
