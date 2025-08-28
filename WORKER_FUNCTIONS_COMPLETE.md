# ADU Worker Functions - Complete Documentation

## Overview
The `adu/worker.py` file contains 26 specialized functions for database export operations, with comprehensive support for PostgreSQL, Greenplum, and Vertica databases. The system uses database-driven schema extraction to ensure data type consistency across parallel processing operations.

## Core Architecture

### Database Schema Management
- **Schema-Driven Processing**: All data reading operations use database metadata rather than inference
- **Multi-Database Support**: PostgreSQL, Greenplum, and Vertica with unified type mapping
- **Consistent Types**: Eliminates schema inference conflicts in parallel chunk processing

### Async Processing Integration
- **Celery Tasks**: Full integration with async job processing system
- **Progress Tracking**: Real-time job status and cancellation support
- **Production Ready**: Multi-worker deployment with Redis broker

## Function Catalog

### 1. Database Connection and Discovery Functions

#### `get_database_connection(**conn_params)`
- **Purpose**: Establishes database connections with standardized parameters
- **Returns**: Database connection object
- **Usage**: Central connection factory for all database operations

#### `discover_tables_by_schema(db_conn, db_type)`
- **Purpose**: Discovers all tables with full schema information (schema.table_name)
- **Parameters**: Database connection, database type (postgresql/greenplum/vertica)
- **Returns**: List of table info dictionaries with full names and metadata
- **Usage**: Primary table discovery method for export operations

#### `discover_tables(db_conn, db_type)`
- **Purpose**: Legacy compatibility function - returns simple table name list
- **Returns**: List of full table names (schema.table_name format)
- **Usage**: Backward compatibility for existing code

#### `get_tables_with_row_counts(db_conn, db_type)`
- **Purpose**: Gets all tables with their exact row counts for sizing decisions
- **Returns**: Dictionary mapping table names to row counts
- **Usage**: Export strategy planning and progress estimation

#### `get_table_row_count(db_conn, table_name)`
- **Purpose**: Gets exact row count for a specific table
- **Returns**: Integer row count
- **Usage**: Table partitioning and chunk size calculations

### 2. Schema Extraction and Type Management

#### `detect_database_type(db_conn)`
- **Purpose**: Auto-detects database type from connection version info
- **Returns**: String ('postgresql', 'greenplum', 'vertica')
- **Usage**: Enables database-specific schema handling

#### `get_table_schema_from_database(db_conn, table_name, db_type=None)`
- **Purpose**: Extracts table schema directly from database metadata
- **Parameters**: 
  - `db_conn`: Database connection
  - `table_name`: Full table name (schema.table_name)
  - `db_type`: Database type (auto-detected if None)
- **Returns**: Polars schema dictionary mapping column names to types
- **Features**:
  - Auto-detection of database type if not provided
  - Unified type mapping across PostgreSQL/Greenplum/Vertica
  - Comprehensive data type support (numeric, temporal, string, binary, JSON, UUID, network types)
  - Error handling with detailed logging
- **Usage**: Core function for schema-driven data processing

#### `read_database_with_schema(query, db_conn, table_schema)`
- **Purpose**: Reads database query results using predefined schema (no inference)
- **Parameters**:
  - `query`: SQL query string
  - `db_conn`: Database connection
  - `table_schema`: Polars schema dictionary from get_table_schema_from_database
- **Returns**: Polars DataFrame with enforced schema
- **Features**:
  - Uses pandas for initial read, converts to polars with schema enforcement
  - Fallback to polars connectorx if available
  - Eliminates schema inference conflicts in parallel processing
- **Usage**: All data reading operations for consistent type handling

#### `create_basic_schema(df)`
- **Purpose**: Creates a basic schema from DataFrame for fallback scenarios
- **Returns**: Basic schema dictionary
- **Usage**: Emergency fallback when database schema extraction fails

### 3. Export Strategy and Partitioning

#### `get_partitioning_strategy(table_name, row_count, total_export_size_gb)`
- **Purpose**: Determines optimal export strategy based on table size
- **Parameters**:
  - `table_name`: Name of table to export
  - `row_count`: Number of rows in table
  - `total_export_size_gb`: Estimated total export size
- **Returns**: Dictionary with strategy details (single-file vs partitioned)
- **Logic**:
  - Small tables (<1M rows): Single file export
  - Large tables (>=1M rows): Partitioned export with chunk sizing
  - Adaptive chunk sizes based on total export size
- **Usage**: Export planning and resource allocation

#### `execute_partitioned_export_with_limited_workers(table_name, table_dir, partition_info, db_conn_params, max_chunk_workers=4)`
- **Purpose**: Executes parallel chunk export with worker pool limits
- **Parameters**:
  - `table_name`: Full table name to export
  - `table_dir`: Output directory for parquet files
  - `partition_info`: Strategy details from get_partitioning_strategy
  - `db_conn_params`: Database connection parameters
  - `max_chunk_workers`: Maximum concurrent workers (default 4)
- **Features**:
  - Thread pool executor for parallel chunk processing
  - Schema consistency across all chunks using database metadata
  - Worker pool limits to prevent resource exhaustion
  - Comprehensive error handling and cleanup
- **Returns**: Success status and file list
- **Usage**: Primary function for large table exports

#### `process_single_chunk(args_tuple)`
- **Purpose**: Processes a single data chunk in parallel export operations
- **Parameters**: Tuple containing (chunk_number, offset, limit, table_name, table_dir, db_conn_params, table_schema)
- **Features**:
  - Individual database connection per chunk
  - Schema-driven data reading with no inference
  - Unique file naming with chunk numbers
  - Error isolation per chunk
- **Returns**: Tuple (success, file_path, error_message)
- **Usage**: Called by thread pool workers in parallel exports

### 4. Single Table Export Functions

#### `export_small_table_single_file(db_conn, table_name, table_dir, source_row_count)`
- **Purpose**: Exports small tables as single parquet files
- **Features**:
  - Database schema extraction with auto-detection
  - Schema-driven data reading
  - Row count validation
  - Single file output for efficiency
- **Returns**: Success status and file path
- **Usage**: Small table exports (<1M rows)

#### `export_large_table_partitioned(db_conn, table_name, table_dir, source_row_count, chunk_size)`
- **Purpose**: Exports large tables as multiple partitioned parquet files
- **Features**:
  - Database schema extraction with type detection
  - Sequential chunk processing
  - Progress tracking per chunk
  - File list accumulation
- **Returns**: Success status and file list
- **Usage**: Large table exports when parallel processing not needed

### 5. Job Management and Organization

#### `update_database_paths_in_db(job_id, paths_data)`
- **Purpose**: Updates job database with export file paths and metadata
- **Parameters**:
  - `job_id`: Job identifier
  - `paths_data`: Dictionary with file paths and table information
- **Usage**: Job tracking and file management

#### `get_export_organization_recommendation(tables_with_counts, db_type)`
- **Purpose**: Recommends optimal organization strategy for exports
- **Parameters**:
  - `tables_with_counts`: Dictionary of table names to row counts
  - `db_type`: Database type for type-specific recommendations
- **Returns**: Organization strategy with categorization logic
- **Usage**: Export planning and organization

#### `execute_export_organization(tables_with_counts, selected_strategy, base_export_path, db_conn_params, job_id=None)`
- **Purpose**: Executes the recommended export organization strategy
- **Features**:
  - Multi-strategy support (by_size, by_schema, single_directory)
  - Progress tracking integration
  - Error handling and rollback
  - Job status updates
- **Returns**: Success status and organization results
- **Usage**: Main execution function for organized exports

### 6. Database Testing and Validation

#### `test_database_connection(conn_params)`
- **Purpose**: Tests database connectivity with comprehensive validation
- **Features**:
  - Connection establishment testing
  - Database type detection
  - Basic query execution
  - Error reporting with details
- **Returns**: Success status and connection details
- **Usage**: Connection validation before export operations

#### `validate_export_setup(export_config)`
- **Purpose**: Validates complete export configuration
- **Parameters**: Export configuration dictionary
- **Returns**: Validation results with error details
- **Usage**: Pre-export validation

### 7. Progress Tracking and Monitoring

#### `get_job_progress(job_id)`
- **Purpose**: Retrieves current job progress from database
- **Returns**: Progress percentage and status information
- **Usage**: Real-time progress monitoring

#### `update_job_progress(job_id, progress_percent, status_message="")`
- **Purpose**: Updates job progress in database
- **Parameters**:
  - `job_id`: Job identifier
  - `progress_percent`: Current progress (0-100)
  - `status_message`: Optional status description
- **Usage**: Progress tracking during export operations

#### `cancel_export_job(job_id)`
- **Purpose**: Cancels running export job with cleanup
- **Features**:
  - Job status update to 'cancelled'
  - Resource cleanup
  - Partial file cleanup options
- **Returns**: Cancellation success status
- **Usage**: Job cancellation from web interface

### 8. Utility and Helper Functions

#### `get_file_size_mb(file_path)`
- **Purpose**: Gets file size in megabytes
- **Returns**: File size as float
- **Usage**: Export size tracking and validation

#### `cleanup_temp_files(file_paths)`
- **Purpose**: Cleans up temporary files after operations
- **Parameters**: List of file paths to remove
- **Usage**: Resource cleanup after failed operations

#### `format_export_summary(export_results)`
- **Purpose**: Formats export results for user display
- **Returns**: Formatted summary string
- **Usage**: User interface display of export results

#### `get_memory_usage()`
- **Purpose**: Gets current memory usage statistics
- **Returns**: Memory usage information
- **Usage**: Performance monitoring and optimization

## Database Type Support

### PostgreSQL
- **Full Support**: All standard PostgreSQL data types
- **Special Types**: JSON, JSONB, UUID, network types (inet, cidr)
- **Array Types**: Converted to string representation
- **Temporal Types**: Full date, time, timestamp, interval support

### Greenplum
- **Compatibility**: Full PostgreSQL compatibility plus Greenplum extensions
- **Distributed Tables**: Handled transparently
- **Performance**: Optimized for large-scale analytics workloads
- **Partitioning**: Native support for partitioned table exports

### Vertica
- **Analytics Focus**: Optimized for analytical data types
- **Columnar Storage**: Efficient reading of wide tables
- **Projection Handling**: Automatic projection selection
- **Performance**: High-speed exports for analytics workloads

## Schema Type Mapping

### Numeric Types
- **Integer Types**: int2 → Int16, int4 → Int32, int8 → Int64
- **Floating Point**: float4 → Float32, float8 → Float64, numeric → Float64
- **Decimal**: All decimal/numeric types → Float64

### String Types
- **Text Types**: text, varchar, char → String
- **JSON**: json, jsonb → String (preserves structure)
- **UUID**: uuid → String
- **Enum**: All enum types → String

### Temporal Types
- **Date**: date → Date
- **Time**: time, timetz → Time
- **Timestamp**: timestamp, timestamptz → Datetime
- **Interval**: interval → String (formatted representation)

### Binary Types
- **Binary Data**: bytea, blob → Binary
- **Large Objects**: Handled as binary streams

### Boolean Types
- **Boolean**: bool, boolean → Boolean

### Network Types
- **IP Addresses**: inet, cidr → String
- **MAC Addresses**: macaddr → String

### Array Types
- **All Arrays**: Converted to string representation
- **Preserves Structure**: Maintains array format in string

## Performance Characteristics

### Memory Usage
- **Streaming Processing**: Chunk-based reading for large tables
- **Schema Caching**: Database schema cached per table
- **Connection Pooling**: Efficient connection management

### Parallel Processing
- **Thread Pool**: Configurable worker limits (default 4)
- **Schema Consistency**: Shared schema across all workers
- **Fault Isolation**: Individual chunk error handling

### Export Sizes
- **Small Tables**: <1M rows → Single file
- **Medium Tables**: 1M-10M rows → 2-4 chunks
- **Large Tables**: >10M rows → 8+ chunks
- **Adaptive Sizing**: Based on total export size

## Error Handling

### Connection Errors
- **Retry Logic**: Automatic connection retry with backoff
- **Timeout Handling**: Configurable connection timeouts
- **Resource Cleanup**: Automatic connection cleanup

### Schema Errors
- **Type Mapping**: Fallback to string for unknown types
- **Missing Tables**: Clear error messages with suggestions
- **Permission Issues**: Detailed access error reporting

### Export Errors
- **Partial Failures**: Individual chunk error isolation
- **Rollback Support**: Cleanup of partial exports
- **Progress Preservation**: Resume capability for large exports

## Integration Points

### Celery Tasks
- **Task Definitions**: Full integration with async task system
- **Progress Updates**: Real-time progress reporting
- **Cancellation**: Graceful task cancellation support

### Flask Application
- **API Endpoints**: RESTful endpoints for all operations
- **Status Monitoring**: Real-time job status updates
- **File Management**: Export file serving and cleanup

### Docker Deployment
- **Multi-Service**: Web, worker, and Redis containers
- **Scaling**: Horizontal worker scaling support
- **Health Checks**: Container health monitoring

## Configuration Options

### Database Settings
- **Connection Pools**: Configurable pool sizes
- **Query Timeouts**: Adjustable timeout values
- **SSL Support**: Full SSL/TLS connection support

### Export Settings
- **Chunk Sizes**: Configurable chunk sizes by table size
- **Worker Limits**: Adjustable parallel worker counts
- **File Formats**: Parquet with configurable compression

### Performance Tuning
- **Memory Limits**: Configurable memory usage limits
- **Disk Space**: Automatic disk space monitoring
- **Network Optimization**: Batch size optimization

## Best Practices

### Schema Management
- **Database-Driven**: Always use database schema over inference
- **Type Consistency**: Maintain consistent types across chunks
- **Error Logging**: Comprehensive logging for troubleshooting

### Performance Optimization
- **Worker Tuning**: Adjust worker counts based on system resources
- **Chunk Sizing**: Optimize chunk sizes for memory and performance
- **Connection Management**: Use connection pooling for large exports

### Error Recovery
- **Partial Recovery**: Support for resuming failed exports
- **Cleanup Procedures**: Automatic cleanup of failed operations
- **Monitoring**: Active monitoring of export progress and health

This comprehensive documentation covers all 26 functions in the worker.py file, providing a complete reference for the database export system with multi-database support and schema-driven processing.
