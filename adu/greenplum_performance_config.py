#!/usr/bin/env python3
"""
Greenplum Performance Configuration for Large Table Exports (100M+ rows)
Optimized settings to avoid 8+ hour export times
"""

# PERFORMANCE CONFIGURATION FOR GREENPLUM LARGE TABLE EXPORTS
GREENPLUM_LARGE_TABLE_CONFIG = {
    # Connection and pooling optimizations
    'connection_pool': {
        'min_connections': 4,
        'max_connections': 16,  # OPTIMIZED: More connections for parallel processing
        'connection_timeout': 300,  # 5 minutes
        'query_timeout': 3600,  # 1 hour for large queries
        'keepalive_idle': 600,  # 10 minutes
        'keepalive_interval': 30,
        'keepalive_count': 3
    },
    
    # Range chunking optimizations (PRIMARY PERFORMANCE BOOST)
    'range_chunking': {
        'enabled': True,
        'priority_threshold': 10000000,  # Use range chunking for tables > 10M rows
        'force_threshold': 100000000,    # Force range chunking for tables > 100M rows (CRITICAL)
        'min_chunk_size': 2000000,       # OPTIMIZED: 2M rows minimum for Greenplum segments
        'max_chunk_size': 20000000,      # OPTIMIZED: 20M rows maximum for balance
        'max_workers': 16,               # AGGRESSIVE: Use up to 16 workers for range chunks
        'segment_alignment': True,       # Align chunks with Greenplum segments
        'segment_multiplier': 100000,    # Round chunk sizes to 100K boundaries
    },
    
    # DuckDB streaming optimizations
    'duckdb_streaming': {
        'chunk_size_rows': 10000000,     # OPTIMIZED: 10M rows per chunk
        'single_file_threshold': 100000000,  # Use single file for < 100M rows
        'offset_threshold': 50000000,    # CRITICAL: Avoid OFFSET above 50M rows
        'compression': 'snappy',         # Fast compression
        'memory_check_interval': 5,      # Check memory every 5 chunks
    },
    
    # Export method selection priorities for large tables
    'method_selection': {
        'small_table_threshold': 500000,        # < 500K rows: Direct export
        'range_preferred_threshold': 10000000,  # 10M+ rows: Prefer range chunking
        'offset_danger_threshold': 50000000,    # 50M+ rows: Avoid OFFSET methods
        'massive_table_threshold': 100000000,   # 100M+ rows: Force optimal methods
    },
    
    # Greenplum-specific optimizations
    'greenplum_specific': {
        'use_parallel_append': True,     # Enable parallel processing hints
        'segment_aware_chunking': True,  # Align with segment boundaries
        'prefer_range_scans': True,      # Use range scans over index scans
        'batch_insert_size': 50000,      # Batch size for operations
        'work_mem': '256MB',            # Work memory per connection
        'maintenance_work_mem': '1GB',   # Maintenance work memory
    },
    
    # Progress and monitoring
    'monitoring': {
        'progress_interval': 10,         # Report progress every 10 chunks
        'throughput_window': 60,         # Calculate throughput over 60 seconds
        'memory_warning_threshold': 0.8, # Warn at 80% memory usage
        'log_slow_chunks': True,         # Log chunks taking > 30 seconds
        'slow_chunk_threshold': 30,      # Seconds
    },
    
    # File output optimizations
    'output': {
        'compression': 'snappy',         # Fast compression for speed
        'row_group_size': 1000000,       # 1M rows per row group
        'page_size': 1048576,           # 1MB page size
        'target_file_size_mb': 400,     # Target 400MB files for large tables
        'max_files_per_table': 200,     # Limit number of files
    }
}

# PERFORMANCE THRESHOLDS - Used to detect and prevent slow export methods
PERFORMANCE_THRESHOLDS = {
    'offset_becomes_slow': 50000000,    # OFFSET becomes slow above 50M rows
    'offset_becomes_very_slow': 100000000,  # OFFSET becomes very slow above 100M rows
    'range_chunking_recommended': 10000000,  # Recommend range chunking above 10M rows
    'single_file_preferred': 100000000,      # Single files preferred above 100M rows (when no range cols)
    'massive_table': 500000000,             # Tables above 500M rows need special handling
}

# GREENPLUM SEGMENT OPTIMIZATION
GREENPLUM_SEGMENT_CONFIG = {
    'typical_segment_count': 8,          # Typical Greenplum segment count
    'rows_per_segment_optimal': 10000000, # Optimal rows per segment for chunking
    'chunk_alignment': 100000,           # Align chunk boundaries to 100K rows
    'parallel_factor': 2,                # Use 2x segments for parallelism
}

def get_optimal_chunk_size(table_row_count: int, available_workers: int = 8) -> int:
    """
    Calculate optimal chunk size for a given table size and worker count
    OPTIMIZED FOR GREENPLUM PERFORMANCE
    
    Args:
        table_row_count: Number of rows in the table
        available_workers: Number of available worker threads
        
    Returns:
        Optimal chunk size in rows
    """
    config = GREENPLUM_LARGE_TABLE_CONFIG['range_chunking']
    
    if table_row_count <= 0:
        return config['min_chunk_size']
    
    # Calculate target chunk size based on table size and workers
    if table_row_count > 1000000000:  # 1B+ rows
        target_chunks = min(available_workers * 4, 200)  # 4 chunks per worker, max 200
        min_chunk_size = 5000000  # 5M minimum for ultra-large tables
    elif table_row_count > 100000000:  # 100M+ rows (PRIMARY TARGET)
        target_chunks = min(available_workers * 3, 100)  # 3 chunks per worker, max 100
        min_chunk_size = 2000000  # 2M minimum for large tables
    else:  # Smaller tables
        target_chunks = min(available_workers * 2, 50)   # 2 chunks per worker, max 50
        min_chunk_size = 1000000  # 1M minimum for medium tables
    
    # Calculate chunk size
    chunk_size = max(min_chunk_size, table_row_count // target_chunks)
    
    # Align to segment boundaries for Greenplum optimization
    alignment = GREENPLUM_SEGMENT_CONFIG['chunk_alignment']
    chunk_size = ((chunk_size + alignment - 1) // alignment) * alignment
    
    # Apply bounds
    chunk_size = max(config['min_chunk_size'], min(chunk_size, config['max_chunk_size']))
    
    return chunk_size

def get_optimal_worker_count(table_row_count: int, max_available: int = 16) -> int:
    """
    Calculate optimal worker count for a given table size
    
    Args:
        table_row_count: Number of rows in the table
        max_available: Maximum available workers
        
    Returns:
        Optimal number of workers
    """
    if table_row_count > 500000000:  # 500M+ rows
        return min(max_available, 16)  # Use maximum workers for massive tables
    elif table_row_count > 100000000:  # 100M+ rows
        return min(max_available, 12)  # Use most workers for large tables
    elif table_row_count > 10000000:   # 10M+ rows
        return min(max_available, 8)   # Use moderate workers for medium tables
    else:
        return min(max_available, 6)   # Use fewer workers for smaller tables

def should_use_range_chunking(table_row_count: int, has_range_column: bool) -> bool:
    """
    Determine if range chunking should be used for optimal performance
    
    Args:
        table_row_count: Number of rows in the table
        has_range_column: Whether table has a suitable range column
        
    Returns:
        True if range chunking should be used
    """
    config = GREENPLUM_LARGE_TABLE_CONFIG['method_selection']
    
    # Force range chunking for massive tables if range column available
    if table_row_count >= config['massive_table_threshold'] and has_range_column:
        return True
    
    # Recommend range chunking for large tables with range columns
    if table_row_count >= config['range_preferred_threshold'] and has_range_column:
        return True
    
    return False

def should_avoid_offset_methods(table_row_count: int) -> bool:
    """
    Determine if OFFSET-based methods should be avoided due to performance issues
    
    Args:
        table_row_count: Number of rows in the table
        
    Returns:
        True if OFFSET methods should be avoided
    """
    return table_row_count >= PERFORMANCE_THRESHOLDS['offset_becomes_slow']

def get_performance_warning(table_row_count: int, method: str) -> str:
    """
    Get performance warning message for given table size and method
    
    Args:
        table_row_count: Number of rows in the table
        method: Export method being used
        
    Returns:
        Warning message or empty string
    """
    if table_row_count >= PERFORMANCE_THRESHOLDS['offset_becomes_very_slow']:
        if 'offset' in method.lower() or 'parallel' in method.lower():
            return f"🚨 CRITICAL: {method} may take 8+ hours for {table_row_count:,} rows. Use range chunking!"
    
    if table_row_count >= PERFORMANCE_THRESHOLDS['offset_becomes_slow']:
        if 'offset' in method.lower() or 'parallel' in method.lower():
            return f"⚠️  WARNING: {method} may be slow for {table_row_count:,} rows. Consider range chunking."
    
    return ""