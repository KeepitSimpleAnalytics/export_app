#!/usr/bin/env python3
"""
Cursor Streaming Compatibility Layer
Redirects to the new DuckDB streaming implementation for backward compatibility
"""

# Import the new DuckDB streaming functions and re-export them with the old names
from adu.duckdb_streaming import (
    export_large_table_with_duckdb_streaming as export_large_table_with_cursor_streaming,
    can_use_duckdb_streaming as can_use_cursor_streaming,
    estimate_duckdb_streaming_benefit as estimate_streaming_benefit
)

# Legacy compatibility - in case any code still imports these old classes/functions
# They now use the superior DuckDB approach instead of Polars

def export_large_table_with_cursor_streaming(*args, **kwargs):
    """
    Legacy cursor streaming function - now uses efficient DuckDB streaming
    This maintains backward compatibility while providing much better performance
    """
    return export_large_table_with_duckdb_streaming(*args, **kwargs)

def can_use_cursor_streaming(db_type: str) -> bool:
    """Legacy compatibility function - now checks DuckDB streaming support"""
    return can_use_duckdb_streaming(db_type)

def estimate_streaming_benefit(table_row_count: int, chunk_size: int = None) -> str:
    """Legacy compatibility function - now uses DuckDB benefit estimation"""
    # Old function had chunk_size parameter, new one doesn't need it
    return estimate_duckdb_streaming_benefit(table_row_count)

# Placeholder for any code that might try to import the old CursorStreamer class
class CursorStreamer:
    """
    Legacy compatibility class - now redirects to DuckDB streaming
    This class is deprecated and will be removed in future versions
    """
    def __init__(self, table_name: str, job_id: str):
        import warnings
        warnings.warn(
            "CursorStreamer is deprecated. Use DuckDBStreamer from duckdb_streaming module instead.",
            DeprecationWarning,
            stacklevel=2
        )
        self.table_name = table_name
        self.job_id = job_id
    
    def stream_to_parquet_chunks(self, *args, **kwargs):
        """Deprecated method - use DuckDB streaming instead"""
        raise NotImplementedError(
            "CursorStreamer is deprecated. Use export_large_table_with_duckdb_streaming() instead."
        )