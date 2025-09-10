#!/usr/bin/env python3
"""
DuckDB-based Streaming Export Module
Replaces complex Polars cursor streaming with efficient DuckDB native streaming
"""

import time
import json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
from dataclasses import dataclass

from adu.enhanced_logger import logger
from adu.duckdb_exporter import create_duckdb_connection, check_memory_safety


@dataclass
class DuckDBStreamingConfig:
    """Configuration for DuckDB streaming operations"""
    chunk_size_rows: int = 5000000  # 5M rows per chunk for very large tables
    max_chunks: int = 1000  # Maximum chunks to prevent runaway operations
    compression: str = 'snappy'
    use_chunking_threshold: int = 50000000  # Use chunking for tables > 50M rows (FIXED: was 500M, causing performance bug)
    offset_performance_threshold: int = 100000000  # OFFSET becomes very slow above 100M rows
    memory_check_interval: int = 10  # Check memory every N chunks


class DuckDBStreamer:
    """
    High-performance streaming export using DuckDB's native COPY command
    Eliminates Python memory overhead by streaming directly from database to Parquet
    """
    
    def __init__(self, table_name: str, job_id: str, db_config: Dict[str, Any]):
        self.table_name = table_name
        self.job_id = job_id
        self.db_config = db_config
        self.config = DuckDBStreamingConfig()
        
        # Statistics tracking
        self.total_rows_exported = 0
        self.chunks_created = 0
        self.start_time = time.time()
        self.files_created = []
    
    def stream_single_file(self, output_file: Path, select_query: Optional[str] = None) -> Tuple[bool, int]:
        """
        Stream entire table to single Parquet file using DuckDB COPY
        Most efficient method for tables that fit comfortably in available storage
        
        Args:
            output_file: Output Parquet file path
            select_query: Optional custom SELECT query (defaults to SELECT *)
            
        Returns:
            Tuple of (success: bool, rows_exported: int)
        """
        duck_conn = None
        try:
            logger.info(f"Starting DuckDB single-file streaming for {self.table_name}")
            logger.table_started(self.table_name, 0, "DuckDB-Streaming", 0)
            
            # Memory safety check
            memory_safe, memory_msg = check_memory_safety()
            if not memory_safe:
                logger.error(f"Memory safety check failed: {memory_msg}")
                return False, 0
            
            # Create DuckDB connection with connection pool mode
            connection_pool_config = {'use_connection_pool': True, 'db_type': 'postgresql'}
            try:
                duck_conn = create_duckdb_connection(connection_pool_config)
            except Exception as conn_error:
                logger.error(f"Failed to create DuckDB connection: {conn_error}")
                logger.error("This may be due to connection pool not being initialized or database connectivity issues")
                return False, 0
            
            # Build query with proper DuckDB remote table prefix
            if not select_query:
                select_query = f"SELECT * FROM remote_db.{self.table_name}"
            
            # Execute streaming export
            export_query = f"""
                COPY ({select_query}) 
                TO '{output_file}' 
                (FORMAT PARQUET, COMPRESSION '{self.config.compression}')
            """
            
            logger.info(f"Executing DuckDB streaming export: {self.table_name}")
            result = duck_conn.execute(export_query)
            
            # Get rows exported from DuckDB result
            rows_exported = duck_conn.fetchall()[0][0] if result else 0
            
            # Verify file creation
            if not output_file.exists():
                logger.error(f"Export file not created: {output_file}")
                return False, 0
            
            file_size_mb = output_file.stat().st_size / 1024 / 1024
            elapsed = time.time() - self.start_time
            
            logger.table_completed(
                self.table_name,
                rows_exported, 
                elapsed,
                file_size_mb
            )
            
            self.total_rows_exported = rows_exported
            self.files_created.append(output_file)
            
            return True, rows_exported
            
        except Exception as e:
            error_msg = f"DuckDB streaming failed: {str(e)}"
            logger.table_failed(self.table_name, error_msg)
            logger.error(f"DuckDB streaming error details: {error_msg}")
            logger.error("Possible causes: (1) Connection pool not initialized, (2) Database connectivity issues, (3) Table does not exist")
            return False, 0
        finally:
            if duck_conn:
                duck_conn.close()
    
    def stream_chunked_files(self, output_dir: Path, select_query: Optional[str] = None) -> Tuple[bool, int, List[Path]]:
        """
        Stream table to multiple Parquet files using DuckDB chunking
        Used for very large tables that benefit from parallel processing
        
        Args:
            output_dir: Output directory for chunk files
            select_query: Optional custom SELECT query (defaults to SELECT *)
            
        Returns:
            Tuple of (success: bool, total_rows_exported: int, chunk_files: List[Path])
        """
        chunk_files = []
        
        try:
            logger.info(f"Starting DuckDB chunked streaming for {self.table_name}")
            logger.table_started(self.table_name, 0, "DuckDB-Chunked-Streaming", 0)
            
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Build base query with proper DuckDB remote table prefix
            if not select_query:
                base_query = f"SELECT * FROM remote_db.{self.table_name}"
            else:
                # If custom query provided, ensure it has remote_db prefix if needed
                if "remote_db." not in select_query and "FROM " in select_query.upper():
                    # Simple replacement for basic queries - for complex queries, user should include remote_db
                    base_query = select_query.replace(f"FROM {self.table_name}", f"FROM remote_db.{self.table_name}")
                else:
                    base_query = select_query
            
            chunk_num = 0
            total_rows = 0
            
            while chunk_num < self.config.max_chunks:
                # Memory safety check every N chunks
                if chunk_num % self.config.memory_check_interval == 0:
                    memory_safe, memory_msg = check_memory_safety()
                    if not memory_safe:
                        logger.warning(f"Memory safety check failed at chunk {chunk_num}: {memory_msg}")
                        break
                
                # Create chunk file path
                chunk_file = output_dir / f"part_{chunk_num:04d}.parquet"
                
                # Build chunked query using LIMIT/OFFSET
                offset = chunk_num * self.config.chunk_size_rows
                chunked_query = f"""
                    {base_query} 
                    LIMIT {self.config.chunk_size_rows} 
                    OFFSET {offset}
                """
                
                # Execute chunk export
                success, rows_in_chunk = self._export_single_chunk(chunk_file, chunked_query)
                
                if not success:
                    logger.error(f"Failed to export chunk {chunk_num}")
                    break
                
                if rows_in_chunk == 0:
                    # No more rows - we're done
                    logger.info(f"Chunk {chunk_num} returned 0 rows - export complete")
                    # Remove empty file if created
                    if chunk_file.exists() and chunk_file.stat().st_size == 0:
                        chunk_file.unlink()
                    break
                
                chunk_files.append(chunk_file)
                total_rows += rows_in_chunk
                chunk_num += 1
                
                # Log progress
                elapsed = time.time() - self.start_time
                throughput = int(total_rows / elapsed) if elapsed > 0 else 0
                logger.table_progress(
                    self.table_name,
                    total_rows,
                    chunk_num,
                    throughput
                )
                
                logger.info(f"Chunk {chunk_num-1} completed: {rows_in_chunk:,} rows")
            
            # Final statistics
            elapsed = time.time() - self.start_time
            total_size_mb = sum(f.stat().st_size for f in chunk_files if f.exists()) / 1024 / 1024
            
            if total_rows > 0:
                logger.table_completed(
                    self.table_name,
                    total_rows,
                    elapsed, 
                    total_size_mb
                )
                
                self.total_rows_exported = total_rows
                self.chunks_created = len(chunk_files)
                self.files_created = chunk_files
                
                return True, total_rows, chunk_files
            else:
                logger.table_failed(self.table_name, "No data exported")
                return False, 0, []
                
        except Exception as e:
            error_msg = f"DuckDB chunked streaming failed: {str(e)}"
            logger.table_failed(self.table_name, error_msg)
            logger.error(f"DuckDB chunked streaming error details: {error_msg}")
            logger.error("Possible causes: (1) Connection pool not initialized, (2) Database connectivity issues, (3) Table does not exist")
            return False, 0, []
    
    def _export_single_chunk(self, chunk_file: Path, query: str) -> Tuple[bool, int]:
        """
        Export a single chunk using DuckDB
        
        Args:
            chunk_file: Output file for this chunk
            query: SQL query for this chunk
            
        Returns:
            Tuple of (success: bool, rows_exported: int)
        """
        duck_conn = None
        try:
            # Create DuckDB connection for this chunk with connection pool mode
            connection_pool_config = {'use_connection_pool': True, 'db_type': 'postgresql'}
            try:
                duck_conn = create_duckdb_connection(connection_pool_config)
            except Exception as conn_error:
                logger.error(f"Failed to create DuckDB connection for chunk: {conn_error}")
                return False, 0
            
            # Execute chunk export
            export_query = f"""
                COPY ({query}) 
                TO '{chunk_file}' 
                (FORMAT PARQUET, COMPRESSION '{self.config.compression}')
            """
            
            result = duck_conn.execute(export_query)
            rows_exported = duck_conn.fetchall()[0][0] if result else 0
            
            return True, rows_exported
            
        except Exception as e:
            logger.error(f"Failed to export chunk to {chunk_file}: {str(e)}")
            return False, 0
        finally:
            if duck_conn:
                duck_conn.close()


def export_large_table_with_duckdb_streaming(
    job_id: str,
    table_name: str,
    output_dir: Path,
    db_config: Dict[str, Any],
    select_query: Optional[str] = None,
    estimated_rows: int = 0
) -> Tuple[bool, int]:
    """
    Export large table using DuckDB streaming - replaces Polars cursor streaming
    
    Args:
        job_id: Job identifier for logging
        table_name: Name of table to export
        output_dir: Directory for output files
        db_config: Database connection configuration
        select_query: Optional custom SELECT query
        estimated_rows: Estimated row count for chunking decisions
        
    Returns:
        Tuple of (success: bool, total_rows_exported: int)
    """
    
    logger.info(f"Starting DuckDB streaming export for {table_name}")
    logger.info(f"Estimated rows: {estimated_rows:,}" if estimated_rows > 0 else "Estimated rows: unknown")
    
    # Create streamer instance
    streamer = DuckDBStreamer(table_name, job_id, db_config)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Decision: single file vs chunked based on estimated size
    # IMPORTANT: OFFSET-based chunking becomes exponentially slower with large tables
    # For tables > 100M rows, single file streaming is much faster than OFFSET chunking
    
    # Check for OFFSET performance issues
    offset_will_be_slow = estimated_rows > streamer.config.offset_performance_threshold
    if offset_will_be_slow:
        logger.warning(f"🚨 PERFORMANCE ALERT: Table has {estimated_rows:,} rows - OFFSET chunking would cause severe performance degradation (8+ hours)")
        logger.info(f"✅ SOLUTION: Using single-file streaming instead to avoid OFFSET performance penalty (30-60 minutes expected)")
    
    # Enhanced chunking decision logic - prioritize performance over file size
    use_chunking = (
        estimated_rows > streamer.config.use_chunking_threshold and
        not offset_will_be_slow and  # Avoid OFFSET performance issues (PRIMARY CONSTRAINT)
        estimated_rows != 0  # Known size
    )
    
    # Handle unknown size conservatively
    if estimated_rows == 0:
        logger.info("Unknown table size - using single file streaming to avoid potential OFFSET issues")
        use_chunking = False
    
    # SAFETY CHECK: Prevent accidental chunked streaming for 100M+ rows
    if use_chunking and estimated_rows >= 100000000:
        logger.error(f"❌ SAFETY OVERRIDE: Preventing chunked streaming for {estimated_rows:,} rows (would cause 8+ hour exports)")
        logger.info("🔄 OVERRIDE: Forcing single-file streaming for optimal performance")
        use_chunking = False
    
    if use_chunking:
        logger.info(f"📝 Using chunked streaming for table (estimated: {estimated_rows:,} rows)")
        logger.warning("⚠️  CAUTION: Chunked streaming uses OFFSET which may degrade performance over time")
        
        # Calculate expected performance warning
        expected_chunks = (estimated_rows + streamer.config.chunk_size_rows - 1) // streamer.config.chunk_size_rows
        if expected_chunks > 20:
            logger.warning(f"📊 PERFORMANCE WARNING: {expected_chunks} chunks expected - later chunks will be slower due to OFFSET")
            logger.info(f"💡 RECOMMENDATION: Consider increasing chunk_size_rows from {streamer.config.chunk_size_rows:,} to reduce chunk count")
        success, total_rows, chunk_files = streamer.stream_chunked_files(output_dir, select_query)
        
        if success and chunk_files:
            # Create metadata file
            metadata = {
                'table_name': table_name,
                'total_rows': total_rows,
                'export_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'complete',
                'method': 'duckdb_streaming_chunked',
                'partitioned': len(chunk_files) > 1,
                'files': [f.name for f in chunk_files],
                'chunk_count': len(chunk_files)
            }
            
            metadata_file = output_dir / "_export_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
        
        return success, total_rows
    else:
        if offset_will_be_slow:
            logger.info(f"🚀 OPTIMAL: Using single-file streaming for large table (estimated: {estimated_rows:,} rows) - avoiding OFFSET performance issues")
            logger.info(f"📈 EXPECTED PERFORMANCE: 30-60 minutes vs 8+ hours with chunked streaming")
        else:
            logger.info(f"📝 Using single-file streaming for manageable table (estimated: {estimated_rows:,} rows)")
        output_file = output_dir / f"{table_name.replace('.', '_')}.parquet"
        success, total_rows = streamer.stream_single_file(output_file, select_query)
        
        if success:
            # Create metadata file
            metadata = {
                'table_name': table_name,
                'total_rows': total_rows,
                'export_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'complete',
                'method': 'duckdb_streaming_single',
                'partitioned': False,
                'files': [output_file.name]
            }
            
            metadata_file = output_dir / "_export_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
        
        return success, total_rows


def can_use_duckdb_streaming(db_type: str) -> bool:
    """
    Check if DuckDB streaming is supported for the database type
    
    Args:
        db_type: Database type (postgresql, greenplum, vertica)
        
    Returns:
        True if DuckDB streaming is supported
    """
    # DuckDB supports PostgreSQL and Greenplum via postgres extension
    supported_types = {'postgresql', 'greenplum'}
    return db_type.lower() in supported_types


def estimate_duckdb_streaming_benefit(table_row_count: int) -> str:
    """
    Estimate performance benefit of DuckDB streaming
    
    Args:
        table_row_count: Total rows in table
        
    Returns:
        Human-readable benefit estimate
    """
    if table_row_count < 1000000:  # 1M rows
        return "minimal (small table - direct export recommended)"
    elif table_row_count < 10000000:  # 10M rows
        return "moderate (medium table - single file streaming)"
    elif table_row_count < 100000000:  # 100M rows
        return "high (large table - chunked streaming recommended)"
    else:
        return "extreme (very large table - chunked streaming highly recommended)"