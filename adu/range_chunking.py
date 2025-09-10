#!/usr/bin/env python3
"""
Range-Based Chunking for Large Table Exports
Uses WHERE clauses with ranges instead of OFFSET for constant performance per chunk
"""

import time
import polars as pl
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from adu.enhanced_logger import logger
from adu.greenplum_pool import get_database_connection
from adu.duckdb_exporter import export_table_chunk_duckdb
from adu.database_utils import get_table_schema, create_data_source_connection


@dataclass
class RangeInfo:
    """Information about a rangeable column in a table"""
    column_name: str
    data_type: str
    min_value: Any
    max_value: Any
    is_sequential: bool = False  # True if values are sequential (e.g., auto-increment ID)
    null_count: int = 0


class RangeAnalyzer:
    """
    Analyzes tables to find suitable columns for range-based chunking
    """
    
    def __init__(self, table_name: str):
        self.table_name = table_name
    
    def find_rangeable_columns(self) -> List[RangeInfo]:
        """
        Find columns suitable for range-based chunking
        
        Returns:
            List of RangeInfo objects for suitable columns
        """
        rangeable_columns = []
        
        try:
            with get_database_connection() as db_conn:
                cursor = db_conn.cursor()
                
                # Get column information from information_schema
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = %s
                    AND data_type IN (
                        'integer', 'bigint', 'smallint', 'serial', 'bigserial',
                        'timestamp', 'timestamptz', 'date', 
                        'numeric', 'decimal', 'real', 'double precision'
                    )
                    ORDER BY ordinal_position
                """, (self.table_name.split('.')[-1],))  # Remove schema prefix if present
                
                columns = cursor.fetchall()
                
                for column_name, data_type, is_nullable in columns:
                    try:
                        range_info = self._analyze_column_range(cursor, column_name, data_type)
                        if range_info and self._is_suitable_for_range_chunking(range_info):
                            rangeable_columns.append(range_info)
                    except Exception as e:
                        logger.debug(f"Could not analyze column {column_name}: {e}")
                        continue
                
        except Exception as e:
            logger.warning(f"Error finding rangeable columns for {self.table_name}: {e}")
        
        return rangeable_columns
    
    def _analyze_column_range(self, cursor, column_name: str, data_type: str) -> Optional[RangeInfo]:
        """
        Analyze a specific column to determine its range and characteristics
        
        Args:
            cursor: Database cursor
            column_name: Name of column to analyze
            data_type: Data type of the column
            
        Returns:
            RangeInfo object or None if not suitable
        """
        try:
            # Get min, max, null count, and approximate distinct count
            cursor.execute("SET statement_timeout = 30000")  # 30 second timeout
            cursor.execute(f"""
                SELECT 
                    MIN({column_name}) as min_val,
                    MAX({column_name}) as max_val,
                    COUNT(*) - COUNT({column_name}) as null_count,
                    COUNT(*) as total_count
                FROM {self.table_name}
            """)
            
            result = cursor.fetchone()
            if not result:
                return None
            
            min_val, max_val, null_count, total_count = result
            
            if min_val is None or max_val is None:
                return None  # No data or all nulls
            
            # Check if column appears to be sequential (for numeric types)
            is_sequential = False
            if data_type in ('integer', 'bigint', 'smallint', 'serial', 'bigserial'):
                is_sequential = self._check_if_sequential(cursor, column_name, min_val, max_val, total_count)
            
            return RangeInfo(
                column_name=column_name,
                data_type=data_type,
                min_value=min_val,
                max_value=max_val,
                is_sequential=is_sequential,
                null_count=null_count
            )
            
        except Exception as e:
            logger.debug(f"Error analyzing column {column_name}: {e}")
            return None
    
    def _check_if_sequential(self, cursor, column_name: str, min_val: int, max_val: int, total_count: int) -> bool:
        """
        Check if a numeric column has sequential values (good for range chunking)
        
        Args:
            cursor: Database cursor
            column_name: Column name
            min_val: Minimum value
            max_val: Maximum value  
            total_count: Total row count
            
        Returns:
            True if column appears sequential
        """
        try:
            # If the range is close to the row count, it's likely sequential
            value_range = max_val - min_val + 1
            density = total_count / value_range if value_range > 0 else 0
            
            # Consider sequential if density > 50% (not too many gaps)
            return density > 0.5
            
        except:
            return False
    
    def _is_suitable_for_range_chunking(self, range_info: RangeInfo) -> bool:
        """
        Determine if a column is suitable for range-based chunking
        
        Args:
            range_info: RangeInfo object
            
        Returns:
            True if suitable for range chunking
        """
        # Too many nulls make range chunking less effective
        if range_info.null_count > 0.2 * (range_info.null_count + 1000000):  # Rough estimate
            return False
        
        # Sequential numeric columns are ideal
        if range_info.is_sequential:
            return True
        
        # Timestamp columns are usually good for range chunking
        if 'timestamp' in range_info.data_type or 'date' in range_info.data_type:
            return True
        
        # Other numeric columns might work depending on distribution
        return True
    
    def get_best_range_column(self) -> Optional[RangeInfo]:
        """
        Get the best column for range-based chunking
        
        Returns:
            RangeInfo for best column or None
        """
        columns = self.find_rangeable_columns()
        if not columns:
            return None
        
        # Priority order:
        # 1. Sequential integer columns (auto-increment IDs)
        # 2. Timestamp/date columns  
        # 3. Other numeric columns
        
        # First, look for sequential integer columns
        for col in columns:
            if col.is_sequential and col.data_type in ('integer', 'bigint', 'serial', 'bigserial'):
                return col
        
        # Then, look for timestamp columns
        for col in columns:
            if 'timestamp' in col.data_type or 'date' in col.data_type:
                return col
        
        # Finally, any other suitable column
        return columns[0] if columns else None


class RangeChunker:
    """
    Handles range-based chunking for large table exports
    """
    
    def __init__(self, table_name: str, range_info: RangeInfo, job_id: str):
        self.table_name = table_name
        self.range_info = range_info
        self.job_id = job_id
        self.chunks_created = 0
    
    def calculate_ranges(self, target_chunk_size: int) -> List[Tuple[Any, Any]]:
        """
        Calculate range boundaries for chunking
        
        Args:
            target_chunk_size: Target number of rows per chunk
            
        Returns:
            List of (start_value, end_value) tuples
        """
        ranges = []
        
        if self.range_info.data_type in ('integer', 'bigint', 'smallint', 'serial', 'bigserial'):
            # Numeric range chunking
            ranges = self._calculate_numeric_ranges(target_chunk_size)
        elif 'timestamp' in self.range_info.data_type or 'date' in self.range_info.data_type:
            # Time-based range chunking
            ranges = self._calculate_time_ranges(target_chunk_size)
        else:
            # Generic numeric range chunking
            ranges = self._calculate_numeric_ranges(target_chunk_size)
        
        logger.info(f"Calculated {len(ranges)} ranges for {self.table_name} using column {self.range_info.column_name}")
        return ranges
    
    def _calculate_numeric_ranges(self, target_chunk_size: int) -> List[Tuple[int, int]]:
        """Calculate numeric ranges for chunking based on actual row count estimation
        OPTIMIZED FOR GREENPLUM PERFORMANCE with large tables (100M+ rows)"""
        ranges = []
        
        min_val = self.range_info.min_value
        max_val = self.range_info.max_value
        
        # Get actual row count for better estimation
        try:
            with get_database_connection() as db_conn:
                cursor = db_conn.cursor()
                # PERFORMANCE OPTIMIZATION: Use shorter timeout for large tables
                cursor.execute("SET statement_timeout = 60000")  # 60 second timeout (was 30)
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                actual_row_count = cursor.fetchone()[0]
                
                if actual_row_count == 0:
                    return ranges
                
                # Calculate optimal number of chunks based on actual data
                optimal_chunk_count = max(1, (actual_row_count + target_chunk_size - 1) // target_chunk_size)
                
                # GREENPLUM OPTIMIZATION: Enhanced chunk limits for better segment utilization
                if actual_row_count > 1000000000:  # 1B+ rows - Ultra-massive tables
                    min_chunks = 1
                    max_chunks = min(200, actual_row_count // 5000000)   # OPTIMIZED: At least 5M rows per chunk
                    min_rows_per_chunk = 5000000    # OPTIMIZED: Larger chunks for ultra-massive tables
                elif actual_row_count > 500000000:  # 500M+ rows - Massive tables  
                    min_chunks = 1
                    max_chunks = min(150, actual_row_count // 3000000)   # At least 3M rows per chunk
                    min_rows_per_chunk = 3000000
                elif actual_row_count > 100000000:  # 100M+ rows - Large tables (PRIMARY TARGET)
                    min_chunks = 1
                    max_chunks = min(100, actual_row_count // 2000000)   # OPTIMIZED: At least 2M rows per chunk  
                    min_rows_per_chunk = 2000000    # OPTIMIZED: Larger minimum for 100M+ tables
                elif actual_row_count > 10000000:   # 10M+ rows - Medium-large tables
                    min_chunks = 1
                    max_chunks = min(80, actual_row_count // 1000000)    # At least 1M rows per chunk
                    min_rows_per_chunk = 1000000
                else:  # <10M rows - Smaller tables
                    min_chunks = 1
                    max_chunks = min(50, actual_row_count // 500000)     # At least 500K rows per chunk
                    min_rows_per_chunk = 500000
                
                optimal_chunk_count = max(min_chunks, min(optimal_chunk_count, max_chunks))
                
                # Ensure we meet minimum rows per chunk requirement
                if actual_row_count // optimal_chunk_count < min_rows_per_chunk:
                    optimal_chunk_count = max(1, actual_row_count // min_rows_per_chunk)
                
                logger.info(f"OPTIMIZED CHUNKING for {self.table_name}: {actual_row_count:,} rows -> {optimal_chunk_count} chunks "
                           f"(target: {target_chunk_size:,} rows/chunk, min: {min_rows_per_chunk:,})")
                
                # PERFORMANCE OPTIMIZATION: Always use simple range division to avoid expensive operations
                # This prevents hanging on large tables (even with 100M+ rows)
                total_range = max_val - min_val
                chunk_range_size = max(1, total_range // optimal_chunk_count)
                
                current_start = min_val
                for i in range(optimal_chunk_count):
                    if i == optimal_chunk_count - 1:
                        # Last chunk gets all remaining values
                        current_end = max_val
                    else:
                        current_end = min(current_start + chunk_range_size, max_val)
                    
                    ranges.append((current_start, current_end))
                    current_start = current_end + 1
                    
                    if current_start > max_val:
                        break
                
                logger.info(f"Generated {len(ranges)} ranges for {self.table_name} "
                           f"(avg range size: {chunk_range_size:,}, total range: {total_range:,})")
                
        except Exception as e:
            logger.warning(f"Error calculating optimized ranges for {self.table_name}: {e}, using fallback")
            # Fallback to simple approach with performance-oriented defaults
            estimated_chunks = max(1, min(50, (max_val - min_val) // max(1000000, target_chunk_size)))
            chunk_range_size = (max_val - min_val) // estimated_chunks if estimated_chunks > 0 else (max_val - min_val)
            
            current_start = min_val
            for i in range(estimated_chunks):
                current_end = min_val + (i + 1) * chunk_range_size
                if i == estimated_chunks - 1:
                    current_end = max_val
                ranges.append((current_start, current_end))
                current_start = current_end + 1
        
        return ranges
    
    def _calculate_percentile_ranges(self, chunk_count: int) -> List[Tuple[int, int]]:
        """DEPRECATED: Use simple numeric ranges to avoid expensive percentile operations"""
        logger.warning("Percentile-based chunking disabled to prevent hanging on large tables")
        return self._calculate_simple_numeric_ranges(chunk_count)
    
    def _calculate_simple_numeric_ranges(self, chunk_count: int) -> List[Tuple[int, int]]:
        """Simple fallback range calculation"""
        ranges = []
        min_val = self.range_info.min_value
        max_val = self.range_info.max_value
        
        chunk_range_size = (max_val - min_val) // chunk_count
        
        current_start = min_val
        for i in range(chunk_count):
            current_end = min_val + (i + 1) * chunk_range_size
            if i == chunk_count - 1:
                current_end = max_val
            ranges.append((current_start, current_end))
            current_start = current_end + 1
            
        return ranges
    
    def _calculate_time_ranges(self, target_chunk_size: int) -> List[Tuple[str, str]]:
        """Calculate time-based ranges for chunking with optimization"""
        ranges = []
        
        try:
            with get_database_connection() as db_conn:
                cursor = db_conn.cursor()
                
                # Add timeout protection for row count query
                cursor.execute("SET statement_timeout = 30000")  # 30 second timeout
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                total_rows = cursor.fetchone()[0]
                
                if total_rows == 0:
                    return ranges
                
                # Calculate optimal chunk count with limits
                optimal_chunk_count = max(1, (total_rows + target_chunk_size - 1) // target_chunk_size)
                
                # Tiered limits for time-based chunking
                if total_rows > 1000000000:  # 1B+ rows
                    min_chunks = 1
                    max_chunks = min(200, total_rows // 2000000)  # At least 2M rows per chunk for time data
                elif total_rows > 100000000:  # 100M+ rows
                    min_chunks = 1
                    max_chunks = min(75, total_rows // 1000000)   # At least 1M rows per chunk
                else:  # <100M rows
                    min_chunks = 1
                    max_chunks = min(50, total_rows // 100000)    # At least 100K rows per chunk
                
                optimal_chunk_count = max(min_chunks, min(optimal_chunk_count, max_chunks))
                
                logger.info(f"Time-based chunking for {self.table_name}: {total_rows:,} rows -> {optimal_chunk_count} chunks")
                
                # Use simple time-based range division to avoid expensive percentile calculations
                # Calculate time intervals based on min/max values
                min_time = self.range_info.min_value
                max_time = self.range_info.max_value
                
                # Convert timestamps to seconds for calculation
                if hasattr(min_time, 'timestamp'):
                    min_seconds = min_time.timestamp()
                    max_seconds = max_time.timestamp()
                else:
                    # Handle string timestamps
                    import datetime
                    if isinstance(min_time, str):
                        min_dt = datetime.datetime.fromisoformat(min_time.replace('Z', '+00:00'))
                        max_dt = datetime.datetime.fromisoformat(max_time.replace('Z', '+00:00'))
                        min_seconds = min_dt.timestamp()
                        max_seconds = max_dt.timestamp()
                    else:
                        min_seconds = 0
                        max_seconds = optimal_chunk_count
                
                # Calculate time intervals
                total_seconds = max_seconds - min_seconds
                seconds_per_chunk = total_seconds / optimal_chunk_count
                
                for i in range(optimal_chunk_count):
                    start_seconds = min_seconds + (i * seconds_per_chunk)
                    end_seconds = min_seconds + ((i + 1) * seconds_per_chunk)
                    
                    if i == optimal_chunk_count - 1:
                        end_seconds = max_seconds  # Last chunk gets everything remaining
                    
                    # Convert back to timestamp format
                    import datetime
                    start_time = datetime.datetime.fromtimestamp(start_seconds, tz=datetime.timezone.utc)
                    end_time = datetime.datetime.fromtimestamp(end_seconds, tz=datetime.timezone.utc)
                    
                    ranges.append((start_time, end_time))
        
        except Exception as e:
            logger.warning(f"Error calculating time ranges: {e}, falling back to simple approach")
            # Fallback: use min/max with simple division
            ranges = [(self.range_info.min_value, self.range_info.max_value)]
        
        return ranges
    
    def export_with_ranges(self, output_dir: Path, ranges: List[Tuple[Any, Any]], 
                          max_workers: int = 6, use_duckdb: bool = True) -> Tuple[bool, int]:
        """
        Export table using range-based chunking with parallel processing
        OPTIMIZED FOR GREENPLUM LARGE TABLE PERFORMANCE
        
        Args:
            output_dir: Output directory for chunks
            ranges: List of (start, end) range tuples
            max_workers: Maximum concurrent workers
            use_duckdb: Whether to use DuckDB for export (recommended)
            
        Returns:
            Tuple of (success: bool, total_rows_exported: int)
        """
        logger.info(f"🚀 STARTING OPTIMIZED RANGE-BASED EXPORT: {self.table_name}")
        logger.info(f"📊 Configuration: {len(ranges)} ranges, {max_workers} workers, DuckDB: {use_duckdb}")
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        total_rows_exported = 0
        successful_chunks = 0
        failed_chunks = 0
        
        # Set table context for logging
        logger.set_table_context(self.table_name, 0, len(ranges), "Range-Based-Optimized")
        
        start_time = time.time()
        
        # GREENPLUM OPTIMIZATION: Use more aggressive parallelism for large tables
        effective_workers = max_workers
        if len(ranges) > 50:  # Large number of ranges
            effective_workers = min(max_workers + 2, 16)  # Boost workers for large exports
            logger.info(f"🔥 PERFORMANCE BOOST: Using {effective_workers} workers for {len(ranges)} ranges")
        
        with ThreadPoolExecutor(max_workers=effective_workers, thread_name_prefix=f"RangeChunk-{self.table_name}") as executor:
            # Submit all range export tasks
            future_to_range = {}
            
            for i, (start_val, end_val) in enumerate(ranges):
                future = executor.submit(
                    self._export_range_chunk,
                    i, start_val, end_val, output_dir, use_duckdb
                )
                future_to_range[future] = (i, start_val, end_val)
            
            # Process completed chunks with enhanced progress reporting
            chunks_completed_since_last_log = 0
            last_progress_log = time.time()
            
            for future in as_completed(future_to_range):
                chunk_num, start_val, end_val = future_to_range[future]
                
                try:
                    success, rows_exported = future.result()
                    
                    if success:
                        successful_chunks += 1
                        total_rows_exported += rows_exported
                        chunks_completed_since_last_log += 1
                        
                        # Enhanced progress logging for large exports
                        elapsed = time.time() - start_time
                        throughput = int(total_rows_exported / elapsed) if elapsed > 0 else 0
                        progress_percent = (successful_chunks / len(ranges)) * 100
                        
                        # Log progress every 10 chunks or every 30 seconds for large exports
                        time_since_log = time.time() - last_progress_log
                        should_log = (chunks_completed_since_last_log >= 10 or 
                                    time_since_log >= 30 or 
                                    successful_chunks % max(len(ranges) // 10, 1) == 0)
                        
                        if should_log:
                            logger.info(f"⚡ PROGRESS: {successful_chunks}/{len(ranges)} chunks ({progress_percent:.1f}%) | "
                                       f"{total_rows_exported:,} rows | {throughput:,} rows/sec | "
                                       f"Range {chunk_num + 1}: {rows_exported:,} rows ({start_val} to {end_val})")
                            chunks_completed_since_last_log = 0
                            last_progress_log = time.time()
                        
                        # Update table progress for WebSocket updates
                        logger.table_progress(
                            self.table_name,
                            total_rows_exported,
                            successful_chunks,
                            throughput
                        )
                        
                    else:
                        failed_chunks += 1
                        logger.error(f"❌ Range chunk {chunk_num + 1} failed: {start_val} to {end_val}")
                        
                except Exception as e:
                    failed_chunks += 1
                    logger.error(f"💥 Range chunk {chunk_num + 1} exception: {str(e)}")
        
        # Final logging with performance summary
        elapsed = time.time() - start_time
        throughput = int(total_rows_exported / elapsed) if elapsed > 0 else 0
        
        if failed_chunks == 0:
            total_size_mb = sum(
                f.stat().st_size for f in output_dir.glob("*.parquet") 
                if f.exists()
            ) / 1024 / 1024
            
            logger.info(f"🎉 RANGE-BASED EXPORT COMPLETED SUCCESSFULLY!")
            logger.info(f"📈 PERFORMANCE SUMMARY:")
            logger.info(f"   • Total rows: {total_rows_exported:,}")
            logger.info(f"   • Total chunks: {successful_chunks}")
            logger.info(f"   • Duration: {elapsed:.1f}s ({elapsed/60:.1f}m)")
            logger.info(f"   • Throughput: {throughput:,} rows/sec")
            logger.info(f"   • File size: {total_size_mb:.1f} MB")
            logger.info(f"   • Avg chunk size: {total_rows_exported//successful_chunks:,} rows" if successful_chunks > 0 else "")
            
            logger.table_completed(self.table_name, total_rows_exported, elapsed, total_size_mb)
            return True, total_rows_exported
        else:
            logger.error(f"❌ RANGE-BASED EXPORT FAILED:")
            logger.error(f"   • Successful chunks: {successful_chunks}/{len(ranges)}")
            logger.error(f"   • Failed chunks: {failed_chunks}")
            logger.error(f"   • Rows exported: {total_rows_exported:,}")
            
            logger.table_failed(
                self.table_name, 
                f"Range-based export failed: {failed_chunks}/{len(ranges)} chunks failed"
            )
            return False, 0
    
    def _export_range_chunk(self, chunk_num: int, start_val: Any, end_val: Any, 
                           output_dir: Path, use_duckdb: bool) -> Tuple[bool, int]:
        """
        Export a single range chunk
        
        Args:
            chunk_num: Chunk number for filename
            start_val: Start value for range
            end_val: End value for range
            output_dir: Output directory
            use_duckdb: Whether to use DuckDB export
            
        Returns:
            Tuple of (success: bool, rows_exported: int)
        """
        try:
            chunk_file = output_dir / f"part_{chunk_num:04d}.parquet"
            
            # Build WHERE clause for range
            if isinstance(start_val, str) and ('timestamp' in self.range_info.data_type or 'date' in self.range_info.data_type):
                where_clause = f"{self.range_info.column_name} >= '{start_val}' AND {self.range_info.column_name} <= '{end_val}'"
            else:
                where_clause = f"{self.range_info.column_name} >= {start_val} AND {self.range_info.column_name} <= {end_val}"
            
            if use_duckdb:
                # Use DuckDB for export with range-based WHERE clause
                from adu.duckdb_exporter import export_table_chunk_duckdb
                
                # Get schema from database for type enforcement
                polars_schema = None
                try:
                    with get_database_connection() as db_conn:
                        polars_schema = get_table_schema(db_conn, 'postgresql', self.table_name)
                except Exception as e:
                    logger.warning(f"Could not get schema for {self.table_name}: {str(e)}")
                
                # Mark config as using connection pool mode
                db_config = {'use_connection_pool': True, 'db_type': 'postgresql'}
                
                success, message, rows_exported = export_table_chunk_duckdb(
                    db_config, self.table_name, chunk_file, 0, 1000000,  # offset/limit not used with custom WHERE
                    polars_schema=polars_schema, custom_where=where_clause
                )
                
                return success, rows_exported
            else:
                # Use direct Polars export (fallback)
                return self._export_range_with_polars(chunk_file, where_clause)
                
        except Exception as e:
            logger.error(f"Error exporting range chunk {chunk_num}: {str(e)}")
            return False, 0
    
    def _export_range_with_polars(self, chunk_file: Path, where_clause: str) -> Tuple[bool, int]:
        """
        Export range using direct Polars (fallback method)
        
        Args:
            chunk_file: Output file path
            where_clause: WHERE clause for filtering
            
        Returns:
            Tuple of (success: bool, rows_exported: int)
        """
        try:
            with get_database_connection() as db_conn:
                cursor = db_conn.cursor()
                
                # Execute query with range filter
                query = f"SELECT * FROM {self.table_name} WHERE {where_clause}"
                cursor.execute(query)
                
                # Get column names
                column_names = [desc[0] for desc in cursor.description]
                
                # Fetch all data for this range
                rows = cursor.fetchall()
                
                if not rows:
                    return True, 0  # Empty range is still success
                
                # Convert to Polars DataFrame
                df_dict = {}
                for i, col_name in enumerate(column_names):
                    df_dict[col_name] = [row[i] if i < len(row) else None for row in rows]
                
                df = pl.DataFrame(df_dict)
                
                # Write to Parquet
                df.write_parquet(
                    chunk_file,
                    compression="snappy",
                    use_pyarrow=True
                )
                
                return True, len(rows)
                
        except Exception as e:
            logger.error(f"Error in Polars range export: {str(e)}")
            return False, 0


def export_large_table_with_range_chunking(
    job_id: str,
    table_name: str,
    output_dir: Path,
    target_chunk_size: int = 1000000,
    max_workers: int = 6
) -> Tuple[bool, int, Optional[RangeInfo]]:
    """
    Export large table using range-based chunking if suitable column found
    
    Args:
        job_id: Job identifier
        table_name: Table name to export
        output_dir: Output directory
        target_chunk_size: Target rows per chunk
        max_workers: Maximum concurrent workers
        
    Returns:
        Tuple of (success: bool, total_rows_exported: int, range_info_used: RangeInfo)
    """
    
    logger.info(f"Analyzing {table_name} for range-based chunking")
    
    # Analyze table for suitable range columns
    analyzer = RangeAnalyzer(table_name)
    best_column = analyzer.get_best_range_column()
    
    if not best_column:
        logger.info(f"No suitable columns found for range chunking in {table_name}")
        return False, 0, None
    
    logger.info(f"Using column '{best_column.column_name}' ({best_column.data_type}) for range chunking")
    logger.info(f"Range: {best_column.min_value} to {best_column.max_value} "
               f"(sequential: {best_column.is_sequential})")
    
    # Create range chunker
    chunker = RangeChunker(table_name, best_column, job_id)
    
    # Calculate ranges
    ranges = chunker.calculate_ranges(target_chunk_size)
    
    if not ranges:
        logger.warning(f"Could not calculate ranges for {table_name}")
        return False, 0, best_column
    
    # Export using ranges
    success, total_rows = chunker.export_with_ranges(
        output_dir, ranges, max_workers, use_duckdb=True
    )
    
    return success, total_rows, best_column