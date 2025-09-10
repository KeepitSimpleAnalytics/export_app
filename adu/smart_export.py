#!/usr/bin/env python3
"""
Smart Export Method Selection
Automatically chooses the optimal export method based on table characteristics
"""

import time
import json
import psutil
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from adu.enhanced_logger import logger
from adu.greenplum_pool import get_database_connection
from adu.sqlite_writer import get_sqlite_writer
from adu.greenplum_performance_config import (
    get_optimal_chunk_size, 
    get_optimal_worker_count,
    should_use_range_chunking,
    should_avoid_offset_methods,
    get_performance_warning,
    GREENPLUM_LARGE_TABLE_CONFIG
)
from adu.duckdb_streaming import (
    export_large_table_with_duckdb_streaming,
    can_use_duckdb_streaming,
    estimate_duckdb_streaming_benefit
)
from adu.range_chunking import (
    export_large_table_with_range_chunking,
    RangeAnalyzer
)
from adu.duckdb_exporter import (
    export_small_table_duckdb,
    export_large_table_with_duckdb
)
from adu.parallel_duckdb_functions import export_large_table_with_duckdb_parallel


class ExportMethod(Enum):
    """Available export methods"""
    DIRECT_DUCKDB = "direct_duckdb"           # Single DuckDB export for small tables
    RANGE_CHUNKING = "range_chunking"         # Range-based chunking with WHERE clauses
    CURSOR_STREAMING = "cursor_streaming"     # Server-side cursor streaming
    PARALLEL_DUCKDB = "parallel_duckdb"       # Parallel DuckDB with OFFSET (fallback)


@dataclass
class TableCharacteristics:
    """Analysis of table characteristics for export method selection"""
    row_count: int
    estimated_size_mb: float
    has_suitable_range_column: bool
    range_column_info: Optional[str]
    has_primary_key: bool
    is_partitioned: bool
    db_type: str
    supports_cursors: bool
    
    # Performance estimates
    offset_chunk_count: int
    estimated_offset_penalty: str
    memory_requirements_mb: float


class TableAnalyzer:
    """
    Analyzes table characteristics to recommend optimal export method
    """
    
    def __init__(self, table_name: str, db_type: str):
        self.table_name = table_name
        self.db_type = db_type
    
    def analyze_table(self, target_chunk_size: int = 1000000) -> TableCharacteristics:
        """
        Analyze table to determine optimal export method
        
        Args:
            target_chunk_size: Target chunk size for calculations
            
        Returns:
            TableCharacteristics object with analysis results
        """
        logger.info(f"Analyzing table characteristics: {self.table_name}")
        
        try:
            with get_database_connection() as db_conn:
                cursor = db_conn.cursor()
                
                # Get basic table statistics
                row_count = self._get_row_count(cursor)
                estimated_size_mb = self._estimate_table_size_mb(cursor, row_count)
                
                # Analyze range column suitability
                range_analyzer = RangeAnalyzer(self.table_name)
                best_range_column = range_analyzer.get_best_range_column()
                
                has_suitable_range_column = best_range_column is not None
                range_column_info = None
                if best_range_column:
                    range_column_info = f"{best_range_column.column_name} ({best_range_column.data_type})"
                
                # Check for primary key
                has_primary_key = self._has_primary_key(cursor)
                
                # Check if table is partitioned (Greenplum specific)
                is_partitioned = self._is_partitioned_table(cursor) if self.db_type.lower() == 'greenplum' else False
                
                # Calculate performance estimates
                offset_chunk_count = (row_count + target_chunk_size - 1) // target_chunk_size
                estimated_offset_penalty = estimate_duckdb_streaming_benefit(row_count)
                
                # Estimate memory requirements (rough calculation)
                memory_requirements_mb = min(
                    estimated_size_mb * 0.1,  # Assume 10% of table size for processing
                    2048  # Cap at 2GB
                )
                
                characteristics = TableCharacteristics(
                    row_count=row_count,
                    estimated_size_mb=estimated_size_mb,
                    has_suitable_range_column=has_suitable_range_column,
                    range_column_info=range_column_info,
                    has_primary_key=has_primary_key,
                    is_partitioned=is_partitioned,
                    db_type=self.db_type,
                    supports_cursors=can_use_duckdb_streaming(self.db_type),
                    offset_chunk_count=offset_chunk_count,
                    estimated_offset_penalty=estimated_offset_penalty,
                    memory_requirements_mb=memory_requirements_mb
                )
                
                self._log_analysis_results(characteristics)
                return characteristics
                
        except Exception as e:
            logger.error(f"Error analyzing table {self.table_name}: {e}")
            # Return default characteristics for fallback
            return TableCharacteristics(
                row_count=0,
                estimated_size_mb=0,
                has_suitable_range_column=False,
                range_column_info=None,
                has_primary_key=False,
                is_partitioned=False,
                db_type=self.db_type,
                supports_cursors=can_use_duckdb_streaming(self.db_type),
                offset_chunk_count=0,
                estimated_offset_penalty="unknown",
                memory_requirements_mb=1024
            )
    
    def _get_row_count(self, cursor) -> int:
        """Get total row count for table"""
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.warning(f"Could not get row count for {self.table_name}: {e}")
            return 0
    
    def _estimate_table_size_mb(self, cursor, row_count: int) -> float:
        """Estimate table size in MB"""
        try:
            # Try to get actual size statistics if available (PostgreSQL/Greenplum)
            cursor.execute(f"""
                SELECT pg_total_relation_size('{self.table_name}'::regclass) / 1024.0 / 1024.0
            """)
            result = cursor.fetchone()
            if result and result[0]:
                return float(result[0])
        except:
            pass
        
        # Fallback: estimate based on row count
        # Assume average row size of 500 bytes (rough estimate)
        return (row_count * 500) / 1024 / 1024
    
    def _has_primary_key(self, cursor) -> bool:
        """Check if table has a primary key"""
        try:
            cursor.execute(f"""
                SELECT 1 FROM information_schema.table_constraints 
                WHERE table_name = %s AND constraint_type = 'PRIMARY KEY'
                LIMIT 1
            """, (self.table_name.split('.')[-1],))
            return cursor.fetchone() is not None
        except:
            return False
    
    def _is_partitioned_table(self, cursor) -> bool:
        """Check if table is partitioned (Greenplum specific)"""
        try:
            cursor.execute(f"""
                SELECT 1 FROM pg_partitions 
                WHERE tablename = %s
                LIMIT 1
            """, (self.table_name.split('.')[-1],))
            return cursor.fetchone() is not None
        except:
            return False
    
    def _log_analysis_results(self, chars: TableCharacteristics):
        """Log analysis results for debugging"""
        logger.info(f"Table analysis results for {self.table_name}:")
        logger.info(f"  Rows: {chars.row_count:,}")
        logger.info(f"  Estimated size: {chars.estimated_size_mb:.1f}MB")
        logger.info(f"  Range column: {chars.range_column_info or 'None suitable'}")
        logger.info(f"  Primary key: {chars.has_primary_key}")
        logger.info(f"  Partitioned: {chars.is_partitioned}")
        logger.info(f"  Supports cursors: {chars.supports_cursors}")
        logger.info(f"  OFFSET chunks: {chars.offset_chunk_count}")
        logger.info(f"  OFFSET penalty: {chars.estimated_offset_penalty}")


class SmartExportSelector:
    """
    Selects the optimal export method based on table characteristics
    """
    
    def __init__(self):
        self.available_memory_gb = psutil.virtual_memory().available / 1024 / 1024 / 1024
        self.cpu_count = psutil.cpu_count()
    
    def select_export_method(self, characteristics: TableCharacteristics) -> ExportMethod:
        """
        Select the optimal export method based on table characteristics
        OPTIMIZED FOR GREENPLUM LARGE TABLE EXPORTS (100M+ rows)
        
        Args:
            characteristics: Table characteristics from analysis
            
        Returns:
            Recommended ExportMethod
        """
        row_count = characteristics.row_count
        
        # Check for performance warnings using the configuration
        perf_warning = get_performance_warning(row_count, "current_analysis")
        if perf_warning:
            logger.warning(perf_warning)
        
        # Use performance configuration to make optimal decisions
        should_use_range = should_use_range_chunking(row_count, characteristics.has_suitable_range_column)
        should_avoid_offset = should_avoid_offset_methods(row_count)
        
        # CRITICAL OPTIMIZATION: Prioritize range chunking for large tables to avoid 8+ hour OFFSET issues
        
        # 1. Small tables: Direct DuckDB export
        if row_count < GREENPLUM_LARGE_TABLE_CONFIG['method_selection']['small_table_threshold']:
            logger.info("Selected DIRECT_DUCKDB: Small table")
            return ExportMethod.DIRECT_DUCKDB
        
        # 2. PRIORITY: Use configuration-driven range chunking decision
        elif should_use_range:
            logger.info(f"🚀 PERFORMANCE OPTIMIZED: Selected RANGE_CHUNKING for {row_count:,} rows")
            logger.info(f"⚡ AVOIDING OFFSET: Using range column ({characteristics.range_column_info}) for optimal performance")
            return ExportMethod.RANGE_CHUNKING
        
        # 3. CRITICAL: Large tables that should avoid OFFSET but have no range columns
        elif should_avoid_offset:
            if characteristics.supports_cursors:
                logger.warning(f"⚠️  NO RANGE COLUMN: Using CURSOR_STREAMING for {row_count:,} rows")
                logger.warning(f"💡 RECOMMENDATION: Add an auto-increment ID column for optimal performance")
                return ExportMethod.CURSOR_STREAMING
            else:
                logger.error(f"❌ SUBOPTIMAL: No range column and no cursor support for {row_count:,} rows")
                logger.error(f"🐌 WARNING: Export may take several hours")
                return ExportMethod.PARALLEL_DUCKDB
        
        # 4. Medium tables with cursor support: DuckDB streaming 
        elif (row_count >= 5000000 and characteristics.supports_cursors):
            logger.info("Selected CURSOR_STREAMING: Medium-large table with cursor support")
            return ExportMethod.CURSOR_STREAMING
        
        # 5. Fallback for medium tables
        else:
            logger.info("Selected PARALLEL_DUCKDB: Medium table fallback")
            if row_count > 10000000:
                logger.warning(f"Performance may be suboptimal for {row_count:,} rows without range columns")
            return ExportMethod.PARALLEL_DUCKDB
    
    def get_method_parameters(self, method: ExportMethod, characteristics: TableCharacteristics) -> Dict[str, Any]:
        """
        Get optimal parameters for the selected export method
        
        Args:
            method: Selected export method
            characteristics: Table characteristics
            
        Returns:
            Dictionary of parameters for the export method
        """
        params = {}
        
        if method == ExportMethod.DIRECT_DUCKDB:
            # No special parameters needed
            pass
        
        elif method == ExportMethod.RANGE_CHUNKING:
            # OPTIMIZED CHUNKING STRATEGY FOR GREENPLUM LARGE TABLES using performance config
            row_count = characteristics.row_count
            
            # Use performance configuration for optimal parameters
            optimal_chunk_size = get_optimal_chunk_size(row_count, self.cpu_count)
            optimal_workers = get_optimal_worker_count(row_count, min(16, self.cpu_count))
            
            # Get configuration for detailed parameters
            config = GREENPLUM_LARGE_TABLE_CONFIG['range_chunking']
            output_config = GREENPLUM_LARGE_TABLE_CONFIG['output']
            
            # Calculate chunks and category based on optimized chunk size
            calculated_chunks = (row_count + optimal_chunk_size - 1) // optimal_chunk_size
            
            # Determine table category for logging
            if row_count > 1000000000:
                table_category = "ultra-massive"
            elif row_count > 500000000:
                table_category = "massive"
            elif row_count > 100000000:
                table_category = "large"
            elif row_count > 10000000:
                table_category = "medium-large"
            else:
                table_category = "medium"
            
            params = {
                'target_chunk_size': optimal_chunk_size,
                'max_workers': optimal_workers,  # OPTIMIZED: Configuration-driven worker count
                'target_file_size_mb': output_config['target_file_size_mb'],
                'estimated_chunks': calculated_chunks,
                'table_category': table_category,
                'max_chunks': min(calculated_chunks, output_config['max_files_per_table']),
                'min_chunk_size': config['min_chunk_size'],
                'max_chunk_size': config['max_chunk_size'],
                'greenplum_optimized': characteristics.db_type.lower() == 'greenplum',
                'segment_aligned': config['segment_alignment'],  # New: Greenplum segment alignment
                'performance_optimized': True  # Flag indicating this uses the performance config
            }
        
        elif method == ExportMethod.CURSOR_STREAMING:
            # DuckDB streaming parameters - much simpler than Polars approach
            chunk_size_rows = 5000000  # 5M rows per chunk for very large tables
            if characteristics.row_count < 50000000:  # Smaller tables
                chunk_size_rows = 2000000  # 2M rows per chunk
            
            params = {
                'chunk_size_rows': chunk_size_rows,
                'compression': 'snappy',
                'use_single_file_threshold': 20000000  # Use single file for < 20M rows
            }
        
        elif method == ExportMethod.PARALLEL_DUCKDB:
            # Current implementation parameters
            chunk_size = min(1000000, max(100000, characteristics.row_count // 50))
            
            params = {
                'chunk_size': chunk_size,
                'max_workers': min(8, max(4, self.cpu_count // 2))
            }
        
        return params


def smart_export_table(
    job_id: str,
    table_name: str,
    output_dir: Path,
    db_type: str,
    db_config: Optional[Dict[str, Any]] = None
) -> Tuple[bool, int, str]:
    """
    Smart export that automatically selects the best method for the table
    
    Args:
        job_id: Job identifier
        table_name: Table name to export
        output_dir: Output directory
        db_type: Database type
        db_config: Database configuration (for DuckDB fallback)
        
    Returns:
        Tuple of (success: bool, total_rows_exported: int, method_used: str)
    """
    
    logger.info(f"Starting smart export for table: {table_name}")
    
    # Get SQLite writer for table tracking
    sqlite_writer = get_sqlite_writer()
    
    try:
        # Analyze table characteristics
        analyzer = TableAnalyzer(table_name, db_type)
        characteristics = analyzer.analyze_table()
        
        # Record table start in SQLite database
        sqlite_writer.table_started(job_id, table_name, characteristics.row_count)
        
        # Select optimal export method
        selector = SmartExportSelector()
        method = selector.select_export_method(characteristics)
        params = selector.get_method_parameters(method, characteristics)
        
        logger.info(f"Selected method: {method.value} with params: {params}")
        
        # Log chunking strategy for range-based exports
        if method == ExportMethod.RANGE_CHUNKING and 'table_category' in params:
            logger.info(f"Table {table_name} categorized as '{params['table_category']}' "
                       f"({characteristics.row_count:,} rows) -> "
                       f"target: {params['target_chunk_size']:,} rows/chunk, "
                       f"max chunks: {params['max_chunks']}, "
                       f"estimated chunks: {params['estimated_chunks']}")
        
        # Execute the selected export method
        start_time = time.time()
        
        if method == ExportMethod.DIRECT_DUCKDB:
            success, rows_exported = _execute_direct_duckdb_export(
                table_name, output_dir, db_config
            )
        
        elif method == ExportMethod.RANGE_CHUNKING:
            success, rows_exported, _ = export_large_table_with_range_chunking(
                job_id, table_name, output_dir,
                target_chunk_size=params.get('target_chunk_size', 1000000),
                max_workers=params.get('max_workers', 6)
            )
        
        elif method == ExportMethod.CURSOR_STREAMING:
            # DuckDB streaming uses connection pool internally, no db_config needed
            success, rows_exported = export_large_table_with_duckdb_streaming(
                job_id, table_name, output_dir, {},  # Empty db_config - uses connection pool
                estimated_rows=characteristics.row_count
            )
        
        elif method == ExportMethod.PARALLEL_DUCKDB:
            success, rows_exported = _execute_parallel_duckdb_export(
                table_name, output_dir, db_config, 
                characteristics.row_count, params.get('chunk_size', 1000000),
                params.get('max_workers', 8)
            )
        
        else:
            raise ValueError(f"Unknown export method: {method}")
        
        # Log results
        elapsed = time.time() - start_time
        if success:
            throughput = int(rows_exported / elapsed) if elapsed > 0 else 0
            logger.info(f"Smart export completed successfully:")
            logger.info(f"  Method: {method.value}")
            logger.info(f"  Rows exported: {rows_exported:,}")
            logger.info(f"  Duration: {elapsed:.1f}s")
            logger.info(f"  Throughput: {throughput:,} rows/sec")
            
            # Calculate file size if possible
            file_size_mb = 0
            try:
                # Try to calculate total size of all files in output directory
                for file_path in output_dir.rglob("*.parquet"):
                    file_size_mb += file_path.stat().st_size / (1024 * 1024)
            except:
                pass  # If we can't calculate size, use 0
                
            # Record table completion in SQLite database
            sqlite_writer.table_completed(
                job_id=job_id,
                table_name=table_name,
                rows_processed=rows_exported,
                file_path=str(output_dir),
                file_size_mb=file_size_mb,
                throughput_rows_per_sec=throughput
            )
        else:
            logger.error(f"Smart export failed using method: {method.value}")
            
            # Record table failure in SQLite database
            sqlite_writer.table_update(
                job_id=job_id,
                table_name=table_name,
                status='failed',
                error_message=f"Smart export failed using method: {method.value}"
            )
        
        return success, rows_exported, method.value
        
    except Exception as e:
        logger.error(f"Smart export error for {table_name}: {str(e)}")
        
        # Record table failure in SQLite database
        try:
            sqlite_writer.table_update(
                job_id=job_id,
                table_name=table_name,
                status='failed',
                error_message=f"Smart export error: {str(e)}"
            )
        except:
            pass  # Don't let SQLite errors propagate
            
        return False, 0, "error"


def _execute_direct_duckdb_export(table_name: str, output_dir: Path, 
                                 db_config: Optional[Dict[str, Any]]) -> Tuple[bool, int]:
    """Execute direct DuckDB export for small tables"""
    try:
        output_file = output_dir / f"{table_name.replace('.', '_')}.parquet"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if db_config:
            success, message, rows_exported = export_small_table_duckdb(
                db_config, table_name, output_file
            )
            
            if success:
                logger.info(f"Direct DuckDB export successful: {message}")
                
                # Create metadata file for consistency with other export methods
                try:
                    metadata = {
                        'table_name': table_name,
                        'total_rows': rows_exported,
                        'export_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                        'status': 'complete',
                        'partitioned': False,
                        'files': [output_file.name]
                    }
                    
                    metadata_file = output_dir / "_export_metadata.json"
                    with open(metadata_file, 'w') as f:
                        json.dump(metadata, f, indent=2)
                        
                except Exception as e:
                    logger.warning(f"Could not create metadata file: {e}")
                    # Don't fail the export for metadata issues
                    
            else:
                logger.error(f"Direct DuckDB export failed: {message}")
                
            return success, rows_exported
        else:
            # Use connection pool approach
            logger.error("Direct DuckDB export needs db_config, missing database configuration")
            return False, 0
            
    except Exception as e:
        logger.error(f"Direct DuckDB export failed with exception: {e}")
        return False, 0


def _execute_parallel_duckdb_export(table_name: str, output_dir: Path, 
                                   db_config: Optional[Dict[str, Any]],
                                   row_count: int, chunk_size: int, 
                                   max_workers: int) -> Tuple[bool, int]:
    """Execute parallel DuckDB export (current implementation)"""
    try:
        if db_config:
            success, rows_exported = export_large_table_with_duckdb_parallel(
                db_config, table_name, output_dir, row_count, chunk_size, max_workers
            )
            
            if success:
                # Create metadata file if it doesn't exist (some DuckDB methods may create it)
                metadata_file = output_dir / "_export_metadata.json"
                if not metadata_file.exists():
                    try:
                        # Count chunks created
                        chunk_files = list(output_dir.glob("part_*.parquet"))
                        
                        metadata = {
                            'table_name': table_name,
                            'total_rows': rows_exported,
                            'export_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                            'status': 'complete',
                            'partitioned': len(chunk_files) > 1,
                            'files': [f.name for f in chunk_files] if chunk_files else ['data.parquet']
                        }
                        
                        if len(chunk_files) > 1:
                            metadata['chunk_count'] = len(chunk_files)
                            metadata['chunk_size'] = chunk_size
                        
                        with open(metadata_file, 'w') as f:
                            json.dump(metadata, f, indent=2)
                            
                    except Exception as e:
                        logger.warning(f"Could not create metadata file for parallel DuckDB: {e}")
                        
            return success, rows_exported
        else:
            logger.error("Parallel DuckDB export needs db_config, missing database configuration")
            return False, 0
            
    except Exception as e:
        logger.error(f"Parallel DuckDB export failed with exception: {e}")
        return False, 0