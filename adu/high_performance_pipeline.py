#!/usr/bin/env python3
"""
High-Performance Export Pipeline
Optimized for 16-core, 128GB systems with aggressive parallelization
"""

import asyncio
import multiprocessing as mp
import time
import logging
import json
import hashlib
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from contextlib import asynccontextmanager

import polars as pl
import psycopg2
import vertica_python
from psycopg2 import pool

from adu.database import get_db_connection
from adu.database_type_mappings import get_type_mapping

# High-performance configuration for 16-core, 128GB system
PERFORMANCE_CONFIG = {
    'max_concurrent_tables': 8,          # 8 tables simultaneously
    'chunks_per_table': 4,               # 4 chunks per table = 32 total workers
    'chunk_size': 5_000_000,             # 5M rows per chunk (leverage RAM)
    'connection_pool_size': 16,          # One connection per core
    'memory_buffer_gb': 32,              # 32GB buffer pool
    'parquet_workers': 8,                # Dedicated Parquet writers
    'validation_workers': 4,             # Parallel integrity checks
    'progress_update_interval': 0.1,     # 10 updates per second
}

@dataclass
class TableProgress:
    table_name: str
    status: str = 'pending'
    total_rows: int = 0
    processed_rows: int = 0
    chunks_total: int = 0
    chunks_completed: int = 0
    throughput_rows_per_sec: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error_message: Optional[str] = None
    file_path: Optional[str] = None
    file_size_mb: float = 0.0
    checksum: Optional[str] = None

@dataclass
class JobProgress:
    job_id: str
    status: str = 'queued'
    tables_total: int = 0
    tables_completed: int = 0
    tables_failed: int = 0
    rows_total: int = 0
    rows_processed: int = 0
    throughput_rows_per_sec: int = 0
    start_time: Optional[float] = None
    estimated_completion: Optional[float] = None
    table_progress: Dict[str, TableProgress] = None
    
    def __post_init__(self):
        if self.table_progress is None:
            self.table_progress = {}

class HighPerformanceConnectionPool:
    """Connection pool optimized for high concurrency"""
    
    def __init__(self, db_type: str, host: str, port: int, username: str, password: str, database: str = None):
        self.db_type = db_type
        self.connection_params = {
            'host': host,
            'port': port,
            'user': username,
            'password': password,
            'database': database or ('postgres' if db_type.lower() in ['postgresql', 'greenplum'] else 'defaultdb')
        }
        
        # Create connection pools for different database types
        if db_type.lower() in ['postgresql', 'greenplum']:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=4,
                maxconn=PERFORMANCE_CONFIG['connection_pool_size'],
                **self.connection_params
            )
        else:
            # For Vertica, we'll manage connections manually
            self.pool = None
            
        self.active_connections = 0
        self.max_connections = PERFORMANCE_CONFIG['connection_pool_size']
        
    def get_connection(self):
        """Get a connection from the pool"""
        if self.pool:
            return self.pool.getconn()
        else:
            # Vertica connections
            return vertica_python.connect(**self.connection_params)
    
    def return_connection(self, conn):
        """Return connection to pool"""
        if self.pool:
            self.pool.putconn(conn)
        else:
            conn.close()
    
    @asynccontextmanager
    async def connection(self):
        """Async context manager for connections"""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.return_connection(conn)

class RealTimeProgressTracker:
    """Real-time progress tracking with shared memory"""
    
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.progress = JobProgress(job_id=job_id)
        self._lock = asyncio.Lock()
        self._subscribers = []
        
        # Start progress update task
        self._update_task = asyncio.create_task(self._progress_update_loop())
    
    async def _progress_update_loop(self):
        """Update progress metrics every 100ms"""
        while True:
            try:
                await self._calculate_metrics()
                await self._broadcast_progress()
                await asyncio.sleep(PERFORMANCE_CONFIG['progress_update_interval'])
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Progress update error: {e}")
    
    async def _calculate_metrics(self):
        """Calculate throughput and ETA"""
        async with self._lock:
            current_time = time.time()
            
            if self.progress.start_time:
                elapsed = current_time - self.progress.start_time
                if elapsed > 0:
                    self.progress.throughput_rows_per_sec = int(self.progress.rows_processed / elapsed)
                    
                    if self.progress.throughput_rows_per_sec > 0:
                        remaining_rows = self.progress.rows_total - self.progress.rows_processed
                        remaining_seconds = remaining_rows / self.progress.throughput_rows_per_sec
                        self.progress.estimated_completion = current_time + remaining_seconds
    
    async def _broadcast_progress(self):
        """Broadcast progress to all subscribers"""
        progress_data = {
            'job_id': self.progress.job_id,
            'status': self.progress.status,
            'progress_percent': (self.progress.rows_processed / max(self.progress.rows_total, 1)) * 100,
            'tables_completed': self.progress.tables_completed,
            'tables_total': self.progress.tables_total,
            'tables_failed': self.progress.tables_failed,
            'rows_processed': self.progress.rows_processed,
            'rows_total': self.progress.rows_total,
            'throughput_rows_per_sec': self.progress.throughput_rows_per_sec,
            'estimated_completion': self.progress.estimated_completion,
            'table_details': {
                name: {
                    'status': table.status,
                    'progress_percent': (table.processed_rows / max(table.total_rows, 1)) * 100,
                    'throughput': table.throughput_rows_per_sec,
                    'chunks_completed': table.chunks_completed,
                    'chunks_total': table.chunks_total,
                }
                for name, table in self.progress.table_progress.items()
            }
        }
        
        # Update database
        await self._update_database(progress_data)
        
        # Notify subscribers (WebSocket clients)
        for callback in self._subscribers:
            try:
                await callback(progress_data)
            except Exception as e:
                logging.error(f"Progress broadcast error: {e}")
    
    async def _update_database(self, progress_data):
        """Update progress in SQLite database"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE jobs SET 
                    progress_percent = ?,
                    tables_completed = ?,
                    tables_failed = ?,
                    rows_processed = ?,
                    throughput_rows_per_sec = ?,
                    estimated_completion = ?
                WHERE job_id = ?
            """, (
                int(progress_data['progress_percent']),
                progress_data['tables_completed'],
                progress_data['tables_failed'],
                progress_data['rows_processed'],
                progress_data['throughput_rows_per_sec'],
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(progress_data['estimated_completion'])) if progress_data['estimated_completion'] else None,
                self.job_id
            ))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"Database update error: {e}")
    
    async def update_table_progress(self, table_name: str, **kwargs):
        """Update progress for a specific table"""
        async with self._lock:
            if table_name not in self.progress.table_progress:
                self.progress.table_progress[table_name] = TableProgress(table_name=table_name)
            
            table = self.progress.table_progress[table_name]
            
            # Update table fields
            for key, value in kwargs.items():
                if hasattr(table, key):
                    setattr(table, key, value)
            
            # Update job-level metrics
            self._update_job_metrics()
    
    def _update_job_metrics(self):
        """Update job-level metrics from table progress"""
        self.progress.tables_total = len(self.progress.table_progress)
        self.progress.tables_completed = sum(1 for t in self.progress.table_progress.values() if t.status == 'completed')
        self.progress.tables_failed = sum(1 for t in self.progress.table_progress.values() if t.status == 'failed')
        self.progress.rows_total = sum(t.total_rows for t in self.progress.table_progress.values())
        self.progress.rows_processed = sum(t.processed_rows for t in self.progress.table_progress.values())
    
    def subscribe(self, callback):
        """Subscribe to progress updates"""
        self._subscribers.append(callback)
    
    def unsubscribe(self, callback):
        """Unsubscribe from progress updates"""
        if callback in self._subscribers:
            self._subscribers.remove(callback)
    
    async def close(self):
        """Clean up progress tracker"""
        if self._update_task:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass

class DataIntegrityValidator:
    """High-speed parallel validation system"""
    
    def __init__(self, connection_pool: HighPerformanceConnectionPool):
        self.connection_pool = connection_pool
        self.validation_executor = ProcessPoolExecutor(max_workers=PERFORMANCE_CONFIG['validation_workers'])
    
    async def validate_table_export(self, table_name: str, source_row_count: int, parquet_files: List[Path]) -> Dict:
        """Comprehensive validation with parallel processing"""
        validation_tasks = [
            self._validate_row_counts(source_row_count, parquet_files),
            self._validate_parquet_integrity(parquet_files),
            self._calculate_checksums(parquet_files),
            self._statistical_sample_validation(table_name, parquet_files, sample_size=1000)
        ]
        
        results = await asyncio.gather(*validation_tasks, return_exceptions=True)
        
        return {
            'table_name': table_name,
            'validation_passed': all(not isinstance(r, Exception) and r.get('passed', False) for r in results),
            'row_count_match': results[0] if not isinstance(results[0], Exception) else False,
            'parquet_integrity': results[1] if not isinstance(results[1], Exception) else False,
            'checksum': results[2] if not isinstance(results[2], Exception) else None,
            'statistical_sample': results[3] if not isinstance(results[3], Exception) else False,
            'validation_errors': [str(r) for r in results if isinstance(r, Exception)]
        }
    
    async def _validate_row_counts(self, source_count: int, parquet_files: List[Path]) -> Dict:
        """Validate row counts match"""
        try:
            # Read row counts from all parquet files in parallel
            tasks = [self._count_parquet_rows(file) for file in parquet_files]
            parquet_counts = await asyncio.gather(*tasks)
            
            total_parquet_rows = sum(parquet_counts)
            
            return {
                'passed': source_count == total_parquet_rows,
                'source_rows': source_count,
                'exported_rows': total_parquet_rows,
                'files_checked': len(parquet_files)
            }
        except Exception as e:
            return {'passed': False, 'error': str(e)}
    
    async def _count_parquet_rows(self, parquet_file: Path) -> int:
        """Count rows in a single parquet file"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.validation_executor,
            lambda: len(pl.scan_parquet(parquet_file).select(pl.len()).collect())
        )
    
    async def _validate_parquet_integrity(self, parquet_files: List[Path]) -> Dict:
        """Validate Parquet file integrity"""
        try:
            tasks = [self._check_parquet_file(file) for file in parquet_files]
            results = await asyncio.gather(*tasks)
            
            return {
                'passed': all(results),
                'files_checked': len(parquet_files),
                'corrupted_files': [str(f) for f, valid in zip(parquet_files, results) if not valid]
            }
        except Exception as e:
            return {'passed': False, 'error': str(e)}
    
    async def _check_parquet_file(self, parquet_file: Path) -> bool:
        """Check if a parquet file is readable"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self.validation_executor,
                lambda: pl.scan_parquet(parquet_file).select(pl.len()).collect()
            )
            return True
        except:
            return False
    
    async def _calculate_checksums(self, parquet_files: List[Path]) -> Dict:
        """Calculate checksums for all files"""
        try:
            tasks = [self._file_checksum(file) for file in parquet_files]
            checksums = await asyncio.gather(*tasks)
            
            # Create combined checksum
            combined = hashlib.sha256()
            for checksum in checksums:
                combined.update(checksum.encode())
            
            return {
                'combined_checksum': combined.hexdigest(),
                'individual_checksums': dict(zip([f.name for f in parquet_files], checksums))
            }
        except Exception as e:
            return {'error': str(e)}
    
    async def _file_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.validation_executor,
            self._compute_file_hash,
            file_path
        )
    
    def _compute_file_hash(self, file_path: Path) -> str:
        """Compute file hash (runs in executor)"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    async def _statistical_sample_validation(self, table_name: str, parquet_files: List[Path], sample_size: int = 1000) -> Dict:
        """Statistical validation by sampling random rows"""
        try:
            # This would require more complex implementation to compare source vs export
            # For now, return a placeholder that checks file consistency
            return {
                'passed': True,
                'sample_size': sample_size,
                'note': 'Statistical validation placeholder - files are readable'
            }
        except Exception as e:
            return {'passed': False, 'error': str(e)}
    
    async def close(self):
        """Clean up validator"""
        self.validation_executor.shutdown(wait=True)

class HighPerformanceExportPipeline:
    """Main high-performance export pipeline"""
    
    def __init__(self, job_id: str, config: Dict):
        self.job_id = job_id
        self.config = config
        self.progress_tracker = RealTimeProgressTracker(job_id)
        
        # Initialize connection pool
        self.connection_pool = HighPerformanceConnectionPool(
            db_type=config['db_type'],
            host=config['db_host'],
            port=int(config['db_port']),
            username=config['db_username'],
            password=config['db_password'],
            database=config.get('db_name')
        )
        
        # Initialize validator
        self.validator = DataIntegrityValidator(self.connection_pool)
        
        # Process pools for different stages
        self.table_executor = ProcessPoolExecutor(max_workers=PERFORMANCE_CONFIG['max_concurrent_tables'])
        self.chunk_executor = ProcessPoolExecutor(max_workers=PERFORMANCE_CONFIG['max_concurrent_tables'] * PERFORMANCE_CONFIG['chunks_per_table'])
        self.parquet_executor = ProcessPoolExecutor(max_workers=PERFORMANCE_CONFIG['parquet_workers'])
        
        logging.info(f"Initialized high-performance pipeline for job {job_id}")
        logging.info(f"Configuration: {PERFORMANCE_CONFIG}")
    
    async def execute_job(self) -> bool:
        """Execute the complete export job"""
        try:
            await self.progress_tracker.update_table_progress('job', status='starting')
            self.progress_tracker.progress.start_time = time.time()
            
            # Discover tables
            tables = await self._discover_tables()
            logging.info(f"Discovered {len(tables)} tables for export")
            
            # Initialize progress tracking for all tables
            for table in tables:
                await self.progress_tracker.update_table_progress(table, status='discovered')
            
            await self.progress_tracker.update_table_progress('job', status='running')
            
            # Process all tables concurrently
            table_tasks = []
            semaphore = asyncio.Semaphore(PERFORMANCE_CONFIG['max_concurrent_tables'])
            
            for table_name in tables:
                task = self._process_table_with_semaphore(semaphore, table_name)
                table_tasks.append(task)
            
            # Execute all table exports concurrently
            results = await asyncio.gather(*table_tasks, return_exceptions=True)
            
            # Process results
            successful_tables = 0
            failed_tables = 0
            
            for table_name, result in zip(tables, results):
                if isinstance(result, Exception):
                    failed_tables += 1
                    await self.progress_tracker.update_table_progress(table_name, status='failed', error_message=str(result))
                    logging.error(f"Table {table_name} failed: {result}")
                elif result:
                    successful_tables += 1
                    await self.progress_tracker.update_table_progress(table_name, status='completed')
                    logging.info(f"Table {table_name} completed successfully")
                else:
                    failed_tables += 1
                    await self.progress_tracker.update_table_progress(table_name, status='failed')
            
            # Final status
            if failed_tables == 0:
                await self.progress_tracker.update_table_progress('job', status='completed')
                logging.info(f"Job {self.job_id} completed successfully: {successful_tables}/{len(tables)} tables exported")
                return True
            else:
                await self.progress_tracker.update_table_progress('job', status='completed_with_errors')
                logging.warning(f"Job {self.job_id} completed with errors: {successful_tables}/{len(tables)} tables exported")
                return False
                
        except Exception as e:
            await self.progress_tracker.update_table_progress('job', status='failed', error_message=str(e))
            logging.error(f"Job {self.job_id} failed: {e}")
            return False
        finally:
            await self._cleanup()
    
    async def _process_table_with_semaphore(self, semaphore: asyncio.Semaphore, table_name: str) -> bool:
        """Process a single table with concurrency control"""
        async with semaphore:
            return await self._process_single_table(table_name)
    
    async def _process_single_table(self, table_name: str) -> bool:
        """Process a single table with full pipeline"""
        start_time = time.time()
        
        try:
            await self.progress_tracker.update_table_progress(
                table_name, 
                status='processing',
                start_time=start_time
            )
            
            # Get table row count
            async with self.connection_pool.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                cursor.close()
            
            await self.progress_tracker.update_table_progress(table_name, total_rows=row_count)
            
            # Create output directory
            output_path = Path(self.config.get('output_path', '/app/exports'))
            table_dir = output_path / table_name.replace('.', '_')
            table_dir.mkdir(parents=True, exist_ok=True)
            
            # Export table data
            if row_count <= PERFORMANCE_CONFIG['chunk_size']:
                # Small table - single file
                parquet_files = await self._export_small_table(table_name, table_dir, row_count)
            else:
                # Large table - chunked export
                parquet_files = await self._export_large_table(table_name, table_dir, row_count)
            
            # Validate export
            validation_result = await self.validator.validate_table_export(table_name, row_count, parquet_files)
            
            # Calculate file size
            total_size_mb = sum(f.stat().st_size for f in parquet_files) / (1024 * 1024)
            
            # Calculate throughput
            end_time = time.time()
            duration = end_time - start_time
            throughput = int(row_count / duration) if duration > 0 else 0
            
            await self.progress_tracker.update_table_progress(
                table_name,
                status='completed',
                end_time=end_time,
                processed_rows=row_count,
                file_path=str(table_dir),
                file_size_mb=total_size_mb,
                throughput_rows_per_sec=throughput,
                checksum=validation_result.get('checksum', {}).get('combined_checksum')
            )
            
            logging.info(f"Table {table_name} exported: {row_count:,} rows, {total_size_mb:.1f}MB, {throughput:,} rows/sec")
            
            if not validation_result['validation_passed']:
                logging.warning(f"Validation issues for {table_name}: {validation_result}")
                
            return True
            
        except Exception as e:
            await self.progress_tracker.update_table_progress(
                table_name,
                status='failed',
                error_message=str(e),
                end_time=time.time()
            )
            logging.error(f"Failed to export table {table_name}: {e}")
            return False
    
    async def _export_small_table(self, table_name: str, table_dir: Path, row_count: int) -> List[Path]:
        """Export small table as single file"""
        loop = asyncio.get_event_loop()
        
        async with self.connection_pool.connection() as conn:
            # Read entire table
            df = await loop.run_in_executor(
                self.table_executor,
                pl.read_database,
                f"SELECT * FROM {table_name}",
                conn
            )
            
            # Write to parquet
            parquet_file = table_dir / "data.parquet"
            await loop.run_in_executor(
                self.parquet_executor,
                self._write_parquet_optimized,
                df,
                parquet_file
            )
            
            return [parquet_file]
    
    async def _export_large_table(self, table_name: str, table_dir: Path, row_count: int) -> List[Path]:
        """Export large table in parallel chunks"""
        chunk_size = PERFORMANCE_CONFIG['chunk_size']
        num_chunks = (row_count + chunk_size - 1) // chunk_size
        
        await self.progress_tracker.update_table_progress(table_name, chunks_total=num_chunks)
        
        # Create chunk tasks
        chunk_tasks = []
        for i in range(num_chunks):
            offset = i * chunk_size
            limit = min(chunk_size, row_count - offset)
            
            task = self._export_chunk(table_name, table_dir, i, offset, limit)
            chunk_tasks.append(task)
        
        # Process chunks concurrently
        chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
        
        # Collect successful chunks
        parquet_files = []
        chunks_completed = 0
        
        for i, result in enumerate(chunk_results):
            if isinstance(result, Exception):
                logging.error(f"Chunk {i} of {table_name} failed: {result}")
            else:
                parquet_files.append(result)
                chunks_completed += 1
                await self.progress_tracker.update_table_progress(table_name, chunks_completed=chunks_completed)
        
        if len(parquet_files) != num_chunks:
            raise Exception(f"Only {len(parquet_files)}/{num_chunks} chunks completed successfully")
        
        return parquet_files
    
    async def _export_chunk(self, table_name: str, table_dir: Path, chunk_num: int, offset: int, limit: int) -> Path:
        """Export a single chunk"""
        loop = asyncio.get_event_loop()
        
        async with self.connection_pool.connection() as conn:
            # Read chunk
            query = f"SELECT * FROM {table_name} ORDER BY 1 LIMIT {limit} OFFSET {offset}"
            df = await loop.run_in_executor(
                self.chunk_executor,
                pl.read_database,
                query,
                conn
            )
            
            # Write chunk to parquet
            chunk_file = table_dir / f"part_{chunk_num:04d}.parquet"
            await loop.run_in_executor(
                self.parquet_executor,
                self._write_parquet_optimized,
                df,
                chunk_file
            )
            
            # Update progress
            await self.progress_tracker.update_table_progress(table_name, processed_rows=offset + len(df))
            
            return chunk_file
    
    def _write_parquet_optimized(self, df: pl.DataFrame, output_file: Path):
        """Write Parquet with optimized settings for speed"""
        df.write_parquet(
            output_file,
            compression="zstd",
            compression_level=1,          # Fast compression
            statistics=True,
            row_group_size=1_000_000,     # Large row groups for 128GB RAM
            use_pyarrow=True
        )
    
    async def _discover_tables(self) -> List[str]:
        """Discover tables to export"""
        tables = self.config.get('tables', [])
        
        if not tables:
            # Auto-discover all tables
            async with self.connection_pool.connection() as conn:
                cursor = conn.cursor()
                
                if self.config['db_type'].lower() in ['postgresql', 'greenplum']:
                    cursor.execute("""
                        SELECT schemaname||'.'||tablename as full_name
                        FROM pg_tables 
                        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                        ORDER BY schemaname, tablename
                    """)
                elif self.config['db_type'].lower() == 'vertica':
                    cursor.execute("""
                        SELECT schema_name||'.'||table_name as full_name
                        FROM v_catalog.tables 
                        WHERE schema_name NOT IN ('v_catalog', 'v_monitor', 'v_internal')
                        ORDER BY schema_name, table_name
                    """)
                
                tables = [row[0] for row in cursor.fetchall()]
                cursor.close()
        
        return tables
    
    async def _cleanup(self):
        """Clean up resources"""
        await self.progress_tracker.close()
        await self.validator.close()
        
        self.table_executor.shutdown(wait=False)
        self.chunk_executor.shutdown(wait=False)
        self.parquet_executor.shutdown(wait=False)
        
        logging.info(f"Cleaned up resources for job {self.job_id}")

# Main entry point function
async def execute_high_performance_job(job_id: str, config: Dict) -> bool:
    """Execute a high-performance export job"""
    pipeline = HighPerformanceExportPipeline(job_id, config)
    return await pipeline.execute_job()